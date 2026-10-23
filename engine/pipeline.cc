// pipeline.cc - the v3 engine: physical execution of a PipelineDesc.
#include "pipeline.h"

#include "core.h"
#include "chunk_sort.h"
#include "run_store.h"
#include "merge.h"
#include "operators.h"
#include "shuffle.h"

#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <map>
#include <deque>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <random>
#include <algorithm>

namespace ts {

namespace {

// ===========================================================================
// shared plumbing
// ===========================================================================

struct RawChunk {
  char* data = nullptr;
  size_t size = 0;
};

// Bounded FIFO of raw chunks. The byte cap bounds memory held between reader
// and workers (workers call Release after freeing their chunk).
// Chunks carry an EXPLICIT sequence number: parallel span readers stamp
// seq = (span << kSpanShift) | k so the ordered hub (stream mode) still emits
// file order while several reader threads read disjoint file spans at once.
struct ChunkQueue {
  static constexpr size_t kSpanShift = 44;  // seq = span<<44 | within-span idx
  std::mutex mu;
  std::condition_variable cv_space, cv_item;
  std::deque<RawChunk> items;
  std::deque<size_t> seqs;  // chunk sequence numbers (stream mode ordering)
  size_t inflight = 0, inflight_cap;
  size_t next_seq = 0;
  bool closed = false;
  int readers_left = 1;  // the queue closes when every reader finishes
  // per-span pipeline state for the ordered hub (stream mode): a chunk of
  // span s is either in the deque (span_inq[s]) or held by a worker between
  // Pop and Submit/drop (span_held[s]). Both counters move under `mu`, so
  // "span s is drained" is decided atomically with the pop itself - there
  // is no window in which a dequeued chunk is unaccounted for.
  std::vector<size_t> span_inq, span_held;

  static size_t SeqSpan(size_t seq) { return seq >> kSpanShift; }

  explicit ChunkQueue(size_t cap_bytes) : inflight_cap(cap_bytes) {}

  void AddReader(int n = 1) {
    std::lock_guard<std::mutex> g(mu);
    readers_left += n;
  }
  bool Push(RawChunk c, size_t seq) {  // blocks until there is room
    std::unique_lock<std::mutex> lk(mu);
    // inflight == 0: admit an oversized chunk (a single huge line can be
    // larger than the whole cap; refusing it would wedge its reader forever)
    cv_space.wait(lk, [&] {
      return closed || inflight == 0 || inflight + c.size <= inflight_cap;
    });
    if (closed) return false;
    items.push_back(c);
    seqs.push_back(seq);
    size_t sp = SeqSpan(seq);
    if (span_inq.size() <= sp) {
      span_inq.resize(sp + 1, 0);
      span_held.resize(sp + 1, 0);
    }
    span_inq[sp]++;
    inflight += c.size;
    cv_item.notify_one();
    return true;
  }
  bool Push(RawChunk c) {  // sequential self-numbering
    size_t seq;
    {
      std::lock_guard<std::mutex> g(mu);
      seq = next_seq++;
    }
    return Push(std::move(c), seq);
  }
  bool Pop(RawChunk* out, size_t* seq = nullptr) {
    std::unique_lock<std::mutex> lk(mu);
    cv_item.wait(lk, [&] { return !items.empty() || closed; });
    if (items.empty()) return false;
    *out = items.front();
    size_t s = seqs.front();
    if (seq) *seq = s;
    items.pop_front();
    seqs.pop_front();
    size_t sp = SeqSpan(s);
    if (sp < span_inq.size()) {
      span_inq[sp]--;   // left the deque ...
      span_held[sp]++;  // ... now held until Submit/drop (holds the gate)
    }
    return true;
  }
  // the worker is done with a popped chunk (submitted to the hub, or
  // dropped): the span gate may now advance past it.
  void NoteSubmitted(size_t seq) {
    std::lock_guard<std::mutex> g(mu);
    size_t sp = SeqSpan(seq);
    if (sp < span_held.size() && span_held[sp] > 0) span_held[sp]--;
  }
  // span drained: reader finished, nothing of it queued, nothing held.
  bool SpanDrained(size_t sp) {
    std::lock_guard<std::mutex> g(mu);
    return sp >= span_inq.size() || (span_inq[sp] == 0 && span_held[sp] == 0);
  }
  // Stream mode: pop the LOWEST-seq chunk, but only when `gate` allows it
  // (bounded look-ahead past the writer). Workers park HERE - holding
  // nothing - so the exact chunk the writer waits for can always be picked
  // up by some worker. Once closed, drain ungated (order is moot on
  // teardown, and workers must reach their exit path).
  template <class Gate>
  bool PopGated(RawChunk* out, size_t* seq, Gate gate) {
    std::unique_lock<std::mutex> lk(mu);
    for (;;) {
      cv_item.wait_for(lk, std::chrono::milliseconds(50),
                       [&] { return !items.empty() || closed; });
      if (items.empty()) {
        if (closed) return false;  // drained
        continue;                  // timeout: keep waiting for an item
      }
      size_t best = 0;
      for (size_t i = 1; i < seqs.size(); i++)
        if (seqs[i] < seqs[best]) best = i;
      // Congestion override: workers may park ONLY while the queue is less
      // than half full - then every normal-chunk reader still has room to
      // push, so the chunk everyone waits for can always enter. Once the
      // queue is half full, pop the min-seq chunk anyway to keep space
      // flowing (memory stays bounded: queue cap + window + threads*chunk).
      bool congested = inflight * 2 >= inflight_cap;
      if (closed || congested || gate(seqs[best])) {
        *out = items[best];
        *seq = seqs[best];
        items.erase(items.begin() + (std::ptrdiff_t)best);
        seqs.erase(seqs.begin() + (std::ptrdiff_t)best);
        size_t sp = SeqSpan(*seq);
        if (sp < span_inq.size()) {
          span_inq[sp]--;   // left the deque ...
          span_held[sp]++;  // ... now held until Submit/drop (holds the gate)
        }
        return true;
      }
      cv_item.wait_for(lk, std::chrono::milliseconds(50));
    }
  }
  void Release(size_t n) {
    {
      std::lock_guard<std::mutex> g(mu);
      inflight = inflight >= n ? inflight - n : 0;
    }
    cv_space.notify_all();
  }
  void Close() {  // one reader finished; close only when ALL are done
    bool do_close = false;
    {
      std::lock_guard<std::mutex> g(mu);
      if (--readers_left <= 0) { closed = true; do_close = true; }
    }
    if (do_close) {
      cv_item.notify_all();
      cv_space.notify_all();
    }
  }
};

// Reader producing '\n'-aligned chunks. For seekable files, MULTIPLE
// SpanReaders each own one byte span [start,end) and pread in parallel:
//   * a line belongs to the span containing its FIRST byte (never lost,
//     never duplicated across spans)
//   * head: one pread at start-1 tells whether start sits mid-line; if so the
//     partial first line is dropped (the previous span emits it whole via its
//     tail extension)
//   * tail: reading continues past `end` until the '\n' of any line that
//     started inside the span
//   * stdin/pipes: a single span with end==0, plain read(), until EOF
struct SpanReader {
  int fd;
  bool is_pipe;
  uint64_t start, end;  // [start, end); end==0 => until EOF (pipe)
  int span_idx;
  size_t chunk_bytes;
  ChunkQueue* q;
  std::atomic<bool>* ok;

  static constexpr size_t kTailStep = 64 << 10;  // step while finishing a tail line
  static constexpr size_t kMaxLine = 3u << 30;

  ssize_t ReadSome(char* p, size_t want, uint64_t at) {
    if (is_pipe) return ::read(fd, p, want);  // sequential; `at` ignored
    return ::pread(fd, p, want, (off_t)at);
  }

  void Run() {
    uint64_t pos = start;
    std::string buf;  // bytes after the last emitted '\n' (contains none)
    bool drop_first = false;
    if (!is_pipe && start > 0) {
      char prev = 0;
      ssize_t r = ::pread(fd, &prev, 1, (off_t)(start - 1));
      drop_first = !(r == 1 && prev == '\n');
    }
    size_t seq_k = 0;
    auto push = [&](RawChunk& c) {
      size_t seq = ((size_t)(uint64_t)span_idx << ChunkQueue::kSpanShift) |
                   (seq_k & ((size_t{1} << ChunkQueue::kSpanShift) - 1));
      seq_k++;
      return q->Push(std::move(c), seq);
    };

    for (;;) {
      bool span_done = !is_pipe && end != 0 && pos >= end;
      if (span_done && buf.empty()) break;  // span fully emitted

      size_t want;
      if (span_done) {
        want = kTailStep;  // finishing a line that started inside the span
      } else if (is_pipe || end == 0) {
        want = chunk_bytes;
      } else {
        want = std::min(chunk_bytes, (size_t)(end - pos));
      }

      RawChunk c;
      // +1 spare byte: lets downstream zero-copy compaction append a '\n' to
      // an unterminated final line in place
      c.data = new (std::nothrow) char[buf.size() + want + 1];
      if (!c.data) {
        fprintf(stderr, "tsort: out of memory (chunk)\n");
        ok->store(false);
        break;
      }
      if (!buf.empty()) memcpy(c.data, buf.data(), buf.size());
      ssize_t r = ReadSome(c.data + buf.size(), want, pos);
      if (r < 0) {
        if (errno == EINTR) { delete[] c.data; continue; }
        fprintf(stderr, "tsort: read error: %s\n", strerror(errno));
        ok->store(false);
        delete[] c.data;
        break;
      }
      if (r == 0) {  // EOF: maybe a final line without '\n'
        if (ok->load() && !buf.empty() && !drop_first) {
          RawChunk t;
          t.size = buf.size();
          t.data = new (std::nothrow) char[t.size];
          if (t.data) {
            memcpy(t.data, buf.data(), t.size);
            if (!push(t)) delete[] t.data;
          }
        }
        delete[] c.data;
        buf.clear();
        break;
      }
      size_t got = buf.size() + (size_t)r;
      pos += (uint64_t)r;
      size_t cut = 0;
      if (span_done) {
        // tail: finish ONLY the line that started inside this span - emit
        // up to the FIRST '\n'; every byte after it belongs to the next
        // span, which emits it itself (nothing duplicated, nothing lost).
        const char* nl = (const char*)memchr(c.data, '\n', got);
        cut = nl ? (size_t)(nl - c.data) + 1 : 0;
      } else {
        // reverse scan for the last '\n' - vectorized (simd.h), replaces
        // the byte-by-byte backward walk
        const char* rnl = VecFindLast(c.data, got, '\n');
        if (rnl) cut = (size_t)(rnl - c.data) + 1;
      }
      if (cut == 0) {  // an unfinished (possibly very long) line
        if (got > kMaxLine) {
          fprintf(stderr, "tsort: line exceeds the 3GB limit\n");
          ok->store(false);
          delete[] c.data;
          break;
        }
        buf.assign(c.data, got);
        delete[] c.data;
        continue;
      }
      if (span_done) {
        // the boundary line is complete: push it (unless it is also our
        // dropped head - a span shorter than one line) and finish the span
        buf.clear();
        c.size = cut;
        if (drop_first) { c.size = 0; drop_first = false; }
        if (c.size) { if (!push(c)) delete[] c.data; }
        else { delete[] c.data; }
        break;
      }
      buf.assign(c.data + cut, got - cut);
      c.size = cut;
      if (drop_first) {  // strip the partial line owned by the previous span
        const char* nl = (const char*)memchr(c.data, '\n', c.size);
        size_t skip = nl ? (size_t)(nl - c.data) + 1 : c.size;
        c.size -= std::min(skip, c.size);
        memmove(c.data, c.data + skip, c.size);
        drop_first = false;
        if (c.size == 0) { delete[] c.data; continue; }
      }
      if (!push(c)) { delete[] c.data; break; }
    }
    q->Close();  // fd closed by the caller after ALL readers join
  }
};

// Launch span readers over the input. Regular files split into up to
// min(threads, 32) spans (each >= 2 chunks); pipes use one reader.
// The queue's reader count is pre-armed so Close() fires when all finish.
// span_done[i] (if given, array size >= returned span count) is set by each
// reader thread right before it exits.
int StartReaders(int fd, bool is_stdin, size_t fsize, size_t chunk_bytes,
                 int threads, ChunkQueue* q, std::atomic<bool>* ok,
                 std::vector<std::thread>& out,
                 std::atomic<bool>* span_done = nullptr,
                 std::vector<uint64_t>* span_bases = nullptr) {
  bool is_pipe = is_stdin;
  if (!is_stdin && fsize == 0) is_pipe = true;  // e.g. /dev/stdin, fifos

  int n = 1;
  if (!is_pipe) {
    size_t min_span = std::max<size_t>(chunk_bytes * 2, 8 << 20);
    size_t by_size = fsize / min_span + (fsize % min_span ? 1 : 0);
    n = (int)std::min<size_t>(std::max<size_t>(by_size, 1),
                              (size_t)std::max(1, std::min(threads, 32)));
  }
  q->AddReader(n - 1);
  for (int i = 0; i < n; i++) {
    uint64_t s = 0, e = 0;
    if (!is_pipe) {
      s = (uint64_t)((unsigned __int128)fsize * (unsigned)i / (unsigned)n);
      e = (uint64_t)((unsigned __int128)fsize * (unsigned)(i + 1) / (unsigned)n);
    }
    if (span_bases) span_bases->push_back(s);
    out.emplace_back([=] {
      SpanReader r{fd, is_pipe, s, e, i, chunk_bytes, q, ok};
      r.Run();
      if (span_done) span_done[i].store(true, std::memory_order_release);
    });
  }
  return n;
}

// Close the input after every reader finished with it.
void CloseInputAfter(int fd, bool is_stdin, std::vector<std::thread>& readers) {
  for (auto& t : readers) t.join();
  if (!is_stdin) ::close(fd);
}

int OpenInput(const std::string& input, bool* is_stdin) {
  if (input == "-") { *is_stdin = true; return 0; }
  *is_stdin = false;
  int fd = ::open(input.c_str(), O_RDONLY);
  if (fd < 0) fprintf(stderr, "tsort: cannot open input %s\n", input.c_str());
  return fd;
}

// Open the output; "-" maps to stdout (fd 1, no truncation games).
int OpenOutput(const std::string& output) {
  if (output == "-") return 1;
  int fd = ::open(output.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) fprintf(stderr, "tsort: cannot open output %s\n", output.c_str());
  return fd;
}

size_t FileSizeOf(int fd) {
  struct stat st;
  if (fd >= 0 && ::fstat(fd, &st) == 0 && S_ISREG(st.st_mode))
    return (size_t)st.st_size;
  return 0;
}

size_t RlimitNoFile() {
  struct rlimit rl;
  if (::getrlimit(RLIMIT_NOFILE, &rl) != 0) return 256;
  return (size_t)rl.rlim_cur;
}

void TryRaiseNoFile() {
  struct rlimit rl;
  if (::getrlimit(RLIMIT_NOFILE, &rl) != 0) return;
  rlim_t want = rl.rlim_max < (rlim_t)65536 ? rl.rlim_max : (rlim_t)65536;
  if (rl.rlim_cur < want) {
    rl.rlim_cur = want;
    ::setrlimit(RLIMIT_NOFILE, &rl);  // best effort
  }
}

bool WriteAll(int fd, const char* p, size_t n) {
  size_t off = 0;
  while (off < n) {
    ssize_t w = ::write(fd, p + off, n - off);
    if (w < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    off += (size_t)w;
  }
  return true;
}

// ---------------------------------------------------------------------------
// ordered output writer: post chain, global head cap, tail reverse buffer
// ---------------------------------------------------------------------------
struct ReverseBuf {
  std::vector<char> data;
  std::vector<std::pair<uint32_t, uint32_t>> lines;  // (offset, len)
  size_t cap_bytes;
  bool warned = false;

  explicit ReverseBuf(size_t cap) : cap_bytes(cap) {}

  bool Append(const LineView& v) {
    if (data.size() + v.len > cap_bytes) {
      if (!warned) {
        warned = true;
        fprintf(stderr, "tsort: --tail buffer exceeds %.1fGB; use a smaller N\n",
                cap_bytes / 1e9);
      }
      return false;
    }
    lines.emplace_back((uint32_t)data.size(), v.len);
    data.insert(data.end(), v.p, v.p + v.len);
    return true;
  }
  bool WriteReversed(int fd) {
    static const char nl = '\n';
    for (auto it = lines.rbegin(); it != lines.rend(); ++it) {
      if (!WriteAll(fd, data.data() + it->first, it->second)) return false;
      if (!WriteAll(fd, &nl, 1)) return false;
    }
    return true;
  }
};

struct WriterCtx {
  int fd = -1;
  PostChain* post = nullptr;
  long head_cap = -1;  // -1 = off
  long written = 0;
  ReverseBuf* rev = nullptr;  // tail mode
  std::atomic<bool>* stop_flag = nullptr;
  std::vector<char> post_out;
  std::vector<char> obuf;     // output block buffer (few syscalls, not per line)
  bool failed = false;

  bool done() const { return failed || (head_cap >= 0 && written >= head_cap); }
  void SignalStop() {
    if (stop_flag) stop_flag->store(true);
  }

  bool FlushOut() {
    if (obuf.empty()) return true;
    bool ok = WriteAll(fd, obuf.data(), obuf.size());
    obuf.clear();
    if (!ok) { failed = true; SignalStop(); }
    return ok;
  }

  bool EmitLine(const char* p, uint32_t len) {
    if (head_cap >= 0 && written >= head_cap) { SignalStop(); return false; }
    written++;
    if (rev) return rev->Append(LineView(p, len));
    obuf.insert(obuf.end(), p, p + len);
    obuf.push_back('\n');
    if (obuf.size() >= (4 << 20)) return FlushOut();
    return true;
  }

  // Emit every complete line inside [p, p+n).
  bool EmitRawLines(const char* p, size_t n) {
    const char* s = p, * end = p + n;
    while (s < end) {
      const char* nl = (const char*)memchr(s, '\n', (size_t)(end - s));
      if (!nl) break;
      if (!EmitLine(s, (uint32_t)(nl - s))) return false;
      s = nl + 1;
    }
    return true;
  }

  // Process a block of complete '\n'-terminated payload lines.
  bool ProcessBytes(const char* p, size_t n) {
    if (done()) return false;
    // fast path: no per-line work at all -> write the block as-is
    if (!post && head_cap < 0 && !rev) {
      obuf.insert(obuf.end(), p, p + n);
      if (obuf.size() >= (4 << 20)) return FlushOut();
      return true;
    }
    const char* s = p, * end = p + n;
    while (s < end) {
      const char* nl = (const char*)memchr(s, '\n', (size_t)(end - s));
      if (!nl) break;
      if (post) {
        post_out.clear();
        bool ok = post->Run(LineView(s, (uint32_t)(nl - s)), &post_out);
        if (!post_out.empty() && !EmitRawLines(post_out.data(), post_out.size()))
          return false;
        if (!ok) { SignalStop(); return false; }
      } else {
        if (!EmitLine(s, (uint32_t)(nl - s))) return false;
      }
      if (done()) return false;
      s = nl + 1;
    }
    return true;
  }

  // End of all input: flush the post chain, buffers, then the tail reverse.
  bool Finish() {
    if (post) {
      post_out.clear();
      post->Flush(&post_out);
      if (!post_out.empty() && !EmitRawLines(post_out.data(), post_out.size()))
        return false;
    }
    if (!FlushOut()) return false;
    if (rev) {
      if (!rev->WriteReversed(fd)) { failed = true; return false; }
    }
    return !failed;
  }
};

// ---------------------------------------------------------------------------
// bounded SPSC block queue between one slice merger and the writer
// ---------------------------------------------------------------------------
struct SliceQueue {
  std::mutex mu;
  std::condition_variable cv;
  std::deque<std::vector<char>> q;
  size_t cap;  // max blocks in flight
  bool write_closed = false;

  explicit SliceQueue(size_t c) : cap(std::max<size_t>(c, 1)) {}

  bool Push(std::vector<char> b) {
    std::unique_lock<std::mutex> lk(mu);
    cv.wait(lk, [&] { return q.size() < cap || write_closed; });
    if (write_closed) return false;
    q.push_back(std::move(b));
    cv.notify_one();
    return true;
  }
  bool Pop(std::vector<char>* out) {  // false when closed and drained
    std::unique_lock<std::mutex> lk(mu);
    cv.wait(lk, [&] { return !q.empty() || write_closed; });
    if (q.empty()) return false;
    *out = std::move(q.front());
    q.pop_front();
    cv.notify_one();
    return true;
  }
  void CloseWrite() {
    {
      std::lock_guard<std::mutex> g(mu);
      write_closed = true;
    }
    cv.notify_all();
  }
};

// LineSink producing '\n'-terminated blocks into a SliceQueue.
struct SliceBlockSink : LineSink {
  SliceQueue* q;
  std::vector<char> block;
  size_t cap = 2 << 20;

  explicit SliceBlockSink(SliceQueue* qq) : q(qq) { block.reserve(cap + 4096); }

  bool Line(const LineView& v) override {
    if (block.size() + v.len + 1 > cap && !FlushBlock()) return false;
    block.insert(block.end(), v.p, v.p + v.len);
    block.push_back('\n');
    if (block.size() >= cap) return FlushBlock();
    return true;
  }
  bool FlushBlock() {
    if (block.empty()) return true;
    std::vector<char> b(std::move(block));
    block.clear();
    block.reserve(cap + 4096);
    return q->Push(std::move(b));
  }
  void Finish() {  // flush + close (even on failure, to release the writer)
    FlushBlock();
    q->CloseWrite();
  }
};

// LineSink appending records to a run file (pre-merge passes).
struct RunSink : LineSink {
  RunAppender* app;
  explicit RunSink(RunAppender* a) : app(a) {}
  bool Line(const LineView& v) override { return app->Append(v); }
};

// Counting turn gate (from v2, proven): slices start only inside the sliding
// window [written_idx, written_idx + window).
struct TurnGate {
  std::mutex mu;
  std::condition_variable cv;
  int next_turn;
  int limit;
  explicit TurnGate(int window, int n) : next_turn(std::min(window, n)), limit(n) {}
  void WaitTurn(int i) {
    std::unique_lock<std::mutex> lk(mu);
    cv.wait(lk, [&] { return i < next_turn; });
  }
  void AdvanceAfter(int written_idx, int window) {
    {
      std::lock_guard<std::mutex> g(mu);
      next_turn = std::min(limit, written_idx + 1 + window);
    }
    cv.notify_all();
  }
};

std::string TempPath(const std::string& tmpdir, const char* tag, int seq) {
  char path[512];
  snprintf(path, sizeof(path), "%s/ts_%s_%d_%d.bin", tmpdir.c_str(), tag,
           (int)getpid(), seq);
  return path;
}

}  // namespace

// ===========================================================================
// shuffle implementation
// ===========================================================================
namespace {

// Split one line-oriented file (or the pipeline input) into R hash shards.
// Returns the shard paths; empty on failure. Applies pre ops only when
// `apply_ops` (re-sharding works on already-transformed lines).
bool ShardOneFile(const std::string& path, int in_fd,
                  const std::vector<std::string>& shard_paths,
                  PipelineDesc* pipe, bool apply_ops) {
  int R = (int)shard_paths.size();
  std::vector<shuffle_detail::ShardWriter> shards(R);
  for (int i = 0; i < R; i++)
    if (!shards[i].Open(shard_paths[i])) return false;

  int fd = in_fd;
  bool opened = false;
  if (fd < 0) {
    fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return false;
    opened = true;
  }
  FadviseSequential(fd);

  ChainRunner runner;
  runner.chain = &pipe->pre;
  LineView v;

  std::vector<char> buf(8 << 20);
  std::string pending;
  bool ok = true;
  for (;;) {
    ssize_t r = ::read(fd, buf.data(), buf.size());
    if (r < 0) {
      if (errno == EINTR) continue;
      ok = false;
      break;
    }
    if (r == 0) break;
    size_t got = pending.size() + (size_t)r;
    if (pending.size()) {
      std::vector<char> merged(got);
      memcpy(merged.data(), pending.data(), pending.size());
      memcpy(merged.data() + pending.size(), buf.data(), (size_t)r);
      buf.swap(merged);
      buf.resize(got);
    }
    size_t cut = 0;
    {
      const char* rnl = VecFindLast(buf.data(), got, '\n');
      if (rnl) cut = (size_t)(rnl - buf.data()) + 1;
    }
    if (cut == 0) {
      pending.assign(buf.data(), got);
      continue;
    }
    size_t pos = 0;
    while (pos < cut) {
      const char* nl = (const char*)memchr(buf.data() + pos, '\n', cut - pos);
      size_t end = nl ? (size_t)(nl - buf.data()) : cut;
      LineView in(buf.data() + pos, (uint32_t)(end - pos));
      if (!apply_ops || pipe->pre.Empty() || runner.Run(in, &v)) {
        const LineView& keep = apply_ops && !pipe->pre.Empty() ? v : in;
        uint64_t h = ShardHash(keep.p, keep.len);
        uint32_t s = (uint32_t)(h % (uint64_t)R);
        shards[s].Append(keep.p, keep.len);
        shards[s].Append("\n", 1);
      }
      pos = end + (nl ? 1 : 0);
    }
    pending.assign(buf.data() + cut, got - cut);
    buf.resize(8 << 20);
  }
  if (ok && !pending.empty()) {  // final line without '\n'
    LineView in(pending.data(), (uint32_t)pending.size());
    if (!apply_ops || pipe->pre.Empty() || runner.Run(in, &v)) {
      const LineView& keep = apply_ops && !pipe->pre.Empty() ? v : in;
      uint64_t h = ShardHash(keep.p, keep.len);
      uint32_t s = (uint32_t)(h % (uint64_t)R);
      shards[s].Append(keep.p, keep.len);
      shards[s].Append("\n", 1);
    }
  }
  if (opened) ::close(fd);
  for (auto& s : shards) {
    s.Close();
    ok = ok && !s.failed;
  }
  return ok;
}

}  // namespace

int RunShufflePipeline(const std::string& input, const std::string& output,
                       PipelineDesc* pipe, const EngineConfig& cfg_in,
                       bool verbose, unsigned seed) {
  EngineConfig cfg = cfg_in;
  if (cfg.threads <= 0) {
    unsigned h = std::thread::hardware_concurrency();
    cfg.threads = h ? (int)h : 4;
  }
  if (cfg.mem_budget == 0) cfg.mem_budget = PhysicalMemBytes() / 2;

  double t0 = NowSec();
  bool is_stdin = false;
  int fd = OpenInput(input, &is_stdin);
  if (fd < 0) return 1;
  size_t fsize = is_stdin ? 0 : FileSizeOf(fd);

  // shard RAM budget: one shard must fit comfortably in memory
  size_t shard_cap = std::max<size_t>(cfg.mem_budget / 4, 64 << 20);
  auto shards_for = [&](size_t bytes, int floor_) {
    size_t by_mem = bytes ? (bytes + shard_cap - 1) / shard_cap : 0;
    size_t r = std::max<size_t>({(size_t)floor_, by_mem, 8});
    return (int)std::min<size_t>(r, 512);
  };

  std::vector<std::string> shards;
  {
    std::vector<std::string> first(shards_for(fsize, cfg.threads));
    for (int i = 0; i < (int)first.size(); i++)
      first[i] = TempPath(cfg.tmpdir, "shard", i);
    if (!ShardOneFile(input, fd, first, pipe, /*apply_ops=*/true)) {
      fprintf(stderr, "tsort: sharding failed\n");
      return 1;
    }
    shards = std::move(first);
  }
  // re-shard oversize shards until every shard fits the RAM budget
  for (int guard = 0; guard < 8; guard++) {
    bool any_big = false;
    for (size_t i = 0; i < shards.size(); i++) {
      struct stat st;
      if (::stat(shards[i].c_str(), &st) != 0) continue;
      if ((size_t)st.st_size <= shard_cap) continue;
      any_big = true;
      int R2 = shards_for((size_t)st.st_size, 2);
      std::vector<std::string> sub(R2);
      for (int k = 0; k < R2; k++)
        sub[k] = shards[i] + "." + std::to_string(k);
      if (!ShardOneFile(shards[i], -1, sub, pipe, /*apply_ops=*/false)) return 1;
      ::unlink(shards[i].c_str());
      shards.erase(shards.begin() + (long)i);
      shards.insert(shards.begin() + (long)i, sub.begin(), sub.end());
      i += sub.size() - 1;
    }
    if (!any_big) break;
  }
  double t1 = NowSec();
  if (verbose)
    fprintf(stderr, "[shuffle] shard %.2fs (%zu shards)\n", t1 - t0, shards.size());

  // pass 2: shard i shuffled in RAM, streamed out in shard order
  int out_fd = OpenOutput(output);
  if (out_fd < 0) return 1;
  WriterCtx wctx;
  wctx.fd = out_fd;
  wctx.post = pipe->post.Empty() ? nullptr : &pipe->post;
  if (pipe->head_n >= 0) wctx.head_cap = pipe->head_n;
  if (pipe->tail_n > 0) {
    wctx.head_cap = pipe->tail_n;
    wctx.rev = new ReverseBuf(
        std::min<size_t>(size_t{4} << 30, std::max<size_t>(cfg.mem_budget / 4, size_t{256} << 20)));
  }

  size_t total_lines = 0;
  for (size_t i = 0; i < shards.size() && !wctx.done(); i++) {
    int sfd = ::open(shards[i].c_str(), O_RDONLY);
    if (sfd < 0) return 1;
    size_t sz = FileSizeOf(sfd);
    std::vector<char> buf(sz);
    {
      size_t off = 0;
      while (off < sz) {
        ssize_t r = ::pread(sfd, buf.data() + off, sz - off, (off_t)off);
        if (r <= 0) break;
        off += (size_t)r;
      }
    }
    ::close(sfd);
    std::vector<std::pair<uint32_t, uint32_t>> lines_;
    size_t pos = 0;
    while (pos < buf.size()) {
      const char* nl =
          (const char*)memchr(buf.data() + pos, '\n', buf.size() - pos);
      if (!nl) break;
      lines_.emplace_back((uint32_t)pos, (uint32_t)(nl - (buf.data() + pos)));
      pos = (size_t)(nl - buf.data()) + 1;
    }
    std::mt19937_64 rng(seed * 1000003ULL + (uint64_t)i);  // deterministic
    for (size_t k = lines_.size(); k > 1; k--) {
      size_t j = (size_t)(rng() % (uint64_t)k);
      std::swap(lines_[k - 1], lines_[j]);
    }
    std::vector<char> out;
    out.reserve(2 << 20);
    for (auto& lv : lines_) {
      if (wctx.done()) break;
      out.insert(out.end(), buf.data() + lv.first, buf.data() + lv.first + lv.second);
      out.push_back('\n');
      if (out.size() >= (2 << 20)) {
        if (!wctx.ProcessBytes(out.data(), out.size())) break;
        out.clear();
      }
    }
    if (!out.empty() && !wctx.done()) wctx.ProcessBytes(out.data(), out.size());
    total_lines += lines_.size();
    if (wctx.failed) return 1;
  }
  bool ok = wctx.Finish();
  ::close(out_fd);
  delete wctx.rev;
  double t2 = NowSec();
  if (verbose)
    fprintf(stderr, "[shuffle] pass2 %.2fs | total %.2fs | lines=%zu\n", t2 - t1,
            t2 - t0, total_lines);
  for (auto& p : shards) ::unlink(p.c_str());
  return ok ? 0 : 1;
}

// ===========================================================================
// main engine
// ===========================================================================
namespace {

std::vector<std::string> SampleAnchors(const std::vector<SortedSource>& sources,
                                       const MergeOrder& order, int P) {
  std::vector<std::string> anchors;
  if (P <= 1) return anchors;
  std::vector<std::string> samples;
  const int per_src = 32;
  for (auto& s : sources) {
    if (s.Count() == 0) continue;
    const auto& sp = s.is_mem ? s.mem.sparse : s.file.sparse;
    if (sp.empty()) continue;
    size_t stride = std::max<size_t>(1, sp.size() / per_src);
    int fd = -1;
    if (!s.is_mem) {
      fd = ::open(s.file.path.c_str(), O_RDONLY);
      if (fd < 0) continue;
    }
    for (size_t j = 0; j < sp.size(); j += stride) {
      uint32_t len = 0;
      const char* lp = nullptr;
      std::string big;
      if (s.is_mem) {
        memcpy(&len, s.mem.packed.data() + sp[j].off, 4);
        lp = s.mem.packed.data() + sp[j].off + 4;
      } else {
        uint32_t l4;
        if (::pread(fd, &l4, 4, (off_t)sp[j].off) == 4) {
          len = l4;
          big.resize(len);
          if (::pread(fd, big.data(), len, (off_t)(sp[j].off + 4)) != (ssize_t)len)
            continue;
          lp = big.data();
        } else {
          continue;
        }
      }
      samples.emplace_back(lp, len);
    }
    if (fd >= 0) ::close(fd);
  }
  if (samples.empty()) return anchors;
  std::sort(samples.begin(), samples.end(),
            [&](const std::string& a, const std::string& b) {
              return order.Less(LineView(a.data(), (uint32_t)a.size()),
                                LineView(b.data(), (uint32_t)b.size()));
            });
  size_t want = (size_t)P - 1;
  for (size_t i = 0; i < want && samples.size() > 1; i++) {
    const std::string& a =
        samples[(size_t)((double)(i + 1) * samples.size() / (want + 1))];
    if (anchors.empty() || anchors.back() != a) anchors.push_back(a);
  }
  return anchors;
}

// One pre-merge pass: merge file runs in groups of `fanin` into fewer files.
bool PreMergePass(std::vector<SortedSource>* sources, const MergeOrder& order,
                  DedupMode dedup, size_t fanin, size_t target,
                  const EngineConfig& cfg, int max_parallel, int threads,
                  std::atomic<int>* seq, const std::atomic<bool>* stop,
                  bool verbose, int pass) {
  std::vector<int> file_idx;
  for (size_t i = 0; i < sources->size(); i++)
    if (!(*sources)[i].is_mem) file_idx.push_back((int)i);
  if (file_idx.size() <= target) return true;

  std::vector<std::vector<SortedSource*>> groups;
  for (size_t o = 0; o < file_idx.size(); o += fanin) {
    std::vector<SortedSource*> g;
    for (size_t j = o; j < std::min(o + fanin, file_idx.size()); j++)
      g.push_back(&(*sources)[file_idx[j]]);
    groups.push_back(std::move(g));
  }

  std::vector<std::string> paths(groups.size());
  for (size_t g = 0; g < groups.size(); g++)
    paths[g] = TempPath(cfg.tmpdir, "pre", seq->fetch_add(1));

  std::vector<RunFile> new_runs(groups.size());
  std::atomic<size_t> next_group{0};
  std::atomic<bool> failed{false};

  int nthreads = std::max(1, std::min<int>({threads, (int)groups.size(), max_parallel}));
  std::vector<std::thread> pool;
  for (int t = 0; t < nthreads; t++) {
    pool.emplace_back([&] {
      for (;;) {
        size_t g = next_group.fetch_add(1);
        if (g >= groups.size() || failed.load()) break;
        RunAppender app;
        if (!app.Open(paths[g], order)) { failed.store(true); return; }
        RunSink sink(&app);
        MergeRange(groups[g], order, LineView(), LineView(), dedup, -1, stop, &sink);
        if (!app.Finish(&new_runs[g])) {
          failed.store(true);
          ::unlink(paths[g].c_str());
          return;
        }
      }
    });
  }
  for (auto& t : pool) t.join();
  if (failed.load()) return false;

  std::vector<SortedSource> out;
  std::vector<std::string> old_paths;
  for (size_t i = 0; i < sources->size(); i++) {
    if ((*sources)[i].is_mem) out.push_back(std::move((*sources)[i]));
    else old_paths.push_back((*sources)[i].file.path);
  }
  for (auto& rf : new_runs) {
    if (rf.count == 0) continue;
    SortedSource s;
    s.is_mem = false;
    s.file = std::move(rf);
    out.push_back(std::move(s));
  }
  *sources = std::move(out);
  for (auto& p : old_paths) ::unlink(p.c_str());
  if (verbose)
    fprintf(stderr,
            "[engine] pre-merge pass %d: %zu file runs -> %zu (fan-in %zu)\n",
            pass, file_idx.size(), new_runs.size(), fanin);
  return true;
}

int RunSortPipeline(const std::string& input, const std::string& output,
                    PipelineDesc* pipe, EngineConfig cfg, bool verbose) {
  if (cfg.threads <= 0) {
    unsigned h = std::thread::hardware_concurrency();
    cfg.threads = h ? (int)h : 4;
  }
  if (cfg.mem_budget == 0) cfg.mem_budget = PhysicalMemBytes() / 2;
  cfg.chunk_bytes = std::min<size_t>(cfg.chunk_bytes, 3 << 30);
  TryRaiseNoFile();
  size_t nofile = RlimitNoFile();

  // A tiny budget must also shrink the chunk size (a chunk bigger than the
  // in-flight cap would deadlock the reader).
  if ((size_t)cfg.threads * 3 > 0)
    cfg.chunk_bytes = std::min<size_t>(
        cfg.chunk_bytes,
        std::max<size_t>(1 << 20, cfg.mem_budget / ((size_t)cfg.threads * 3)));

  MergeOrder order;
  order.descending = pipe->reverse;  // --reverse or --tail
  order.key = pipe->sort_key;

  double t_start = NowSec();
  size_t chunk_cap = std::max<size_t>(cfg.chunk_bytes * 3,
                                      std::min<size_t>(cfg.mem_budget / 2, 2 << 30));
  size_t budget_runs = cfg.mem_budget - std::min<size_t>(cfg.mem_budget / 4, 1 << 30);

  bool is_stdin = false;
  int fd = OpenInput(input, &is_stdin);
  if (fd < 0) return 1;

  ChunkQueue q(chunk_cap);
  std::atomic<bool> read_ok(true);
  size_t fsize = is_stdin ? 0 : FileSizeOf(fd);
  std::vector<std::thread> reader_threads;
  StartReaders(fd, is_stdin, fsize, cfg.chunk_bytes, cfg.threads, &q,
               &read_ok, reader_threads);

  // ---------------- Phase A: read -> ops -> chunk sort -> runs ----------------
  struct PhaseA {
    std::mutex mu;
    std::vector<SortedSource> sources;
    size_t mem_used = 0;
    size_t total_lines = 0;
    bool io_failed = false;
    std::atomic<int> run_seq{0};
  } pa;

  auto worker = [&] {
    ChainRunner runner;
    runner.chain = &pipe->pre;
    std::vector<char> extra;
    for (;;) {
      RawChunk c;
      if (!q.Pop(&c)) break;
      extra.clear();
      SortedMemRun run = SortChunk(c.data, c.size,
                                   pipe->pre.Empty() ? nullptr : &runner,
                                   &extra, order);
      delete[] c.data;
      q.Release(c.size);
      std::lock_guard<std::mutex> g(pa.mu);
      pa.total_lines += run.count;
      if (run.count == 0) continue;  // empty run: drop
      bool spill =
          cfg.force_external || pa.mem_used + run.packed.size() > budget_runs;
      if (spill) {
        std::string path = TempPath(cfg.tmpdir, "run", pa.run_seq.fetch_add(1));
        RunFile rf;
        if (RunFile::Write(path, run, order, &rf)) {
          SortedSource s;
          s.is_mem = false;
          s.file = std::move(rf);
          pa.sources.push_back(std::move(s));
        } else {
          fprintf(stderr, "tsort: failed to write run %s\n", path.c_str());
          pa.io_failed = true;
        }
      } else {
        pa.mem_used += run.packed.size();
        SortedSource s;
        s.mem = std::move(run);
        pa.sources.push_back(std::move(s));
      }
    }
  };
  std::vector<std::thread> workers;
  workers.reserve(cfg.threads);
  for (int i = 0; i < cfg.threads; i++) workers.emplace_back(worker);
  CloseInputAfter(fd, is_stdin, reader_threads);
  for (auto& w : workers) w.join();
  double t_sorted = NowSec();
  size_t n_file = 0;
  for (auto& s : pa.sources) n_file += s.is_mem ? 0 : 1;
  if (verbose)
    fprintf(stderr,
            "[engine] phase A %.2fs | runs=%zu (mem=%zu file=%zu) lines=%zu "
            "runmem=%.1fGB\n",
            t_sorted - t_start, pa.sources.size(), pa.sources.size() - n_file,
            n_file, pa.total_lines, pa.mem_used / 1e9);
  if (!read_ok.load() || pa.io_failed) return 1;

  if (pa.total_lines == 0) {  // empty input: still create the output
    int ofd = OpenOutput(output);
    if (ofd >= 1) ::close(ofd);
    return 0;
  }

  // ---------------- Phase B0: pre-merge passes (external) ----------------
  {
    size_t target = std::min<size_t>(48, std::max<size_t>(8, nofile / 32));
    size_t fanin = std::min<size_t>(32, std::max<size_t>(8, nofile / 64));
    int max_par = (int)std::max<size_t>(1, (nofile - 64) / (fanin + 4));
    int pass = 1;
    while (n_file > target) {
      std::atomic<bool> dummy_stop{false};
      if (!PreMergePass(&pa.sources, order,
                        pipe->dedup == DedupMode::Unique ? DedupMode::Unique
                                                         : DedupMode::None,
                        fanin, target, cfg, max_par, cfg.threads, &pa.run_seq,
                        &dummy_stop, verbose, pass))
        return 1;
      n_file = 0;
      for (auto& s : pa.sources) n_file += s.is_mem ? 0 : 1;
      if (++pass > 8) break;
    }
  }

  // ---------------- Phase B1: anchors ----------------
  std::vector<SortedSource>& sources = pa.sources;
  int P = std::max(1, std::min<int>(cfg.threads, 128));
  std::vector<std::string> anchors = SampleAnchors(sources, order, P);
  const int slices = (int)anchors.size() + 1;

  std::vector<SortedSource*> all;
  for (auto& s : sources) all.push_back(&s);

  int window = slices;
  if (n_file > 0) {
    size_t per_slice_fds = n_file + 4;
    window = (int)std::max<size_t>(
        2, std::min<size_t>((size_t)slices, (nofile - 64) / per_slice_fds));
    window = std::min(window, 4);
  }
  TurnGate gate(window, slices);

  long head_cap = -1;
  if (pipe->tail_n > 0) head_cap = pipe->tail_n;
  else if (pipe->head_n >= 0) head_cap = pipe->head_n;

  std::atomic<bool> stop_flag(false);
  std::vector<std::unique_ptr<SliceQueue>> squeues;
  for (int i = 0; i < slices; i++)
    squeues.push_back(std::make_unique<SliceQueue>(3));
  std::atomic<size_t> total_in{0}, total_out{0}, total_dups{0};

  // ---------------- Phase C: parallel slice merge + ordered writer ----------
  int out_fd = OpenOutput(output);
  if (out_fd < 0) return 1;

  std::vector<std::thread> mergers;
  for (int i = 0; i < slices; i++) {
    mergers.emplace_back([&, i] {
      gate.WaitTurn(i);
      if (stop_flag.load(std::memory_order_relaxed)) {
        squeues[i]->CloseWrite();
        return;
      }
      LineView lo, hi;
      if (i > 0)
        lo = LineView(anchors[i - 1].data(), (uint32_t)anchors[i - 1].size());
      if (i + 1 < slices)
        hi = LineView(anchors[i].data(), (uint32_t)anchors[i].size());
      SliceBlockSink sink(squeues[i].get());
      MergeStats st = MergeRange(all, order, lo, hi, pipe->dedup, head_cap,
                                 &stop_flag, &sink);
      total_in.fetch_add(st.in_lines);
      total_out.fetch_add(st.out_lines);
      total_dups.fetch_add(st.dups);
      sink.Finish();  // flush + close even on abort
    });
  }

  WriterCtx wctx;
  wctx.fd = out_fd;
  wctx.post = pipe->post.Empty() ? nullptr : &pipe->post;
  wctx.head_cap = head_cap;
  wctx.stop_flag = &stop_flag;
  ReverseBuf rev(pipe->tail_n > 0
                     ? std::min<size_t>(size_t{4} << 30,
                                        std::max<size_t>(cfg.mem_budget / 4, size_t{256} << 20))
                     : 0);
  if (pipe->tail_n > 0) wctx.rev = &rev;

  std::vector<char> block;
  for (int i = 0; i < slices; i++) {
    while (squeues[i]->Pop(&block)) {
      if (!wctx.done()) wctx.ProcessBytes(block.data(), block.size());
      // keep draining so the merger never blocks
    }
    gate.AdvanceAfter(i, window);
  }
  for (auto& t : mergers) t.join();

  bool ok = wctx.Finish();
  ::close(out_fd);
  double t_done = NowSec();
  if (verbose)
    fprintf(stderr,
            "[engine] phase B+C %.2fs | total %.2fs | in=%zu out=%zu dups=%zu "
            "slices=%d window=%d\n",
            t_done - t_sorted, t_done - t_start, total_in.load(),
            total_out.load(), total_dups.load(), slices, window);
  for (auto& s : sources)
    if (!s.is_mem) ::unlink(s.file.path.c_str());
  return ok ? 0 : 1;
}

// ---------------------------------------------------------------------------
// pure streaming mode (no sort barrier)
// ---------------------------------------------------------------------------
int RunStreamPipeline(const std::string& input, const std::string& output,
                      PipelineDesc* pipe, EngineConfig cfg, bool verbose) {
  if (cfg.threads <= 0) {
    unsigned h = std::thread::hardware_concurrency();
    cfg.threads = h ? (int)h : 4;
  }
  if (cfg.mem_budget == 0) cfg.mem_budget = PhysicalMemBytes() / 2;
  // Streaming ops are cheap per line: small chunks keep the resident set low
  // (a 64MB chunk x threads x in/out copies reached ~2GB RSS) while the
  // ordered hub still saturates all cores.
  cfg.chunk_bytes = std::min<size_t>(cfg.chunk_bytes, 8 << 20);
  cfg.chunk_bytes = std::max<size_t>(cfg.chunk_bytes, 1 << 20);
  cfg.chunk_bytes = std::min<size_t>(
      cfg.chunk_bytes,
      std::max<size_t>(1 << 20, cfg.mem_budget / ((size_t)cfg.threads * 4)));

  double t_start = NowSec();
  bool is_stdin = false;
  int fd = OpenInput(input, &is_stdin);
  if (fd < 0) return 1;

  size_t chunk_cap = std::max<size_t>(cfg.chunk_bytes * 3,
                                      std::min<size_t>(cfg.mem_budget / 2, 1 << 30));
  ChunkQueue q(chunk_cap);
  std::atomic<bool> read_ok(true);
  size_t fsize = is_stdin ? 0 : FileSizeOf(fd);
  std::vector<std::thread> reader_threads;
  // span completion flags: reader i sets its flag right before exiting. The
  // ordered hub advances from span s to s+1 only when the flag is set AND
  // every popped chunk of s has been submitted (span_pending[s] == 0). The
  // array covers the max span count StartReaders may pick: min(threads, 32).
  std::vector<std::atomic<bool>> span_done(
      (size_t)std::max(1, std::min(cfg.threads, 32)));
  std::vector<uint64_t> span_bases;  // span start offsets (seq -> byte pos)
  int n_spans = StartReaders(fd, is_stdin, fsize, cfg.chunk_bytes, cfg.threads,
                             &q, &read_ok, reader_threads, span_done.data(),
                             &span_bases);

  // processed chunks land in an ordered hub
  struct Hub {
    std::mutex mu;
    std::condition_variable cv_w;
    std::map<size_t, RawChunk> ready;  // keyed by seq = span<<44 | k
    size_t inflight = 0;  // bytes sitting in `ready` (stats/sanity)
    bool failed = false;
    bool input_closed = false;
    // per-span gating: the writer may advance to span s+1 only when span s
    // is fully emitted (reader finished + nothing of it queued or held (see
    // ChunkQueue::SpanDrained) + nothing of it left in `ready`).
    int n_spans = 1;
    const std::atomic<bool>* span_reader_done;  // set by reader threads
    ChunkQueue* srcq;                           // drained-span oracle
    int cur_span = 0;
    // strict emission order: seq = span<<44 | k. Only the EXACT next seq is
    // popped, so a slow worker holding (s,k) cannot let (s,k+1) jump ahead.
    // The encoding is contiguous across spans, so k runs on; the gate reset
    // below jumps it to the next span's head (spans may push 0 chunks).
    std::atomic<size_t> next_seq{0};
    // byte-order position of a chunk: span start + k * chunk size. Raw seqs
    // from DIFFERENT spans are not comparable (span<<44 vs k bits) - byte
    // positions are, and they drive the look-ahead window below.
    std::vector<uint64_t> span_base;
    size_t cbytes = 1;
    size_t look_bytes = 0;
    uint64_t SeqPos(size_t seq) const {
      size_t s = seq >> ChunkQueue::kSpanShift;
      size_t k = seq & ((size_t{1} << ChunkQueue::kSpanShift) - 1);
      return (s < span_base.size() ? span_base[s] : 0) + (uint64_t)k * cbytes;
    }
    // may a worker take this chunk yet? Bounded look-ahead past the writer
    // bounds `ready`; the worker parks in ChunkQueue::PopGated HOLDING
    // NOTHING, so the exact chunk the writer waits for can always be picked
    // up. No Submit-side sleeping anywhere: total memory is bounded
    // structurally by queue cap + threads*chunk + look_bytes.
    bool WithinLookahead(size_t seq) const {
      return SeqPos(seq) <=
             SeqPos(next_seq.load(std::memory_order_acquire)) + look_bytes;
    }
    Hub(int spans, const std::atomic<bool>* done, ChunkQueue* q,
        std::vector<uint64_t> bases, size_t chunk, size_t look)
        : n_spans(std::max(1, spans)), span_reader_done(done), srcq(q),
          span_base(std::move(bases)), cbytes(chunk ? chunk : 1),
          look_bytes(look) {}
    void Kick() { cv_w.notify_all(); }  // dropped chunk: gate may advance
    void Submit(size_t seq, RawChunk c) {
      std::lock_guard<std::mutex> g(mu);
      if (failed) { delete[] c.data; return; }
      inflight += c.size;
      ready[seq] = c;
      cv_w.notify_one();
    }
    bool PopNext(RawChunk* out) {  // strict file order across parallel spans
      for (;;) {
        if (failed) return false;
        {  // exact next chunk ready? (hub state under `mu`)
          std::lock_guard<std::mutex> g(mu);
          auto it = ready.find(next_seq.load(std::memory_order_relaxed));
          if (it != ready.end()) {
            *out = it->second;
            ready.erase(it);
            next_seq.fetch_add(1, std::memory_order_release);
            return true;
          }
        }
        // cur_span drained? reader finished + nothing queued/held. Asked
        // under the QUEUE's lock (atomic with the pops themselves - a chunk
        // can never slip between the queue and this check), and NOT while
        // holding `mu`.
        if (span_reader_done[cur_span].load(std::memory_order_acquire) &&
            srcq->SpanDrained((size_t)cur_span)) {
          std::lock_guard<std::mutex> g(mu);
          if (ready.find(next_seq.load(std::memory_order_relaxed)) ==
              ready.end()) {  // re-validate
            if (++cur_span >= n_spans) return false;
            next_seq.store(
                (size_t)(unsigned)cur_span << ChunkQueue::kSpanShift,
                std::memory_order_release);
            continue;  // gate advanced into the next span (k = 0)
          }
          continue;  // the exact chunk appeared meanwhile: pop it above
        }
        // wait for progress on cur_span; producers notify, timeout as a
        // belt-and-braces against a missed edge
        std::unique_lock<std::mutex> lk(mu);
        cv_w.wait_for(lk, std::chrono::milliseconds(100));
      }
    }
    void Release(size_t n) {
      std::lock_guard<std::mutex> g(mu);
      inflight = inflight >= n ? inflight - n : 0;
    }
    void Fail() {
      {
        std::lock_guard<std::mutex> g(mu);
        failed = true;
      }
      cv_w.notify_all();
    }
    void CloseInput() {
      {
        std::lock_guard<std::mutex> g(mu);
        input_closed = true;
      }
      cv_w.notify_all();
    }
  } hub(n_spans, span_done.data(), &q, std::move(span_bases), cfg.chunk_bytes,
        std::max<size_t>(cfg.mem_budget / 2,
                         ((size_t)cfg.threads + 4) * cfg.chunk_bytes));

  std::atomic<bool> stop_flag(false);
  std::atomic<int> workers_left{cfg.threads};
  const bool pure = pipe->pre.PureFilterChain();  // zero-copy compaction

  auto worker = [&] {
    ChainRunner runner;
    runner.chain = &pipe->pre;
    LineView v;
    for (;;) {
      RawChunk c;
      size_t seq = 0;
      // lowest-seq chunk within the writer's look-ahead window; parks while
      // gated holding NOTHING, so the writer's chunk is always pick-up-able.
      // The pop also marks the span held (span-drain gate).
      if (!q.PopGated(&c, &seq,
                      [&](size_t s) { return hub.WithinLookahead(s); }))
        break;
      if (stop_flag.load(std::memory_order_relaxed)) {
        delete[] c.data;
        q.Release(c.size);
        q.NoteSubmitted(seq);  // dropped without a Submit: gate may advance
        hub.Kick();
        continue;
      }
      if (pure) {
        // in-place compaction: surviving lines are memmoved to the chunk
        // front; the reader's buffer is handed to the hub unchanged.
        // (readers allocate one spare byte so an unterminated final line can
        // get its '\n' written in place without a reallocation)
        const size_t orig_size = c.size;
        size_t w = 0, pos = 0;
        while (pos < c.size) {
          const char* nl =
              (const char*)memchr(c.data + pos, '\n', c.size - pos);
          size_t end = nl ? (size_t)(nl - c.data) : c.size;
          LineView in(c.data + pos, (uint32_t)(end - pos));
          bool keep = true;
          if (!pipe->pre.Empty()) keep = runner.Run(in, &v);
          if (keep) {
            const LineView& lv = pipe->pre.Empty() ? in : v;  // aliases `in`
            if (w != pos) memmove(c.data + w, lv.p, lv.len);
            w += lv.len;
            c.data[w++] = '\n';  // unterminated tail gets one too
          }
          pos = end + (nl ? 1 : 0);
        }
        c.size = w;
        q.Release(orig_size);
        if (c.size == 0) {  // whole chunk filtered out: no Submit at all
          delete[] c.data;
          q.NoteSubmitted(seq);
          hub.Kick();
          continue;
        }
        RawChunk pc = c;  // reuse the SAME memory: no copy, no alloc
        hub.Submit(seq, pc);   // in `ready` now; Submit wakes the writer
        q.NoteSubmitted(seq);  // AFTER Submit: chunk visible in `ready` first
        continue;
      }
      std::vector<char> out;
      out.reserve(c.size);
      size_t pos = 0;
      while (pos < c.size) {
        const char* nl = (const char*)memchr(c.data + pos, '\n', c.size - pos);
        size_t end = nl ? (size_t)(nl - c.data) : c.size;
        LineView in(c.data + pos, (uint32_t)(end - pos));
        if (pipe->pre.Empty() || runner.Run(in, &v)) {
          const LineView& keep = pipe->pre.Empty() ? in : v;
          out.insert(out.end(), keep.p, keep.p + keep.len);
          out.push_back('\n');
        }
        pos = end + (nl ? 1 : 0);
      }
      delete[] c.data;
      q.Release(c.size);
      RawChunk pc;
      pc.size = out.size();
      pc.data = new (std::nothrow) char[pc.size ? pc.size : 1];
      if (!pc.data) {
        hub.Fail();
        q.NoteSubmitted(seq);
        break;
      }
      if (pc.size) memcpy(pc.data, out.data(), pc.size);
      hub.Submit(seq, pc);   // in `ready` now; Submit wakes the writer
      q.NoteSubmitted(seq);  // AFTER Submit: chunk visible in `ready` first
    }
    if (workers_left.fetch_sub(1) == 1) hub.CloseInput();
  };
  std::vector<std::thread> workers;
  for (int i = 0; i < cfg.threads; i++) workers.emplace_back(worker);

  int out_fd = OpenOutput(output);
  if (out_fd < 0) {
    q.Close();
    hub.Fail();
    for (auto& w : workers) w.join();
    CloseInputAfter(fd, is_stdin, reader_threads);
    return 1;
  }
  WriterCtx wctx;
  wctx.fd = out_fd;
  wctx.post = pipe->post.Empty() ? nullptr : &pipe->post;
  wctx.head_cap = pipe->head_n;  // tail is rejected in stream mode by the CLI
  wctx.stop_flag = &stop_flag;

  RawChunk pc;
  size_t written_lines = 0;
  while (hub.PopNext(&pc)) {
    if (!wctx.done()) wctx.ProcessBytes(pc.data, pc.size);
    written_lines += (size_t)std::count(pc.data, pc.data + pc.size, '\n');
    delete[] pc.data;
    hub.Release(pc.size);
  }
  for (auto& w : workers) w.join();
  CloseInputAfter(fd, is_stdin, reader_threads);
  bool ok = wctx.Finish() && read_ok.load();
  ::close(out_fd);
  double t_done = NowSec();
  if (verbose)
    fprintf(stderr, "[stream] total %.2fs | lines=%zu\n", t_done - t_start,
            written_lines);
  return ok ? 0 : 1;
}

}  // namespace

int RunPipeline(const std::string& input, const std::string& output,
                PipelineDesc* pipe, const EngineConfig& cfg, bool verbose) {
  if (pipe->shuffle)
    return RunShufflePipeline(input, output, pipe, cfg, verbose, cfg.shuffle_seed);
  if (pipe->need_sort) return RunSortPipeline(input, output, pipe, cfg, verbose);
  return RunStreamPipeline(input, output, pipe, cfg, verbose);
}

}  // namespace ts
