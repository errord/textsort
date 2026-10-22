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
struct ChunkQueue {
  std::mutex mu;
  std::condition_variable cv_space, cv_item;
  std::deque<RawChunk> items;
  std::deque<size_t> seqs;  // chunk sequence numbers (stream mode ordering)
  size_t inflight = 0, inflight_cap;
  size_t next_seq = 0;
  bool closed = false;

  explicit ChunkQueue(size_t cap_bytes) : inflight_cap(cap_bytes) {}

  bool Push(RawChunk c) {  // blocks until there is room
    std::unique_lock<std::mutex> lk(mu);
    cv_space.wait(lk, [&] { return closed || inflight + c.size <= inflight_cap; });
    if (closed) return false;
    items.push_back(c);
    seqs.push_back(next_seq++);
    inflight += c.size;
    cv_item.notify_one();
    return true;
  }
  bool Pop(RawChunk* out, size_t* seq = nullptr) {
    std::unique_lock<std::mutex> lk(mu);
    cv_item.wait(lk, [&] { return !items.empty() || closed; });
    if (items.empty()) return false;
    *out = items.front();
    if (seq) *seq = seqs.front();
    items.pop_front();
    seqs.pop_front();
    return true;
  }
  void Release(size_t n) {
    {
      std::lock_guard<std::mutex> g(mu);
      inflight = inflight >= n ? inflight - n : 0;
    }
    cv_space.notify_all();
  }
  void Close() {
    {
      std::lock_guard<std::mutex> g(mu);
      closed = true;
    }
    cv_item.notify_all();
    cv_space.notify_all();
  }
};

// Sequential reader producing '\n'-aligned chunks (the final chunk may lack a
// trailing '\n' at EOF). Reads straight into the chunk buffer: no extra copy.
struct Reader {
  int fd;
  bool is_stdin;
  size_t chunk_bytes;
  ChunkQueue* q;
  std::atomic<bool>* ok;

  void Run() {
    std::string pending;  // bytes after the last '\n' (never contains '\n')
    for (;;) {
      RawChunk c;
      c.data = new (std::nothrow) char[pending.size() + chunk_bytes];
      if (!c.data) {
        fprintf(stderr, "tsort: out of memory (chunk)\n");
        ok->store(false);
        break;
      }
      memcpy(c.data, pending.data(), pending.size());
      ssize_t r = ::read(fd, c.data + pending.size(), chunk_bytes);
      if (r < 0) {
        if (errno == EINTR) { delete[] c.data; continue; }
        fprintf(stderr, "tsort: read error: %s\n", strerror(errno));
        ok->store(false);
        delete[] c.data;
        break;
      }
      if (r == 0) {  // EOF
        delete[] c.data;
        break;
      }
      size_t got = pending.size() + (size_t)r;
      size_t cut = 0;
      for (ssize_t i = (ssize_t)got - 1; i >= 0; i--) {
        if (c.data[i] == '\n') { cut = (size_t)i + 1; break; }
      }
      if (cut == 0) {  // a line longer than the chunk: keep accumulating
        pending.assign(c.data, got);
        delete[] c.data;
        if (pending.size() > (3u << 30)) {
          fprintf(stderr, "tsort: line exceeds the 3GB limit\n");
          ok->store(false);
          break;
        }
        continue;
      }
      pending.assign(c.data + cut, got - cut);
      c.size = cut;
      if (!q->Push(c)) { delete[] c.data; break; }
    }
    if (ok->load() && !pending.empty()) {  // final line without '\n'
      RawChunk c;
      c.size = pending.size();
      c.data = new (std::nothrow) char[c.size];
      if (c.data) {
        memcpy(c.data, pending.data(), c.size);
        if (!q->Push(c)) delete[] c.data;
      }
    }
    if (!is_stdin) ::close(fd);
    q->Close();
  }
};

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
    for (ssize_t i = (ssize_t)got - 1; i >= 0; i--)
      if (buf[i] == '\n') { cut = (size_t)i + 1; break; }
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
        uint64_t h = SampleOp::HashBytes(keep.p, keep.len);
        shards[(int)(h % (uint64_t)R)].Append(keep.p, keep.len);
        shards[(int)(h % (uint64_t)R)].Append("\n", 1);
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
      uint64_t h = SampleOp::HashBytes(keep.p, keep.len);
      shards[(int)(h % (uint64_t)R)].Append(keep.p, keep.len);
      shards[(int)(h % (uint64_t)R)].Append("\n", 1);
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
  Reader reader{fd, is_stdin, cfg.chunk_bytes, &q, &read_ok};

  // ---------------- Phase A: read -> ops -> chunk sort -> runs ----------------
  struct PhaseA {
    std::mutex mu;
    std::vector<SortedSource> sources;
    size_t mem_used = 0;
    size_t total_lines = 0;
    bool io_failed = false;
    std::atomic<int> run_seq{0};
  } pa;

  std::thread io_thread([&] { reader.Run(); });

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
  io_thread.join();
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
  Reader reader{fd, is_stdin, cfg.chunk_bytes, &q, &read_ok};
  std::thread io_thread([&] { reader.Run(); });

  // processed chunks land in an ordered hub
  struct Hub {
    std::mutex mu;
    std::condition_variable cv_w, cv_p;
    std::map<size_t, RawChunk> ready;
    size_t inflight = 0, inflight_cap;
    size_t next_pop = 0;
    bool failed = false;
    bool input_closed = false;
    explicit Hub(size_t cap) : inflight_cap(cap) {}
    void Submit(size_t seq, RawChunk c) {
      std::unique_lock<std::mutex> lk(mu);
      cv_p.wait(lk, [&] { return failed || inflight + c.size <= inflight_cap; });
      if (failed) { delete[] c.data; return; }
      inflight += c.size;
      ready[seq] = c;
      cv_w.notify_one();
    }
    bool PopNext(RawChunk* out) {  // in seq order; false at end of input
      std::unique_lock<std::mutex> lk(mu);
      cv_w.wait(lk, [&] {
        return failed || input_closed || ready.count(next_pop) > 0;
      });
      if (failed) return false;
      auto it = ready.find(next_pop);
      if (it == ready.end()) return false;  // closed and drained
      *out = it->second;
      ready.erase(it);
      next_pop++;
      return true;
    }
    void Release(size_t n) {
      {
        std::lock_guard<std::mutex> g(mu);
        inflight = inflight >= n ? inflight - n : 0;
      }
      cv_p.notify_all();
    }
    void Fail() {
      {
        std::lock_guard<std::mutex> g(mu);
        failed = true;
      }
      cv_w.notify_all();
      cv_p.notify_all();
    }
    void CloseInput() {
      {
        std::lock_guard<std::mutex> g(mu);
        input_closed = true;
      }
      cv_w.notify_all();
    }
  } hub(std::min<size_t>(cfg.mem_budget / 2, 1 << 30));

  std::atomic<bool> stop_flag(false);
  std::atomic<int> workers_left{cfg.threads};

  auto worker = [&] {
    ChainRunner runner;
    runner.chain = &pipe->pre;
    LineView v;
    for (;;) {
      RawChunk c;
      size_t seq = 0;
      if (!q.Pop(&c, &seq)) break;
      if (stop_flag.load(std::memory_order_relaxed)) {
        delete[] c.data;
        q.Release(c.size);
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
      if (!pc.data) { hub.Fail(); break; }
      if (pc.size) memcpy(pc.data, out.data(), pc.size);
      hub.Submit(seq, pc);
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
    io_thread.join();
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
  io_thread.join();
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
