// engine.cc - pipeline orchestration for the v2 engine.
#include "engine.h"

#include "core.h"
#include "chunk_sort.h"
#include "run_store.h"
#include "merge.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <set>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <algorithm>

namespace ts {

namespace {

// ---------------- shared queues ----------------
struct RawChunk {
  char* data = nullptr;
  size_t size = 0;
};

struct Queue {
  std::mutex mu;
  std::condition_variable cv;
  std::vector<RawChunk> items;
  bool closed = false;
  void Push(RawChunk c) {
    {
      std::lock_guard<std::mutex> g(mu);
      items.push_back(c);
    }
    cv.notify_one();
  }
  bool Pop(RawChunk* out) {
    std::unique_lock<std::mutex> lk(mu);
    cv.wait(lk, [&] { return !items.empty() || closed; });
    if (items.empty()) return false;
    *out = items.front();
    items.erase(items.begin());
    return true;
  }
  void Close() {
    {
      std::lock_guard<std::mutex> g(mu);
      closed = true;
    }
    cv.notify_all();
  }
};

struct GrepFilter : LineFilter {
  std::string needle;
  GrepFilter() = default;
  explicit GrepFilter(const std::string& n) : needle(n) {}
  bool Keep(const char* p, uint32_t len) override {
    return memmem(p, len, needle.data(), needle.size()) != nullptr;
  }
};

size_t PhysicalMemBytes() {
  long pages = sysconf(_SC_PHYS_PAGES);
  long page = sysconf(_SC_PAGE_SIZE);
  return pages > 0 && page > 0 ? (size_t)pages * (size_t)page : (size_t)8 << 30;
}

struct PhaseA {
  std::mutex mu;
  std::condition_variable cv;
  size_t done = 0;         // chunks fully processed
  size_t total = 0;        // set when input is exhausted
  std::vector<SortedSource> sources;  // appended by workers (guarded by mu)
};

// Counting turn gate: slices start only inside the sliding window
// [written_idx, written_idx + window). This guarantees the slice the writer
// is waiting for is always inside the window -> no permit inversion deadlock.
struct TurnGate {
  std::mutex mu;
  std::condition_variable cv;
  int next_turn;   // slices with index < next_turn may start
  int limit;       // cap == number of slices
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

}  // namespace

int RunEngine(const std::string& input, const std::string& output,
              const EngineConfig& cfg_in, bool verbose) {
  EngineConfig cfg = cfg_in;
  if (cfg.threads <= 0) {
    unsigned h = std::thread::hardware_concurrency();
    cfg.threads = h ? (int)h : 4;
  }
  if (cfg.mem_budget == 0) cfg.mem_budget = PhysicalMemBytes() * 55 / 100;
  cfg.chunk_bytes = std::min(cfg.chunk_bytes, (size_t)3 << 30);  // uint32 offsets

  double t_start = NowSec();

  // ---- decide memory vs external mode ----
  struct stat st;
  if (::stat(input.c_str(), &st) != 0) {
    fprintf(stderr, "tsort: cannot stat input %s\n", input.c_str());
    return 1;
  }
  uint64_t fsize = (uint64_t)st.st_size;
  // in-mem needs: packed runs (~fsize) + output slices (~fsize) + slack
  bool external = cfg.force_external || (fsize * 2 + (fsize >> 2)) > cfg.mem_budget;
  if (verbose) fprintf(stderr, "[engine] mode=%s input=%.2fGB budget=%.1fGB threads=%d\n",
                       external ? "external" : "memory", fsize / 1e9,
                       cfg.mem_budget / 1e9, cfg.threads);

  GrepFilter grep;
  if (!cfg.grep.empty()) grep.needle = cfg.grep;
  LineFilter* filter = cfg.grep.empty() ? nullptr : (LineFilter*)&grep;

  PhaseA pa;

  // ---------------- Phase A: read -> sort -> (keep | spill) ----------------
  Queue chunk_q;
  std::thread io_thread([&] {
    int fd = ::open(input.c_str(), O_RDONLY);
    if (fd < 0) {
      fprintf(stderr, "tsort: cannot open input %s\n", input.c_str());
      chunk_q.Close();
      return;
    }
    ::posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
    std::string pending;  // bytes after the last '\n' of the previous read
    std::vector<char> buf(cfg.chunk_bytes);
    for (;;) {
      ssize_t r = ::read(fd, buf.data(), buf.size());
      if (r <= 0) break;
      size_t cut = 0;
      for (ssize_t i = r - 1; i >= 0; i--) {
        if (buf[i] == '\n') { cut = (size_t)(i + 1); break; }
      }
      if (cut == 0) {  // a line longer than the chunk: extend and read more
        pending.append(buf.data(), (size_t)r);
        continue;
      }
      RawChunk c;
      c.size = pending.size() + cut;
      c.data = new char[c.size];
      memcpy(c.data, pending.data(), pending.size());
      memcpy(c.data + pending.size(), buf.data(), cut);
      pending.assign(buf.data() + cut, (size_t)r - cut);
      chunk_q.Push(c);
    }
    if (!pending.empty()) {  // final line without trailing '\n'
      RawChunk c;
      c.size = pending.size();
      c.data = new char[c.size];
      memcpy(c.data, pending.data(), c.size);
      chunk_q.Push(c);
    }
    ::close(fd);
  });

  std::atomic<int> run_seq{0};
  auto worker = [&] {
    for (;;) {
      RawChunk c;
      if (!chunk_q.Pop(&c)) break;
      SortedMemRun run = SortChunk(c.data, c.size, filter);
      delete[] c.data;
      {
        std::lock_guard<std::mutex> g(pa.mu);
        if (external) {
          char path[512];
          snprintf(path, sizeof(path), "%s/tsrun_%d_%d.bin",
                   cfg.tmpdir.c_str(), (int)getpid(), run_seq.fetch_add(1));
          SortedSource s;
          s.is_mem = false;
          if (!run.count) {
            // keep empty runs as trivial mem sources (no file needed)
            s.is_mem = true;
          } else if (RunFile::Write(path, run, &s.file)) {
          } else {
            fprintf(stderr, "tsort: failed to write run %s\n", path);
          }
          pa.sources.push_back(std::move(s));  // run.packed freed on move/destroy
        } else {
          SortedSource s;
          s.is_mem = true;
          s.mem = std::move(run);
          pa.sources.push_back(std::move(s));
        }
        pa.done++;
      }
      pa.cv.notify_one();
    }
  };

  std::vector<std::thread> workers;
  workers.reserve(cfg.threads);
  for (int i = 0; i < cfg.threads; i++) workers.emplace_back(worker);

  io_thread.join();
  chunk_q.Close();
  for (auto& w : workers) w.join();
  double t_sorted = NowSec();
  if (verbose) fprintf(stderr, "[engine] phase A (read+sort+compact%s) %.2fs, runs=%zu\n",
                       external ? "+spill" : "", t_sorted - t_start, pa.sources.size());

  // ---------------- Phase B: sample anchors ----------------
  std::vector<SortedSource>& sources = pa.sources;
  size_t total_lines = 0;
  for (auto& s : sources) total_lines += s.Count();
  if (total_lines == 0) {  // empty input: still create the output file
    int fd = ::open(output.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd >= 0) ::close(fd);
    return 0;
  }

  const int P = std::max(1, std::min<int>(cfg.threads, 128));
  std::vector<std::string> anchors;  // P-1 anchor lines
  if (P > 1) {
    std::vector<std::string> samples;
    const int per_src = 32;
    for (auto& s : sources) {
      if (s.Count() == 0) continue;
      auto& sp = s.is_mem ? s.mem.sparse : s.file.sparse;
      if (sp.empty()) continue;
      size_t stride = std::max<size_t>(1, sp.size() / per_src);
      for (size_t j = 0; j < sp.size(); j += stride) {
        uint32_t len = 0;
        char small[8192];
        bool got = false;
        std::string big;
        if (s.is_mem) {
          memcpy(&len, s.mem.packed.data() + sp[j].off, 4);
          const char* lp = s.mem.packed.data() + sp[j].off + 4;
          if (len <= sizeof(small)) { memcpy(small, lp, len); got = true; }
          else { big.assign(lp, len); got = true; }
        } else {
          int fd = ::open(s.file.path.c_str(), O_RDONLY);
          if (fd >= 0) {
            uint32_t l4;
            if (::pread(fd, &l4, 4, (off_t)sp[j].off) == 4) {
              len = l4;
              char* dst = len <= sizeof(small) ? small : (big.resize(len), big.data());
              got = ::pread(fd, dst, len, (off_t)(sp[j].off + 4)) == (ssize_t)len;
            }
            ::close(fd);
          }
        }
        if (got) {
          if (len <= sizeof(small)) samples.emplace_back(small, len);
          else samples.push_back(big);
        }
      }
    }
    std::sort(samples.begin(), samples.end(), [](const std::string& a, const std::string& b) {
      return FullLess(LineView{a.data(), (uint32_t)a.size()},
                      LineView{b.data(), (uint32_t)b.size()});
    });
    size_t want = (size_t)P - 1;
    if (!samples.empty() && want > 0) {
      for (size_t i = 0; i < want; i++) {
        const std::string& a = samples[(size_t)((double)(i + 1) * samples.size() / (want + 1))];
        if (anchors.empty() || anchors.back() != a) anchors.push_back(a);
      }
    }
  }

  // ---------------- Phase B: parallel slice merge ----------------
  const int slices = (int)anchors.size() + 1;
  std::vector<SliceResult> results(slices);
  std::vector<std::atomic<bool>> slice_done(slices);
  for (auto& d : slice_done) d = false;
  std::mutex done_mu;
  std::condition_variable done_cv;
  std::set<int> done_set;

  // head_n limits total output; small enough => merge with single slice usage
  size_t global_head = cfg.head_n > 0 ? (size_t)cfg.head_n : 0;

  const int window = external ? 2 : slices;
  TurnGate gate(window, slices);

  std::vector<std::thread> mergers;
  for (int i = 0; i < slices; i++) {
    if (global_head && (size_t)i >= global_head) {  // far beyond head: skip
      slice_done[i] = true;
      {
        std::lock_guard<std::mutex> g(done_mu);
        done_set.insert(i);
      }
      done_cv.notify_one();
      continue;
    }
    mergers.emplace_back([&, i] {
      LineView lo{nullptr, 0}, hi{nullptr, 0};
      if (i > 0) lo = LineView{anchors[i - 1].data(), (uint32_t)anchors[i - 1].size()};
      if (i + 1 < slices) hi = LineView{anchors[i].data(), (uint32_t)anchors[i].size()};
      gate.WaitTurn(i);  // start only when inside the writer's sliding window
      // cross-slice unique: skip a leading line equal to the previous slice's
      // last emitted line (fetched after that slice finished)
      const std::string* skip = nullptr;
      if (cfg.unique && i > 0) {
        std::unique_lock<std::mutex> lk(done_mu);
        done_cv.wait(lk, [&] { return slice_done[i - 1].load(); });
        skip = &results[i - 1].last;
        if (skip->empty()) skip = nullptr;
      }
      results[i] = MergeSlice(&sources, lo, hi, cfg.unique, skip, global_head);
      slice_done[i] = true;
      {
        std::lock_guard<std::mutex> g(done_mu);
        done_set.insert(i);
      }
      done_cv.notify_all();
      // window slides forward when the writer flushes this slice
    });
  }

  // ---------------- output writer (ordered) ----------------
  int out_fd = ::open(output.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (out_fd < 0) {
    fprintf(stderr, "tsort: cannot open output %s\n", output.c_str());
    return 1;
  }
  size_t written_lines = 0, written_bytes = 0;
  std::string prev_last;
  for (int i = 0; i < slices; i++) {
    std::unique_lock<std::mutex> lk(done_mu);
    done_cv.wait(lk, [&] { return done_set.count(i) > 0; });
    lk.unlock();

    SliceResult& s = results[i];
    const char* p = s.out.data();
    size_t n = s.out.size();
    // cross-slice duplicate (only possible once, at the head of the slice)
    if (cfg.unique && !prev_last.empty() && !s.first.empty() &&
        s.first.size() == prev_last.size() &&
        memcmp(s.first.data(), prev_last.data(), s.first.size()) == 0) {
      p += s.first.size() + 1;
      n -= s.first.size() + 1;
      s.count--;
    }
    if (global_head) {
      size_t cap = global_head > written_lines ? global_head - written_lines : 0;
      if (s.count > cap) {
        // truncate to cap lines
        size_t keep = 0, cnt = 0;
        while (keep < n && cnt < cap) {
          const char* nl = (const char*)memchr(p + keep, '\n', n - keep);
          keep = (size_t)(nl - p) + 1;
          cnt++;
        }
        n = keep;
        s.count = cnt;
      }
    }
    size_t off = 0;
    while (off < n) {
      ssize_t w = ::write(out_fd, p + off, n - off);
      if (w <= 0) { fprintf(stderr, "tsort: write error\n"); return 1; }
      off += (size_t)w;
    }
    written_lines += s.count;
    written_bytes += n;
    if (!s.last.empty()) prev_last = s.last;
    results[i].out.clear();
    results[i].out.shrink_to_fit();
    gate.AdvanceAfter(i, window);  // let a new slice enter the window
  }
  ::close(out_fd);
  for (auto& t : mergers) t.join();
  double t_done = NowSec();
  if (verbose)
    fprintf(stderr, "[engine] phase B (merge+write) %.2fs | total %.2fs | lines=%zu out=%.2fGB\n",
            t_done - t_sorted, t_done - t_start, written_lines, written_bytes / 1e9);

  if (external) {
    for (auto& s : sources)
      if (!s.is_mem) ::unlink(s.file.path.c_str());
  }
  return 0;
}

}  // namespace ts
