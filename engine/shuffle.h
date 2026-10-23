// shuffle.h - unlimited-size deterministic shuffle.
//
// Pass 1 (parallel): every line is hashed to one of R shard files (per-worker
// buffers keep the writes large and sequential).
// Pass 2 (pipelined): shard i is loaded into RAM, Fisher-Yates shuffled with
// seed+i, and streamed to the output while shard i+1 is being shuffled.
// Memory: max shard size ~= total / R; R is chosen so this fits the budget.
#ifndef TS_SHUFFLE_H_
#define TS_SHUFFLE_H_

#include "core.h"
#include "ops.h"
#include "operators.h"  // SampleOp::HashBytes

#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <thread>
#include <mutex>
#include <atomic>
#include <cstdio>
#include <vector>
#include <string>
#include <random>

namespace ts {

namespace shuffle_detail {

struct ShardWriter {
  int fd = -1;
  std::vector<char> buf;
  size_t total = 0;
  // 4MB flush threshold: a fully-random shard hash (CRC) interleaves 32+
  // writers; larger blocks keep each write big and the destination buffer's
  // append tail cache-hot for longer runs.
  static constexpr size_t kFlush = 4 << 20;
  bool Open(const std::string& path) {
    fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    buf.reserve(kFlush + (2 << 20));
    return fd >= 0;
  }
  void Append(const char* p, size_t n) {
    buf.insert(buf.end(), p, p + n);
    if (buf.size() >= kFlush) Flush();
  }
  void Flush() {
    size_t w = 0;
    while (w < buf.size()) {
      ssize_t k = ::write(fd, buf.data() + w, buf.size() - w);
      if (k <= 0) { failed = true; break; }
      w += (size_t)k;
    }
    total += w;
    buf.clear();
  }
  bool failed = false;
  void Close() {
    if (fd >= 0) { Flush(); ::close(fd); fd = -1; }
  }
};

}  // namespace shuffle_detail

// Executed by the engine (pipeline.cc) - declared here, defined there.
int RunShufflePipeline(const std::string& input, const std::string& output,
                       struct PipelineDesc* pipe, const struct EngineConfig& cfg,
                       bool verbose, unsigned seed);

}  // namespace ts

#endif  // TS_SHUFFLE_H_
