// engine.h - v2 text pipeline engine.
#ifndef TS_ENGINE_H_
#define TS_ENGINE_H_

#include <string>
#include <atomic>

namespace ts {

struct EngineConfig {
  int threads = 0;             // 0 => hardware_concurrency
  size_t chunk_bytes = 64 << 20;
  size_t mem_budget = 0;       // bytes; 0 => 55% of RAM
  std::string tmpdir = "/tmp";
  bool force_external = false; // always spill sorted runs to disk
  // operators
  bool unique = false;         // sort -u
  long head_n = -1;            // head N lines after (unique) sort; -1 = off
  std::string grep;            // keep only lines containing this substring
};

// Full pipeline: read -> [grep] -> chunk sort -> (mem | disk runs)
//            -> sample-slice parallel merge -> [unique] [head] -> write.
// Returns 0 on success; prints timing to stderr when verbose.
int RunEngine(const std::string& input, const std::string& output,
              const EngineConfig& cfg, bool verbose);

}  // namespace ts

#endif  // TS_ENGINE_H_
