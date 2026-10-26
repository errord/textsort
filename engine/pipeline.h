// pipeline.h - v3 engine: physical execution planning.
//
// RunPipeline turns a PipelineDesc (operator chain) into one of:
//   * pure streaming   - no sort barrier: parallel chunked map/filter,
//                        ordered streaming output
//   * sort             - chunk sort (in pipeline order) -> memory runs under
//                        the budget + spilled run files -> optional multi-pass
//                        fan-in-limited pre-merges -> sample-slice parallel
//                        final merge -> bounded streaming ordered output
//   * shuffle          - hash shard to disk (parallel), shuffle each shard in
//                        RAM, stream out in shard order
// Every path bounds its memory: input size is unlimited.
#ifndef BT_PIPELINE_H_
#define BT_PIPELINE_H_

#include "ops.h"
#include <string>
#include <atomic>

namespace bt {

struct EngineConfig {
  int threads = 0;               // 0 => hardware_concurrency
  size_t chunk_bytes = 64 << 20; // read/sort chunk size
  size_t mem_budget = 0;         // bytes; 0 => 50% of physical RAM
  std::string tmpdir = "/tmp";
  bool force_external = false;   // always spill sorted runs
  unsigned shuffle_seed = 42;
};

// Executes the pipeline; returns 0 on success. Timing + stats go to stderr
// when verbose.
int RunPipeline(const std::string& input, const std::string& output,
                PipelineDesc* pipe, const EngineConfig& cfg, bool verbose);

}  // namespace ts

#endif  // BT_PIPELINE_H_
