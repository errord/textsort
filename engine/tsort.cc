// tsort.cc - CLI for the v2 text pipeline engine.
//
// usage: tsort [options] input output
//   --unique            drop duplicate lines (sort -u)
//   --head N            keep only the first N lines after sorting
//   --grep SUBSTRING    keep only lines containing SUBSTRING (pre-sort filter)
//   --external          force spilled runs + external merge (test the spill path)
//   --threads N         worker threads (default: all cores)
//   --chunk-mb M        read/sort chunk size in MB (default 64)
//   --tmpdir DIR        directory for spilled runs (default /tmp)
//   -q                  quiet (no timing output)
#include "engine.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

static void Usage() {
  fprintf(stderr,
      "usage: tsort [options] input output\n"
      "  --unique          drop duplicate lines (sort -u)\n"
      "  --head N          keep first N lines after sort\n"
      "  --grep SUBSTRING  keep lines containing SUBSTRING before sort\n"
      "  --external        force external (spilled runs) merge path\n"
      "  --threads N       worker threads (default: all cores)\n"
      "  --chunk-mb M      chunk size in MB (default 64)\n"
      "  --tmpdir DIR      spill directory (default /tmp)\n"
      "  -q                quiet\n");
}

int main(int argc, char** argv) {
  ts::EngineConfig cfg;
  bool verbose = true;
  std::string in, out;
  for (int i = 1; i < argc; i++) {
    std::string a = argv[i];
    if (a == "--unique") cfg.unique = true;
    else if (a == "--external") cfg.force_external = true;
    else if (a == "-q") verbose = false;
    else if (a == "--grep" && i + 1 < argc) cfg.grep = argv[++i];
    else if (a == "--head" && i + 1 < argc) cfg.head_n = atol(argv[++i]);
    else if (a == "--threads" && i + 1 < argc) cfg.threads = atoi(argv[++i]);
    else if (a == "--chunk-mb" && i + 1 < argc) cfg.chunk_bytes = (size_t)atol(argv[++i]) << 20;
    else if (a == "--tmpdir" && i + 1 < argc) cfg.tmpdir = argv[++i];
    else if (a == "-h" || a == "--help") { Usage(); return 0; }
    else if (!a.empty() && a[0] == '-') { fprintf(stderr, "unknown option %s\n", a.c_str()); Usage(); return 2; }
    else if (in.empty()) in = a;
    else if (out.empty()) out = a;
    else { Usage(); return 2; }
  }
  if (in.empty() || out.empty()) { Usage(); return 2; }
  if (cfg.head_n == 0) cfg.head_n = -1;
  return ts::RunEngine(in, out, cfg, verbose);
}
