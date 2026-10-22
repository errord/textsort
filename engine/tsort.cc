// tsort.cc - CLI for the v3 text pipeline engine.
//
// Pipelines compose operator flags; order-dependent operators imply the sort
// barrier automatically. Semantics mirror the classic Unix tools:
//   tsort in out                          LC_ALL=C sort
//   tsort -u in out                       sort -u
//   tsort -c in out                       sort | uniq -c
//   tsort --freq 20 in out                sort | uniq -c | sort -k1,1rn | head -20
//   tsort --grep ERROR --head 100 in out  grep ERROR | sort | head -100
//   tsort --tail 50 in out                sort | tail -50
//   tsort --shuffle in out                shuf
//   tsort --grep ERROR in out             parallel grep (no sort)
//
// usage: tsort [operators] [engine options] input [output|-]
#include "pipeline.h"
#include "operators.h"
#include "postsort.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <memory>

static void Usage() {
  fprintf(stderr,
      "tsort v3 - composable text operators, unlimited data\n"
      "\n"
      "usage: tsort [operators] [engine] input [output|-]\n"
      "  input '-' = stdin, output '-' = stdout\n"
      "\n"
      "stream operators (fused before/after the barrier, in CLI order):\n"
      "  --grep SUB        keep lines containing SUB\n"
      "  --grepv SUB       drop lines containing SUB\n"
      "  --prefix SUB      keep lines starting with SUB\n"
      "  --suffix SUB      keep lines ending with SUB\n"
      "  --regex PAT       keep lines matching regex PAT (std::regex)\n"
      "  --replace A B     replace every occurrence of byte string A with B\n"
      "  --replace-first A B  replace only the first occurrence\n"
      "  --cut LIST        keep fields, e.g. '1,3-5' (cut -f)\n"
      "  --delim D         field delimiter for --cut/--key (default TAB)\n"
      "  --lower           fold A-Z to a-z\n"
      "  --upper           fold a-z to A-Z\n"
      "  --trim            strip leading/trailing blanks\n"
      "  --squeeze         (with --trim) collapse inner blank runs to one space\n"
      "  --sample RATE     keep ~RATE fraction (content-hash, deterministic)\n"
      "  --min-len N       keep lines with >= N bytes (token-budget curation)\n"
      "  --max-len N       keep lines with <= N bytes\n"
      "  --jsonl KEY       extract JSONL field KEY (missing key -> drop line)\n"
      "  --mask-email      mask user@host.tld addresses as [EMAIL]\n"
      "  --mask-ip         mask dotted-quad IPv4 addresses as [IP]\n"
      "  --shuffle         shuffle all lines (disk-based, unlimited size)\n"
      "  --seed N          shuffle seed (default 42)\n"
      "\n"
      "order operators (the default op is a whole-line byte sort):\n"
      "  --no-sort         skip the sort barrier: pure streaming pipeline\n"
      "  --key N           sort by the Nth --delim field (sort -k N,N)\n"
      "  --num             numeric order (with --key: field numeric)\n"
      "  --by-len          sort by line length\n"
      "  -u, --unique      drop duplicates (by the sort KEY under --key)\n"
      "  -c, --count       prefix count per distinct key (sort|uniq -c)\n"
      "  --dedup-key N     = --key N -u : dedupe by field, bounded memory\n"
      "  --freq N          top-N most frequent keys\n"
      "  -r, --reverse     descending order                (sort -r)\n"
      "  --head N          first N lines of the final output\n"
      "  --tail N          last N lines of the final output\n"
      "  --nl              number output lines (cat -n style)\n"
      "  --wc              emit one 'lines words bytes' summary line\n"
      "\n"
      "engine options:\n"
      "  --threads N       worker threads (default: all cores)\n"
      "  --chunk-mb M      read/sort chunk size in MB (default 64)\n"
      "  --mem-budget-mb M memory budget in MB (default: 50%% of RAM;\n"
      "                    larger inputs spill to disk automatically)\n"
      "  --tmpdir DIR      spill directory (default /tmp)\n"
      "  --external        force spilled runs + external merge\n"
      "  -q                quiet\n");
}

static char ParseDelim(const std::string& s) {
  if (s == "\\t") return '\t';
  if (s == "\\n") return '\n';
  if (s == "\\0") return '\0';
  return s.empty() ? '\t' : s[0];
}

int main(int argc, char** argv) {
  ts::PipelineDesc pipe;
  ts::EngineConfig cfg;
  bool verbose = true;
  std::string in, out;
  bool have_cut = false;
  char cut_delim = '\t';
  std::string cut_list;
  bool squeeze = false;
  long freq_n = -1;
  int key_field = 0;      // 0 = whole line
  bool numeric = false;
  bool by_len = false;
  bool need_nl = false, need_wc = false;
  int dedup_key = 0;

  for (int i = 1; i < argc; i++) {
    std::string a = argv[i];
    auto next = [&](const char* what) -> std::string {
      if (i + 1 >= argc) {
        fprintf(stderr, "tsort: %s needs a value\n", what);
        exit(2);
      }
      return argv[++i];
    };
    if (a == "-u" || a == "--unique") pipe.dedup = ts::DedupMode::Unique;
    else if (a == "-c" || a == "--count") pipe.dedup = ts::DedupMode::Count;
    else if (a == "--freq") freq_n = atol(next("--freq").c_str());
    else if (a == "-r" || a == "--reverse") pipe.reverse = true;
    else if (a == "--head") pipe.head_n = atol(next("--head").c_str());
    else if (a == "--tail") pipe.tail_n = atol(next("--tail").c_str());
    else if (a == "--key") key_field = atoi(next("--key").c_str());
    else if (a == "--num" || a == "--numeric") numeric = true;
    else if (a == "--by-len") by_len = true;
    else if (a == "--dedup-key") dedup_key = atoi(next("--dedup-key").c_str());
    else if (a == "--nl") need_nl = true;
    else if (a == "--wc") need_wc = true;
    else if (a == "--grep")
      pipe.pre.ops.push_back(std::make_unique<ts::GrepOp>(next("--grep")));
    else if (a == "--grepv")
      pipe.pre.ops.push_back(std::make_unique<ts::GrepVOp>(next("--grepv")));
    else if (a == "--prefix")
      pipe.pre.ops.push_back(std::make_unique<ts::PrefixOp>(next("--prefix")));
    else if (a == "--suffix")
      pipe.pre.ops.push_back(std::make_unique<ts::SuffixOp>(next("--suffix")));
    else if (a == "--regex")
      pipe.pre.ops.push_back(std::make_unique<ts::RegexOp>(next("--regex")));
    else if (a == "--replace" || a == "--replace-first") {
      std::string from = next("--replace");
      std::string to = next("--replace");
      pipe.pre.ops.push_back(
          std::make_unique<ts::ReplaceOp>(from, to, a == "--replace"));
    } else if (a == "--cut") {
      have_cut = true;
      cut_list = next("--cut");
    } else if (a == "--delim") {
      cut_delim = ParseDelim(next("--delim"));
    } else if (a == "--lower")
      pipe.pre.ops.push_back(std::make_unique<ts::LowerOp>());
    else if (a == "--upper")
      pipe.pre.ops.push_back(std::make_unique<ts::UpperOp>());
    else if (a == "--trim")
      pipe.pre.ops.push_back(std::make_unique<ts::TrimOp>(squeeze));
    else if (a == "--squeeze")
      squeeze = true;
    else if (a == "--sample")
      pipe.pre.ops.push_back(
          std::make_unique<ts::SampleOp>(atof(next("--sample").c_str())));
    else if (a == "--min-len")
      pipe.pre.ops.push_back(std::make_unique<ts::MinLenOp>(
          (uint32_t)atol(next("--min-len").c_str())));
    else if (a == "--max-len")
      pipe.pre.ops.push_back(std::make_unique<ts::MaxLenOp>(
          (uint32_t)atol(next("--max-len").c_str())));
    else if (a == "--jsonl")
      pipe.pre.ops.push_back(
          std::make_unique<ts::JsonlFieldOp>(next("--jsonl")));
    else if (a == "--mask-email")
      pipe.pre.ops.push_back(std::make_unique<ts::MaskEmailOp>());
    else if (a == "--mask-ip")
      pipe.pre.ops.push_back(std::make_unique<ts::MaskIpOp>());
    else if (a == "--shuffle") pipe.shuffle = true;
    else if (a == "--no-sort" || a == "--stream") pipe.no_sort = true;
    else if (a == "--seed") cfg.shuffle_seed = (unsigned)atol(next("--seed").c_str());
    else if (a == "--threads") cfg.threads = atoi(next("--threads").c_str());
    else if (a == "--chunk-mb")
      cfg.chunk_bytes = (size_t)atol(next("--chunk-mb").c_str()) << 20;
    else if (a == "--mem-budget-mb")
      cfg.mem_budget = (size_t)atol(next("--mem-budget-mb").c_str()) << 20;
    else if (a == "--tmpdir") cfg.tmpdir = next("--tmpdir");
    else if (a == "--external") cfg.force_external = true;
    else if (a == "-q") verbose = false;
    else if (a == "-h" || a == "--help") { Usage(); return 0; }
    else if (a == "-") {  // explicit stdin/stdout marker
      if (in.empty()) in = a;
      else if (out.empty()) out = a;
      else { Usage(); return 2; }
    }
    else if (!a.empty() && a[0] == '-') {
      fprintf(stderr, "tsort: unknown option %s\n", a.c_str());
      Usage();
      return 2;
    } else if (in.empty()) in = a;
    else if (out.empty()) out = a;
    else {
      Usage();
      return 2;
    }
  }

  if (in.empty() || out.empty()) { Usage(); return 2; }

  // --squeeze rewrites a pending trim op
  if (squeeze) {
    for (auto& op : pipe.pre.ops)
      if (strcmp(op->Name(), "trim") == 0) static_cast<ts::TrimOp*>(op.get())->squeeze = true;
  }
  if (have_cut)
    pipe.pre.ops.push_back(std::make_unique<ts::CutOp>(cut_delim, cut_list));

  // ---- sort key ----
  if (by_len && (key_field > 0 || numeric || dedup_key > 0)) {
    fprintf(stderr, "tsort: --by-len cannot combine with --key/--num/--dedup-key\n");
    return 2;
  }
  if (key_field > 0 && dedup_key > 0 && key_field != dedup_key) {
    fprintf(stderr, "tsort: --key and --dedup-key disagree on the field\n");
    return 2;
  }
  if (by_len) {
    pipe.sort_key.mode = ts::SortKey::Length;
  } else if (numeric) {
    pipe.sort_key.mode = ts::SortKey::Numeric;
    if (key_field > 0 || dedup_key > 0) {
      pipe.sort_key.field = key_field > 0 ? key_field : dedup_key;
      pipe.sort_key.delim = cut_delim;
    }
  } else if (key_field > 0 || dedup_key > 0) {
    pipe.sort_key.mode = ts::SortKey::Field;
    pipe.sort_key.field = key_field > 0 ? key_field : dedup_key;
    pipe.sort_key.delim = cut_delim;
  }
  if (dedup_key > 0) {
    if (pipe.dedup != ts::DedupMode::None) {
      fprintf(stderr, "tsort: --dedup-key cannot combine with -u/-c\n");
      return 2;
    }
    pipe.dedup = ts::DedupMode::Unique;  // equality via the sort key
  }

  // ---- semantic planning ----
  // tsort sorts by default (the tool's name is its contract); --no-sort
  // selects the pure streaming mode (parallel grep/sed-style pipelines).
  if (pipe.shuffle) {
    if (pipe.dedup != ts::DedupMode::None || pipe.reverse || freq_n > 0 ||
        pipe.tail_n > 0 || need_nl || need_wc ||
        pipe.sort_key.mode != ts::SortKey::Whole) {
      fprintf(stderr,
              "tsort: --shuffle cannot combine with order operators\n");
      return 2;
    }
    pipe.need_sort = false;  // shuffle replaces the barrier entirely
  } else {
    pipe.need_sort = !pipe.no_sort;
    if (pipe.dedup != ts::DedupMode::None || pipe.reverse || freq_n > 0)
      pipe.need_sort = true;
    if (pipe.no_sort) {
      if (pipe.dedup != ts::DedupMode::None || pipe.reverse || freq_n > 0 ||
          pipe.tail_n > 0 || key_field > 0 || dedup_key > 0 || numeric ||
          by_len) {
        fprintf(stderr,
                "tsort: --no-sort cannot combine with -u/-c/-r/--freq/--tail/"
                "--key/--num/--by-len (they need order)\n");
        return 2;
      }
    }
  }
  if (pipe.tail_n > 0) {
    if (pipe.reverse) {
      fprintf(stderr, "tsort: --tail and --reverse conflict\n");
      return 2;
    }
    if (pipe.head_n >= 0) {
      fprintf(stderr, "tsort: --tail and --head conflict\n");
      return 2;
    }
    pipe.need_sort = true;
    pipe.reverse = true;  // engine sorts descending; writer reverses at flush
  }
  if (freq_n > 0) {
    if (pipe.dedup != ts::DedupMode::None) {
      fprintf(stderr, "tsort: --freq cannot combine with -u/-c\n");
      return 2;
    }
    if (pipe.tail_n > 0) {
      fprintf(stderr, "tsort: --freq cannot combine with --tail\n");
      return 2;
    }
    if (need_nl || need_wc) {
      fprintf(stderr, "tsort: --freq cannot combine with --nl/--wc\n");
      return 2;
    }
    pipe.need_sort = true;
    pipe.post.ops.push_back(
        std::make_unique<ts::FreqPost>((size_t)freq_n, pipe.sort_key));
  }
  if (need_nl) pipe.post.ops.push_back(std::make_unique<ts::NlPost>());
  if (need_wc) pipe.post.ops.push_back(std::make_unique<ts::WcPost>());
  if (pipe.shuffle) {
    if (pipe.need_sort || pipe.tail_n > 0) {
      fprintf(stderr, "tsort: --shuffle cannot combine with order operators\n");
      return 2;
    }
    if (freq_n > 0) {
      fprintf(stderr, "tsort: --freq needs a deterministic order\n");
      return 2;
    }
  }
  if (pipe.tail_n > 0 && !pipe.need_sort) pipe.need_sort = true;

  if (verbose) {
    fprintf(stderr, "[tsort] plan:");
    if (pipe.shuffle) fprintf(stderr, " shuffle");
    if (!pipe.pre.Empty()) {
      fprintf(stderr, " pre[");
      for (auto& op : pipe.pre.ops) fprintf(stderr, "%s ", op->Name());
      fprintf(stderr, "]");
    }
    if (pipe.need_sort) {
      const char* dir = pipe.reverse || pipe.tail_n > 0 ? "desc" : "asc";
      const char* kn = "line";
      if (pipe.sort_key.mode == ts::SortKey::Field) kn = "field";
      else if (pipe.sort_key.mode == ts::SortKey::Numeric) kn = "num";
      else if (pipe.sort_key.mode == ts::SortKey::Length) kn = "len";
      fprintf(stderr, " sort(%s by %s)", dir, kn);
    }
    if (pipe.dedup == ts::DedupMode::Unique) fprintf(stderr, " unique");
    if (pipe.dedup == ts::DedupMode::Count) fprintf(stderr, " count");
    if (!pipe.post.Empty())
      for (auto& op : pipe.post.ops) fprintf(stderr, " %s", op->Name());
    if (pipe.head_n >= 0) fprintf(stderr, " head=%ld", pipe.head_n);
    if (pipe.tail_n > 0) fprintf(stderr, " tail=%ld", pipe.tail_n);
    fprintf(stderr, "\n");
  }

  return ts::RunPipeline(in, out, &pipe, cfg, verbose);
}
