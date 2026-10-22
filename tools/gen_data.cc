// tools/gen_data.cc - fast generator of LLM-ish unstructured text corpora.
//
// Mixed line shapes (short tokens, URLs, log lines, long paragraphs), with a
// controllable duplicate ratio - the workload text tools actually face.
//
// usage: gen_data OUT SIZE_MB [DUP_PCT] [SEED]
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <string>
#include <vector>

static uint64_t rng_state = 0x9E3779B97F4A7C15ULL;
static inline uint64_t Rand64() {
  uint64_t x = rng_state;
  x ^= x << 13; x ^= x >> 7; x ^= x << 17;
  rng_state = x;
  return x;
}
static inline uint32_t RandBelow(uint32_t n) { return (uint32_t)(Rand64() % n); }

static const char* kWords[] = {
    "the","of","and","to","in","is","you","that","it","he","was","for","on",
    "are","as","with","his","they","I","at","be","this","have","from","or",
    "one","had","by","word","but","not","what","all","were","we","when","your",
    "can","said","there","use","an","each","which","she","do","how","their",
    "if","will","up","other","about","out","many","then","them","these","so",
    "some","her","would","make","like","him","into","time","has","look","two",
    "more","write","go","see","number","no","way","could","people","my","than",
    "first","water","been","call","who","oil","its","now","find","long","down",
    "day","did","get","come","made","may","part","model","token","vector",
    "transformer","attention","embedding","inference","dataset","training",
    "prompt","context","memory","parameter","gradient","loss","epoch","batch",
    "shard","pipeline","throughput","latency","cache","kernel","tensor"};
static const int kNumWords = sizeof(kWords) / sizeof(kWords[0]);

static const char* kTld[] = {"com","io","ai","org","net","dev","co","app"};

static void GenUrl(std::string* s) {
  static const char* hosts[] = {"github","huggingface","arxiv","stackoverflow",
                                "reddit","medium","wikipedia","openai"};
  s->clear();
  s->append("https://");
  s->append(hosts[RandBelow(8)]);
  s->append(".com/");
  int segs = 1 + (int)RandBelow(4);
  for (int i = 0; i < segs; i++) {
    if (i) s->push_back('/');
    int wl = 3 + (int)RandBelow(10);
    for (int j = 0; j < wl; j++) s->push_back('a' + (char)RandBelow(26));
  }
  s->push_back('.');
  s->append(kTld[RandBelow(8)]);
}

static void GenWords(std::string* s, int n) {
  s->clear();
  for (int i = 0; i < n; i++) {
    if (i) s->push_back(' ');
    s->append(kWords[RandBelow(kNumWords)]);
  }
}

static void GenLine(std::string* s) {
  uint32_t r = RandBelow(100);
  if (r < 15) {           // URL line
    GenUrl(s);
  } else if (r < 35) {    // token/id line (short)
    char buf[64];
    snprintf(buf, sizeof(buf), "id=%llu tok=%llu",
             (unsigned long long)Rand64(), (unsigned long long)(Rand64() & 0xFFFFFF));
    *s = buf;
  } else if (r < 55) {    // log line
    char buf[256];
    snprintf(buf, sizeof(buf),
             "2026-10-%02d %02d:%02d:%02d lvl=%c service=%s %s",
             1 + (int)RandBelow(28), (int)RandBelow(24), (int)RandBelow(60),
             (int)RandBelow(60), "IDWE"[RandBelow(4)],
             kWords[RandBelow(kNumWords)], kWords[RandBelow(kNumWords)]);
    *s = buf;
    std::string tail;
    GenWords(&tail, 3 + (int)RandBelow(8));
    s->append(" ");
    s->append(tail);
  } else {                // paragraph-ish (long)
    GenWords(s, 40 + (int)RandBelow(120));
  }
}

int main(int argc, char** argv) {
  if (argc < 3) {
    fprintf(stderr, "usage: gen_data OUT SIZE_MB [DUP_PCT=10] [SEED=1]\n");
    return 2;
  }
  const char* out_path = argv[1];
  size_t target = (size_t)atoll(argv[2]) << 20;
  int dup_pct = argc > 3 ? atoi(argv[3]) : 10;
  if (argc > 4) rng_state ^= (uint64_t)atoll(argv[4]) * 0x2545F4914F6CDD1DULL + 1;

  FILE* f = fopen(out_path, "wb");
  if (!f) { perror("fopen"); return 1; }

  std::vector<std::string> dups;
  dups.reserve(4096);
  std::string line, tmp;
  size_t written = 0;
  std::vector<char> buf;
  buf.reserve(16 << 20);
  while (written < target) {
    if (!dups.empty() && (int)RandBelow(100) < dup_pct) {
      line = dups[RandBelow((uint32_t)dups.size())];
    } else {
      GenLine(&line);
      if (RandBelow(64) == 0) {  // remember some lines as future duplicates
        dups.push_back(line);
        if (dups.size() > 100000) dups[RandBelow((uint32_t)dups.size())] = line;
      }
    }
    line.push_back('\n');
    buf.insert(buf.end(), line.begin(), line.end());
    written += line.size();
    if (buf.size() >= (15 << 20)) {
      fwrite(buf.data(), 1, buf.size(), f);
      buf.clear();
    }
  }
  fwrite(buf.data(), 1, buf.size(), f);
  fclose(f);
  fprintf(stderr, "gen_data: %s %.1fMB dup=%d%% lines≈%.1fM\n", out_path,
          written / 1e6, dup_pct, written / 110.0 / 1e6);
  return 0;
}
