// operators.h - built-in streaming line operators.
//
// All operators are STATELESS and thread-safe (scratch is passed in), so a
// single chain is shared across worker threads. Sampling uses a content hash
// (FNV-1a) so results are deterministic across runs and threads.
//
// Semantics deliberately mirror the classic Unix text tools so pipelines can
// be cross-checked byte-for-byte:  grep / grep -v / sed s///g / cut -d -f /
// tr a-z A-Z / awk trim.
#ifndef TS_OPERATORS_H_
#define TS_OPERATORS_H_

#include "ops.h"
#include "simd.h"
#include <string>
#include <regex>

namespace ts {

inline const void* FindBytes(const char* hay, size_t hlen,
                             const char* needle, size_t nlen) {
#if defined(__APPLE__) || defined(__linux__)
  return memmem(hay, hlen, needle, nlen);
#else
  if (nlen == 0) return hay;
  if (hlen < nlen) return nullptr;
  const char* last = hay + hlen - nlen;
  for (const char* p = hay; p <= last; p++)
    if (p[0] == needle[0] && memcmp(p, needle, nlen) == 0) return p;
  return nullptr;
#endif
}

// ---------------------------------------------------------------------------
// grep / grep -v (substring), prefix / suffix
// (AVX2 byte kernels now live in simd.h: VecAnyInRange / VecFold /
//  VecFindInRange / VecCountWordStarts / ShardHash)
// ---------------------------------------------------------------------------
struct GrepOp : LineOp {
  std::string needle;
  explicit GrepOp(const std::string& n) : needle(n) {}
  OpResult Process(LineView* in, std::vector<char>*) override {
    return FindBytes(in->p, in->len, needle.data(), needle.size())
               ? OpResult::Inplace
               : OpResult::Drop;
  }
  bool PureFilter() const override { return true; }
  const char* Name() const override { return "grep"; }
};

struct GrepVOp : LineOp {
  std::string needle;
  explicit GrepVOp(const std::string& n) : needle(n) {}
  OpResult Process(LineView* in, std::vector<char>*) override {
    return FindBytes(in->p, in->len, needle.data(), needle.size())
               ? OpResult::Drop
               : OpResult::Inplace;
  }
  bool PureFilter() const override { return true; }
  const char* Name() const override { return "grepv"; }
};

struct PrefixOp : LineOp {
  std::string needle;
  explicit PrefixOp(const std::string& n) : needle(n) {}
  OpResult Process(LineView* in, std::vector<char>*) override {
    return in->len >= needle.size() &&
                   memcmp(in->p, needle.data(), needle.size()) == 0
               ? OpResult::Inplace
               : OpResult::Drop;
  }
  bool PureFilter() const override { return true; }
  const char* Name() const override { return "prefix"; }
};

struct SuffixOp : LineOp {
  std::string needle;
  explicit SuffixOp(const std::string& n) : needle(n) {}
  OpResult Process(LineView* in, std::vector<char>*) override {
    return in->len >= needle.size() &&
                   memcmp(in->p + in->len - needle.size(), needle.data(),
                          needle.size()) == 0
               ? OpResult::Inplace
               : OpResult::Drop;
  }
  bool PureFilter() const override { return true; }
  const char* Name() const override { return "suffix"; }
};

struct RegexOp : LineOp {
  std::regex re;
  explicit RegexOp(const std::string& pattern)
      : re(pattern, std::regex::optimize) {}
  OpResult Process(LineView* in, std::vector<char>*) override {
    thread_local std::string tmp;  // regex needs a string range
    tmp.assign(in->p, in->len);
    return std::regex_search(tmp, re) ? OpResult::Inplace : OpResult::Drop;
  }
  bool PureFilter() const override { return true; }
  const char* Name() const override { return "regex"; }
};

// ---------------------------------------------------------------------------
// sed-style byte replacement: --replace FROM TO [--first]
// ---------------------------------------------------------------------------
struct ReplaceOp : LineOp {
  std::string from, to;
  bool all = true;
  ReplaceOp(const std::string& f, const std::string& t, bool replace_all)
      : from(f), to(t), all(replace_all) {}
  OpResult Process(LineView* in, std::vector<char>* out) override {
    if (from.empty() ||
        !FindBytes(in->p, in->len, from.data(), from.size()))
      return OpResult::Inplace;  // no hit: zero copy
    out->clear();
    const char* p = in->p;
    size_t left = in->len;
    for (;;) {
      const char* h =
          (const char*)FindBytes(p, left, from.data(), from.size());
      if (!h) {
        out->insert(out->end(), p, p + left);
        break;
      }
      out->insert(out->end(), p, h);
      out->insert(out->end(), to.data(), to.data() + to.size());
      p = h + from.size();
      left = in->len - (size_t)(p - in->p);
      if (!all) {
        out->insert(out->end(), p, in->p + in->len);
        break;
      }
    }
    return OpResult::Materialized;
  }
  const char* Name() const override { return "replace"; }
};

// ---------------------------------------------------------------------------
// cut -d DELIM -f LIST   (fields are 1-based; "2,4-6"; output joined by DELIM)
// ---------------------------------------------------------------------------
struct CutOp : LineOp {
  char delim = '\t';
  struct Range { int lo, hi; };
  std::vector<Range> ranges;

  CutOp(char d, const std::string& list) : delim(d) {
    size_t i = 0;
    while (i <= list.size()) {
      size_t comma = list.find(',', i);
      std::string part = list.substr(i, comma == std::string::npos
                                            ? std::string::npos
                                            : comma - i);
      size_t dash = part.find('-');
      if (dash == std::string::npos) {
        int v = atoi(part.c_str());
        if (v > 0) ranges.push_back({v, v});
      } else {
        int lo = atoi(part.substr(0, dash).c_str());
        int hi = atoi(part.substr(dash + 1).c_str());
        if (lo > 0 && hi >= lo) ranges.push_back({lo, hi});
      }
      if (comma == std::string::npos) break;
      i = comma + 1;
    }
  }

  OpResult Process(LineView* in, std::vector<char>* out) override {
    out->clear();
    const char* end = in->p + in->len;
    int field = 0;
    bool wrote = false;
    const char* seg = in->p;
    const char* q = in->p;
    for (;;) {
      // jump to the next delimiter with memchr (SIMD inside libc) instead
      // of walking byte-by-byte
      const char* d = (const char*)memchr(q, delim, (size_t)(end - q));
      const char* stop = d ? d : end;
      field++;  // [seg, stop) is field #field
      for (auto& r : ranges) {
        if (field >= r.lo && field <= r.hi) {
          if (wrote) out->push_back(delim);
          out->insert(out->end(), seg, stop);
          wrote = true;
          break;
        }
      }
      if (!d) break;
      seg = q = d + 1;
    }
    if (!wrote) return OpResult::Inplace;
    return OpResult::Materialized;
  }
  const char* Name() const override { return "cut"; }
};

// ---------------------------------------------------------------------------
// tr-style case folding (ASCII, byte-oriented)
// ---------------------------------------------------------------------------
struct LowerOp : LineOp {
  OpResult Process(LineView* in, std::vector<char>* out) override {
    bool changed;
#if defined(__x86_64__)
    if (in->len >= 32 && CpuHasAvx2()) {
      changed = VecAnyInRange(in->p, in->len, 'A', 'Z');
    } else
#endif
    {
      changed = false;
      for (uint32_t i = 0; i < in->len; i++)
        if (in->p[i] >= 'A' && in->p[i] <= 'Z') { changed = true; break; }
    }
    if (!changed) return OpResult::Inplace;
    out->clear();
    out->resize(in->len);
#if defined(__x86_64__)
    if (in->len >= 32 && CpuHasAvx2()) {
      VecFold(out->data(), in->p, in->len, 'A', 'Z', 'a' - 'A');
    } else
#endif
    {
      for (uint32_t i = 0; i < in->len; i++) {
        char c = in->p[i];
        (*out)[i] = c >= 'A' && c <= 'Z' ? char(c - 'A' + 'a') : c;
      }
    }
    return OpResult::Materialized;
  }
  const char* Name() const override { return "lower"; }
};

struct UpperOp : LineOp {
  OpResult Process(LineView* in, std::vector<char>* out) override {
    bool changed;
#if defined(__x86_64__)
    if (in->len >= 32 && CpuHasAvx2()) {
      changed = VecAnyInRange(in->p, in->len, 'a', 'z');
    } else
#endif
    {
      changed = false;
      for (uint32_t i = 0; i < in->len; i++)
        if (in->p[i] >= 'a' && in->p[i] <= 'z') { changed = true; break; }
    }
    if (!changed) return OpResult::Inplace;
    out->clear();
    out->resize(in->len);
#if defined(__x86_64__)
    if (in->len >= 32 && CpuHasAvx2()) {
      VecFold(out->data(), in->p, in->len, 'a', 'z', 'A' - 'a');
    } else
#endif
    {
      for (uint32_t i = 0; i < in->len; i++) {
        char c = in->p[i];
        (*out)[i] = c >= 'a' && c <= 'z' ? char(c - 'a' + 'A') : c;
      }
    }
    return OpResult::Materialized;
  }
  const char* Name() const override { return "upper"; }
};

// ---------------------------------------------------------------------------
// awk {$1=$1};1 - style blank trim (leading/trailing, optional inner squeeze)
// ---------------------------------------------------------------------------
struct TrimOp : LineOp {
  bool squeeze = false;
  explicit TrimOp(bool sq) : squeeze(sq) {}
  static inline bool Blank(char c) { return c == ' ' || c == '\t'; }
  OpResult Process(LineView* in, std::vector<char>* out) override {
    uint32_t b = 0, e = in->len;
    while (b < e && Blank(in->p[b])) b++;
    while (e > b && Blank(in->p[e - 1])) e--;
    if (!squeeze) {
      in->p += b;              // shrink in place: zero copy
      in->len = e - b;
      return OpResult::Inplace;
    }
    bool multi = false;
    for (uint32_t i = b + 1; i < e; i++)
      if (Blank(in->p[i]) && Blank(in->p[i - 1])) { multi = true; break; }
    if (!multi) {
      in->p += b;
      in->len = e - b;
      return OpResult::Inplace;
    }
    out->clear();
    bool prev_blank = false;
    for (uint32_t i = b; i < e; i++) {
      char c = in->p[i];
      bool blank = Blank(c);
      if (blank && prev_blank) continue;
      out->push_back(blank ? ' ' : c);
      prev_blank = blank;
    }
    return OpResult::Materialized;
  }
  const char* Name() const override { return "trim"; }
};

// ---------------------------------------------------------------------------
// --sample RATE : content-hash Bernoulli sampling (deterministic, thread-safe)
// ---------------------------------------------------------------------------
struct SampleOp : LineOp {
  double rate = 1.0;  // e.g. 0.01 keeps ~1%
  explicit SampleOp(double r) : rate(r) {}
  static uint64_t HashBytes(const char* p, uint32_t len) {
    uint64_t h = 1469598103934665603ULL;  // FNV-1a + finalizer mix
    for (uint32_t i = 0; i < len; i++) {
      h ^= (uint8_t)p[i];
      h *= 1099511628211ULL;
    }
    h ^= h >> 33; h *= 0xff51afd7ed558ccdULL; h ^= h >> 33;
    return h;
  }
  OpResult Process(LineView* in, std::vector<char>*) override {
    uint64_t h = HashBytes(in->p, in->len);
    return (double)(h >> 11) * (1.0 / 9007199254740992.0) < rate
               ? OpResult::Inplace
               : OpResult::Drop;
  }
  bool PureFilter() const override { return true; }
  const char* Name() const override { return "sample"; }
};

// ---------------------------------------------------------------------------
// --min-len N / --max-len N : filter lines by byte length (token-budget
// curation for LLM corpora; awk 'length($0)>=N' equivalent, zero-alloc)
// ---------------------------------------------------------------------------
struct MinLenOp : LineOp {
  uint32_t n;
  explicit MinLenOp(uint32_t N) : n(N) {}
  OpResult Process(LineView* in, std::vector<char>*) override {
    return in->len >= n ? OpResult::Inplace : OpResult::Drop;
  }
  bool PureFilter() const override { return true; }
  const char* Name() const override { return "min-len"; }
};

struct MaxLenOp : LineOp {
  uint32_t n;
  explicit MaxLenOp(uint32_t N) : n(N) {}
  OpResult Process(LineView* in, std::vector<char>*) override {
    return in->len <= n ? OpResult::Inplace : OpResult::Drop;
  }
  bool PureFilter() const override { return true; }
  const char* Name() const override { return "max-len"; }
};

// ---------------------------------------------------------------------------
// --jsonl KEY : extract one scalar field from a JSON Lines record.
//
// A fast scanner (not a full parser): finds "KEY" as a proper JSON string
// token followed by ':', then takes the scalar value - strings unquoted with
// \" \\ \/ \n \t \r \b \f unescaped, numbers/true/false/null verbatim,
// arrays/objects copied verbatim (balanced-bracket scan). Lines without the
// key are DROPPED (grep-like semantics).
// ---------------------------------------------------------------------------
struct JsonlFieldOp : LineOp {
  std::string key;  // stored WITH surrounding quotes: "\"key\""
  explicit JsonlFieldOp(const std::string& k) : key("\"" + k + "\"") {}

  static bool InStringEscape(const char* p, uint32_t len, uint32_t i) {
    // count backslashes immediately before position i
    int bs = 0, j = (int)i - 1;
    while (j >= 0 && p[j] == '\\') { bs++; j--; }
    return (bs & 1) == 1;
  }

  OpResult Process(LineView* in, std::vector<char>* out) override {
    const char* p = in->p;
    const uint32_t len = in->len;

    // find key as a quoted token not inside another string
    uint32_t from = 0;
    const char* kv = nullptr;
    for (;;) {
      const char* h = (const char*)FindBytes(p + from, len - from,
                                             key.data(), key.size());
      if (!h) break;
      uint32_t pos = (uint32_t)(h - p);
      // must be preceded (ignoring blanks) by '{' or ',' to be a key token
      uint32_t q = pos;
      while (q > 0 && (p[q-1] == ' ' || p[q-1] == '\t')) q--;
      bool tok = q == 0 || p[q-1] == '{' || p[q-1] == ',';
      if (tok) { kv = h; break; }
      from = pos + 1;
    }
    if (!kv) return OpResult::Drop;

    uint32_t i = (uint32_t)(kv - p) + (uint32_t)key.size();
    while (i < len && (p[i] == ' ' || p[i] == '\t')) i++;
    if (i >= len || p[i] != ':') return OpResult::Drop;
    i++;
    while (i < len && (p[i] == ' ' || p[i] == '\t')) i++;
    if (i >= len) return OpResult::Drop;

    out->clear();
    if (p[i] == '"') {  // string value: unquote + unescape
      i++;
      while (i < len) {
        char c = p[i];
        if (c == '\\' && i + 1 < len) {
          char e = p[i + 1];
          switch (e) {
            case 'n': out->push_back('\n'); break;
            case 't': out->push_back('\t'); break;
            case 'r': out->push_back('\r'); break;
            case 'b': out->push_back('\b'); break;
            case 'f': out->push_back('\f'); break;
            case '"': out->push_back('"'); break;
            case '\\': out->push_back('\\'); break;
            case '/': out->push_back('/'); break;
            case 'u': {  // \uXXXX -> UTF-8
              uint32_t cp = 0;
              bool ok = i + 5 < len;
              for (int k = 0; ok && k < 4; k++) {
                char h = p[i + 2 + k];
                cp <<= 4;
                if (h >= '0' && h <= '9') cp |= (uint32_t)(h - '0');
                else if (h >= 'a' && h <= 'f') cp |= (uint32_t)(h - 'a' + 10);
                else if (h >= 'A' && h <= 'F') cp |= (uint32_t)(h - 'A' + 10);
                else ok = false;
              }
              if (!ok) { out->push_back('u'); i++; continue; }
              if (cp < 0x80) out->push_back((char)cp);
              else if (cp < 0x800) {
                out->push_back((char)(0xC0 | (cp >> 6)));
                out->push_back((char)(0x80 | (cp & 0x3F)));
              } else {
                out->push_back((char)(0xE0 | (cp >> 12)));
                out->push_back((char)(0x80 | ((cp >> 6) & 0x3F)));
                out->push_back((char)(0x80 | (cp & 0x3F)));
              }
              i += 6;
              continue;
            }
            default: out->push_back(e); break;
          }
          i += 2;
          continue;
        }
        if (c == '"') break;  // end of string
        out->push_back(c);
        i++;
      }
      return OpResult::Materialized;
    }
    if (p[i] == '[' || p[i] == '{') {  // composite: copy verbatim, balanced
      char open = p[i], close = (open == '[') ? ']' : '}';
      int depth = 0;
      bool in_str = false;
      uint32_t start = i;
      while (i < len) {
        char c = p[i];
        if (in_str) {
          if (c == '\\' && i + 1 < len) { i += 2; continue; }
          if (c == '"') in_str = false;
        } else {
          if (c == '"') in_str = true;
          else if (c == open) depth++;
          else if (c == close) {
            depth--;
            if (depth == 0) { i++; break; }
          }
        }
        i++;
      }
      out->insert(out->end(), p + start, p + i);
      return OpResult::Materialized;
    }
    // number / true / false / null: copy until a JSON delimiter
    uint32_t start = i;
    while (i < len && p[i] != ',' && p[i] != '}' && p[i] != ']' &&
           p[i] != ' ' && p[i] != '\t')
      i++;
    if (i == start) return OpResult::Drop;
    out->insert(out->end(), p + start, p + i);
    return OpResult::Materialized;
  }
  const char* Name() const override { return "jsonl"; }
};

// ---------------------------------------------------------------------------
// PII masking for LLM corpus cleaning (hand-rolled scanners - no regex).
//   --mask-email : user@host.tld           -> [EMAIL]
//   --mask-ip    : d.d.d.d (each <= 255)   -> [IP]
// ---------------------------------------------------------------------------
struct MaskEmailOp : LineOp {
  static bool LocalChar(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
           (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-' ||
           c == '+' || c == '%';
  }
  static bool DomainChar(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
           (c >= '0' && c <= '9') || c == '.' || c == '-';
  }
  static bool TailOk(char c) {  // acceptable char right after the address
    return c == ' ' || c == '\t' || c == ',' || c == ';' || c == ')' ||
           c == '"' || c == '\'' || c == '>' || c == ':' || c == ']' ||
           c == '!' || c == '?' || c == '<' || c == '=' || c == '&';
  }
  OpResult Process(LineView* in, std::vector<char>* out) override {
    const char* p = in->p;
    const uint32_t len = in->len;
    out->clear();
    bool hit = false;
    uint32_t i = 0, emit = 0;  // [emit, i) already copied
    while (i < len) {
      // jump straight to the next '@' (memchr is SIMD-accelerated; lines
      // without any '@' - the common case - cost one vector scan)
      const char* at = (const char*)memchr(p + i, '@', len - i);
      if (!at) break;
      i = (uint32_t)(at - p);
      // local part: scan back over local chars (>=1)
      uint32_t ls = i;
      while (ls > 0 && LocalChar(p[ls - 1])) ls--;
      if (ls == i) { i++; continue; }
      // domain: label(.label)+, final label alpha-only with >=2 chars
      uint32_t j = i + 1;
      uint32_t last_dot = 0;
      while (j < len && DomainChar(p[j])) {
        if (p[j] == '.') last_dot = j;
        j++;
      }
      bool ok = j > i + 1 && last_dot > i + 1 && j - last_dot >= 3;
      if (ok && j < len && !TailOk(p[j])) ok = false;
      // TLD must be alphabetic
      for (uint32_t k = last_dot + 1; ok && k < j; k++)
        if (!((p[k] >= 'a' && p[k] <= 'z') || (p[k] >= 'A' && p[k] <= 'Z')))
          ok = false;
      if (!ok) { i++; continue; }
      hit = true;
      out->insert(out->end(), p + emit, p + ls);
      static const char kMask[] = "[EMAIL]";
      out->insert(out->end(), kMask, kMask + sizeof(kMask) - 1);
      emit = j;
      i = j;
    }
    if (!hit) return OpResult::Inplace;
    out->insert(out->end(), p + emit, p + len);
    return OpResult::Materialized;
  }
  const char* Name() const override { return "mask-email"; }
};

struct MaskIpOp : LineOp {
  // returns end of a 1-3 digit run starting at i, or i if none
  static uint32_t Digits(const char* p, uint32_t len, uint32_t i,
                         uint32_t* val) {
    uint32_t v = 0, j = i, d = 0;
    while (j < len && p[j] >= '0' && p[j] <= '9' && d < 3) {
      v = v * 10 + (uint32_t)(p[j] - '0');
      j++; d++;
    }
    *val = v;
    return j;
  }
  OpResult Process(LineView* in, std::vector<char>* out) override {
    const char* p = in->p;
    const uint32_t len = in->len;
    out->clear();
    bool hit = false;
    uint32_t i = 0, emit = 0;  // [emit, i) already copied
    while (i < len) {
      if (p[i] < '0' || p[i] > '9') {
        // skip to the next digit-run start with a vector search
        uint32_t nxt = VecFindInRange(p, len, i, '0', '9');
        if (nxt >= len) break;
        i = nxt;
      }
      uint32_t v;
      uint32_t j = Digits(p, len, i, &v);
      bool cand =
          j > i && v <= 255 && j < len && p[j] == '.';
      if (cand && i > 0) {  // left boundary: not glued to a word/dot
        char c = p[i - 1];
        if ((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') ||
            (c >= 'A' && c <= 'Z') || c == '.' || c == '_')
          cand = false;
      }
      if (cand) {  // try d.d.d.d
        uint32_t k = j + 1, segs = 1;
        bool good = true;
        while (segs < 4 && good) {
          uint32_t v2;
          uint32_t k2 = Digits(p, len, k, &v2);
          if (k2 == k || v2 > 255) { good = false; break; }
          k = k2; segs++;
          if (segs < 4) {
            if (k < len && p[k] == '.') k++;
            else good = false;
          }
        }
        // boundary: next char must not be '.' or digit (else e.g. 1.2.3.4.5)
        if (good && segs == 4 &&
            !(k < len && (p[k] == '.' || (p[k] >= '0' && p[k] <= '9')))) {
          if (!hit) { hit = true; }
          out->insert(out->end(), p + emit, p + i);
          static const char kMask[] = "[IP]";
          out->insert(out->end(), kMask, kMask + sizeof(kMask) - 1);
          emit = k;
          i = k;
          continue;
        }
      }
      i = (j > i) ? j : i + 1;
    }
    if (!hit) return OpResult::Inplace;
    out->insert(out->end(), p + emit, p + len);
    return OpResult::Materialized;
  }
  const char* Name() const override { return "mask-ip"; }
};

}  // namespace ts

#endif  // TS_OPERATORS_H_
