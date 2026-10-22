// core.h - fundamental line types and comparators for the v3 engine.
//
// Line semantics match `LC_ALL=C sort`: raw byte lexicographic order, where a
// shorter line that is a prefix of a longer one sorts first ("AB" < "ABC").
#ifndef TS_CORE_H_
#define TS_CORE_H_

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <string>
#include <vector>
#include <algorithm>

#ifdef __linux__
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#endif

#include <unistd.h>
#ifdef __APPLE__
#include <sys/sysctl.h>
#endif

namespace ts {

// 8-byte big-endian prefix of a line, zero-padded when the line is shorter.
// Big-endian load => numeric uint64 order == lexicographic byte order.
// Zero padding => "AB" gets key 0x414200.. < "ABC" 0x414243.., matching
// LC_ALL=C prefix semantics.
inline uint64_t MakeKey(const char* p, uint32_t len) {
  uint64_t k = 0;
  uint32_t n = len < 8 ? len : 8;
  for (uint32_t i = 0; i < n; i++) k = (k << 8) | (uint8_t)p[i];
  return k << (8 * (8 - n));
}

struct LineRef {  // 16 bytes, one per line inside a chunk
  uint64_t key;   // big-endian 8-byte prefix
  uint32_t off;   // offset of line start inside the chunk buffer
  uint32_t len;   // line length without '\n' (uint32: line length < 4GB)
};

struct LineView {  // borrowed view of one line (no '\n')
  const char* p;
  uint32_t len;
  LineView() : p(nullptr), len(0) {}
  LineView(const char* pp, uint32_t ll) : p(pp), len(ll) {}
  bool Empty() const { return p == nullptr; }
};

// Full-line three-way compare (LC_ALL=C). Returns <0 / 0 / >0.
inline int LineCmp3(const char* a, uint32_t alen, const char* b, uint32_t blen) {
  uint32_t n = alen < blen ? alen : blen;
  int c = memcmp(a, b, n);
  if (c != 0) return c;
  if (alen == blen) return 0;
  return alen < blen ? -1 : 1;
}

// Total order on full lines; fast path on the embedded key.
inline bool FullLess(const LineView& a, const LineView& b) {
  uint64_t ka = MakeKey(a.p, a.len), kb = MakeKey(b.p, b.len);
  if (ka != kb) return ka < kb;
  return LineCmp3(a.p, a.len, b.p, b.len) < 0;
}
inline bool FullEq(const LineView& a, const LineView& b) {
  return a.len == b.len && memcmp(a.p, b.p, a.len) == 0;
}

// ---------------------------------------------------------------------------
// SortKey: what the sort barrier orders lines by.
//
//   Whole   : the full line bytes (LC_ALL=C sort semantics) - the default,
//             zero-overhead fast path
//   Field   : the Nth delimited field (sort -k N,N); whole line is the
//             last-resort tie-break (GNU behavior)
//   Numeric : the numeric value of the line / field (sort -n / -k N,Nn);
//             values parse like GNU: skip blanks, take the numeric prefix,
//             unparsable => 0. int64-exact up to 2^53 via double.
//   Length  : the line length (a cheap "by size" order for corpus auditing)
//
// Head() packs the key into ONE order-preserving uint64 whenever possible so
// sorting/merging keep the 8-byte prefix fast path; Cmp3Lines() is the full
// three-way compare (key, then whole-line tie-break).
struct SortKey {
  enum Mode { Whole = 0, Field = 1, Numeric = 2, Length = 3 };
  Mode mode = Whole;
  char delim = '\t';
  int field = 1;  // 1-based

  bool IsWhole() const { return mode == Whole; }

  // View of the key bytes inside [p, p+len) (no copy). For Numeric the view
  // is the numeric text; for Length/Whole the line itself.
  void View(const char* p, uint32_t len, const char** kp,
            uint32_t* klen) const {
    if (mode == Whole || mode == Length) { *kp = p; *klen = len; return; }
    if (mode == Numeric && field <= 1) {
      // whole-line numeric: ParseNumber skips leading blanks itself
      *kp = p;
      *klen = len;
      return;
    }
    // Field >= 1: walk (field - 1) delimiters, key runs to the next delim.
    // (Field mode with any field, or Numeric mode with field >= 2.)
    const char* s = p;
    const char* end = p + len;
    int n = field;
    while (--n > 0) {
      while (s < end && *s != delim) s++;
      if (s == end) {  // fewer fields than requested: empty key
        *kp = end;
        *klen = 0;
        return;
      }
      s++;  // skip the delimiter
    }
    const char* k = s;
    while (s < end && *s != delim) s++;
    *kp = k;
    *klen = (uint32_t)(s - k);
  }

  // IEEE double bits flipped to uint64 order (classic total-order trick).
  static uint64_t DoubleOrder(double d) {
    uint64_t u;
    memcpy(&u, &d, 8);
    return u ^ ((u >> 63) ? ~0ULL : 0x8000000000000000ULL);
  }

  // GNU -n style parse: skip leading blanks, [+-]?digits[.digits][eE exp].
  // Returns false when no numeric prefix exists (value 0).
  static bool ParseNumber(const char* p, uint32_t len, double* out) {
    uint32_t i = 0;
    while (i < len && (p[i] == ' ' || p[i] == '\t')) i++;
    uint32_t j = i;
    if (j < len && (p[j] == '+' || p[j] == '-')) j++;
    uint32_t digits = 0;
    while (j < len && p[j] >= '0' && p[j] <= '9') { j++; digits++; }
    if (j < len && p[j] == '.') {
      j++;
      while (j < len && p[j] >= '0' && p[j] <= '9') { j++; digits++; }
    }
    if (digits == 0) { *out = 0.0; return false; }
    if (j < len && (p[j] == 'e' || p[j] == 'E')) {  // optional exponent
      uint32_t k = j + 1;
      if (k < len && (p[k] == '+' || p[k] == '-')) k++;
      uint32_t ed = 0;
      while (k < len && p[k] >= '0' && p[k] <= '9') { k++; ed++; }
      if (ed > 0) j = k;
    }
    char buf[64];
    uint32_t n = j - i;
    if (n >= sizeof(buf)) n = sizeof(buf) - 1;
    memcpy(buf, p + i, n);
    buf[n] = 0;
    *out = strtod(buf, nullptr);
    return true;
  }

  // Order-preserving 8-byte head of the key.
  uint64_t Head(const char* p, uint32_t len) const {
    switch (mode) {
      case Whole:
        return MakeKey(p, len);
      case Length:
        return (uint64_t)len;
      case Numeric: {
        const char* kp; uint32_t kl;
        View(p, len, &kp, &kl);
        double d;
        ParseNumber(kp, kl, &d);
        return DoubleOrder(d);
      }
      case Field:
      default: {
        const char* kp; uint32_t kl;
        View(p, len, &kp, &kl);
        return MakeKey(kp, kl);
      }
    }
  }

  // Total three-way compare of two whole lines under this key.
  int Cmp3Lines(const char* a, uint32_t alen, const char* b,
                uint32_t blen) const {
    if (mode == Whole) return LineCmp3(a, alen, b, blen);
    uint64_t ka = Head(a, alen), kb = Head(b, blen);
    if (ka != kb) return ka < kb ? -1 : 1;
    if (mode == Numeric || mode == Length)
      return LineCmp3(a, alen, b, blen);  // heads are exact for these modes
    // Field: head is only an 8-byte prefix - compare the full key views.
    const char *fa, *fb; uint32_t fal, fbl;
    View(a, alen, &fa, &fal);
    View(b, blen, &fb, &fbl);
    int c = LineCmp3(fa, fal, fb, fbl);
    if (c != 0) return c;
    return LineCmp3(a, alen, b, blen);  // last-resort tie-break
  }

  // Do two lines carry the SAME key? (dedup / uniq -c semantics; under
  // Whole this is full-line equality.)
  bool KeysEqual(const LineView& a, const LineView& b) const {
    if (mode == Whole) return FullEq(a, b);
    return Cmp3Keys(a.p, a.len, b.p, b.len) == 0;  // key only - payload may differ
  }

  // Pure KEY comparison (NO whole-line tie-break): does line a's key order
  // strictly before line b's key? Used for slice-range boundaries so that
  // lines sharing one key never straddle two merge slices (this is what
  // keeps -u/-c/--freq grouping correct under --key).
  int Cmp3Keys(const char* a, uint32_t alen, const char* b,
               uint32_t blen) const {
    switch (mode) {
      case Whole:
        return LineCmp3(a, alen, b, blen);
      case Length:
        return alen < blen ? -1 : (alen > blen ? 1 : 0);
      case Numeric: {
        double x, y;
        ParseNumber(a, alen, &x);
        ParseNumber(b, blen, &y);
        uint64_t ka = DoubleOrder(x), kb = DoubleOrder(y);
        return ka < kb ? -1 : (ka > kb ? 1 : 0);
      }
      case Field:
      default: {
        uint64_t ka = Head(a, alen), kb = Head(b, blen);
        if (ka != kb) return ka < kb ? -1 : 1;
        const char *fa, *fb; uint32_t fal, fbl;
        View(a, alen, &fa, &fal);
        View(b, blen, &fb, &fbl);
        return LineCmp3(fa, fal, fb, fbl);
      }
    }
  }
};

// Comparison order for merges: ascending by default, descending when the
// pipeline runs a reverse sort (--reverse / --tail). Carries the sort key.
struct MergeOrder {
  SortKey key;
  bool descending = false;

  // 8-byte order-preserving head of the key (monotone under Cmp3Lines).
  uint64_t KeyHead(const char* p, uint32_t len) const {
    return key.Head(p, len);
  }
  uint64_t KeyHead(const LineView& v) const { return key.Head(v.p, v.len); }

  // Total order over whole lines driven by the key (key first, whole line
  // bytes as the deterministic last-resort tie-break, like GNU sort).
  int Cmp3Lines(const char* a, uint32_t alen, const char* b,
                uint32_t blen) const {
    return key.Cmp3Lines(a, alen, b, blen);
  }

  bool Less(const LineView& a, const LineView& b) const {
    int c = Cmp3Lines(a.p, a.len, b.p, b.len);
    return descending ? c > 0 : c < 0;
  }

  // Key-only order (no whole-line tie-break): for slice boundaries, so one
  // key group never straddles two parallel merge slices.
  bool KeyLess(const LineView& a, const LineView& b) const {
    int c = key.Cmp3Keys(a.p, a.len, b.p, b.len);
    return descending ? c > 0 : c < 0;
  }
};

// Comparator for sorting LineRef arrays against a chunk base pointer.
struct ChunkCmp {
  const char* base;
  bool operator()(const LineRef& x, const LineRef& y) const {
    if (x.key != y.key) return x.key < y.key;
    return LineCmp3(base + x.off, x.len, base + y.off, y.len) < 0;
  }
};

// Small-string-optimized copy of one line, used for cross-slice state
// (previous emitted line for --unique boundaries, merge anchors, ...).
struct LineBox {
  char small[96];
  std::vector<char> big;
  uint32_t len = 0;
  bool valid = false;

  const char* Data() const { return big.empty() ? small : big.data(); }
  void Set(const char* p, uint32_t n) {
    len = n;
    if (n <= sizeof(small)) {
      memcpy(small, p, n);
      big.clear();
    } else {
      big.assign(p, p + n);
    }
    valid = true;
  }
  void Set(const LineView& v) { Set(v.p, v.len); }
  void Clear() { valid = false; len = 0; big.clear(); }
  LineView View() const { return LineView(Data(), len); }
};

inline double NowSec() {
  struct timespec ts_;
  clock_gettime(CLOCK_MONOTONIC, &ts_);
  return ts_.tv_sec + ts_.tv_nsec * 1e-9;
}

// Portable helpers -----------------------------------------------------------
inline void FadviseSequential(int fd) {
#if defined(__linux__)
  ::posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
#else
  (void)fd;
#endif
}

inline size_t PhysicalMemBytes() {
#if defined(__linux__)
  long pages = sysconf(_SC_PHYS_PAGES);
  long page = sysconf(_SC_PAGE_SIZE);
  if (pages > 0 && page > 0) return (size_t)pages * (size_t)page;
  return (size_t)8 << 30;
#elif defined(__APPLE__)
  int64_t mem;
  size_t sz = sizeof(mem);
  if (sysctlbyname("hw.memsize", &mem, &sz, nullptr, 0) == 0 && mem > 0)
    return (size_t)mem;
  return (size_t)8 << 30;
#else
  return (size_t)8 << 30;
#endif
}

}  // namespace ts

#endif  // TS_CORE_H_
