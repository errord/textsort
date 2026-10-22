// core.h - fundamental line types and comparators for the v2 engine.
//
// Line semantics match `LC_ALL=C sort`: raw byte lexicographic order, where a
// shorter line that is a prefix of a longer one sorts first ("AB" < "ABC").
#ifndef TS_CORE_H_
#define TS_CORE_H_

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>

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
  uint32_t len;   // line length without '\n' (uint32: unlimited line length)
};

struct LineView {  // borrowed view of one line
  const char* p;
  uint32_t len;
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

// Comparator for sorting LineRef arrays against a chunk base pointer.
struct ChunkCmp {
  const char* base;
  bool operator()(const LineRef& x, const LineRef& y) const {
    if (x.key != y.key) return x.key < y.key;
    return LineCmp3(base + x.off, x.len, base + y.off, y.len) < 0;
  }
};

inline double NowSec() {
  struct timespec ts_;
  clock_gettime(CLOCK_MONOTONIC, &ts_);
  return ts_.tv_sec + ts_.tv_nsec * 1e-9;
}

}  // namespace ts

#endif  // TS_CORE_H_
