// chunk_sort.h - parse a raw chunk, sort it, compact it into an ordered
// packed buffer of [u32 len][bytes] records, plus a sparse key index.
#ifndef TS_CHUNK_SORT_H_
#define TS_CHUNK_SORT_H_

#include "core.h"
#include "pdqsort.h"

namespace ts {

// Sparse index entry: one per kIndexStride lines of the packed buffer.
struct SparseEntry {
  uint64_t key;   // MakeKey of the line at off (monotone non-decreasing)
  uint64_t off;   // byte offset of that line's [u32 len][bytes] record
};
constexpr size_t kIndexStride = 4096;

struct SortedMemRun {
  std::vector<char> packed;             // [u32 len][bytes] per line, ordered
  std::vector<SparseEntry> sparse;      // every kIndexStride lines
  size_t count = 0;                     // number of lines

  void BuildSparse() {
    sparse.clear();
    sparse.reserve(count / kIndexStride + 2);
    size_t i = 0;
    uint64_t off = 0;
    const char* p = packed.data();
    while (i < count) {
      uint32_t len;
      memcpy(&len, p + off, 4);
      const char* lp = p + off + 4;
      if (i % kIndexStride == 0) sparse.push_back({MakeKey(lp, len), off});
      off += 4 + len;
      i++;
    }
  }
};

// Parse+sort+compact one raw chunk [data, data+size). Lines are separated by
// '\n'; a trailing line without '\n' at the end of the chunk is a full line
// (the pipeline guarantees chunk boundaries land on '\n' except at EOF).
// 'filter' optionally drops lines before sorting (stream pre-operators).
// Returns the sorted, compacted run.
struct LineFilter {
  // return false to drop the line (e.g. grep)
  virtual bool Keep(const char* p, uint32_t len) { return true; }
  virtual ~LineFilter() = default;
};

inline SortedMemRun SortChunk(const char* data, size_t size, LineFilter* filter) {
  SortedMemRun run;
  if (size == 0) return run;

  // 1) scan lines
  std::vector<LineRef> refs;
  refs.reserve(size / 48 + 16);
  size_t pos = 0;
  while (pos < size) {
    const char* nl = (const char*)memchr(data + pos, '\n', size - pos);
    size_t end = nl ? (size_t)(nl - data) : size;
    uint32_t len = (uint32_t)(end - pos);
    if (!filter || filter->Keep(data + pos, len)) {
      LineRef r;
      r.key = MakeKey(data + pos, len);
      r.off = (uint32_t)pos;
      r.len = len;
      refs.push_back(r);
    }
    pos = end + (nl ? 1 : 0);
  }
  run.count = refs.size();
  if (run.count == 0) return run;

  // 2) sort (pdqsort, key fast path in ChunkCmp)
  pdqsort(refs.data(), refs.data() + run.count, ChunkCmp{data});

  // 3) compact: emit [u32 len][bytes] in sorted order
  run.packed.resize([&] {
    size_t total = 0;
    for (auto& r : refs) total += 4 + r.len;
    return total;
  }());
  char* out = run.packed.data();
  for (auto& r : refs) {
    memcpy(out, &r.len, 4);
    out += 4;
    memcpy(out, data + r.off, r.len);
    out += r.len;
  }
  run.BuildSparse();
  return run;
}

// Sequential reader over a packed buffer.
struct MemCursor {
  const char* p;
  uint64_t off = 0, end = 0;

  static MemCursor Open(const SortedMemRun& run) {
    MemCursor c;
    c.p = run.packed.data();
    c.end = run.packed.size();
    return c;
  }
  bool Next(LineView* v) {
    if (off >= end) return false;
    uint32_t len;
    memcpy(&len, p + off, 4);
    v->p = p + off + 4;
    v->len = len;
    off += 4 + len;
    return true;
  }
};

}  // namespace ts

#endif  // TS_CHUNK_SORT_H_
