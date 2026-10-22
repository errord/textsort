// chunk_sort.h - parse a raw chunk, apply the fused streaming operators,
// sort it in the pipeline order, and compact it into a packed run buffer of
// [u32 len][bytes] records plus a sparse key index.
#ifndef TS_CHUNK_SORT_H_
#define TS_CHUNK_SORT_H_

#include "core.h"
#include "ops.h"
#include "pdqsort.h"

namespace ts {

// Sparse index entry: one per kIndexStride lines of the packed buffer.
struct SparseEntry {
  uint64_t key;   // MakeKey of the line at off (monotone in run order)
  uint64_t off;   // byte offset of that line's [u32 len][bytes] record
};
constexpr size_t kIndexStride = 4096;

struct SortedMemRun {
  std::vector<char> packed;        // [u32 len][bytes] per line, ordered
  std::vector<SparseEntry> sparse; // every kIndexStride lines
  size_t count = 0;                // number of lines

  void BuildSparse(const SortKey& sk) {
    sparse.clear();
    sparse.reserve(count / kIndexStride + 2);
    size_t i = 0;
    uint64_t off = 0;
    const char* p = packed.data();
    while (i < count) {
      uint32_t len;
      memcpy(&len, p + off, 4);
      const char* lp = p + off + 4;
      if (i % kIndexStride == 0) sparse.push_back({sk.Head(lp, len), off});
      off += 4 + len;
      i++;
    }
  }
};

// Comparators on LineRef against a chunk base pointer, driven by the
// pipeline's sort order (key mode + direction).
struct ChunkCmpAsc {
  const char* base;
  const SortKey* sk;
  bool operator()(const LineRef& x, const LineRef& y) const {
    if (x.key != y.key) return x.key < y.key;
    return sk->Cmp3Lines(base + x.off, x.len, base + y.off, y.len) < 0;
  }
};
struct ChunkCmpDesc {
  const char* base;
  const SortKey* sk;
  bool operator()(const LineRef& x, const LineRef& y) const {
    if (x.key != y.key) return x.key > y.key;
    return sk->Cmp3Lines(base + x.off, x.len, base + y.off, y.len) > 0;
  }
};

// Parse+apply ops+sort+compact one raw chunk [data, data+size). Lines are
// '\n'-separated; a trailing line without '\n' is a full line (the reader
// guarantees chunk boundaries land on '\n' except at EOF).
//   runner : fused pre-sort stream ops (may be null)
//   extra  : scratch for lines materialized by ops (refs may point here)
//   order  : sort order (key mode + direction)
inline SortedMemRun SortChunk(const char* data, size_t size,
                              ChainRunner* runner, std::vector<char>* extra,
                              const MergeOrder& order) {
  SortedMemRun run;
  if (size == 0) return run;
  const SortKey& sk = order.key;

  // 1) scan lines (+ fused stream ops)
  std::vector<LineRef> refs;
  refs.reserve(size / 48 + 16);
  size_t pos = 0;
  bool r_extra = false;
  LineView v;
  while (pos < size) {
    const char* nl = (const char*)memchr(data + pos, '\n', size - pos);
    size_t end = nl ? (size_t)(nl - data) : size;
    const char* lp = data + pos;
    uint32_t len = (uint32_t)(end - pos);
    if (runner) {
      LineView in(lp, len);
      if (!runner->Run(in, &v)) { pos = end + (nl ? 1 : 0); continue; }
      const auto& ks = runner->keep_store;
      bool in_keep = !ks.empty() && v.p >= ks.data() &&
                     v.p + v.len <= ks.data() + ks.size();
      if (in_keep) {
        // materialized line lives in runner->keep_store, which the next
        // Run() overwrites -> copy into `extra`. Offsets into `extra` are
        // encoded as size + rel so data/extra stay unambiguous.
        extra->insert(extra->end(), v.p, v.p + v.len);
        lp = extra->data() + extra->size() - v.len;
        len = v.len;
        r_extra = true;
      } else {
        // unchanged line, or an in-place shrunk view into `data`
        lp = v.p;
        len = v.len;
        r_extra = false;
      }
    } else {
      r_extra = false;
    }
    LineRef r;
    r.key = sk.Head(lp, len);
    r.off = r_extra ? (uint32_t)(size + (size_t)(lp - extra->data()))
                    : (uint32_t)(size_t)(lp - data);
    r.len = len;
    refs.push_back(r);
    pos = end + (nl ? 1 : 0);
  }
  run.count = refs.size();
  if (run.count == 0) return run;

  // 2) sort
  if (order.descending)
    pdqsort(refs.data(), refs.data() + run.count, ChunkCmpDesc{data, &sk});
  else
    pdqsort(refs.data(), refs.data() + run.count, ChunkCmpAsc{data, &sk});

  // 3) compact: emit [u32 len][bytes] in sorted order
  run.packed.resize([&] {
    size_t total = 0;
    for (auto& r : refs) total += 4 + r.len;
    return total;
  }());
  char* out = run.packed.data();
  for (auto& r : refs) {
    const char* src = r.off < size ? data + r.off
                                   : extra->data() + (r.off - size);
    memcpy(out, &r.len, 4);
    out += 4;
    memcpy(out, src, r.len);
    out += r.len;
  }
  run.BuildSparse(sk);
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
