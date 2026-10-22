// merge.h - parallel sample-slice k-way merge.
//
// The key space is split into P slices by (P-1) anchor lines sampled from the
// sorted runs. Anchor comparison is FULL-line based, so every line (including
// equal-key or equal-content lines) lands in exactly one slice. Each slice is
// merged independently by its own thread, and slices are written to the
// output in order.
#ifndef TS_MERGE_H_
#define TS_MERGE_H_

#include "run_store.h"

#include <queue>

namespace ts {

struct HeapItem {
  LineView v;
  int src;
};
struct HeapCmp {  // min-heap on the current line
  bool operator()(const HeapItem& a, const HeapItem& b) const {
    return FullLess(b.v, a.v);
  }
};

struct SliceResult {
  std::vector<char> out;    // emitted lines, each terminated with '\n'
  std::string first;        // first emitted line (empty slice => empty)
  std::string last;         // last emitted line
  size_t count = 0;         // number of emitted lines
};

// Merge all lines in [lo, hi) from every source into one ordered slice.
//   lo.Empty()  : start from the beginning
//   hi.Empty()  : go to the end
// unique       : drop lines equal to their predecessor inside the slice
// skip_equal_to: if set, additionally drop a leading run equal to this line
//                (handles the slice boundary for --unique across slices)
// max_out      : hard cap on emitted lines for this slice (0 = unlimited)
inline SliceResult MergeSlice(std::vector<SortedSource>* sources,
                              const LineView& lo, const LineView& hi,
                              bool unique, const std::string* skip_equal_to,
                              size_t max_out) {
  SliceResult r;
  const int S = (int)sources->size();
  std::vector<MemCursor> mc(S);
  std::vector<FileCursor> fc(S);
  std::priority_queue<HeapItem, std::vector<HeapItem>, HeapCmp> heap;

  auto src_next = [&](int i, LineView* v) -> bool {
    SortedSource& s = (*sources)[i];
    return s.is_mem ? mc[i].Next(v) : fc[i].Next(v);
  };

  for (int i = 0; i < S; i++) {
    SortedSource& s = (*sources)[i];
    if (s.Count() == 0) continue;
    if (!lo.Empty()) {
      s.SeekGE(lo, &mc[i], &fc[i]);
    } else if (s.is_mem) {
      mc[i] = MemCursor::Open(s.mem);
    } else {
      if (!fc[i].Open(s.file.path)) continue;
    }
    LineView v;
    if (src_next(i, &v)) heap.push({v, i});
  }

  LineView prev{nullptr, 0};
  bool have_prev = false;
  if (skip_equal_to != nullptr) {
    prev = LineView{skip_equal_to->data(), (uint32_t)skip_equal_to->size()};
    have_prev = true;
  }

  r.out.reserve(1 << 20);
  while (!heap.empty()) {
    HeapItem t = heap.top();
    heap.pop();
    if (!hi.Empty() && !FullLess(t.v, hi)) break;  // >= hi: slice done
    bool drop = unique && have_prev && FullEq(prev, t.v);
    if (!drop) {
      if (r.count == 0) r.first.assign(t.v.p, t.v.len);
      r.out.insert(r.out.end(), t.v.p, t.v.p + t.v.len);
      r.out.push_back('\n');
      r.last.assign(t.v.p, t.v.len);
      prev = LineView{r.last.data(), (uint32_t)r.last.size()};
      have_prev = true;
      if (++r.count == max_out) {  // head cap reached
        break;
      }
    }
    LineView v2;
    if (src_next(t.src, &v2)) heap.push({v2, t.src});
  }
  return r;
}

}  // namespace ts

#endif  // TS_MERGE_H_
