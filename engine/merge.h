// merge.h - streaming k-way range merge.
//
// MergeRange merges the lines of all sources that fall inside the ordered
// range [lo, hi) (empty bounds = open) and streams them through:
//   dedup (inlined --unique / --count "uniq -c")  ->  a LineSink.
// The sink either forwards records into a new run file (pre-merge passes)
// or turns lines into '\n'-terminated output blocks (the ordered writer).
// Nothing is materialized beyond one block: input size is unlimited.
#ifndef BT_MERGE_H_
#define BT_MERGE_H_

#include "core.h"
#include "ops.h"
#include "run_store.h"
#include "mergetree.h"

#include <atomic>

namespace bt {

// Consumer of merged, fully post-processed output lines.
struct LineSink {
  // Consume one line (no '\n'). Returns false => abort the merge (head cap
  // reached downstream / IO error).
  virtual bool Line(const LineView& v) = 0;
  virtual ~LineSink() = default;
};

struct MergeStats {
  size_t in_lines = 0;   // lines pulled from sources within [lo, hi)
  size_t out_lines = 0;  // lines handed to the sink
  size_t dups = 0;       // lines collapsed by unique/count
};

// The core merge.
//   sources : run sources (mem or file), all sorted in `order`
//   lo/hi   : ordered range bounds (empty = open)
//   dedup   : inlined post-sort operators
//   max_out : stop after this many OUTPUT lines (-1 = unlimited)
//   stop    : global kill switch (checked regularly; may be null)
inline MergeStats MergeRange(const std::vector<SortedSource*>& sources,
                             const MergeOrder& order, const LineView& lo,
                             const LineView& hi, DedupMode dedup,
                             long max_out, const std::atomic<bool>* stop,
                             LineSink* sink) {
  MergeStats st;
  const int S = (int)sources.size();
  std::vector<MemCursor> mc(S);
  std::vector<FileCursor> fc(S);
  std::vector<int> live;
  live.reserve(S);

  auto src_next = [&](int i, LineView* v) -> bool {
    SortedSource* s = sources[i];
    return s->is_mem ? mc[i].Next(v) : fc[i].Next(v);
  };

  for (int i = 0; i < S; i++) {
    SortedSource* s = sources[i];
    if (s->Count() == 0) continue;
    if (!lo.Empty()) {
      s->SeekGE(lo, order, &mc[i], &fc[i]);
    } else if (s->is_mem) {
      mc[i] = MemCursor::Open(s->mem);
    } else {
      if (!fc[i].Open(s->file.path)) continue;
    }
    live.push_back(i);
  }

  const int k = (int)live.size();
  if (k == 0) return st;

  MergeTree tree;
  tree.Init(k, order);
  {
    std::vector<char> got(k, 0);
    std::vector<LineView> first(k);
    for (int j = 0; j < k; j++) {
      LineView v;
      if (src_next(live[j], &v)) { first[j] = v; got[j] = 1; }
    }
    for (int j = 0; j < k; j++) tree.SetInitial(j, got[j] ? first[j] : LineView());
    tree.Build();
  }

  LineBox prev;        // previous distinct line (unique / count state)
  long run_count = 0;  // current run length (count mode)
  std::vector<char> cnt_buf;

  auto emit_count = [&]() -> bool {
    if (dedup != DedupMode::Count || !prev.valid) return true;
    if (max_out >= 0 && (long)st.out_lines >= max_out) return false;
    char head[24];
    int hl = snprintf(head, sizeof(head), "%7ld ", run_count);
    cnt_buf.clear();
    cnt_buf.insert(cnt_buf.end(), head, head + hl);
    cnt_buf.insert(cnt_buf.end(), prev.Data(), prev.Data() + prev.len);
    st.out_lines++;
    if (!sink->Line(LineView(cnt_buf.data(), (uint32_t)cnt_buf.size()))) return false;
    return true;
  };

  for (;;) {
    if (stop && stop->load(std::memory_order_relaxed)) break;
    int w = tree.Winner();
    if (w < 0) break;
    int src = live[w];
    LineView v = tree.WinLine();

    if (!hi.Empty() && !order.KeyLess(v, hi)) break;  // past the slice range
    // (key-level bound: one key group never straddles two slices)

    st.in_lines++;

    if (dedup == DedupMode::Unique) {
      if (prev.valid && order.key.KeysEqual(prev.View(), v)) {
        st.dups++;
      } else {
        prev.Set(v);
        if (max_out >= 0 && (long)st.out_lines >= max_out) break;
        st.out_lines++;
        if (!sink->Line(v)) break;
      }
    } else if (dedup == DedupMode::Count) {
      if (prev.valid && order.key.KeysEqual(prev.View(), v)) {
        st.dups++;
        run_count++;
      } else {
        if (!emit_count()) break;
        prev.Set(v);
        run_count = 1;
      }
    } else {
      if (max_out >= 0 && (long)st.out_lines >= max_out) break;
      st.out_lines++;
      if (!sink->Line(v)) break;
    }

    LineView nv;
    if (src_next(src, &nv)) tree.Advance(w, nv);
    else tree.Kill(w);
  }
  emit_count();  // final pending run for count mode

  for (int i : live)
    if (!sources[i]->is_mem) fc[i].Close();

  return st;
}

}  // namespace ts

#endif  // BT_MERGE_H_
