// ops.h - the operator model of the v3 engine.
//
// A pipeline is described in terms of composable operators:
//
//   pre  (LineOp)    streaming line -> 0/1 line map/filter, fused into the
//                    chunk pipeline before the sort barrier (grep, replace,
//                    cut, lower, trim, sample, ...)
//   sort barrier     external sort with an order (ascending / descending)
//   dedup            inlined into the merge kernel when it follows a sort
//                    (None / --unique / --count "uniq -c")
//   post (PostOp)    stateful stream operators on the ordered output stream
//                    (head, tail, top-K frequency, ...)
//
// The planner turns an operator chain into one of three physical executions:
//   * pure streaming  (no sort barrier): parallel chunked map/filter
//   * in-memory sort  : packed memory runs + sample-slice parallel merge
//   * external sort   : runs spilled under a memory budget + multi-pass
//                        fan-in-limited pre-merges + parallel final merge
// Every path streams its output through bounded buffers: input size is
// unlimited.
#ifndef BT_OPS_H_
#define BT_OPS_H_

#include "core.h"
#include <memory>
#include <vector>

namespace bt {

// ---------------------------------------------------------------------------
// Streaming line operator: one input line -> 0 or 1 output line.
// ---------------------------------------------------------------------------
enum class OpResult {
  Drop,         // discard this line
  Inplace,      // keep `in` unchanged (pure filter / identity) - zero copy
  Materialized  // use the bytes appended to *out (no '\n')
};

struct LineOp {
  virtual ~LineOp() = default;
  // May adjust *in in place (e.g. trim shrinks the view - zero copy).
  //  Drop        : discard this line
  //  Inplace     : keep (possibly shrunk) *in
  //  Materialized: the new line bytes live in *out (no '\n')
  virtual OpResult Process(LineView* in, std::vector<char>* out) {
    (void)in; (void)out;
    return OpResult::Inplace;  // identity by default
  }
  virtual const char* Name() const { return "op"; }
  // Pure filters only ever return Drop or Inplace WITH `in` unchanged
  // (no shrink, no materialize). When every op in the chain is pure the
  // streaming engine can compact chunks in place - zero copies, zero allocs.
  virtual bool PureFilter() const { return false; }
};


// A chain of LineOps fused into one call. LineOps themselves are STATELESS
// and thread-safe (all mutable state lives in the runner / scratch buffers),
// so one chain can be shared by all worker threads; each thread uses its own
// ChainRunner.
struct LineChain {
  std::vector<std::unique_ptr<LineOp>> ops;
  bool Empty() const { return ops.empty(); }
  // true when every op is a pure filter (or the chain is empty): the chunk
  // can be compacted in place, reusing the reader's buffer.
  bool PureFilterChain() const {
    for (auto& op : ops)
      if (!op->PureFilter()) return false;
    return true;
  }
};

// Per-thread execution context for a LineChain.
struct ChainRunner {
  const LineChain* chain = nullptr;
  std::vector<char> scratch;    // reused across lines (op output)
  std::vector<char> keep_store; // last materialized line (stable until next)

  bool Run(const LineView& in, LineView* v) {
    LineView cur = in;
    for (auto& op : chain->ops) {
      OpResult r = op->Process(&cur, &scratch);
      if (r == OpResult::Drop) return false;
      if (r == OpResult::Materialized) {
        keep_store.assign(scratch.begin(), scratch.end());
        cur = LineView(keep_store.data(), (uint32_t)keep_store.size());
      }
    }
    *v = cur;
    return true;
  }
};


// ---------------------------------------------------------------------------
// Post operators: stateful consumers of the ordered output stream.
// Feed() receives lines in final order and appends whole output lines
// (with '\n') to *emit. Returning false stops the whole pipeline (head).
// ---------------------------------------------------------------------------
struct PostOp {
  virtual ~PostOp() = default;
  virtual bool Feed(const LineView& in, std::vector<char>* emit) = 0;
  virtual void Flush(std::vector<char>* emit) { (void)emit; }  // end of stream
  virtual const char* Name() const { return "post"; }
};

// Chained post operators. The output lines of op i become the input lines of
// op i+1. Line storage is owned via LineBox (safe against buffer reuse).
struct PostChain {
  std::vector<std::unique_ptr<PostOp>> ops;

  bool Empty() const { return ops.empty(); }

  // Feed one ordered input line; appends final output bytes to *out.
  // Returns false when the pipeline must stop (head cap reached).
  bool Run(const LineView& in, std::vector<char>* out) {
    std::vector<LineBox> lines;  // current stage's lines (owned)
    lines.emplace_back();
    lines.back().Set(in);
    for (auto& op : ops) {
      std::vector<char> buf;             // this stage's raw output
      std::vector<LineBox> produced;
      bool stopped = false;
      for (auto& lb : lines) {
        size_t before = buf.size();
        if (!op->Feed(lb.View(), &buf)) { stopped = true; break; }
        // split the newly appended bytes into owned lines
        const char* p = buf.data() + before;
        const char* end = buf.data() + buf.size();
        while (p < end) {
          const char* nl = (const char*)memchr(p, '\n', (size_t)(end - p));
          if (!nl) break;
          produced.emplace_back();
          produced.back().Set(p, (uint32_t)(nl - p));
          p = nl + 1;
        }
      }
      if (stopped) return false;
      lines.swap(produced);
      if (lines.empty()) return true;  // vanished
    }
    for (auto& lb : lines) {
      out->insert(out->end(), lb.Data(), lb.Data() + lb.len);
      out->push_back('\n');
    }
    return true;
  }

  // Flush the LAST op's pending state (freq emits its top-K here); its output
  // then flows through any remaining... (chains are built so the flushing op
  // is terminal; earlier ops keep no pending state).
  void Flush(std::vector<char>* out) {
    if (ops.empty()) return;
    std::vector<char> buf;
    ops.back()->Flush(&buf);
    out->insert(out->end(), buf.begin(), buf.end());
  }
};


// What the merge kernel inlines after the sort barrier.
enum class DedupMode { None, Unique, Count };

// Full pipeline description (what the CLI builds).
struct PipelineDesc {
  LineChain pre;                 // streaming ops before the sort barrier
  bool no_sort = false;          // force pure streaming mode
  bool need_sort = true;         // sort barrier present
  bool reverse = false;          // descending sort order
  SortKey sort_key;              // what the barrier orders by (default whole line)
  DedupMode dedup = DedupMode::None;  // inlined in the merge kernel
  PostChain post;                // stateful ops on the ordered stream
  long head_n = -1;              // global cap on output lines (-1 = off)
  long tail_n = -1;              // last N lines (implies reverse sort + head)
  bool shuffle = false;          // random shuffle (disk-based)
  bool force_external = false;   // always spill runs
};

}  // namespace ts

#endif  // BT_OPS_H_
