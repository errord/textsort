// postsort.h - stateful post operators running on the ordered output stream.
//
// --unique and --count (uniq -c) are INLINED in the merge kernel for speed
// (they only need adjacent-line state); the operators here handle everything
// that needs fuller state: head caps inside a post chain, top-K frequency.
#ifndef TS_POSTSORT_H_
#define TS_POSTSORT_H_

#include "ops.h"
#include <string>
#include <vector>
#include <utility>

namespace ts {

// ---------------------------------------------------------------------------
// head N as a post op (stops the pipeline after N lines)
// ---------------------------------------------------------------------------
struct HeadPost : PostOp {
  long n;
  long seen = 0;
  explicit HeadPost(long N) : n(N) {}
  bool Feed(const LineView& in, std::vector<char>* emit) override {
    if (++seen > n) return false;
    emit->insert(emit->end(), in.p, in.p + in.len);
    emit->push_back('\n');
    return n < 0 || seen < n;  // stop exactly at the cap
  }
  const char* Name() const override { return "head"; }
};

// ---------------------------------------------------------------------------
// freq: top-K most frequent lines over the ordered stream
// (equivalent to: sort | uniq -c | sort -k1,1rn | head -K, ties broken by
//  ascending line bytes for a deterministic order)
// ---------------------------------------------------------------------------
struct FreqPost : PostOp {
  struct Entry {
    long count;
    std::string line;
  };
  // Heap ordering: std::push_heap puts the MAXIMUM per this comparator at
  // heap.front(). We define "less" = "better" (higher count; ties: smaller
  // line), so heap.front() is always the WORST entry - the eviction victim.
  struct WorseLast {
    bool operator()(const Entry& a, const Entry& b) const {
      if (a.count != b.count) return a.count > b.count;  // better: higher count
      return a.line < b.line;                            // better: smaller line
    }
  };
  size_t k;
  std::vector<Entry> heap;
  long cur_count = 0;
  LineBox cur;
  SortKey sk;  // grouping follows the pipeline's sort key

  explicit FreqPost(size_t K, const SortKey& key = SortKey()) : k(K), sk(key) {}

  bool Feed(const LineView& in, std::vector<char>*) override {
    if (cur.valid && sk.KeysEqual(cur.View(), in)) {
      cur_count++;
      return true;
    }
    if (cur.valid) PushCur();
    cur.Set(in);
    cur_count = 1;
    return true;
  }

  void PushCur() {
    Entry e{cur_count, std::string(cur.Data(), cur.len)};
    if (heap.size() < k) {
      heap.push_back(std::move(e));
      std::push_heap(heap.begin(), heap.end(), WorseLast{});
    } else if (!heap.empty() && WorseLast{}(e, heap.front())) {
      // e is better than the current worst -> replace it
      std::pop_heap(heap.begin(), heap.end(), WorseLast{});
      heap.back() = std::move(e);
      std::push_heap(heap.begin(), heap.end(), WorseLast{});
    }
  }

  void Flush(std::vector<char>* emit) override {
    if (cur.valid) { PushCur(); cur.Clear(); }
    // ascending (count, line desc) -> reversed = count desc, line asc
    std::sort(heap.begin(), heap.end(), [](const Entry& a, const Entry& b) {
      if (a.count != b.count) return a.count < b.count;
      return a.line > b.line;
    });
    for (auto it = heap.rbegin(); it != heap.rend(); ++it) {
      char num[32];
      int nl = snprintf(num, sizeof(num), "%7ld ", it->count);
      emit->insert(emit->end(), num, num + nl);
      emit->insert(emit->end(), it->line.data(), it->line.data() + it->line.size());
      emit->push_back('\n');
    }
    heap.clear();
  }
  const char* Name() const override { return "freq"; }
};

// ---------------------------------------------------------------------------
// wc: streaming line/word/byte counters over the whole (ordered or streamed)
// input. Emits ONE summary line "L W B" at Flush - works with or without the
// sort barrier, memory-free.
// ---------------------------------------------------------------------------
struct WcPost : PostOp {
  long lines = 0, words = 0;
  unsigned long long bytes = 0;

  static inline bool BlankB(char c) {
    return c == ' ' || c == '\t';  // line bytes exclude '\n'
  }
  bool Feed(const LineView& in, std::vector<char>*) override {
    lines++;
    bytes += in.len + 1;  // content + the stripped '\n'
    bool in_w = false;    // inside a word?
    for (uint32_t i = 0; i < in.len; i++) {
      if (BlankB(in.p[i])) {
        in_w = false;
      } else {
        if (!in_w) words++;
        in_w = true;
      }
    }
    return true;
  }
  void Flush(std::vector<char>* emit) override {
    char buf[96];
    int n = snprintf(buf, sizeof(buf), "%ld %ld %llu\n", lines, words, bytes);
    emit->insert(emit->end(), buf, buf + n);
  }
  const char* Name() const override { return "wc"; }
};

// ---------------------------------------------------------------------------
// nl: number every output line (cat -n style "%6ld\t"), streaming, stateful.
// The writer applies post ops on the globally ordered stream, so numbers are
// correct in both sort and --no-sort modes.
// ---------------------------------------------------------------------------
struct NlPost : PostOp {
  long n = 0;
  bool Feed(const LineView& in, std::vector<char>* emit) override {
    char head[24];
    int hl = snprintf(head, sizeof(head), "%6ld\t", ++n);
    emit->insert(emit->end(), head, head + hl);
    emit->insert(emit->end(), in.p, in.p + in.len);
    emit->push_back('\n');
    return true;
  }
  const char* Name() const override { return "nl"; }
};

}  // namespace ts

#endif  // TS_POSTSORT_H_
