// mergetree.h - k-way merge selection tree (tournament/winner tree).
//
// Replaces a binary heap: the winner index lives at the root and each replay
// walks one path up with a single compare per level, and each line's 8-byte
// key is computed once when it enters the tree (not once per compare).
#ifndef BT_MERGETREE_H_
#define BT_MERGETREE_H_

#include "core.h"
#include <vector>

namespace bt {

class MergeTree {
 public:
  void Init(int k, const MergeOrder& o) {
    k_ = k;
    order_ = o;
    cur_.assign(k, LineView());
    key_.assign(k, 0);
    alive_.assign(k, 0);
    node_.assign(2 * k, 0);
    for (int i = 0; i < k; i++) node_[k + i] = i;
  }

  // Register source i's current line (empty view => source exhausted).
  void SetInitial(int i, const LineView& v) {
    if (v.Empty()) {
      alive_[i] = 0;
    } else {
      cur_[i] = v;
      key_[i] = MakeKey(v.p, v.len);
      alive_[i] = 1;
    }
  }

  void Build() {
    if (k_ <= 1) return;
    for (int i = k_ - 1; i >= 1; i--) node_[i] = Play(node_[2 * i], node_[2 * i + 1]);
  }

  // Winner source index, or -1 when all sources are exhausted.
  int Winner() const {
    if (k_ == 1) return alive_[0] ? 0 : -1;
    int w = node_[1];
    return alive_[w] ? w : -1;
  }

  const LineView& WinLine() const { return cur_[Winner()]; }

  // Source i produced a new line (call AFTER reading it).
  void Advance(int i, const LineView& v) {
    if (v.Empty()) {
      alive_[i] = 0;
    } else {
      cur_[i] = v;
      key_[i] = MakeKey(v.p, v.len);
      alive_[i] = 1;
    }
    Replay(i);
  }

  // Source i is exhausted.
  void Kill(int i) {
    alive_[i] = 0;
    Replay(i);
  }

 private:
  // Winner between two source indices (dead sources lose).
  int Play(int a, int b) {
    if (!alive_[a]) return b;
    if (!alive_[b]) return a;
    if (key_[a] != key_[b]) {
      bool a_wins = order_.descending ? key_[a] > key_[b] : key_[a] < key_[b];
      return a_wins ? a : b;
    }
    int c = order_.Cmp3Lines(cur_[a].p, cur_[a].len, cur_[b].p, cur_[b].len);
    if (c == 0) return a < b ? a : b;  // stable tie-break
    bool a_wins = order_.descending ? c > 0 : c < 0;
    return a_wins ? a : b;
  }

  void Replay(int i) {
    if (k_ <= 1) return;
    int pos = k_ + i;
    while (pos > 1) {
      int p = pos >> 1;
      node_[p] = Play(node_[2 * p], node_[2 * p + 1]);
      pos = p;
    }
  }

  int k_ = 0;
  MergeOrder order_;
  std::vector<LineView> cur_;
  std::vector<uint64_t> key_;
  std::vector<char> alive_;
  std::vector<int> node_;
};

}  // namespace ts

#endif  // BT_MERGETREE_H_
