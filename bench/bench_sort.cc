// Sort-kernel micro-benchmark on REAL data from data_2g.txt.
// Four algorithms, identical 12-byte SortItem layout and identical comparator
// as textsort's current implementation (post-fix):
//   1. textsort-heap : verbatim copy of TopkSort::MinHeap/HeapSort (min-heap, descending output)
//   2. bottomup-heap : same heap idea, but bottom-up sift-down (Wegener): ~2x fewer compares
//   3. std::sort     : g++ introsort
//   4. pdqsort       : orlp/pdqsort (public domain)
// All produce DESCENDING order (matches textsort's block output; slices are read backwards).
// Correctness: rolling content hash of the sorted sequence must match across algorithms.
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <chrono>
#include <algorithm>
#include <vector>
#include <string>
#include "pdqsort.h"

#pragma pack(push, 4)
struct Line {
  uint32_t offset;
  uint8_t length;
  Line() : offset(0), length(0) {}
};
#pragma pack(pop)

struct SortItem {
  Line line;
  uint32_t idx;
};

static char* base = nullptr;
static_assert(sizeof(Line) == 8, "Line layout");
static_assert(sizeof(SortItem) == 12, "SortItem layout");

// == textsort::Compare, verbatim semantics ==
static inline bool Cmp(const SortItem& a, const SortItem& b) {
  uint8_t alen = a.line.length, blen = b.line.length;
  return memcmp(base + a.line.offset, base + b.line.offset, alen <= blen ? alen : blen) > 0;
}

// == textsort::TopkSort::MinHeap, verbatim ==
static inline void MinHeap(SortItem* first, SortItem* target, SortItem* last) {
  const SortItem temp = *target;
  --first;
  SortItem* child;
  for (; (child = target + (target - first)) <= last; target = child) {
    if (child < last && Cmp(*child, *(child + 1)))
      ++child;
    if (Cmp(temp, *child)) {
      memcpy(target, child, sizeof(SortItem));
    } else {
      break;
    }
  }
  memcpy(target, &temp, sizeof(SortItem));
}

// == textsort::TopkSort::HeapSort, verbatim (descending output) ==
static void TextsortHeapSort(SortItem* begin, SortItem* end) {
  size_t count = end - begin;
  if (count < 2) return;
  end--;
  for (SortItem* ptr = begin + (count / 2 - 1); ptr >= begin; ptr--)
    MinHeap(begin, ptr, end);
  while (end > begin) {
    SortItem t = *begin;
    memcpy(begin, end, sizeof(SortItem));
    *end = t;
    end--;
    MinHeap(begin, begin, end);
  }
}

// == bottom-up heapsort (keep the heap, halve the compares) ==
static void BottomUpHeapSort(SortItem* a, size_t n) {
  if (n < 2) return;
  SortItem* lastp = a + (n - 1);
  for (SortItem* ptr = a + (n / 2 - 1); ptr >= a; ptr--)
    MinHeap(a, ptr, lastp);  // same O(n) build as textsort
  for (size_t end = n - 1; end > 0; --end) {
    SortItem rootv = a[0];   // current min -> goes to a[end]
    SortItem lastv = a[end]; // sifts down from the root
    a[end] = rootv;
    size_t hole = 0;
    while (true) {           // dive picking the smaller child, single compare per level
      size_t lc = 2 * hole + 1;
      if (lc >= end) break;
      size_t smaller = (lc + 1 < end && Cmp(a[lc], a[lc + 1])) ? lc + 1 : lc;
      a[hole] = a[smaller];
      hole = smaller;
    }
    while (hole > 0) {       // climb back up to find insertion point
      size_t parent = (hole - 1) / 2;
      if (!Cmp(a[parent], lastv)) break;
      a[hole] = a[parent];
      hole = parent;
    }
    a[hole] = lastv;
  }
}

static double NowSec() {
  using namespace std::chrono;
  return duration<double>(steady_clock::now().time_since_epoch()).count();
}

static unsigned long long ContentHash(const SortItem* v, size_t n) {
  unsigned long long h = 1469598103934665603ULL;
  for (size_t i = 0; i < n; i++) {
    const char* p = base + v[i].line.offset;
    for (int k = 0; k < v[i].line.length; k++) {
      h = (h ^ (unsigned char)p[k]) * 1099511628211ULL;
    }
    h = (h ^ v[i].line.length) * 1099511628211ULL;
  }
  return h;
}

int main(int argc, char** argv) {
  const char* path = argc > 1 ? argv[1] : "/data/labs/bc/data_2g.txt";
  FILE* f = fopen(path, "rb");
  if (!f) { fprintf(stderr, "open failed\n"); return 1; }
  fseek(f, 0, SEEK_END);
  size_t fsize = ftell(f);
  fseek(f, 0, SEEK_SET);
  base = (char*)malloc(fsize);
  if (fread(base, 1, fsize, f) != fsize) { fprintf(stderr, "read failed\n"); return 1; }
  fclose(f);

  // build index of ALL lines
  std::vector<SortItem> src;
  src.reserve(fsize / 80);
  size_t pos = 0;
  while (pos < fsize) {
    SortItem it;
    it.line.offset = (uint32_t)pos;
    it.idx = (uint32_t)src.size();
    size_t c = pos;
    while (c < fsize && base[c] != '\n') c++;
    it.line.length = (uint8_t)(c - pos);
    pos = c + 1;
    src.push_back(it);
  }
  size_t total = src.size();
  printf("file=%.2fGB lines=%zu item=%zuB index=%.1fMB\n", fsize / 1e9, total,
         sizeof(SortItem), total * sizeof(SortItem) / 1e6);

  const size_t scales[] = {7000000, 0 /*all*/};
  const char* scaleName[] = {"7M (one textsort block)", "ALL (single in-mem sort)"};
  std::vector<SortItem> work;
  unsigned long long refhash[4] = {0, 0, 0, 0};

  for (int s = 0; s < 2; s++) {
    size_t n = scales[s] == 0 ? total : (scales[s] < total ? scales[s] : total);
    printf("\n=== N=%zu  (%s) ===\n", n, scaleName[s]);
    work.resize(n);
    const char* names[4] = {"textsort-heap", "bottomup-heap", "std::sort", "pdqsort"};
    for (int alg = 0; alg < 4; alg++) {
      double times[3];
      for (int rep = 0; rep < 3; rep++) {
        memcpy(work.data(), src.data(), n * sizeof(SortItem));
        double t0 = NowSec();
        switch (alg) {
          case 0: TextsortHeapSort(work.data(), work.data() + n); break;
          case 1: BottomUpHeapSort(work.data(), n); break;
          case 2: std::sort(work.data(), work.data() + n, Cmp); break;
          case 3: pdqsort(work.data(), work.data() + n, Cmp); break;
        }
        times[rep] = NowSec() - t0;
        if (rep == 0) {
          unsigned long long h = ContentHash(work.data(), n);
          if (refhash[alg] == 0) refhash[alg] = h;
        }
      }
      std::sort(times, times + 3);
      printf("%-14s median=%7.2fs  (runs: %.2f %.2f %.2f)\n", names[alg], times[1],
             times[0], times[1], times[2]);
    }
    bool allsame = refhash[0] == refhash[1] && refhash[1] == refhash[2] && refhash[2] == refhash[3];
    printf("content-hash identical across all four: %s (hash=%llx)\n", allsame ? "YES" : "NO", refhash[0]);
    for (int alg = 0; alg < 4; alg++) refhash[alg] = 0;
  }
  free(base);
  return 0;
}
