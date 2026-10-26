// simd.h - shared vector kernels for line operators.
//
// Everything here is a pure function over byte ranges so ANY operator can
// reuse it: range membership tests, byte folding, first/last-byte search,
// word-start counting, and a fast shard hash.
//
// Dispatch: AVX2 fast paths on x86_64 (runtime-checked), NEON on aarch64,
// plain scalar loops elsewhere. Short inputs stay scalar - intrinsic setup
// would cost more than it saves.
#ifndef BT_SIMD_H_
#define BT_SIMD_H_

#include <cstdint>
#include <cstring>
#if defined(__x86_64__)
#include <immintrin.h>  // AVX2 + SSE4.2 crc32
#elif defined(__aarch64__)
#include <arm_neon.h>
#endif

namespace bt {

// ---------------------------------------------------------------------------
// x86_64-only helpers (guarded individually below)
// ---------------------------------------------------------------------------
#if defined(__x86_64__)
inline bool CpuHasAvx2() {
  static const bool ok = __builtin_cpu_supports("avx2");
  return ok;
}
inline bool CpuHasSse42() {
  static const bool ok = __builtin_cpu_supports("sse4.2");
  return ok;
}

// Does [p, p+len) contain any byte in [lo, hi]?  (AVX2, 32B/iter)
inline bool VecAnyInRange(const char* p, uint32_t len, char lo, char hi) {
  const __m256i vlo = _mm256_set1_epi8((char)(lo - 1));
  const __m256i vhi = _mm256_set1_epi8((char)(hi + 1));
  uint32_t i = 0;
  for (; i + 32 <= len; i += 32) {
    __m256i v = _mm256_loadu_si256((const __m256i*)(p + i));
    __m256i m = _mm256_and_si256(_mm256_cmpgt_epi8(v, vlo),
                                 _mm256_cmpgt_epi8(vhi, v));
    if (!_mm256_testz_si256(m, m)) return true;
  }
  for (; i < len; i++)
    if (p[i] >= lo && p[i] <= hi) return true;
  return false;
}

// dst[i] = in-range bytes folded by delta, others copied.  (AVX2)
inline void VecFold(char* dst, const char* p, uint32_t len, char lo, char hi,
                    int delta) {
  const __m256i vlo = _mm256_set1_epi8((char)(lo - 1));
  const __m256i vhi = _mm256_set1_epi8((char)(hi + 1));
  const __m256i vd = _mm256_set1_epi8((char)delta);
  uint32_t i = 0;
  for (; i + 32 <= len; i += 32) {
    __m256i v = _mm256_loadu_si256((const __m256i*)(p + i));
    __m256i m = _mm256_and_si256(_mm256_cmpgt_epi8(v, vlo),
                                 _mm256_cmpgt_epi8(vhi, v));
    __m256i d = _mm256_and_si256(m, vd);
    _mm256_storeu_si256((__m256i*)(dst + i), _mm256_add_epi8(v, d));
  }
  for (; i < len; i++) {
    char c = p[i];
    dst[i] = (c >= lo && c <= hi) ? char(c + delta) : c;
  }
}
#endif  // __x86_64__

// ---------------------------------------------------------------------------
// Portable kernels (each with an x86_64 AVX2 / aarch64 NEON fast path)
// ---------------------------------------------------------------------------

// Index of the first byte in [lo, hi] at or after `from` (len if none).
inline uint32_t VecFindInRange(const char* p, uint32_t len, uint32_t from,
                               char lo, char hi) {
  uint32_t i = from;
#if defined(__x86_64__)
  if (CpuHasAvx2()) {
    const __m256i vlo = _mm256_set1_epi8((char)(lo - 1));
    const __m256i vhi = _mm256_set1_epi8((char)(hi + 1));
    for (; i + 32 <= len; i += 32) {
      __m256i v = _mm256_loadu_si256((const __m256i*)(p + i));
      __m256i m = _mm256_and_si256(_mm256_cmpgt_epi8(v, vlo),
                                   _mm256_cmpgt_epi8(vhi, v));
      uint32_t bits = (uint32_t)_mm256_movemask_epi8(m);
      if (bits) return i + (uint32_t)__builtin_ctz(bits);
    }
  }
#endif
  for (; i < len; i++)
    if (p[i] >= lo && p[i] <= hi) return i;
  return len;
}

// Pointer to the LAST occurrence of byte b in [p, p+n), or nullptr.
// (Replaces platform-gated memrchr with one vectorized helper.)
inline const char* VecFindLast(const char* p, size_t n, char b) {
  if (n == 0) return nullptr;
  size_t i = n;
#if defined(__x86_64__)
  if (CpuHasAvx2()) {
    const __m256i vb = _mm256_set1_epi8(b);
    while (i >= 32) {
      i -= 32;
      __m256i v = _mm256_loadu_si256((const __m256i*)(p + i));
      __m256i m = _mm256_cmpeq_epi8(v, vb);
      uint32_t bits = (uint32_t)_mm256_movemask_epi8(m);
      if (bits) return p + i + (31 - (uint32_t)__builtin_clz(bits));
    }
  }
#endif
  while (i > 0) {
    i--;
    if (p[i] == b) return p + i;
  }
  return nullptr;
}

// Count word starts in [p, p+len): positions where a blank byte (sp/tb) or
// the line START is followed by a non-blank byte. Equals the awk-NF word
// count when blanks are ' ' and '\t'.
inline uint64_t VecCountWordStarts(const char* p, uint32_t len, char sp,
                                   char tb) {
#if defined(__x86_64__)
  if (len >= 64 && CpuHasAvx2()) {
    // word_start[i] = !m[i] & m[i-1]  (m = blank mask, m[-1] = 1: a line
    // starting with a non-blank byte counts one word). Per 32-byte block:
    //   t = ~m & ((m << 1) | prev)   with prev = top bit of the last block.
    uint64_t words = 0;
    uint32_t prev = 1, i = 0;
    for (; i + 32 <= len; i += 32) {
      __m256i v = _mm256_loadu_si256((const __m256i*)(p + i));
      __m256i bm = _mm256_or_si256(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(sp)),
                                   _mm256_cmpeq_epi8(v, _mm256_set1_epi8(tb)));
      uint32_t m = (uint32_t)_mm256_movemask_epi8(bm);
      words += (uint64_t)__builtin_popcount(~m & ((m << 1) | prev));
      prev = m >> 31;
    }
    if (i < len) {  // tail (< 32 bytes): build the mask byte-wise
      uint32_t m = 0;
      for (uint32_t k = i; k < len; k++)
        if (p[k] == sp || p[k] == tb) m |= 1u << (k - i);
      words += (uint64_t)__builtin_popcount(~m & ((m << 1) | prev));
    }
    return words;
  }
#endif
  uint64_t words = 0;
  bool in_w = false;
  for (uint32_t i = 0; i < len; i++) {
    if (p[i] == sp || p[i] == tb) {
      in_w = false;
    } else {
      if (!in_w) words++;
      in_w = true;
    }
  }
  return words;
}

// Fast hash for SHARD PLACEMENT ONLY (decides h % R). 4-way interleaved
// CRC32C over 8-byte blocks + finalizer: hides the 3-cycle crc latency and
// streams several GB/s per thread, vs ~1 GB/s for serial FNV-1a. Any good
// hash is valid here - shard assignment does not need to match any other
// hash (sample determinism uses SampleOp::HashBytes and is unchanged).
inline uint64_t ShardHash(const char* p, uint32_t len) {
#if defined(__x86_64__)
  if (CpuHasSse42()) {
    uint64_t a = 0x9E3779B97F4A7C15ULL ^ len, b = 0xBF58476D1CE4E5B9ULL,
             c = 0x94D049BB133111EBULL, d = 0x2545F4914F6CDD1DULL;
    uint32_t i = 0;
    for (; i + 32 <= len; i += 32) {
      uint64_t w0, w1, w2, w3;
      memcpy(&w0, p + i, 8);
      memcpy(&w1, p + i + 8, 8);
      memcpy(&w2, p + i + 16, 8);
      memcpy(&w3, p + i + 24, 8);
      a = _mm_crc32_u64(a, w0);
      b = _mm_crc32_u64(b, w1);
      c = _mm_crc32_u64(c, w2);
      d = _mm_crc32_u64(d, w3);
    }
    if (i < len) {  // tail: gather byte-wise (never overreads)
      uint64_t t = len - i;
      for (uint32_t k = i; k < len; k++) t = (t << 8) | (uint8_t)p[k];
      a = _mm_crc32_u64(a, t);
    }
    uint64_t h = a * 0x9E3779B97F4A7C15ULL + b * 0xC2B2AE3D27D4EB4FULL +
                 (c << 32) + d * 0x165667B19E3779F9ULL;
    h ^= h >> 29;
    h *= 0xBF58476D1CE4E5B9ULL;
    h ^= h >> 32;
    return h;
  }
#endif
  // FNV-1a fallback (identical formula to SampleOp::HashBytes)
  uint64_t h = 1469598103934665603ULL;
  for (uint32_t i = 0; i < len; i++) {
    h ^= (uint8_t)p[i];
    h *= 1099511628211ULL;
  }
  return h;
}

}  // namespace ts

#endif  // BT_SIMD_H_
