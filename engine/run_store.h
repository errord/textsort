// run_store.h - on-disk sorted runs (external path for data larger than the
// memory budget). File format = concatenation of [u32 len][bytes] records,
// stored in the pipeline's sort order (ascending, or descending for
// --reverse/--tail). A sparse key index kept in RAM locates the first record
// >= a given anchor with one binary search + a short sequential scan.
#ifndef BT_RUN_STORE_H_
#define BT_RUN_STORE_H_

#include "core.h"
#include "chunk_sort.h"  // SparseEntry, kIndexStride

#include <unistd.h>
#include <fcntl.h>

namespace bt {

struct RunFile {
  std::string path;
  std::vector<SparseEntry> sparse;
  size_t count = 0;

  // Write a complete SortedMemRun out and fill the sparse index.
  static bool Write(const std::string& path, const SortedMemRun& run,
                    const MergeOrder& order, RunFile* out) {
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return false;
    const char* p = run.packed.data();
    size_t total = run.packed.size();
    out->sparse.clear();
    out->sparse.reserve(run.count / kIndexStride + 2);
    size_t o = 0;
    for (size_t i = 0; i < run.count; i++) {
      uint32_t len;
      memcpy(&len, p + o, 4);
      if (i % kIndexStride == 0)
        out->sparse.push_back({order.KeyHead(p + o + 4, len), o});
      o += 4 + len;
    }
    size_t written = 0;
    while (written < total) {
      ssize_t w = ::write(fd, p + written, total - written);
      if (w <= 0) { ::close(fd); return false; }
      written += (size_t)w;
    }
    ::close(fd);
    out->path = path;
    out->count = run.count;
    return true;
  }
};

// Streaming run writer used by pre-merge passes: records are appended one by
// one; the sparse index is built incrementally; a large write buffer keeps
// the IO sequential.
struct RunAppender {
  int fd = -1;
  std::string path;
  MergeOrder order;  // sparse keys follow the pipeline's sort key
  std::vector<char> buf;
  uint64_t off = 0;    // file offset of the next record
  size_t count = 0;
  std::vector<SparseEntry> sparse;

  bool Open(const std::string& p, const MergeOrder& ord) {
    path = p;
    order = ord;
    fd = ::open(p.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return false;
    buf.reserve(4 << 20);
    buf.clear();
    return true;
  }

  bool Append(const LineView& v) {
    if (count % kIndexStride == 0)
      sparse.push_back({order.KeyHead(v.p, v.len), off});
    uint32_t len = v.len;
    buf.insert(buf.end(), (const char*)&len, (const char*)&len + 4);
    buf.insert(buf.end(), v.p, v.p + v.len);
    off += 4 + v.len;
    count++;
    if (buf.size() >= (3 << 20)) return FlushBuf();
    return true;
  }

  bool FlushBuf() {
    size_t w = 0;
    while (w < buf.size()) {
      ssize_t n = ::write(fd, buf.data() + w, buf.size() - w);
      if (n <= 0) return false;
      w += (size_t)n;
    }
    buf.clear();
    return true;
  }

  // Finish; fills the RunFile descriptor. Returns false on IO error.
  bool Finish(RunFile* out) {
    if (!FlushBuf()) return false;
    ::close(fd);
    fd = -1;
    out->path = path;
    out->sparse = std::move(sparse);
    out->count = count;
    return true;
  }
};

// Buffered sequential reader over a run file, with anchor seeking.
struct FileCursor {
  int fd = -1;
  uint64_t file_size = 0;
  uint64_t buf_base = 0;   // file offset of buf[0]
  size_t buf_len = 0;      // valid bytes in buf
  size_t buf_pos = 0;      // consumed bytes in buf
  std::vector<char> buf;
  uint64_t logical_pos = 0;  // file offset of next record

  bool Open(const std::string& path) {
    if (fd >= 0) ::close(fd);
    fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return false;
    FadviseSequential(fd);
    file_size = (uint64_t)lseek(fd, 0, SEEK_END);
    buf.resize(256 << 10);
    buf_base = 0; buf_len = 0; buf_pos = 0; logical_pos = 0;
    return true;
  }
  void Close() {
    if (fd >= 0) ::close(fd);
    fd = -1;
  }

  void SeekTo(uint64_t off) {
    logical_pos = off;
    buf_base = off;
    buf_len = 0;
    buf_pos = 0;
  }

  // Rewind to a previously-read record start (cheap if still buffered).
  void RewindTo(uint64_t rec_off) {
    if (rec_off >= buf_base && rec_off <= buf_base + buf_len) {
      buf_pos = (size_t)(rec_off - buf_base);
      logical_pos = rec_off;
    } else {
      SeekTo(rec_off);
    }
  }

  // Read the record at logical_pos into v; advances logical_pos.
  bool Next(LineView* v) {
    if (logical_pos >= file_size) return false;
    if (buf_pos + 4 > buf_len && !Refill()) return false;
    uint32_t len;
    memcpy(&len, buf.data() + buf_pos, 4);
    if (4 + (size_t)len > buf.size())  // giant line: grow the buffer
      buf.resize(4 + (size_t)len + (256 << 10));
    while (buf_pos + 4 + len > buf_len) {
      if (!Refill()) {
        if (buf_pos + 4 + len > buf_len) return false;  // truncated file
        break;
      }
    }
    v->p = buf.data() + buf_pos + 4;
    v->len = len;
    buf_pos += 4 + len;
    logical_pos += 4 + len;
    return true;
  }

  bool Refill() {
    // keep the unconsumed tail at the front, then read the next block.
    // Invariant: buf[i] holds file byte buf_base + i (i < buf_len).
    size_t keep = buf_len - buf_pos;
    if (keep) memmove(buf.data(), buf.data() + buf_pos, keep);
    uint64_t read_at = buf_base + buf_len;  // first byte AFTER the buffer
    ssize_t r = ::pread(fd, buf.data() + keep, buf.size() - keep,
                        (off_t)read_at);
    buf_base += buf_pos;
    buf_pos = 0;
    buf_len = keep + (r > 0 ? (size_t)r : 0);
    return r > 0;
  }
};

// Unified view of one sorted source (in RAM or on disk).
struct SortedSource {
  bool is_mem = true;
  SortedMemRun mem;   // mem
  RunFile file;       // file

  size_t Count() const { return is_mem ? mem.count : file.count; }

  // Position this source at the first line not ordered before `anchor`
  // (i.e. first line v with !order.Less(v, anchor)) in the run's order.
  void SeekGE(const LineView& anchor, const MergeOrder& order,
              MemCursor* mc, FileCursor* fc) {
    uint64_t akey = order.KeyHead(anchor.p, anchor.len);
    const std::vector<SparseEntry>* sp = is_mem ? &mem.sparse : &file.sparse;

    // Binary search over sparse entries. Entries appear in run order; the
    // key relation vs the anchor key is monotone in that order:
    //   ascending run : skip entries with key <  akey
    //   descending run: skip entries with key >  akey
    // Start the scan one entry before the boundary (strict compare) so that
    // entries sharing the anchor's 8-byte key are never jumped past.
    size_t lo = 0, hi = sp->size();
    if (!order.descending) {
      while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        if ((*sp)[mid].key < akey) lo = mid + 1;
        else hi = mid;
      }
    } else {
      while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        if ((*sp)[mid].key > akey) lo = mid + 1;
        else hi = mid;
      }
    }
    size_t idx = lo > 0 ? lo - 1 : 0;

    if (is_mem) {
      *mc = MemCursor::Open(mem);
      mc->off = (*sp)[idx].off;
      uint64_t o = (*sp)[idx].off;
      while (o < mc->end) {
        uint32_t len;
        memcpy(&len, mc->p + o, 4);
        LineView v{mc->p + o + 4, len};
        if (!order.KeyLess(v, anchor)) break;  // key >= anchor's key
        o += 4 + len;
      }
      mc->off = o;
    } else {
      fc->Open(file.path);
      fc->SeekTo((*sp)[idx].off);
      while (fc->logical_pos < fc->file_size) {
        uint64_t rec_off = fc->logical_pos;
        LineView v;
        if (!fc->Next(&v)) break;
        if (!order.KeyLess(v, anchor)) { fc->RewindTo(rec_off); break; }
      }
    }
  }
};

}  // namespace ts

#endif  // BT_RUN_STORE_H_
