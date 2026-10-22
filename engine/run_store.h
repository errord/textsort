// run_store.h - on-disk sorted runs (external path for data larger than the
// memory budget). File format = concatenation of [u32 len][bytes] records.
// A sparse key index (kept in RAM) allows locating the first record >= a
// given anchor line with one binary search + a short sequential scan.
#ifndef TS_RUN_STORE_H_
#define TS_RUN_STORE_H_

#include "core.h"
#include "chunk_sort.h"  // SparseEntry, kIndexStride

#include <unistd.h>
#include <fcntl.h>

namespace ts {

struct RunFile {
  std::string path;
  std::vector<SparseEntry> sparse;
  size_t count = 0;

  // Write a SortedMemRun out and fill the sparse index while writing.
  // Returns false on IO error.
  static bool Write(const std::string& path, const SortedMemRun& run, RunFile* out) {
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return false;
    // Write the sparse index and the payload in one pass.
    out->sparse.reserve(run.count / kIndexStride + 2);
    size_t o = 0;
    for (size_t i = 0; i < run.count; i++) {
      uint32_t len;
      memcpy(&len, p + o, 4);
      if (i % kIndexStride == 0) out->sparse.push_back({MakeKey(p + o + 4, len), o});
      o += 4 + len;
    }
    size_t total = run.packed.size();
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
    fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return false;
    ::posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
    file_size = (uint64_t)lseek(fd, 0, SEEK_END);
    buf.resize(256 << 10);  // small: dozens of concurrent cursors per merge
    return true;
  }

  void SeekTo(uint64_t off) {
    logical_pos = off;
    buf_base = off;
    buf_len = 0;
    buf_pos = 0;
  }

  // Rewind the cursor to a previously-read record start (cheap if the record
  // is still in the buffer).
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
    while (buf_pos + 4 + len > buf_len) {  // record (partially) beyond buffer
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
    // keep the unconsumed tail
    size_t keep = buf_len - buf_pos;
    memmove(buf.data(), buf.data() + buf_pos, keep);
    ssize_t r = ::pread(fd, buf.data() + keep, buf.size() - keep,
                        (off_t)(buf_base + keep));
    buf_base += buf_pos;
    buf_pos = 0;
    buf_len = keep + (r > 0 ? (size_t)r : 0);
    return r > 0;
  }
};

// Unified view of one sorted source (in RAM or on disk).
struct SortedSource {
  bool is_mem = true;
  // mem
  SortedMemRun mem;
  // file
  RunFile file;

  size_t Count() const { return is_mem ? mem.count : file.count; }

  // Position this source at the first line >= anchor (full-line compare).
  // 'at_start' receives the positioned cursor state for mem cursors.
  void SeekGE(const LineView& anchor, MemCursor* mc, FileCursor* fc) {
    uint64_t akey = MakeKey(anchor.p, anchor.len);
    const std::vector<SparseEntry>* sp;
    if (is_mem) sp = &mem.sparse;
    else sp = &file.sparse;

    // binary search: last entry with key <= akey
    size_t lo = 0, hi = sp->size();
    while (lo < hi) {
      size_t mid = (lo + hi) / 2;
      if ((*sp)[mid].key <= akey) lo = mid + 1;
      else hi = mid;
    }
    size_t idx = lo > 0 ? lo - 1 : 0;

    if (is_mem) {
      *mc = MemCursor::Open(mem);
      mc->off = (*sp)[idx].off;
    } else {
      fc->Open(file.path);
      fc->SeekTo((*sp)[idx].off);
    }
    // sequential scan to the first line >= anchor (no cursor overshoot)
    if (is_mem) {
      uint64_t o = (*sp)[idx].off;
      while (o < mc->end) {
        uint32_t len;
        memcpy(&len, mc->p + o, 4);
        LineView v{mc->p + o + 4, len};
        if (!FullLess(v, anchor)) break;  // v >= anchor
        o += 4 + len;
      }
      mc->off = o;  // positioned
    } else {
      while (fc->logical_pos < fc->file_size) {
        uint64_t rec_off = fc->logical_pos;
        LineView v;
        if (!fc->Next(&v)) break;
        if (!FullLess(v, anchor)) { fc->RewindTo(rec_off); break; }
      }
    }
  }
};

}  // namespace ts

#endif  // TS_RUN_STORE_H_
