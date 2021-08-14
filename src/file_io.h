/* Copyright 2021 Errord Authors. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
==============================================================================*/

#ifndef FILE_IO_H_
#define FILE_IO_H_

#include <util.h>

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>
#include <errno.h>
#include <assert.h>

namespace textsort {

class SortArray;

// #pragma pack(push, 4)
// struct Line {
//   const char* const ptr; /// TODO: base + offset
//   uint8_t length;

//   Line() : ptr(nullptr), length(0) {};
//   Line(char* p) : ptr(p), length(0) {};
//   void InitLinePtr(char* p) {
//     // only initialization once
//     assert(ptr == nullptr);
//     *(char**)&ptr = p;
//   };
//   const char* Ptr() const {
//     return ptr;
//   };
// };
// #pragma pack(pop)

const char* GetBaseAddressPointer();
void UnSetBaseAddressPointer();

#pragma pack(push, 4)
struct Line {
  uint32_t offset:24;
  uint8_t length;

  Line() : offset(0), length(0) {};
  Line(char* p) : offset(0), length(0) {

  };
  void InitLinePtr(char* p) {
    assert((p - GetBaseAddressPointer()) < 16777216);
    offset = p - GetBaseAddressPointer();
  };
  const char* Ptr() const {
    return (const char*)(GetBaseAddressPointer() + offset);
  }
};
#pragma pack(pop)

class FileBlock {
public:
  FileBlock()
    : FileBlock(nullptr, 0) {};
  FileBlock(const char* ptr, size_t block_size)
    : fblock_ptr_(ptr), block_size_(block_size), pos_(0), line_num_(0),
      next_fblock_ptr_(nullptr) {};
  void ReadLines(SortArray* array);
  size_t GetBlockSize() const { return block_size_; };
  FileBlock* NextBlock() const { return next_fblock_ptr_; };
  void SetNextBlock(FileBlock* next) { next_fblock_ptr_ = next; };

private:
  const char* const fblock_ptr_;
  FileBlock* next_fblock_ptr_;
  size_t block_size_;
  size_t pos_;
  size_t line_num_;
};

/// File IO Interface class, There are some basic methods to implement
class FileIo {
public:
  FileIo() : fblock_head_ptr_(nullptr), fblock_tail_ptr_(nullptr), fblock_num_(0) {};
  virtual ~FileIo() {
    CloseBlock();
    UnSetBaseAddressPointer();
  };
  virtual bool OpenFile(const char* filename, bool read) = 0;
  virtual void LoadFile() = 0;
  /// read line in file
  /// return -1 if read failed, other return line number
  virtual int ReadLine(Line& line) = 0;
  virtual size_t GetFileSize() = 0;
  virtual long WriteFile(const char* buf, size_t count) = 0;
  virtual int Sync() = 0;
  virtual void Close() = 0;
  virtual char* GetBuffPtr() = 0;

  
  /// Split File to Partition Block
  /// *memory* Fileblock is deleted when FileIo is deleted
  FileBlock* PartitionBlock(int block_num);

  /// get block mode
  /// block mode if head pointer not null
  bool IsBLockMode() { return fblock_head_ptr_ != nullptr ? true : false; };

  /// get FileBlock head pointer
  FileBlock* FileBlockHead() const { return fblock_head_ptr_; };

  /// add new FileBlock into Block linke
  /// return pointer of FileBlock
  FileBlock* AddBlock(const char* ptr, size_t block_size);

  /// get real block number
  int GetBlockNumber() const { return fblock_num_; };

  /// free block linker
  void CloseBlock();

private:
  FileBlock* fblock_head_ptr_;
  FileBlock* fblock_tail_ptr_;
  int fblock_num_;
};

class StdFileIo : public FileIo {
public:
  StdFileIo()
    : fsize_(0), fd_(nullptr), buf_ptr_(nullptr),
      pos_(0), line_number_(0) {};
  ~StdFileIo() {
    Close();
  }
  bool OpenFile(const char* filename, bool read) override;
  void LoadFile() override;
  int ReadLine(Line& line) override;
  size_t GetFileSize() override;
  long WriteFile(const char* buf, size_t count) override;
  int Sync() override;
  void Close() override;

  char* GetBuffPtr() override { return buf_ptr_; };

private:
  size_t fsize_;
  FILE* fd_;
  char* buf_ptr_;
  size_t pos_;
  size_t line_number_;
};

class MMapFile : public FileIo {
public:
  MMapFile() 
  : mmap_buf_({0}), mmap_ptr_(nullptr),
    pos_(0), line_number_(0), fd_(0) {};
  ~MMapFile() {
    Close();
  };
  bool OpenFile(const char* filename, bool read) override;
  void LoadFile() override {};
  int ReadLine(Line& line) override;
  size_t GetFileSize() override { return mmap_buf_.st_size; };
  long WriteFile(const char* buf, size_t count) override;
  int Sync() override { return 0; };
  void Close() override;
  char* GetBuffPtr() override { return mmap_ptr_; };

private:
  int fd_;
  size_t pos_;
  size_t line_number_;
  struct stat mmap_buf_;
  char* mmap_ptr_;
};

} // namespace textsort

#endif // FILE_IO_H_
