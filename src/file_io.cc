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

#include <file_io.h>
#include <sort.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>

namespace textsort {

static const char* FileMemoryBaseAddressPointer = nullptr;

static void SetBaseAddressPointer(char* ptr) {
  FileMemoryBaseAddressPointer = ptr;
}

void UnSetBaseAddressPointer() {
  FileMemoryBaseAddressPointer = nullptr;
}

const char* GetBaseAddressPointer() {
  return FileMemoryBaseAddressPointer;
}

static void ReadLine(Line* line, char* ptr, size_t* pos, size_t total_size) {
  size_t c = *pos;
  line->InitLinePtr(ptr + c);
  while (c < total_size) {
    if (ptr[c] == '\n') {
      c++;
      break;
    }
    c++;
    line->length++;
  }
  *pos = c;
}

void FileBlock::ReadLines(SortArray* array) {
  SortItem* items = array->GetSortItems();
  while (pos_ < block_size_) {
    // safe method, check array index range
    // ReadLine(&(*array)[line_num_].line, (char*)fblock_ptr_, &pos_, block_size_);
    assert(line_num_ <= array->Size());
    ReadLine(&items->line, (char*)fblock_ptr_, &pos_, block_size_);
    items->idx = line_num_++;
    items++;
  }
  array->SetCount(line_num_);
}

FileBlock* FileIo::PartitionBlock(int block_num) {
  // close old file block
  CloseBlock();

  char* header = GetBuffPtr();
  size_t file_size = GetFileSize();
  size_t block_size = file_size / block_num;
  size_t total_size = 0;
  // repeated block_num - 1
  for (int i = 1; i < block_num; i++) {
    size_t max_search = file_size - total_size;
    size_t size = block_size >= max_search ? max_search : block_size;

    while (size < max_search) {
      if (header[size] == '\n') {
        // skip '\n'
        size++;
        break;
      }
      size++;
    }

    AddBlock(header, size);

    // read file end;
    if (size >= max_search) {
      return FileBlockHead();
    }

    // set next block header and search block size
    header += size;
    total_size += size;
  }

  // add end block
  block_size = file_size - total_size;
  AddBlock(header, block_size);

  return FileBlockHead();
}

FileBlock* FileIo::AddBlock(const char* ptr, size_t block_size) {
  FileBlock* block = new FileBlock(ptr, block_size);
  if (!block) {
    return nullptr;
  }

  if (fblock_head_ptr_) {
    fblock_tail_ptr_->SetNextBlock(block);
    fblock_tail_ptr_ = block;
  } else {
    fblock_head_ptr_ = block;
    fblock_tail_ptr_ = block;
  }
  fblock_num_++;
  return block;
}

void FileIo::CloseBlock() {
  if (fblock_head_ptr_) {
    FileBlock* block_ptr = fblock_head_ptr_;
    while (block_ptr) {
      FileBlock* next = block_ptr->NextBlock();
      delete block_ptr;
      block_ptr = next;
    }
    fblock_head_ptr_ = nullptr;
    fblock_tail_ptr_ = nullptr;
  }
  fblock_num_ = 0;
}

bool StdFileIo::OpenFile(const char* filename, bool read) {
  const char* mode = read ? "r" : "wt+";
  fd_ = fopen(filename, mode);
  if (fd_ == nullptr) {
    std::cout << "open file: " << filename << " failed: " << strerror(errno) << std::endl;
    return false;
  }
  struct stat buf;
  int rc = stat(filename, &buf);
  if (rc > 0) {
    std::cout << "stat file: " << filename << " failed: " << strerror(errno) << std::endl;
    return false;
  }
  fsize_ = buf.st_size;

  return true;
}

void StdFileIo::LoadFile() {
  buf_ptr_ = (char*)malloc(sizeof(char) * fsize_);
  if (buf_ptr_ != nullptr) {
    std::cout << "Alloc Memory " << std::to_string(sizeof(char) * fsize_) << " failed.." << std::endl;
  }
  assert(buf_ptr_ != nullptr);
  int rc = fread(buf_ptr_, sizeof(char), GetFileSize(), fd_);
  assert(rc);
  SetBaseAddressPointer(buf_ptr_);
};

int StdFileIo::ReadLine(Line& line) {
  // not read line if use PartitionBlock
  if (IsBLockMode()) {
    return -1;
  }

  if (pos_ == GetFileSize()) {
    return line_number_;
  }

  textsort::ReadLine(&line, buf_ptr_, &pos_, GetFileSize());
  line_number_++;
  return line_number_;
}

long StdFileIo::WriteFile(const char* buf, size_t count) {
  assert(fd_ != nullptr);
  assert(buf != nullptr);
  return fwrite(buf, sizeof(char), count, fd_);
}

size_t StdFileIo::GetFileSize() {
  return fsize_;
}

int StdFileIo::Sync() {
  int fd = fileno(fd_);
  return fsync(fd);
}

void StdFileIo::Close() {
  if (fd_) {
    fclose(fd_);
    fd_ = 0;
  }
}

bool MMapFile::OpenFile(const char* filename, bool read) {
  fd_ = open(filename, O_RDWR);
  if (fd_ < 0) {
    std::cout << "open file: " << filename << " failed: " << strerror(errno) << std::endl;
    return false;
  }

  if (fstat(fd_, &mmap_buf_) < 0) {
    std::cout << "get file state error: " << strerror(errno) << std::endl;
    close(fd_);
    return false;
  }

  mmap_ptr_ = (char *)mmap(NULL, mmap_buf_.st_size, 
    PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
  if (mmap_ptr_ == MAP_FAILED) {
    std::cout << "mmap failed" << std::endl;
    close(fd_);
    return false;
  }

  SetBaseAddressPointer(mmap_ptr_);
  return true;
}

int MMapFile::ReadLine(Line& line) {
  // not read line if use PartitionBlock
  if (IsBLockMode()) {
    return -1;
  }

  if (pos_ == mmap_buf_.st_size) {
    return line_number_;
  }

  textsort::ReadLine(&line, mmap_ptr_, &pos_, mmap_buf_.st_size);
  line_number_++;
  return line_number_;
}

long MMapFile::WriteFile(const char* buf, size_t count) {
  return 0;
}

void MMapFile::Close() {
  if (fd_) {
    close(fd_);
    fd_ = 0;
  }
  CloseBlock();
  munmap(mmap_ptr_, mmap_buf_.st_size);
}

} // namespace textsort
