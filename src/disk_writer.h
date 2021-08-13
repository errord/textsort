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

#ifndef DISK_WRITE_H_
#define DISK_WRITE_H_

#include <file_io.h>
#include <sort.h>
#include <queue.h>
#include <task_message.h>

namespace textsort {

class DiskWriter {
public:
  DiskWriter(std::string file_name, size_t buff_size)
    : file_name_(file_name), diskwrite_queue_(), buff_size_(buff_size),
      write_buff_ptr_(nullptr), file_(nullptr) {
    write_buff_ptr_ = new char[buff_size];
    memset(write_buff_ptr_, 0, sizeof(char) * buff_size);
    file_ = new StdFileIo();
    bool rc = file_->OpenFile(file_name.c_str(), false);
    assert(rc);
  };
  ~DiskWriter() {
    if (write_buff_ptr_ != nullptr) {
      delete [] write_buff_ptr_;
      write_buff_ptr_ = nullptr;
    }
    if (file_ != nullptr) {
      file_->Close();
      delete file_;
      file_ = nullptr;
    }
  };
  void RunDiskWriter();
  void PutDiskWriteMessage(int batch_id, SortArraySlice slice);
  void PutExitMessage();
private:
  void WriteBuff(SortArraySlice& slice);
  void WriteBuffAndSync(size_t count);
  void DirectWrite(SortArraySlice& slice);
  TaskMessage* GetDiskWriteMessage(); 
  FIFOQueue<TaskMessage> diskwrite_queue_;
  FileIo* file_;
  std::string file_name_;
  size_t buff_size_;
  char* write_buff_ptr_;
};

} // namespace textsort

#endif // DISK_WRITE_H_
