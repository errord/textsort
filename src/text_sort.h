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

#ifndef TEXT_SORT_H_
#define TEXT_SORT_H_

#include <allocation.h>
#include <util.h>
#include <file_io.h>
#include <sort.h>
#include <thread.h>
#include <queue.h>
#include <merge_scheduler.h>
#include <disk_writer.h>

namespace textsort {
class TextSort {
public:
  TextSort() : file_io_ptr_(nullptr) {
    config_ = GetConfig();
    file_io_ptr_ = new textsort::MMapFile();
    //file_io_ptr_ = new textsort::StdFileIo();
  };
  ~TextSort() {
    if (file_io_ptr_ != nullptr) {
      delete file_io_ptr_;
      file_io_ptr_ = nullptr;
    }
  };
  void Start();
private:
  void BuildSequenceLoadDataPipeline(MergeScheduler* merge_scheduler);
  void BuildParallelLoadDataPipeline(MergeScheduler* merge_scheduler);
  ThreadPool thread_pool_;
  FileBlock* LoadFileAndPartition();
  const struct Config* config_;
  FileIo* file_io_ptr_;
};

} // namespace textsort

#endif // TEXT_SORT_H_
