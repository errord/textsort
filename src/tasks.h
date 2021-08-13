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

#ifndef TASKS_H_
#define TASKS_H_

#include <util.h>
#include <file_io.h>

namespace textsort {

class TaskThread;
class MergeScheduler;
class DiskWriter;

class Task {
public:
  Task(std::string task_name, bool is_register) : Task(task_name, nullptr, is_register) {};
  Task(std::string task_name, MergeScheduler* merge_scheduler, bool is_register)
    : task_name_(task_name), merge_scheduler_ptr_(merge_scheduler), thread_ptr_(nullptr) {
    if (is_register && merge_scheduler_ptr_) {
      RegisterTaskThreadToSchedule();
    }
  };
  virtual ~Task() {};
  void RunTaskBase();
  void SetThread(TaskThread* thread) {
    thread_ptr_ = thread;
  };
  TaskThread* Thread() const {
    return thread_ptr_;
  };
  MergeScheduler* GetMergeScheduler() {
    return merge_scheduler_ptr_;
  };
  const std::string& GetTaskName() const {
    return task_name_;
  }
private:
  std::string task_name_;
  virtual void RunTask() = 0;
  void RegisterTaskThreadToSchedule();
  TaskThread* thread_ptr_;
  MergeScheduler* merge_scheduler_ptr_;
};

class MergeScheduleTask : public Task {
public:
  MergeScheduleTask(MergeScheduler* merge_scheduler)
    : Task("MergeScheduleTask", merge_scheduler, false) {};
  void RunTask();
};

class SortTask : public Task {
public:
  SortTask(MergeScheduler* merge_scheduler, FileBlock* fblock,
    size_t line_num, size_t top_k, int task_id)
    : Task("SortTask", merge_scheduler, true), sort_array_(nullptr), fblock_ptr_(fblock),
      line_num_(line_num), top_k_(top_k), task_id_(task_id) {};

  SortTask(MergeScheduler* merge_scheduler, FileBlock* fblock,
    SortArray* sort_array, size_t top_k, int batch_id)
    : Task("SortTask", merge_scheduler, true), sort_array_(sort_array), fblock_ptr_(fblock), line_num_(0),
      top_k_(top_k), task_id_(batch_id) {};
  void RunTask();
private:
  void TopkTask();
  void MergeSortTask();
  FileBlock* fblock_ptr_; // *memory* delete at Fileio
  SortArray* sort_array_;
  size_t line_num_;
  size_t top_k_;
  int task_id_;
};

class MergeTask : public Task {
public:
  MergeTask(MergeScheduler* merge_scheduler, bool is_register)
    :  Task("MergeTask", merge_scheduler, is_register) {};
  void RunTask();
};

class CombineTask : public Task {
public:
  CombineTask(MergeScheduler* merge_scheduler, DiskWriter* disk_writer)
    : Task("CombineTask", merge_scheduler, false), disk_writer_(disk_writer) {};
  void RunTask();
private:
  DiskWriter* disk_writer_;
};

class DiskWriteTask : public Task {
public:
  DiskWriteTask(DiskWriter* disk_writer)
  : Task("DiskWriteTask", nullptr, false), disk_writer_(disk_writer) {};
  void RunTask();
private:
  DiskWriter* disk_writer_;
};

} // namespace textsort

#endif // TASKS_H_
