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

#ifndef TASK_MESSAGE_H_
#define TASK_MESSAGE_H_

#include <queue.h>
#include <sort.h>

namespace textsort {

struct TopkTaskStartMessageData {
  int task_id;
  int batch_count;

  TopkTaskStartMessageData(int task_id_, int batch_count_)
    : task_id(task_id_), batch_count(batch_count_) {};
};

struct TopkMessageData {
  int task_id;
  int batch_id;
  SortArraySlice slice;

  TopkMessageData(int task_id_, int batch_id_, SortArraySlice slice_)
    : task_id(task_id_), batch_id(batch_id_), slice(slice_) {};
};

struct MergeMessageData {
  union MergeData {
    SortArraySlice sort_slice;
    SortArray* sort_array_ptr;
    MergeData(): sort_array_ptr(nullptr) {};
  };
  enum {
    Init,
    SliceAndSlice,
    ArrayAndArray,
    SliceAndArray,
    ArrayAndSlice
  } type;
  size_t merge_seq_id;
  int batch_id;
  MergeData one;
  MergeData two;

  MergeMessageData() : type(Init), merge_seq_id(0), batch_id(0), one(), two() {};
  const char* GetTaskTypeName() const;
};

struct MergeDoneMessageData {
  int merge_task_tid;
  int batch_id;
  size_t merge_seq_id;
  SortArray* sort_array;

  MergeDoneMessageData(int merge_task_tid_, size_t merge_seq_id_, int batch_id_, SortArray* sort_array_)
    : merge_task_tid(merge_task_tid_), batch_id(batch_id_), merge_seq_id(merge_seq_id_),
      sort_array(sort_array_) {};
};

struct CombineMessageData {
  int batch_id;
  SortArray* sort_array;

  CombineMessageData(int batch_id_, SortArray* sort_array_)
    : batch_id(batch_id_), sort_array(sort_array_) {};
};

struct DiskWriteMessageData {
  int batch_id;
  SortArraySlice slice;

  DiskWriteMessageData(int batch_id_, SortArraySlice slice_)
    : batch_id(batch_id_), slice(slice_) {};
};

class TaskMessage : public QueueItem<TaskMessage> {
public:
  enum TaskType {
    RegisterTaskThread, // MergeScheduler thread
    TopkTaskStart,      // MergeScheduler thread
    Topk,               // MergeScheduler thread
    TopkTaskDone,       // MergeScheduler thread
    Merge,              // Merge Task thread
    MergeDone,          // MergeScheduler thread
    TaskThreadExit,     // Task thread
    Combine,            // Combine thread
    CombineExit,        // Combine thread
    DiskWrite,          // DiskWrite thread
    DiskWriteExit,      // DiskWrite thread
  };
public:
  TaskMessage(TaskType type)
    : type_(type), data_(nullptr) {};
  TaskMessage(TaskType type, void* data)
    : type_(type), data_(data) {};
  ~TaskMessage() {
    if (data_ != nullptr) {
      switch (type_)
      {
      case TaskType::TopkTaskStart:
        delete GetMessageData<TopkTaskStartMessageData*>();
        break;
      case TaskType::Topk:
        delete GetMessageData<TopkMessageData*>();
        break;
      case TaskType::Merge:
        delete GetMessageData<MergeMessageData*>();
        break;
      case TaskType::MergeDone:
        delete GetMessageData<MergeDoneMessageData*>();
        break;
      case TaskType::Combine:
        delete GetMessageData<CombineMessageData*>();
        break;
      case TaskType::DiskWrite:
        delete GetMessageData<DiskWriteMessageData*>();
        break;
      default:
        break;
      }
      data_ = nullptr;
    }
  };
  TaskType GetTaskType() {
    return type_;
  };
  const char* GetTaskTypeName() const;
  template<typename T>
  T* GetMessageData() const {
    return reinterpret_cast<T*>(data_);
  };
  std::string DumpString() const;
private:
  TaskType type_;
  void* data_;
};


} // namespace textsort

#endif // TASK_MESSAGE_H_
