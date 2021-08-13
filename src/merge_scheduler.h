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

#ifndef MERGE_SCHEDULER_H_
#define MERGE_SCHEDULER_H_

#include <sort.h>
#include <thread.h>
#include <queue.h>
#include <util.h>
#include <task_message.h>

namespace textsort {

class Schedule;

/// TODO: Array Linker
struct TaskNode {
  static int ref_count;
  enum Type {
    Init = 0,
    TopK,
    Merge,
    Watch,
  };
  Type type;
  union {
    // A SortArraySlice value. Valid iff `type_ == TopK`.
    SortArraySlice sort_slice;
    // A pointer to a SortArray. Valid iff `type_ == Merge`.
    SortArray* sort_array_ptr;
    // two pointer to two task and task start time. Valid iff `type_ == Watch`.
    struct {
      size_t merge_seq_id;
      TaskNode* one_task_ptr;
      TaskNode* two_task_ptr;
      Time run_start_time;
    } watch_tasks;
  };
  Time create_time;
  TaskNode* pre_task_ptr;
  TaskNode* next_task_ptr;

  TaskNode()
    : type(Init), pre_task_ptr(nullptr), next_task_ptr(nullptr), create_time(true) {
    ref_count++;
    Debug_log_4(std::cout << "New TaskNode global RefCount: " << ref_count << std::endl);
    };
  ~TaskNode() {
    ref_count--;
    Debug_log_4(std::cout << "Delete TaskNode global RefCount: " << ref_count << std::endl);
  };
};

struct ScheduleMetrics {
  int schedule_run_num;
  int run_merge_task_num;
  int combine_num;
  int add_topk_task_num;
  int add_mergedone_task_num;
  int add_watch_num;
  int remove_watch_num;
  int merge_task_topk_topk_num;
  int merge_task_topk_merge_num;
  int merge_task_merge_topk_num;
  int merge_task_merge_merge_num;
  std::string PrintMetrics(Schedule* schedule_ptr);
};

class Schedule {
public:
  Schedule() : Schedule(nullptr) {};
  Schedule(MergeScheduler* merge_scheduler)
    : merge_scheduler_ptr_(merge_scheduler), run_queue_head_ptr_(nullptr),
      run_queue_tail_ptr_(nullptr), watch_queue_head_ptr_(nullptr),
      watch_queue_tail_ptr_(nullptr), batch_id_(-1), topk_task_num_(0),
      topk_task_done_num_(0), schedule_done_(false), topk_task_batch_num_(0),
      metrics_({0}) {};
  ~Schedule() {
    ReleaseQueue();
  }
  Schedule& operator=(Schedule&& src) {
    merge_scheduler_ptr_ = src.merge_scheduler_ptr_;
    run_queue_head_ptr_ = src.run_queue_head_ptr_;
    run_queue_tail_ptr_ = src.run_queue_tail_ptr_;
    watch_queue_head_ptr_ = src.watch_queue_head_ptr_;
    watch_queue_tail_ptr_ = src.watch_queue_tail_ptr_;
    topk_task_num_ = src.topk_task_num_;
    topk_task_done_num_ = src.topk_task_done_num_;
    topk_task_batch_num_ = src.topk_task_batch_num_;
    schedule_done_ = src.schedule_done_;
    batch_id_ = src.batch_id_;
    metrics_ = src.metrics_;
    src.run_queue_head_ptr_ = nullptr;
    src.run_queue_tail_ptr_ = nullptr;
    src.watch_queue_head_ptr_ = nullptr;
    src.watch_queue_tail_ptr_ = nullptr;
    src.topk_task_num_ = 0;
    src.topk_task_done_num_ = 0;
    src.batch_id_ = -1;
    src.topk_task_batch_num_ = 0;
    src.schedule_done_ = false;
    src.metrics_ = {0};
    return *this;
  }
  // Topk Task to run queue
  void AddTaskToRun(const SortArraySlice& slice);
  // Merge Task to run queue
  void AddTaskToRun(size_t merge_seq_id, const SortArray* sort_array);
  bool ScheduleTask(bool allow_combine);
  void SetSchedulerInfo(
    int batch_id, int topk_task_num, MergeScheduler* merge_scheduler) {
    batch_id_ = batch_id;
    topk_task_num_ = topk_task_num;
    merge_scheduler_ptr_ = merge_scheduler;
  }
  void UpdataTopkTaskDoneNum(int num) {
    topk_task_done_num_ = num;
  }
  bool ScheduleIsDone() const {
    return schedule_done_;
  }
private:
  friend struct ScheduleMetrics;
  friend struct SchedulerMetrics;
  void ScheduleCombineTask(TaskNode* one);
  void ScheduleMergeTask(TaskNode* one, TaskNode* two, size_t merge_seq_id);
  void EnRunQueue(TaskNode* task);
  void AddWatchTask(size_t merge_seq_id, TaskNode* one_task, TaskNode* two_task);
  void ReleaseQueue();
  void ReleaseRunQueue();
  void ReleaseWatchQueue();
  bool RemoveWatchTask(size_t merge_seq_id);
  bool TopkTaskRecvDone() {
    return topk_task_num_ == topk_task_done_num_;
  }
  bool TopkBatchDone() {
    return topk_task_num_ == topk_task_batch_num_;
  }
  bool AllWatchTaskDone() {
    return watch_queue_head_ptr_ == nullptr;
  }
  MergeScheduler* merge_scheduler_ptr_;
  TaskNode* run_queue_head_ptr_; // singly linked list 
  TaskNode* run_queue_tail_ptr_;
  TaskNode* watch_queue_head_ptr_; // double linked list 
  TaskNode* watch_queue_tail_ptr_;
  bool schedule_done_;
  int batch_id_;
  int topk_task_num_;
  int topk_task_done_num_;
  int topk_task_batch_num_;
  ScheduleMetrics metrics_;
};

struct SchedulerMetrics {
  int topk_task_start_num;
  int topk_task_done_num;
  int topk_task_num;
  int merge_task_num;
  int merge_done_num;
  int combine_num;

  std::string PrintMetrics(MergeScheduler* scheduler_ptr);
};

/// TODO: Task and worker thread monitoring and failedover
class MergeScheduler {
public:
  MergeScheduler()
    : schedule_list_ptr_(nullptr), topk_task_num_(0), topk_batch_num_(0),
      topk_task_done_num_(0), in_queue_(), out_queue_(), combine_queue_(),
      merge_seq_id_(0), topk_slice_ptr_list_(), metrics_({0}), task_thread_num_(0) {};
  ~MergeScheduler() {
    ReleaseResouce();
  };
  void InitSchedule(int topk_task_num);
  /// Message TopkTaskStart
  void TopkTaskStartToSchedule(int task_id, int batch_count) {
    TopkTaskStartMessageData* msg_data = new TopkTaskStartMessageData(task_id, batch_count);
    TaskMessage* msg = new TaskMessage(
      TaskMessage::TaskType::TopkTaskStart, static_cast<void*>(msg_data));
    PutMessageToSchedule(msg);
  };
  /// Message Topk
  void TopkToSchedule(int task_id, int batch_id, SortArraySlice slice) {
    TopkMessageData* msg_data = new TopkMessageData(
      task_id, batch_id, slice);
    TaskMessage* msg = new TaskMessage(
      TaskMessage::TaskType::Topk, static_cast<void*>(msg_data));
    PutMessageToSchedule(msg);
  };
  /// Message TopkTaskDone
  void TopkTaskDoneToSchedule() {
    TaskMessage* msg = new TaskMessage(
      TaskMessage::TaskType::TopkTaskDone);
    PutMessageToSchedule(msg);
  };
  /// Message Combine
  void CombineToCombineTask(int batch_id, SortArray* sort_array) {
    CombineMessageData* msg_data = new CombineMessageData(
      batch_id, sort_array);
    TaskMessage* msg = new TaskMessage(
      TaskMessage::TaskType::Combine, static_cast<void*>(msg_data));
    PutCombineMessage(msg);
  };

  /// Message Merge
  void ScheduleMergeTaskToOutQueue(size_t merge_seq_id,
    int batch_id, SortArraySlice sort_slice, SortArray* sort_array);
  void ScheduleMergeTaskToOutQueue(size_t merge_seq_id,
    int batch_id, SortArray* sort_array, SortArraySlice sort_slice);
  void ScheduleMergeTaskToOutQueue(size_t merge_seq_id,
    int batch_id, SortArray* sort_array_one, SortArray* sort_array_two);
  void ScheduleMergeTaskToOutQueue(size_t merge_seq_id,
    int batch_id, SortArraySlice sort_slice_one, SortArraySlice sort_slice_two);

  /// Message MergeDone
  void MergeDoneToSchedule(int merge_task_tid, size_t merge_seq_id, int batch_id, SortArray* sort_array) {
    MergeDoneMessageData* msg_data = new MergeDoneMessageData(
      merge_task_tid, merge_seq_id, batch_id, sort_array);
    TaskMessage* msg = new TaskMessage(
      TaskMessage::TaskType::MergeDone, static_cast<void*>(msg_data));
    PutMessageToSchedule(msg);
  };

  /// Message RegisterTaskThread
  void RegisterTaskThreadToSchedule() {
    TaskMessage* msg = new TaskMessage(TaskMessage::TaskType::RegisterTaskThread);
    PutMessageToSchedule(msg);  
  };

  /// Message TaskThreadExit
  void TaskThreadExitToOutQueue() {
    TaskMessage* msg = new TaskMessage(TaskMessage::TaskType::TaskThreadExit);
    PutMessageToOutQueue(msg);    
  };

  /// Put message to schedule
  void PutMessageToSchedule(TaskMessage* msg) {
    in_queue_.Enqueue(msg);
  };
  /// Put message to out queue
  void PutMessageToOutQueue(TaskMessage* msg) {
    out_queue_.Enqueue(msg);
  };
  TaskMessage* GetMessageFromSchedule() {
    return out_queue_.Dequeue();
  };
  TaskMessage* GetCombineMessageFromSchedule() {
    return combine_queue_.Dequeue();
  };
  void CombineExitToCombineTask() {
    TaskMessage* msg = new TaskMessage(TaskMessage::TaskType::CombineExit);
    PutCombineMessage(msg);
  }
  void PutCombineMessage(TaskMessage* msg) {
    combine_queue_.Enqueue(msg);
  };
  size_t GenMergeSeqId() {
    return ++merge_seq_id_;
  };
  size_t MergeSeqId() const {
    return merge_seq_id_;
  }
  SchedulerMetrics* GetMetrics() {
    return &metrics_;
  };
  void Scheduler();
private:
  friend struct SchedulerMetrics;
  bool ScheduleTask();
  void ReleaseResouce();
  void ProcessTopkTaskStart(TaskMessage* msg, std::string& task_info);
  void ProcessTopkTaskDone(TaskMessage* msg, std::string& task_info);
  void ProcessTopk(TaskMessage* msg, std::string& task_info);
  void ProcessMergeDone(TaskMessage* msg, std::string& task_info);
  Schedule* GetSchedule(int idx) {
    return schedule_list_ptr_ + idx;
  };
  Schedule* schedule_list_ptr_;
  int task_thread_num_;
  int topk_task_num_;
  int topk_task_done_num_;
  int topk_batch_num_;
  size_t merge_seq_id_;
  LinkList<SortArraySlice> topk_slice_ptr_list_;
  FIFOQueue<TaskMessage> in_queue_;
  FIFOQueue<TaskMessage> out_queue_;
  FIFOQueue<TaskMessage> combine_queue_;
  SchedulerMetrics metrics_;
};

} // namespace textsort

#endif // MERGE_SCHEDULER_H_
