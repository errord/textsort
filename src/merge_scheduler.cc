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

#include <merge_scheduler.h>

namespace textsort {

int TaskNode::ref_count = 0;

/// TODO: add impl
void Schedule::ReleaseQueue() {
  ReleaseRunQueue();
  ReleaseWatchQueue();
}

void Schedule::ReleaseRunQueue() {
  assert(run_queue_head_ptr_ == nullptr);
}

void Schedule::ReleaseWatchQueue() {
  assert(watch_queue_head_ptr_ == nullptr);
}

bool Schedule::RemoveWatchTask(size_t merge_seq_id) {
  assert(watch_queue_head_ptr_ != nullptr);
  TaskNode* task = watch_queue_head_ptr_;

  while (task != nullptr) {
    if (task->watch_tasks.merge_seq_id == merge_seq_id) {
      std::string t = task->watch_tasks.run_start_time.Total();
      Debug_log_3(std::cout << "Schedule Watch merge_seq_id: " << merge_seq_id 
                            << " task remove, task run time: " << t << std::endl;);
      delete task->watch_tasks.one_task_ptr;
      delete task->watch_tasks.two_task_ptr;
      // linker list
      if (task->pre_task_ptr == nullptr) {
        // head pointer
        watch_queue_head_ptr_ = task->next_task_ptr;
        task->next_task_ptr = nullptr;
        // head and tail
        if (watch_queue_head_ptr_ != nullptr) {
          watch_queue_head_ptr_->pre_task_ptr = nullptr;
        } else {
          watch_queue_tail_ptr_ = nullptr;
        }
      } else if (task->next_task_ptr == nullptr) {
        // tail pointer
        watch_queue_tail_ptr_ = task->pre_task_ptr;
        task->pre_task_ptr = nullptr;
        // head and tail
        if (watch_queue_tail_ptr_ != nullptr) {
          watch_queue_tail_ptr_->next_task_ptr = nullptr;
        } else {
          watch_queue_head_ptr_ = nullptr;
        }
      } else {
        task->pre_task_ptr->next_task_ptr = task->next_task_ptr;
        task->next_task_ptr->pre_task_ptr = task->pre_task_ptr;
        task->pre_task_ptr = nullptr;
        task->next_task_ptr = nullptr;
      }
      delete task;
      MetricsCodeBlock(metrics_.remove_watch_num++;);
      return true; 
    }
    task = task->next_task_ptr;
  }
  return false;
}

void Schedule::AddTaskToRun(const SortArraySlice& slice) {
  TaskNode* task = new TaskNode();
  task->type = TaskNode::Type::TopK;
  task->sort_slice = slice;
  topk_task_batch_num_++;
  EnRunQueue(task);
  MetricsCodeBlock(metrics_.add_topk_task_num++;);
}

void Schedule::AddTaskToRun(size_t merge_seq_id, const SortArray* sort_array) {
  TaskNode* task = new TaskNode();
  task->type = TaskNode::Type::Merge;
  task->sort_array_ptr = const_cast<SortArray*>(sort_array);
  EnRunQueue(task);
  MetricsCodeBlock(metrics_.add_mergedone_task_num++;);
  // recv merge result, delete merge watch task
  bool ok = RemoveWatchTask(merge_seq_id);
  assert(ok);
}

void Schedule::ScheduleCombineTask(TaskNode* one) {
  assert(one->type == TaskNode::Type::Merge || one->type == TaskNode::Type::TopK);
  SortArray* array_ptr;
  if (one->type == TaskNode::Type::Merge) {
    array_ptr = one->sort_array_ptr;
  } else { // TopK, memory is combine release
    array_ptr = new SortArray(one->sort_slice);
  }
  merge_scheduler_ptr_->CombineToCombineTask(batch_id_, array_ptr);
  run_queue_head_ptr_ = nullptr;
  // last task node
  delete one;
  schedule_done_ = true;
  MetricsCodeBlock({
    metrics_.combine_num++;
    merge_scheduler_ptr_->GetMetrics()->combine_num++;
  });
}

void Schedule::ScheduleMergeTask(TaskNode* one, TaskNode* two, size_t merge_seq_id) {
if (one->type == TaskNode::TopK && two->type == TaskNode::TopK) {
    SortArraySlice sort_slice_one = one->sort_slice;
    SortArraySlice sort_slice_two = two->sort_slice;
    merge_scheduler_ptr_->ScheduleMergeTaskToOutQueue(
      merge_seq_id, batch_id_, sort_slice_one, sort_slice_two);
    MetricsCodeBlock(metrics_.merge_task_topk_topk_num++;);
  } else if (one->type == TaskNode::TopK && two->type == TaskNode::Merge) {
    SortArraySlice sort_slice_one = one->sort_slice;
    SortArray* sort_array_two = two->sort_array_ptr;
    merge_scheduler_ptr_->ScheduleMergeTaskToOutQueue(
      merge_seq_id, batch_id_, sort_slice_one, sort_array_two);
    MetricsCodeBlock(metrics_.merge_task_topk_merge_num++;);
  } else if (one->type == TaskNode::Merge && two->type == TaskNode::TopK) {
    SortArray* sort_array_one = one->sort_array_ptr;
    SortArraySlice sort_slice_two = two->sort_slice;
    merge_scheduler_ptr_->ScheduleMergeTaskToOutQueue(
      merge_seq_id, batch_id_, sort_array_one, sort_slice_two);
    MetricsCodeBlock(metrics_.merge_task_merge_topk_num++;);
  } else if (one->type == TaskNode::Merge && two->type == TaskNode::Merge) {
    SortArray* sort_array_one = one->sort_array_ptr;
    SortArray* sort_array_two = two->sort_array_ptr;
    merge_scheduler_ptr_->ScheduleMergeTaskToOutQueue(
      merge_seq_id, batch_id_, sort_array_one, sort_array_two);
    MetricsCodeBlock(metrics_.merge_task_merge_merge_num++;);
  }
  MetricsCodeBlock({
    metrics_.run_merge_task_num++;
    merge_scheduler_ptr_->GetMetrics()->merge_task_num++;
  });
}

/// TODO: send all ready task
bool Schedule::ScheduleTask(bool allow_combine) {
  assert(merge_scheduler_ptr_ != nullptr);
  MetricsCodeBlock(metrics_.schedule_run_num++;);

  if (ScheduleIsDone()) {
    return true;
  }
  if (run_queue_head_ptr_ == nullptr) {
    return false;
  }

  TaskNode* one = run_queue_head_ptr_;

  while (one != nullptr) {
    TaskNode* two = one->next_task_ptr;
    if (two == nullptr) {
      // check last merge done task, *Schedule Done*
      if (allow_combine && (TopkBatchDone() || TopkTaskRecvDone()) && AllWatchTaskDone()) {
        ScheduleCombineTask(one);
        return true;
      }
      // wait other node
      return false;
    }
    run_queue_head_ptr_ = two->next_task_ptr;
    size_t merge_seq_id = merge_scheduler_ptr_->GenMergeSeqId();

    // task to out queue
    ScheduleMergeTask(one, two, merge_seq_id);
    // add watch
    AddWatchTask(merge_seq_id, one, two);

    one = run_queue_head_ptr_;
  }
  return false;
}

void Schedule::AddWatchTask(size_t merge_seq_id, TaskNode* one_task, TaskNode* two_task) {
  TaskNode* task = new TaskNode();
  task->type = TaskNode::Type::Watch;
  task->watch_tasks.merge_seq_id = merge_seq_id;
  task->watch_tasks.one_task_ptr = one_task;
  task->watch_tasks.two_task_ptr = two_task;
  task->watch_tasks.run_start_time.Start();
  // add task to queue tail
  if (watch_queue_head_ptr_ != nullptr) {
    watch_queue_tail_ptr_->next_task_ptr = task;
    task->pre_task_ptr = watch_queue_tail_ptr_;
    watch_queue_tail_ptr_ = task;
  } else {
    watch_queue_head_ptr_ = task;
    watch_queue_tail_ptr_ = task;
  }
  MetricsCodeBlock(metrics_.add_watch_num++;);
}

void Schedule::EnRunQueue(TaskNode* task) {
  // add task to queue tail
  if (run_queue_head_ptr_ != nullptr) {
    run_queue_tail_ptr_->next_task_ptr = task;
    run_queue_tail_ptr_ = task;
  } else {
    run_queue_head_ptr_ = task;
    run_queue_tail_ptr_ = task;
  }
}

void MergeScheduler::InitSchedule(int topk_task_num) {
  topk_task_num_ = topk_task_num;
  /// Some initialization code...
}

void MergeScheduler::ScheduleMergeTaskToOutQueue(size_t merge_seq_id,
  int batch_id, SortArraySlice sort_slice, SortArray* sort_array) {
  MergeMessageData* msg_data = new MergeMessageData();
  msg_data->type = MergeMessageData::SliceAndArray;
  msg_data->one.sort_slice = sort_slice;
  msg_data->two.sort_array_ptr = sort_array;
  msg_data->batch_id = batch_id;
  msg_data->merge_seq_id = merge_seq_id;
  TaskMessage* msg = new TaskMessage(
    TaskMessage::TaskType::Merge, static_cast<void*>(msg_data));
  PutMessageToOutQueue(msg);
}

void MergeScheduler::ScheduleMergeTaskToOutQueue(size_t merge_seq_id,
  int batch_id, SortArray* sort_array, SortArraySlice sort_slice) {
  MergeMessageData* msg_data = new MergeMessageData();
  msg_data->type = MergeMessageData::ArrayAndSlice;
  msg_data->one.sort_array_ptr = sort_array;
  msg_data->two.sort_slice = sort_slice;
  msg_data->batch_id = batch_id;
  msg_data->merge_seq_id = merge_seq_id;
  TaskMessage* msg = new TaskMessage(
    TaskMessage::TaskType::Merge, static_cast<void*>(msg_data));
  PutMessageToOutQueue(msg);
}

void MergeScheduler::ScheduleMergeTaskToOutQueue(size_t merge_seq_id,
  int batch_id, SortArray* sort_array_one, SortArray* sort_array_two) {
  MergeMessageData* msg_data = new MergeMessageData();
  msg_data->type = MergeMessageData::ArrayAndArray;
  msg_data->one.sort_array_ptr = sort_array_one;
  msg_data->two.sort_array_ptr = sort_array_two;
  msg_data->batch_id = batch_id;
  msg_data->merge_seq_id = merge_seq_id;
  TaskMessage* msg = new TaskMessage(
    TaskMessage::TaskType::Merge, static_cast<void*>(msg_data));
  PutMessageToOutQueue(msg);
}

void MergeScheduler::ScheduleMergeTaskToOutQueue(size_t merge_seq_id,
  int batch_id, SortArraySlice sort_slice_one, SortArraySlice sort_slice_two) {
  MergeMessageData* msg_data = new MergeMessageData();
  msg_data->type = MergeMessageData::SliceAndSlice;
  msg_data->one.sort_slice = sort_slice_one;
  msg_data->two.sort_slice = sort_slice_two;
  msg_data->batch_id = batch_id;
  msg_data->merge_seq_id = merge_seq_id;
  TaskMessage* msg = new TaskMessage(
    TaskMessage::TaskType::Merge, static_cast<void*>(msg_data));
  PutMessageToOutQueue(msg);
}

void MergeScheduler::ProcessTopkTaskStart(TaskMessage* msg, std::string& task_info) {
  auto* data = msg->GetMessageData<TopkTaskStartMessageData>();
  Debug_log_1({
    task_info += "tid: " + std::to_string(data->task_id);
    task_info += " batch_count: " + std::to_string(data->batch_count);
  });
  int batch_count = data->batch_count;
  if (batch_count > topk_batch_num_) {
    Debug_log_2(std::cout << "MergeScheduler realloc schedule list old: " << topk_batch_num_
              << " new: " << batch_count << std::endl;);
    Schedule* new_schedule_ptr = new Schedule[batch_count];
    // realloc new schedule, old move to new and release old schedule
    if (schedule_list_ptr_ != nullptr) {
      for (int i = 0; i < topk_batch_num_; i++) {
        new_schedule_ptr[i] = std::move(schedule_list_ptr_[i]);
      }
      delete [] schedule_list_ptr_;
    }
    for (int i = topk_batch_num_; i < batch_count; i++) {
      new_schedule_ptr[i].SetSchedulerInfo(i, topk_task_num_, this);
    }
    schedule_list_ptr_ = new_schedule_ptr;
    topk_batch_num_ = batch_count;
  }
}

void MergeScheduler::ProcessTopkTaskDone(TaskMessage* msg, std::string& task_info) {
  topk_task_done_num_++;
}

void MergeScheduler::ProcessTopk(TaskMessage* msg, std::string& task_info) {
  auto* data = msg->GetMessageData<TopkMessageData>();
  Debug_log_1({
    task_info += "tid: " + std::to_string(data->task_id);
    task_info += " batch_id: " + std::to_string(data->batch_id);
  });
  // save tail slice ptr
  if (data->slice.sort_array_ptr != nullptr) {
    topk_slice_ptr_list_.Append(data->slice);
    data->slice.sort_array_ptr = nullptr;
  }
  int batch_id = data->batch_id;
  assert(batch_id <= topk_batch_num_);
  Schedule* schedule = GetSchedule(batch_id);
  schedule->AddTaskToRun(data->slice);
}

void MergeScheduler::ProcessMergeDone(TaskMessage* msg, std::string& task_info) {
  auto* data = msg->GetMessageData<MergeDoneMessageData>();
  Debug_log_1({
    task_info += "merge_task_tid: " + std::to_string(data->merge_task_tid);
    task_info += " batch_id: " + std::to_string(data->batch_id);
    task_info += " merge_seq_id: " + std::to_string(data->merge_seq_id);
  });
  Schedule* schedule = GetSchedule(data->batch_id);
  schedule->AddTaskToRun(data->merge_seq_id, data->sort_array);
}

void MergeScheduler::ReleaseResouce() {
  Debug_log_2(
    std::cout << "Topk slice list size: " << topk_slice_ptr_list_.Count() << std::endl;);
  topk_slice_ptr_list_.ForEach([](SortArraySlice slice) {
    delete slice.sort_array_ptr;
  });
  topk_slice_ptr_list_.Clear();
  if (schedule_list_ptr_ != nullptr) {
    delete [] schedule_list_ptr_;
    schedule_list_ptr_ = nullptr;
  }

  Debug_log_4({
    if (in_queue_.Count() != 0) {
      std::cout << " Missing message in_queue: \n" << in_queue_.DumpMessage() << std::endl;
    }
    if (out_queue_.Count() != 0) {
      std::cout << " Missing message out_queue: \n" << out_queue_.DumpMessage() << std::endl;
    }
    if (combine_queue_.Count() != 0) {
      std::cout << " Missing message combine_queue: \n" << combine_queue_.DumpMessage() << std::endl;
    }
  });

  assert(in_queue_.Count() == 0);
  assert(out_queue_.Count() == 0);
  assert(combine_queue_.Count() == 0);
}

void MergeScheduler::Scheduler() {
  bool schedule_done = false;
  Time time;
  while (1) {
    TaskMessage* msg = in_queue_.Dequeue();
    std::string task_info = "";
    Debug_log_1(time.Start());
    switch (msg->GetTaskType()) {
      case TaskMessage::RegisterTaskThread:
        task_thread_num_++;
        break;
      case TaskMessage::TopkTaskStart:
        MetricsCodeBlock(metrics_.topk_task_start_num++;);
        ProcessTopkTaskStart(msg, task_info);
        break;
      case TaskMessage::TopkTaskDone:
        MetricsCodeBlock(metrics_.topk_task_done_num++;);
        ProcessTopkTaskDone(msg, task_info);
        break;
      case TaskMessage::Topk:
        MetricsCodeBlock(metrics_.topk_task_num++;);
        ProcessTopk(msg, task_info);
        break;
      case TaskMessage::MergeDone:
        MetricsCodeBlock(metrics_.merge_done_num++;);
        ProcessMergeDone(msg, task_info);
        break;
      default:
        std::cout << "MergeScheduler::Scheduler not process mssage: " 
                  << msg->GetTaskTypeName() << std::endl;
        break;
    }
    schedule_done = ScheduleTask();
    const char* type_name = msg->GetTaskTypeName();
    delete msg;
    Debug_log_1({
      std::cout << "*MergeScheduler::Scheduler*" << " process message: [" << type_name << "]"
                << " " << task_info
                << " " << time.Total();
                Debug_log_4(std::cout << "\n" << metrics_.PrintMetrics(this));
                std::cout << std::endl;
    };);
    if (schedule_done) {
      while (task_thread_num_--) {
        TaskThreadExitToOutQueue();
      }
      CombineExitToCombineTask();
      break;
    }
  }
  Debug_log_1(std::cout << "MergeScheduler::Scheduler Done, exit.." << std::endl;);
}

bool MergeScheduler::ScheduleTask() {
  if (topk_batch_num_ < 1) {
    return false;
  }
  bool schedule_done = true;
  for (int batch_id = 0; batch_id < topk_batch_num_; batch_id++) {
    Schedule* schedule = schedule_list_ptr_ + batch_id;
    schedule->UpdataTopkTaskDoneNum(topk_task_done_num_);
    schedule_done = schedule->ScheduleTask(schedule_done);
  }
  return schedule_done; 
}

} // namespace textsort
