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

#include <tasks.h>
#include <thread.h>
#include <sort.h>
#include <merge_scheduler.h>
#include <combine.h>
#include <disk_writer.h>

namespace textsort {

void Task::RunTaskBase() {
  TaskThread* thread = Thread();
  int tid = thread->ThreadPoolId();
  Debug_log(0, std::cout << "Tid: " << tid << " " << GetTaskName() << " RunTask Start" << std::endl;);
  RunTask();
  Debug_log(0, std::cout << "Tid: " << tid << " " << GetTaskName() << " RunTask End" << std::endl;);
};

void Task::RegisterTaskThreadToSchedule() {
  merge_scheduler_ptr_->RegisterTaskThreadToSchedule();
}

void SortTask::RunTask() {
  //TaskThread* thread = Thread();
  assert(sort_array_->Count() > 0);
  USE_THREAD(Thread());
  Time time(true);
  size_t block_size = fblock_ptr_->GetBlockSize();
  Debug_log_2(std::cout << "block size: " << block_size 
            << " lines: " << line_num_ << " " << time.Snap() << std::endl;);
  TopkSort sort;
  if (sort_array_ == nullptr) {
    sort.InitTopkSort(fblock_ptr_, line_num_);
    Debug_log_2(std::cout << "Create TopSort readlins " << time.Snap() << std::endl;);
  } else {
    sort.InitTopkSort(fblock_ptr_, sort_array_);
  }

  // send topk task start message
  int batch_size =
    (sort_array_->Count() / top_k_) + ((sort_array_->Count() % top_k_) > 0 ? 1 : 0);
  GetMergeScheduler()->TopkTaskStartToSchedule(task_id_, batch_size);

  SortArraySlice slice;
  int batch_id = 0;
  while (1) {
    if (sort_array_->Count() > 1) {
      slice = sort.GetTopK(top_k_);
    } else {
      slice.count = 1;
      slice.sort_array_ptr = sort_array_;
      slice.start = sort_array_->GetSortItems();
    }
    LOG_LOCK(Debug_log_2(std::cout << "Tid: " << task_id_ << " batch id: " << batch_id
      << " GetTopK count: " << slice.count << " "
      << time.Snap() << std::endl));
    if (slice.count > 0) {
      GetMergeScheduler()->TopkToSchedule(task_id_, batch_id++, slice);
    }
    if (slice.sort_array_ptr != nullptr) {
      break;
    }
  }
  GetMergeScheduler()->TopkTaskDoneToSchedule();
}

void MergeTask::RunTask() {
  TaskThread* thread = Thread();
  int tid = thread->ThreadPoolId();
  MergeSort merge_sort;
  Time time;
  while (1) {
    time.Start();
    Debug_log_2(std::cout << "MergeTask::RunTask tid: " << tid << " Get Message start" << std::endl;);
    TaskMessage* msg = GetMergeScheduler()->GetMessageFromSchedule();
    Debug_log_2(std::cout << "MergeTask::RunTask tid: " << tid << " Get Message end " << time.Total() << std::endl;);
    if (msg->GetTaskType() == TaskMessage::TaskThreadExit) {
      delete msg;
      break;
    }
    time.Start();
    auto* data = msg->GetMessageData<MergeMessageData>();
    switch (data->type) {
      case MergeMessageData::SliceAndSlice: {
        SortArraySlice one = data->one.sort_slice;
        SortArraySlice two = data->two.sort_slice;
        merge_sort.MergeSortArraySlice(&one, &two);
        }
        break;
      case MergeMessageData::ArrayAndArray: {
        SortArray* one = data->one.sort_array_ptr;
        SortArray* two = data->two.sort_array_ptr;
        merge_sort.MergeSortArray(one, two);
        }
        break;
      case MergeMessageData::SliceAndArray: {
        SortArraySlice one = data->one.sort_slice;
        SortArray* two = data->two.sort_array_ptr;
        merge_sort.MergeAndDeleteSortArray(two, &one, -1);
        }
        break;
      case MergeMessageData::ArrayAndSlice: {
        SortArray* one = data->one.sort_array_ptr;
        SortArraySlice two = data->two.sort_slice;
        SortItem* item1 = two.start;
        SortItem* item2 = two.start+1;
        merge_sort.MergeAndDeleteSortArray(one, &two, -1);
        }
        break;
      default:
        break;
    }
    SortArray* sort_array = merge_sort.MoveMergeArray();
    GetMergeScheduler()->MergeDoneToSchedule(
      tid, data->merge_seq_id, data->batch_id, sort_array);
    Debug_log_2(std::cout << "*MergeTask::RunTask* " << "Tid: " << tid <<" process message: [" << data->GetTaskTypeName() << "] "
                          << " batch: " << data->batch_id << " " << time.Total() << std::endl;);
    delete msg;
  }
}

void MergeScheduleTask::RunTask() {
  GetMergeScheduler()->Scheduler();
}

void CombineTask::RunTask() {
  TaskThread* thread = Thread();
  int tid = thread->ThreadPoolId();
  int last_batch_id = 0;
  size_t total_items = 0;
  Combine combine;
  while (1) {
    TaskMessage* msg = GetMergeScheduler()->GetCombineMessageFromSchedule();
    if (msg->GetTaskType() == TaskMessage::CombineExit) {
      // send last SortArray and exit message
      SortArraySlice slice = combine.GetLastSlice();
      disk_writer_->PutDiskWriteMessage(last_batch_id, slice);
      disk_writer_->PutExitMessage();
      delete msg;
      break;
    } else { // process TaskMessage::Combine
      auto* data = msg->GetMessageData<CombineMessageData>();
      last_batch_id = data->batch_id;
      size_t count = data->sort_array->Count();
      total_items += count;
      SortArraySlice slice = combine.CombineSortArray(data->sort_array);
      if (slice.start) {
        disk_writer_->PutDiskWriteMessage(last_batch_id, slice);
      }
      Debug_log_1({
        std::cout << "CombineTask Tid: " << tid
                  << " Combine process message"
                  << " batch: " << last_batch_id
                  << " total size: " << total_items
                  << " sarray count: " << count
                  << " diskwrite: " << (slice.start == nullptr ? "False" : "True")
                  << std::endl;
      });
    }
    delete msg;
  }
  Debug_log_1({
    std::cout << "CombineTask Tid: " << tid
              << " Exit total size: " << total_items << std::endl;
  });
}

void DiskWriteTask::RunTask() {
  disk_writer_->RunDiskWriter();
}

} // namespace textsort
