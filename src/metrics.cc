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

std::string SchedulerMetrics::PrintMetrics(MergeScheduler* scheduler_ptr) {
  std::string metrics = "SchedulerMetrics: \n";
  metrics += " topk_task_num: " + std::to_string(scheduler_ptr->topk_task_num_) + "\n";
  metrics += " topk_task_done_num: " + std::to_string(scheduler_ptr->topk_task_done_num_) + "\n";
  metrics += " topk_batch_num: " + std::to_string(scheduler_ptr->topk_batch_num_) + "\n";
  metrics += " topk_task_start_num: " + std::to_string(topk_task_start_num) + "\n";
  metrics += " topk_task_done_num: " + std::to_string(topk_task_done_num) + "\n";
  metrics += " topk_task_num: " + std::to_string(topk_task_num) + "\n";
  metrics += " merge_task_num: " + std::to_string(merge_task_num) + "\n";
  metrics += " merge_done_num: " + std::to_string(merge_done_num) + "\n";
  metrics += " combine_num: " + std::to_string(combine_num) + "\n";
  metrics += " merge_seq_id: " + std::to_string(scheduler_ptr->merge_seq_id_) + "\n";
  metrics += " topk_slice_ptr_list size: " + std::to_string(scheduler_ptr->topk_slice_ptr_list_.Count()) + "\n";
  metrics += " in_queue size: " + std::to_string(scheduler_ptr->in_queue_.Count()) + "\n";
  metrics += " out_queue size: " + std::to_string(scheduler_ptr->out_queue_.Count()) + "\n";

  for (int i = 0; i < scheduler_ptr->topk_batch_num_; i++) {
    Schedule* schedule = scheduler_ptr->GetSchedule(i);
    metrics += schedule->metrics_.PrintMetrics(schedule);
  }

  return metrics;
}

std::string ScheduleMetrics::PrintMetrics(Schedule* schedule_ptr) {
  std::string metrics = "ScheduleMetrics ID: " + std::to_string(schedule_ptr->batch_id_)
    + " [" + (schedule_ptr->schedule_done_ ? "DONE" : "RUNNING") + "]\n";
  metrics += " topk_task_num: " + std::to_string(schedule_ptr->topk_task_num_) + "\n";
  metrics += " topk_task_done_num: " + std::to_string(schedule_ptr->topk_task_done_num_) + "\n";
  metrics += " topk_task_batch_num: " + std::to_string(schedule_ptr->topk_task_batch_num_) + "\n";
  metrics += " schedule_run_num: " + std::to_string(schedule_ptr->metrics_.schedule_run_num) + "\n";
  metrics += " combine_num: " + std::to_string(schedule_ptr->metrics_.combine_num) + "\n";
  metrics += " add_topk_task_num: " + std::to_string(schedule_ptr->metrics_.add_topk_task_num) + "\n";
  metrics += " add_mergedone_task_num: " + std::to_string(schedule_ptr->metrics_.add_mergedone_task_num) + "\n";
  metrics += " add_watch_num: " + std::to_string(schedule_ptr->metrics_.add_watch_num) + "\n";
  metrics += " remove_watch_num: " + std::to_string(schedule_ptr->metrics_.remove_watch_num) + "\n";
  metrics += " run_merge_task_num: " + std::to_string(schedule_ptr->metrics_.run_merge_task_num) + "\n";
  metrics += " merge_task_topk_topk_num: " + std::to_string(schedule_ptr->metrics_.merge_task_topk_topk_num) + "\n";
  metrics += " merge_task_topk_merge_num: " + std::to_string(schedule_ptr->metrics_.merge_task_topk_merge_num) + "\n";
  metrics += " merge_task_merge_topk_num: " + std::to_string(schedule_ptr->metrics_.merge_task_merge_topk_num) + "\n";
  metrics += " merge_task_merge_merge_num: " + std::to_string(schedule_ptr->metrics_.merge_task_merge_merge_num) + "\n";

  int rq_init_node_num = 0;
  int rq_topk_node_num = 0;
  int rq_merge_node_num = 0;
  int wq_task_num = 0;

  TaskNode* ptr = schedule_ptr->run_queue_head_ptr_;
  while (ptr != nullptr) {
    switch (ptr->type) {
      case TaskNode::Init:
        rq_init_node_num++;
        break;
      case TaskNode::TopK:
        rq_topk_node_num++;
        break;
      case TaskNode::Merge:
        rq_merge_node_num++;
        break;
      case TaskNode::Watch:
        std::cout << "Run queue has watch node!!!" << std::endl;
        assert(0);
        break;
      default:
        std::cout << "ScheduleMetrics: Unknow TaskNode type: "
                  << (int)ptr->type << std::endl;
        break;
    }
    ptr = ptr->next_task_ptr;
  }
  ptr = schedule_ptr->watch_queue_head_ptr_;
  while (ptr != nullptr) {
    switch (ptr->type) {
      case TaskNode::Watch:
        wq_task_num++;
        break;
      case TaskNode::Init:
      case TaskNode::TopK:
      case TaskNode::Merge:
        std::cout << "Watch queue has task node!!!" << std::endl;
        assert(0);
        break;
      default:
        std::cout << "ScheduleMetrics: Unknow TaskNode type: "
                  << (int)ptr->type << std::endl;
        break;
    }
    ptr = ptr->next_task_ptr;
  }
  metrics += " rq_init_node_num: " + std::to_string(rq_init_node_num) + "\n";
  metrics += " rq_topk_node_num: " + std::to_string(rq_topk_node_num) + "\n";
  metrics += " rq_merge_node_num: " + std::to_string(rq_merge_node_num) + "\n";
  metrics += " wq_task_num: " + std::to_string(wq_task_num) + "\n";

  return metrics;
}

} // namespace textsort
