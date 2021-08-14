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

#include <text_sort.h>

namespace textsort {

/// All tasks and linklist are released by the thread  
void TextSort::BuildSequenceLoadDataPipeline(MergeScheduler* merge_scheduler) {
  Task* task = nullptr;
  // Create sort task
  FileBlock* block = LoadFileAndPartition();
  // Get the real number of tasks after partition
  merge_scheduler->InitSchedule(file_io_ptr_->GetBlockNumber());
  int task_id = 1;
  int total_line = 0;
  Time time(true);
  /// TODO: each read one block at to run the task, instead of reading all completed 
  /// to run all tasks
  while (block != nullptr) {
    SortArray* sort_array = new SortArray(config_->fileblock_line_num);
    block->ReadLines(sort_array);
    Time copy_time(true);
    SortArray* tp = sort_array;
    sort_array = new SortArray(sort_array);
    delete tp;
    total_line += sort_array->Count();
    Debug_log_2(std::cout << "Task: " << task_id
              << " SequenceLoadData block: " << block
              << " size: " << block->GetBlockSize()
              << " copy array " << copy_time.Total()
              << " " << time.Snap() << std::endl;);
    // sort task done, run merge task
    // Task is deleted when the thread completes
    LinkList<Task*>* tasks = new LinkList<Task*>;
    task = new SortTask(
      merge_scheduler, block, sort_array, config_->top_k, task_id++);
    tasks->Push(task);
    task = new MergeTask(merge_scheduler, false);
    tasks->Append(task);
    if (config_->sequence_load_immediate) {
      thread_pool_.RunTask(tasks);
    } else {
      thread_pool_.AddTask(tasks);
    }
    block = block->NextBlock();
  }
  Debug_log_1(std::cout << "SequenceLoadData total line: " << total_line << " time: " << time.Total() << std::endl;);
}

void TextSort::BuildParallelLoadDataPipeline(MergeScheduler* merge_scheduler) {
  Task* task = nullptr;
  // Create sort task
  FileBlock* block = LoadFileAndPartition();
  merge_scheduler->InitSchedule(file_io_ptr_->GetBlockNumber());
  int task_id = 1;
  while (block != nullptr) {
    // Task is deleted when the thread completes
    LinkList<Task*>* tasks = new LinkList<Task*>;
    task = new SortTask(
        merge_scheduler, block, config_->fileblock_line_num, config_->top_k, task_id++);
    tasks->Push(task);
    task = new MergeTask(merge_scheduler, false);
    tasks->Append(task);
    thread_pool_.AddTask(tasks);
    block = block->NextBlock();
  }
}

void TextSort::Start() {
  MergeScheduler merge_scheduler;
  DiskWriter disk_writer(config_->output_file_path, config_->write_buff_size);

  Task* task = nullptr;

  // create merge scheduler Task
  task = new MergeScheduleTask(&merge_scheduler);
  thread_pool_.RunTask(task);

  // create init merge Task
  for (int i = 0; i < config_->init_merge_thread_num; i++) {
    task = new MergeTask(&merge_scheduler, true);
    thread_pool_.RunTask(task);
  }

  // create combine task
  task = new CombineTask(&merge_scheduler, &disk_writer);
  thread_pool_.RunTask(task);

  // create disk write task
  task = new DiskWriteTask(&disk_writer);
  thread_pool_.RunTask(task);

  // build sort task
  switch (config_->load_data_mode)
  {
  case Config::LoadDataMode::Sequence:
    BuildSequenceLoadDataPipeline(&merge_scheduler);
    break;
  case Config::LoadDataMode::Parallel:
    BuildParallelLoadDataPipeline(&merge_scheduler);
    break;
  default:
    std::cout << "Unknow load data model (" << config_->load_data_mode << ")"
              << std::endl;
    return;
  }

  // run all task
  thread_pool_.RunTasks();
  thread_pool_.WaitTaskDone();
}

FileBlock* TextSort::LoadFileAndPartition() {
  assert(file_io_ptr_ != nullptr);
  if (!file_io_ptr_->OpenFile(config_->input_file_path.c_str(), true)) {
    return nullptr;
  }
  file_io_ptr_->LoadFile();
  textsort::FileBlock* block_ptr = file_io_ptr_->PartitionBlock(config_->block_num);
  Debug_log_1(std::cout << "PartitionBlock Preset: " << config_->block_num 
            << " actual: " << file_io_ptr_->GetBlockNumber() << std::endl;);
  return block_ptr;
}

} // namespace textsort
