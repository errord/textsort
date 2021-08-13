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

#include <thread.h>

namespace textsort {

void* ThreadFn(void* arg) {
  USE_THREAD((TaskThread*)arg);
  thread->SetThreadState(TaskThread::ThreadState::Running);
  LOG_LOCK(std::cout << "TaskThread start Id: " << thread->ThreadPoolId() << std::endl);
  thread->RunTask();
  LOG_LOCK(std::cout << "TaskThread done Id: " << thread->ThreadPoolId() << std::endl);
  thread->SetThreadState(TaskThread::ThreadState::Done);
  return nullptr;
}

void TaskThread::RunTask() {
  tasks_->ForEach([](Task* task) {
    task->RunTaskBase();
  });
}

bool TaskThread::StartThread() {
  SetThreadState(ThreadState::PreStart);
  IsRunning();
  int tret = pthread_create(&pthread_id, NULL, ThreadFn, (void*)this);
  if (tret != 0) {
    std::cout << "TaskThread create thread failed!" << std::endl;
    return false;
  }
  return true;
}

TaskThread* ThreadPool::AddTask(Task* task) {
  LinkList<Task*>* list = new LinkList<Task*>;
  list->Push(task);
  return AddTask(list);
}

TaskThread* ThreadPool::AddTask(LinkList<Task*>* tasks) {
  TaskThread* thread_ptr = new TaskThread(this, ++pool_max_id_, tasks);
  if (thread_head_ptr_ == nullptr) {
    thread_head_ptr_ = thread_ptr;
    thread_tail_ptr_ = thread_ptr;
  } else {
    thread_tail_ptr_->SetNextThread(thread_ptr);
    thread_tail_ptr_ = thread_ptr;
  }
  return thread_ptr;
}

void ThreadPool::RunTask(Task* task) {
  TaskThread* thread_ptr = AddTask(task);
  thread_ptr->StartThread();
}

void ThreadPool::RunTask(LinkList<Task*>* tasks) {
  TaskThread* thread_ptr = AddTask(tasks);
  thread_ptr->StartThread();
}

void ThreadPool::RunTasks() {
  if (thread_head_ptr_ == nullptr) {
    return;
  }
  TaskThread* thread_ptr = thread_head_ptr_;
  while (thread_ptr != nullptr) {
    if (thread_ptr->IsInitial()) {
      thread_ptr->StartThread();
    }
    thread_ptr = thread_ptr->NextThread();
  }
}

void ThreadPool::WaitTaskDone() {
  TaskThread* thread_ptr = thread_head_ptr_;
  while (thread_ptr != nullptr) {
    pthread_join(thread_ptr->PthreadId(), nullptr);
    thread_ptr = thread_ptr->NextThread();
  }
}

void ThreadPool::Close() {
  if (thread_head_ptr_ != nullptr) {
    while (thread_head_ptr_ != nullptr) {
      TaskThread* ptr = thread_head_ptr_;
      thread_head_ptr_ = thread_head_ptr_->NextThread();
      delete ptr;
    }
    thread_tail_ptr_ = nullptr;
  }
}


} // namespace textsort
