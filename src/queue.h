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

#ifndef QUEUE_H_
#define QUEUE_H_

#include <thread.h>

namespace textsort {

template<typename T>
class QueueItem {
public:
  QueueItem() : next_ptr_(nullptr) {};
  virtual std::string DumpString() const = 0;
  void SetNext(T* ptr) {
    next_ptr_ = ptr;
  }
  T* GetNext() const {
    return next_ptr_;
  }
private:
  T* next_ptr_;
};

/// Thread safe FIFO Queue base on linker
/// The lifecycle of an element is managed by the caller
/// TODO: 1. Ring Queue 2. lock free
template<typename T>
class FIFOQueue {
public:
  FIFOQueue() 
    : queue_count_(0), queue_head_ptr_(nullptr), queue_tail_ptr_(nullptr) {};
  void Enqueue(T* element) {
    ThreadLock::Sync sync = queue_wait_lock_.GetSync();
    if (queue_head_ptr_ == nullptr) {
      queue_head_ptr_ = element;
      queue_tail_ptr_ = element;
    } else {
      queue_tail_ptr_->SetNext(element);
      queue_tail_ptr_ = element;
    }
    queue_count_++;
    queue_wait_lock_.Notify();
  };
  T* Dequeue() {
    ThreadLock::Sync sync = queue_wait_lock_.GetSync();
    while (queue_head_ptr_ == nullptr) {
      queue_wait_lock_.Wait();
    }
    T* element = queue_head_ptr_;
    queue_head_ptr_ = element->GetNext();
    queue_count_--;
    return element;
  };
  size_t Count() {
    ThreadLock::Sync sync = queue_wait_lock_.GetSync();
    return queue_count_;
  }
  std::string DumpMessage() {
    ThreadLock::Sync sync = queue_wait_lock_.GetSync();
    std::string dump = "";
    T* ptr = queue_head_ptr_;
    while (ptr != nullptr) {
      dump += "Queue Messge " + ptr->DumpString() + "\n";
      ptr = ptr->GetNext();
    }
    return dump;
  }
private:
  ThreadWait queue_wait_lock_;
  size_t queue_count_;
  T* queue_head_ptr_;
  T* queue_tail_ptr_;
};

} // namespace textsort

#endif // QUEUE_H_
