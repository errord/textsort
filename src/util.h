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

#ifndef UTIL_H_
#define UTIL_H_

#include <iostream>
#include <sys/time.h>
#include <unistd.h>
#include <string>
#include <functional>

namespace textsort {

#define ptr_t long unsigned int

struct Config {
  int debug;
  enum LoadDataMode {
    Sequence,
    Parallel
  };
  std::string input_file_path;
  std::string output_file_path;
  int block_num;
  int init_merge_thread_num;
  size_t fileblock_line_num;
  size_t top_k;
  LoadDataMode load_data_mode;
  bool sequence_load_immediate;
  size_t write_buff_size;
};

template<typename T>
struct LinkNode {
  T value;
  LinkNode<T>* next_ptr;
  LinkNode() : next_ptr(nullptr) {};
};

template<typename T>
class LinkList {
public:
  LinkList()
    : list_head_ptr_(nullptr), list_tail_ptr_(nullptr), count_(0) {};
  ~LinkList() {
    Clear();
  };
  // add to tail
  void Append(T t) {
    LinkNode<T>* node = new LinkNode<T>;
    node->value = t;
    if (list_tail_ptr_ == nullptr) {
      list_head_ptr_ = node;
      list_tail_ptr_ = node;
    } else {
      list_tail_ptr_->next_ptr = node;
      list_tail_ptr_ = node;
    }
    count_++;
  };
  // add to head
  void Push(T t) {
    LinkNode<T>* node = new LinkNode<T>;
    node->value = t;
    if (list_head_ptr_ == nullptr) {
      list_head_ptr_ = node;
      list_tail_ptr_ = node;
    } else {
      list_head_ptr_->next_ptr = node;
      list_head_ptr_ = node;
    }
    count_++;
  };
  bool Pop(T* t) {
    if (list_head_ptr_ == nullptr) {
      return false;
    }
    *t = list_head_ptr_->value;
    list_head_ptr_ = list_head_ptr_->next_ptr;
    count_--;
    return true;
  }
  void ForEach(std::function<void(T&)> func) {
    LinkNode<T>* ptr = list_head_ptr_;
    while (ptr != nullptr) {
      func(ptr->value);
      ptr = ptr->next_ptr;
    }
  };
  size_t Count() const {
    return count_;
  };
  void Clear() {
    while (list_head_ptr_ != nullptr) {
      LinkNode<T>* ptr = list_head_ptr_;
      list_head_ptr_ = list_head_ptr_->next_ptr;
      delete ptr;
      count_--;
    }
    list_tail_ptr_ = nullptr;
  };
private:
  LinkNode<T>* list_head_ptr_;
  LinkNode<T>* list_tail_ptr_;
  size_t count_;
};

class Time {
public:
  Time() {};
  Time(bool start) {
    if (start) {
      Start();
    }
  };

  static void Sleep(int milliseconds);

  void Start();
  void ResetSnap();
  std::string Snap();
  std::string Total();

private:
  std::string GetMsg(struct timeval& start, struct timeval& end);

  struct timeval start_;
  struct timeval snap_;
};

std::string PtrToHex(ptr_t num);
struct Config* GetConfig();

void Loglock();
void UnLoglock();

#ifdef DEBUG
#define Debug_log(LEVEL, LOG) {             \
    if (LEVEL <= GetConfig()->debug) {      \
      LOG;                                  \
    }                                       \
  } while (0)

#define Debug_log_1(LOG) Debug_log(1, LOG) // trace main flow
#define Debug_log_2(LOG) Debug_log(2, LOG) // trace core logic log
#define Debug_log_3(LOG) Debug_log(3, LOG) // trace performance, time
#define Debug_log_4(LOG) Debug_log(4, LOG) // trace memory and metrics
#define Debug_log_5(LOG) Debug_log(5, LOG) // text detail
#define MetricsCodeBlock(CODEBLOCK) Debug_log(4, CODEBLOCK)
#else
#define Debug_log(LEVEL, LOG)
#define Debug_log_1(LOG)
#define Debug_log_2(LOG)
#define Debug_log_3(LOG)
#define Debug_log_4(LOG)
#define Debug_log_5(LOG)
#define MetricsCodeBlock(CODEBLOCK)
#endif // TS_DEBUG


} // namespace textsort

#endif // UTIL_H_
