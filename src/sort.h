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

#ifndef SORT_H_
#define SORT_H_

#include <allocation.h>
#include <file_io.h>
#include <string.h>

namespace textsort {

class SortArraySlice;

struct SortItem {
  Line line;
  uint32_t idx;
};

class SortArray {
public:
  SortArray(size_t size) 
    : size_(size), count_(0), items_ptr_(nullptr) {
    items_ptr_ = AllocationTrace<SortItem>::NewArray(size);
  };
  SortArray(SortArray* array) {
    size_ = array->count_;
    count_ = array->count_;
    items_ptr_ = AllocationTrace<SortItem>::NewArray(size_);
    memcpy(items_ptr_, array->items_ptr_, sizeof(SortItem) * size_);
  };
  SortArray(SortArraySlice& slice);
  ~SortArray() {
    if (items_ptr_) {
      AllocationTrace<SortItem>::DeleteArray(items_ptr_);
      items_ptr_ = nullptr;
    }
  };

  /// set self
  SortArraySlice ToSlice();
  SortArraySlice ToSlice(size_t count, bool self);

  SortItem& operator[] (size_t idx) {
    if (idx >= size_) {
      Debug_log_2(std::cout << "SortArray too long size: " << size_ << " index: " << idx << std::endl;);
    }
    return items_ptr_[idx];
  };

  SortItem* GetSortItems() const { return items_ptr_; };
  SortItem* GetSortItemsTail() const {
    assert(count_ > 0);
    return items_ptr_ + (count_ - 1);
  };

  void AddCount() { count_++; };
  void AddCount(size_t count) { count_ += count; };

  void SetCount(size_t count) {
    count_ = count;
  }

  size_t Count() const {
    return count_;
  }

  size_t Size() const {
    return size_;
  }

  std::string DebugString(bool inverted=false) const;

private:
  size_t size_;
  size_t count_;
  SortItem* items_ptr_;
};

struct SortArraySlice {
  // The sort array is the min heap. It is arranged in descending order. 
  // The start points to the tail, so the next element is the start--
  SortItem* start;
  SortArray* sort_array_ptr; // If the last slice is not nullptr, the receiver deletes sortarray
  size_t count;

  SortArraySlice() : start(nullptr), sort_array_ptr(nullptr), count(0) {};
  std::string DebugString() const;
};

class TopkSort {
public:
  TopkSort() 
    : block_(nullptr), sort_array_(nullptr),
      topk_begin_(nullptr), topk_end_(nullptr) {};

  /// Data read outside TopkSort 
  void InitTopkSort(FileBlock* block, SortArray* sort_array) {
    block_ = block;
    sort_array_ = sort_array;
  };

  /// Data read in TopkSort
  /// item_count: guess one block item total count
  void InitTopkSort(FileBlock* block, size_t item_count) {
    block_ = block;
    sort_array_ = new SortArray(item_count);
    block_->ReadLines(sort_array_);
    Time copy_time(true);
    SortArray* tp = sort_array_;
    sort_array_ = new SortArray(sort_array_);
    delete tp;
    Debug_log_2(std::cout << "InitTopkSort copy array " 
                          << copy_time.Total() << std::endl);
  };

  void Swap(SortItem* a, SortItem* b) {
    SortItem temp = *b;
    memcpy(b, a, sizeof(SortItem));
    memcpy(a, &temp, sizeof(SortItem));
  }

  void HeapSort() {
    HeapSort(sort_array_->GetSortItems(), 
      sort_array_->GetSortItems() + sort_array_->Count());
  }

  const SortArray* GetSortArray() const {
    return sort_array_;
  }

  SortArraySlice GetTopK(size_t k);
  void MinHeap(SortItem* first, SortItem* target, SortItem* last);
  void HeapSort(SortItem* beg, SortItem* end);

private:
  void CreateHeap();
  size_t AdjustHeap(size_t k);
  SortArray* sort_array_; // *memory*  Responsible for deleting the last merge
  FileBlock* block_; // *memory* Ownership is FileIo
  SortItem* topk_begin_;
  SortItem* topk_end_;
}; 

class MergeSort {
public:
  MergeSort() : merge_array_(nullptr) {};
  /// Merge for topk slice
  void MergeSortArraySlice(SortArraySlice* one, SortArraySlice* two);
  void MergeAndDeleteSortArray(SortArray* one, SortArraySlice* two, int two_offset);
  void MergeSortArraySlice(SortArraySlice* one);

  /// Merge for lower level merge sort
  void MergeSortArray(SortArray* one, SortArray* two);
  void MergeSortArray(SortArray* one);

  SortArray* GetMergeArray() const { return merge_array_; };
  SortArray* MoveMergeArray() {
    SortArray* ptr = merge_array_;
    merge_array_ = nullptr;
    return ptr;
  };
private:
  void MergeSortItems(
    SortItem* one_ptr, size_t one_count, int one_offset,
    SortItem* two_ptr, size_t two_count, int two_offset);

  // *memory* Send to advanced merge, which is responsible for delete 
  SortArray* merge_array_;
};

inline bool Compare(const Line* a, const Line* b) {
  uint8_t alen = a->length;
  uint8_t blen = b->length;
  return memcmp(a->Ptr(), b->Ptr(), alen <= blen ? alen : blen) > 0 ? true : false;
}

} // namespace textsort

#endif // SORT_H_
