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

#include <util.h>
#include <sort.h>

namespace textsort {

SortArray::SortArray(SortArraySlice& slice) {
  size_ = slice.count;
  count_ = slice.count;
  items_ptr_ = AllocationTrace<SortItem>::NewArray(size_);
  memcpy(items_ptr_, slice.start, sizeof(SortItem) * size_);
}

SortArraySlice SortArray::ToSlice() {
  SortArraySlice slice;
  slice.start = GetSortItems();
  slice.count = Count();
  slice.sort_array_ptr = this;
  return slice;
}

SortArraySlice SortArray::ToSlice(size_t count, bool self) {
  SortArraySlice slice;
  slice.start = GetSortItems();
  slice.count = count;
  slice.sort_array_ptr = self ? this : nullptr;
  return slice;
}

/// TODO: test inline performance
inline void TopkSort::MinHeap(SortItem* first, SortItem* target, SortItem* last)
{
    Debug_log_5(std::cout << "MinHeap start:\n" << sort_array_->DebugString() << std::endl;);
    const SortItem temp = *target;
    --first;
    SortItem* child;
    for (; (child = target + (target - first)) <= last; target = child) {
      if (child < last && Compare(&child->line, &(child + 1)->line))
          ++child;
      if (Compare(&temp.line, &child->line)) {
          memcpy(target, child, sizeof(SortItem));
      } else {
          break;
      }
    }
    memcpy(target, &temp, sizeof(SortItem));
    Debug_log_5(std::cout << "MinHeap end:\n" << sort_array_->DebugString() << std::endl;);
}

void TopkSort::HeapSort(SortItem* begin, SortItem* end) {
  size_t count = end - begin;
  if (count < 2) {
    return;
  }

  end--;
  for (SortItem* ptr = begin + (count / 2 - 1); ptr >= begin; ptr--) {
    MinHeap(begin, ptr, end);
  }

  while (end > begin) {
    Swap(begin, end--);
    MinHeap(begin, begin, end);
  }
}

void TopkSort::CreateHeap() {
  size_t count = topk_end_ - topk_begin_;
  if (count < 2) {
    return;
  }
  topk_end_--;
  for (SortItem* ptr = topk_begin_ + (count / 2 - 1); ptr >= topk_begin_; ptr--) {
    MinHeap(topk_begin_, ptr, topk_end_);
  }
}

size_t TopkSort::AdjustHeap(size_t k) {
  size_t t = k;
  while (topk_end_ > topk_begin_) {
    Swap(topk_begin_, topk_end_);
    topk_end_--;
    MinHeap(topk_begin_, topk_begin_, topk_end_);
    k--;
    if (k <= 0) {
      return k;
    }
  }
  // The last swap is an ordered swap of two elements
  // t equals k, no sorting
  return t == k ? k : --k;
}

/// use heap sort
SortArraySlice TopkSort::GetTopK(size_t k) {
  assert(sort_array_ != nullptr);
  SortArraySlice slice;
  // not init
  if (topk_begin_ == nullptr) {
    Time time(true);
    topk_begin_ = sort_array_->GetSortItems();
    // index by zero, top_end_ point last element + 1
    topk_end_ = sort_array_->GetSortItems() + sort_array_->Count();
    CreateHeap();
    Debug_log_4(std::cout << "GetTopK create heap time: " << time.Total() << std::endl;);
  }
  slice.start = topk_end_;
  size_t n = AdjustHeap(k);
  if (n == 0) {
    slice.count = k;
  } else {
    slice.count = k - n;
  }

  // The last slice sets the sort array ptr for deletion
  // like Move Semantics
  if (topk_end_ <= topk_begin_) {
    slice.sort_array_ptr = sort_array_;
    sort_array_ = nullptr;
    topk_begin_ = nullptr;
    topk_end_ = nullptr;
  }

  return slice;
}

void MergeSort::MergeSortArraySlice(SortArraySlice* one, SortArraySlice* two) {
  assert(one != nullptr);
  assert(two != nullptr);

  // SortArraySlice from min heap is descending order
  MergeSortItems(one->start, one->count, -1, two->start, two->count, -1);

  // Delete SortArray
  if (one->sort_array_ptr) {
    delete one->sort_array_ptr;
    one->sort_array_ptr = nullptr;
  }
  if (two->sort_array_ptr) {
    delete two->sort_array_ptr;
    two->sort_array_ptr = nullptr;
  }
}

void MergeSort::MergeSortArraySlice(SortArraySlice* one) {
  assert(one != nullptr);
  assert(merge_array_ != nullptr);

  SortArray* old = merge_array_;
  merge_array_ = nullptr;

  MergeSortItems(
    old->GetSortItems(), old->Count(), 1,
    one->start, one->count, -1);

  // Delete SortArray
  if (one->sort_array_ptr) {
    delete one->sort_array_ptr;
    one->sort_array_ptr = nullptr;
  }
  delete old;
}

void MergeSort::MergeSortArray(SortArray* one, SortArray* two) {
  assert(one != nullptr);
  assert(two != nullptr);

  // SortArray from lower level merge is ascending order
  MergeSortItems(
    one->GetSortItems(), one->Count(), 1,
    two->GetSortItems(), two->Count(), 1);

  // Delete lower level SortArray
  delete one;
  delete two;
}

void MergeSort::MergeAndDeleteSortArray(SortArray* one, SortArraySlice* two, int two_offset) {
  assert(one != nullptr);
  assert(two != nullptr);

  MergeSortItems(
    one->GetSortItems(), one->Count(), 1,
    two->start, two->count, two_offset);

  // Only delete SortArray(one)
  delete one;
}

void MergeSort::MergeSortArray(SortArray* one) {
  assert(one != nullptr);
  assert(merge_array_ != nullptr);

  SortArray* old = merge_array_;
  merge_array_ = nullptr;
  
  MergeSortItems(
    one->GetSortItems(), one->Count(), 1,
    old->GetSortItems(), old->Count(), 1);

  delete one;
  delete old;
}

void MergeSort::MergeSortItems(
    SortItem* one_ptr, size_t one_count, int one_offset,
    SortItem* two_ptr, size_t two_count, int two_offset) {
  assert(merge_array_ == nullptr);
  SortItem* merge_ptr;
  merge_array_ = new SortArray(one_count + two_count);
  merge_ptr = merge_array_->GetSortItems();

  while (one_count > 0 && two_count > 0) {
    if (Compare(&one_ptr->line, &two_ptr->line)) {
      memcpy(merge_ptr, two_ptr, sizeof(SortItem));
      two_ptr += two_offset;
      two_count--;
    } else {
      memcpy(merge_ptr, one_ptr, sizeof(SortItem));
      one_ptr += one_offset;
      one_count--;
    }
    merge_ptr++;
    merge_array_->AddCount();
  }

  if (one_count > 0) {
    merge_array_->AddCount(one_count);
    // Same direction, direct copy
    if (one_offset < 0) {
      while (one_count--) {
        memcpy(merge_ptr++, one_ptr, sizeof(SortItem));
        one_ptr += one_offset;
      };      
    } else {
      memcpy(merge_ptr, one_ptr, sizeof(SortItem) * one_count);
    }
  }
  if (two_count > 0) {
    merge_array_->AddCount(two_count);
    if (two_offset < 0) {
      while (two_count--) {
        memcpy(merge_ptr++, two_ptr, sizeof(SortItem));
        two_ptr += two_offset;
      }
    } else {
      memcpy(merge_ptr, two_ptr, sizeof(SortItem) * two_count);
    }
  }
}

std::string SortArraySlice::DebugString() const {
  std::string debug_str =
      "<SortArraySlice Ptr: " + PtrToHex(reinterpret_cast<ptr_t>(this)) +
      " Items Ptr: " + PtrToHex(reinterpret_cast<ptr_t>(start)) +
      " Last: " + (sort_array_ptr != nullptr ? "True" : "False") +
      " Count: " + std::to_string(count) + " Lines:\n";
  SortItem *p = start;
  for (int i = 0; i < count; i++) {
    debug_str += " " + std::to_string(i+1) + ". "
    + std::string(p->line.Ptr(), p->line.length)
    + "\n";
    p--;
  }
  debug_str += ">\n";
  return debug_str;
}

std::string SortArray::DebugString(bool inverted) const {
  std::string debug_str =
      "<SortArray Ptr: " + PtrToHex(reinterpret_cast<ptr_t>(this)) +
      " Items Ptr: " + PtrToHex(reinterpret_cast<ptr_t>(items_ptr_)) +
      " Size: " + std::to_string(size_) +
      " Count: " + std::to_string(count_) + " Lines:\n";
  auto lamdba = [&](int i, int n) {
    debug_str += " " + std::to_string(n) + ". "
    + "(" + std::to_string(items_ptr_[i].idx) + ") "
    + std::string(items_ptr_[i].line.Ptr(), items_ptr_[i].line.length)
    + "\n";
  };
  if (!inverted) {
    for (int i = 0, n = 1; i < count_; i++, n++) { lamdba(i, n); }
  } else {
    for (int i = count_ - 1, n = 1; i >= 0; i--, n++) { lamdba(i, n); }
  }
  debug_str += ">\n";
  return debug_str;
}

} // namespace textsort
