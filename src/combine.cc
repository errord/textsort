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

#include <combine.h>

namespace textsort {

SortArraySlice Combine::GetLastSlice() {
  assert(combine_array_ != nullptr);
  return combine_array_->ToSlice();
}

SortArraySlice Combine::CombineSortArray(SortArray* sort_array) {
  assert(sort_array->Count() > 0);
  SortArraySlice slice;

  if (combine_array_ == nullptr) {
    combine_array_ = sort_array;
    return slice;
  }

  const SortArray* del_ptr = nullptr; 
  size_t sentinel = FindSentinel(sort_array->GetSortItems()[0].line);
  if (sentinel > 0) {
    slice = combine_array_->ToSlice(sentinel, true);
  } else {
    del_ptr = combine_array_;
  }
  Merge(sentinel, sort_array);
  // Not send ot other role delete, so self delete
  if (del_ptr) {
    delete del_ptr;
  }
  return slice;
}

size_t Combine::FindSentinel(const Line& line) {
  const SortItem* pos_ptr = combine_array_->GetSortItemsTail();
  size_t sentinel = combine_array_->Count();
  
  /// TODO: Other more efficient search algorithms, such as binary search
  /// current is linear search
  while (sentinel > 0) {
    if (Compare(&line, &pos_ptr->line)) {
      return sentinel;
    }
    sentinel--;
    pos_ptr--;
  }
  return sentinel;
}

void Combine::Merge(size_t sentinel, SortArray* sort_array) {
  SortArraySlice slice;
  MergeSort merge_sort;
  slice.start = combine_array_->GetSortItems() + (sentinel == 0 ? 0 : sentinel - 1);
  slice.count = combine_array_->Count() - sentinel;
  merge_sort.MergeAndDeleteSortArray(sort_array, &slice, 1);
  combine_array_ = merge_sort.MoveMergeArray();
}

} // namespace textsort
