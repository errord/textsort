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

#ifndef COMBINE_H_
#define COMBINE_H_

#include <sort.h>

namespace textsort {

class Combine {
public:
  Combine() : combine_array_(nullptr) {};

  /// The split slice, combine sort array
  /// return StableSlice. if slice.start is nullptr, this Not ready
  /// not send to other role
  SortArraySlice CombineSortArray(SortArray* sort_array);
  SortArraySlice GetLastSlice();
private:
  size_t FindSentinel(const Line& line);
  void Merge(size_t sentinel, SortArray* sort_array);
  SortArray* combine_array_;
};

} // namespace textsort

#endif // COMBINE_H_
