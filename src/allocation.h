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

#ifndef ALLOCATION_H_
#define ALLOCATION_H_

#include <util.h>
#include <atomic>
#include <assert.h>

namespace textsort {

class AllocationTotal {
public:
  static std::atomic_size_t total_count;
  static std::atomic_size_t total_size;

  static void Add() { total_count++; };
  static void Sub() { total_count--; };
  static size_t TotalCount() { return total_count; };
  static void AddSize(size_t size) { total_size += size; };
  static size_t TotalSize() { return total_size; };
};

/// TODO: rewrite new operator
template<typename T>
class AllocationTrace {
public:
  static T* New() {
    T* ptr = (T*)new T;
    assert(ptr != nullptr);
    AllocationTotal::Add();
    AllocationTotal::AddSize(sizeof(T));
    Debug_log_4({
      std::cout << "new object ptr: " << ptr << " size: " << sizeof(T) 
                << " total alloc count: " << AllocationTotal::TotalCount()
                << " total alloc size: " << AllocationTotal::TotalSize()
                << std::endl;
    });
    return ptr;
  }

  static T* NewArray(size_t n) {
    T* ptr = (T*)new T[n];
    assert(ptr != nullptr);
    AllocationTotal::Add();
    AllocationTotal::AddSize(sizeof(T) * n);
    Debug_log_4({
      std::cout << "new object array ptr: " << ptr 
                << " size: " << sizeof(T) << "x" << n 
                << " total: " << sizeof(T) * n 
                << " total alloc count: " << AllocationTotal::TotalCount()
                << " total alloc size: " << AllocationTotal::TotalSize()
                << std::endl;
    });
    return ptr;
  }

  static void Delete(T* ptr) {
    assert(ptr != nullptr);
    T* p = ptr;
    delete ptr;
    AllocationTotal::Sub();
    Debug_log_4({
      std::cout << "delete object ptr: " << p
                << " total alloc count: " << AllocationTotal::TotalCount()
                << " total alloc size: " << AllocationTotal::TotalSize()
                << std::endl;
    });
  }

  static void DeleteArray(T* ptr) {
    assert(ptr != nullptr);
    T* p = ptr;
    delete [] ptr;
    AllocationTotal::Sub();
    Debug_log_4({
      std::cout << "delete object array ptr: " << p
                << " total alloc count: " << AllocationTotal::TotalCount()
                << " total alloc size: " << AllocationTotal::TotalSize()
                << std::endl;
    });

  }
};

} // namespace textsort

#endif // ALLOCATION_H_
