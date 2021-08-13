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

/// c++ iostream
#include <iostream>
#include <fstream>

/// linux sys api
#include<unistd.h>
#include<sys/types.h>
#include<sys/stat.h>
#include<fcntl.h>

/// mmap
#include <stdio.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>

//std::string file = "/data/temp/tt_100w.csv";
//std::string filename = "../data/datafile_all";
//std::string filename = "../data/datafile_10kw";
std::string filename = "../data/datafile";
//std::string filename = "../data/datafile_1kw";
int read_num = 10;

void ifstream_read() {
  std::ifstream rfile(filename, std::ios::in);
  if(!rfile) {
    std::cout<<"不可以打开文件"<<std::endl;
    exit(1);
  }
  char str[256];
  long long count = 0;
  textsort::Time time(true);
  for (int i=1; i <= read_num; i++) {
    rfile.getline(str, 256); //读到'\n'终止
    char* p = str;
    while (*p != '\0') {
      count++;
      p++;
    }
    //std::cout << i << ". " << str << " " + time.Snap() << std::endl;
  }
  std::cout << "ifstream_read text size: " << count << " total time: " << time.Total() << std::endl;
  rfile.close();
}

void sysapi_read() {
  int fd;
  char buffer[409];
  fd = open(filename.c_str(), O_RDONLY);
  size_t count = 0;
  int line_count = 0;
  textsort::Time time(true);
  while (line_count < read_num) {
    int size = read(fd, buffer, sizeof(buffer));
    for (int i = 0; i < size; i++) {
      if (buffer[i] == '\n') {
        line_count++;
        if (line_count >= read_num) {
          break;
        }
      } else {
        count++;
      }
    }
  }
  std::cout << "glibc_read text size: " << count << " total time: " << time.Total() << std::endl;
  close(fd);
}

void mmap_read() {
  int fd = 0;
  char* ptr = NULL;
  struct stat buf = {0};

  if ((fd = open(filename.c_str(), O_RDWR)) < 0) {
    printf("open file error\n");
    return;
  }

  if (fstat(fd, &buf) < 0) {
    printf("get file state error:%d\n", errno);
    close(fd);
    return;
  }
 
  ptr = (char *)mmap(NULL, buf.st_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
  if (ptr == MAP_FAILED) {
    printf("mmap failed\n");
    close(fd);
    return;
  }
  int line_count = 0;
  size_t count = 0;
  size_t i = 0;
  textsort::Time time(true);
  while (line_count < read_num && i < buf.st_size) {
    if (ptr[i] == '\n') {
      line_count++;
    } else {
      count++;
    }
    i++;
  }
  std::cout << "mmap_read text size: " << count << " total time: " << time.Total() << std::endl;

  close(fd);
  munmap(ptr, buf.st_size);
}

void mmap_read2() {
  size_t count = 0;
  textsort::FileIo* file = new textsort::MMapFile();
  file->OpenFile(filename.c_str(), true);
  textsort::Time time(true);
  int n = read_num;
  while (n--) {
    textsort::Line line;
    size_t line_num = file->ReadLine(line);
    if (!line.length) {
      break;
    }
    count += line.length;
    std::cout << line_num << ". " << std::string(line.Ptr(), line.length) << std::endl;
  }
  std::cout << "mmap_read2 text size: " << count << " total time: " << time.Total() << std::endl;
  file->Close();
}


void mmap_partition() {
  std::cout << "run mmap_partition -----" << std::endl;
  size_t count = 0;
  textsort::FileIo* file = new textsort::MMapFile();
  file->OpenFile(filename.c_str(), true);
  int block_num = 4;
  textsort::Time time(true);
  textsort::FileBlock* block_ptr = file->PartitionBlock(block_num);
  std::cout << "PartitionBlock time: " << time.Snap() << std::endl;
  size_t block_total = 0;
  for (int i = 0; i < block_num; i++) {
    textsort::SortArray array(26000000);
    time.ResetSnap();
    block_total += block_ptr->GetBlockSize();
    std::cout << "GetBlockSize time: " << time.Snap() << std::endl;
    std::cout << "block size: " << block_ptr->GetBlockSize() << std::endl;
    block_ptr->ReadLines(&array);
    std::cout << "ReadLines time: " << time.Snap() << std::endl;
    //std::cout << array.DebugString() << std::endl;
    block_ptr = block_ptr->NextBlock();
  }
  
  std::cout << "mmap_partition " << "Time: " << time.Total()
            << " file size: " << file->GetFileSize() << " block total size: " << block_total << std::endl;
  file->Close();

  assert(textsort::AllocationTotal::TotalCount() == 0);
}

void test_topk_heapsort() {
  std::cout << "run test_topk_heapsort -----" << std::endl;
  size_t count = 0;
  textsort::FileIo* file = new textsort::MMapFile();
  file->OpenFile(filename.c_str(), true);
  int block_num = 1;
  textsort::Time time(true);
  textsort::FileBlock* block_ptr = file->PartitionBlock(block_num);
  std::cout << "PartitionBlock time: " << time.Snap() << std::endl;

  textsort::TopkSort sort;
  sort.InitTopkSort(block_ptr, 10000000);
  //std::cout << sort.GetSortArray()->DebugString() << std::endl;
  time.ResetSnap();
  sort.HeapSort();
  std::cout << "HeapSort time: " << time.Snap() << std::endl;
  //std::cout << sort.GetSortArray()->DebugString(true) << std::endl;
  
  std::cout << "test_topk_heapsort " << "Time: " << time.Total()
            << " file size: " << file->GetFileSize() << std::endl;
  file->Close();
}

void test_get_top_k() {
  std::cout << "run test_get_top_k -----" << std::endl;
  size_t count = 0;
  textsort::FileIo* file = new textsort::MMapFile();
  file->OpenFile(filename.c_str(), true);
  int block_num = 1;
  textsort::Time time(true);
  textsort::FileBlock* block_ptr = file->PartitionBlock(block_num);
  std::cout << "PartitionBlock time: " << time.Snap() << std::endl;

  textsort::TopkSort sort;
  sort.InitTopkSort(block_ptr, 10000000);
  //std::cout << sort.GetSortArray()->DebugString() << std::endl;

  for (int i = 0; i < (read_num / 4) + 1; i++) {
    time.ResetSnap();
    textsort::SortArraySlice slice = sort.GetTopK(4);
    std::cout << "iter: " << i+1 << " GetTopK: " << time.Snap() << std::endl;
    std::cout << slice.DebugString() << std::endl;
  }

  std::cout << "test_get_top_k " << "Time: " << time.Total()
            << " file size: " << file->GetFileSize() << std::endl;
  file->Close();
}

void test_get_merge_sort() {
  std::cout << "run test_get_merge_sort -----" << std::endl;
  size_t count = 0;
  textsort::FileIo* file = new textsort::MMapFile();
  file->OpenFile(filename.c_str(), true);
  int block_num = 1;
  textsort::Time time(true);
  textsort::FileBlock* block_ptr = file->PartitionBlock(block_num);
  std::cout << "PartitionBlock time: " << time.Snap() << std::endl;

  textsort::TopkSort topk_sort;
  topk_sort.InitTopkSort(block_ptr, 10000000);
  //std::cout << sort.GetSortArray()->DebugString() << std::endl;

  time.ResetSnap();
  textsort::SortArraySlice slice1 = topk_sort.GetTopK(4);
  std::cout << "iter: " << 1 << " GetTopK: " << time.Snap() << std::endl;
  std::cout << slice1.DebugString() << std::endl;

  time.ResetSnap();
  textsort::SortArraySlice slice2 = topk_sort.GetTopK(4);
  std::cout << "iter: " << 2 << " GetTopK: " << time.Snap() << std::endl;
  std::cout << slice2.DebugString() << std::endl;

  time.ResetSnap();
  textsort::SortArraySlice slice3 = topk_sort.GetTopK(4);
  std::cout << "iter: " << 3 << " GetTopK: " << time.Snap() << std::endl;
  std::cout << slice3.DebugString() << std::endl;

  textsort::MergeSort merge_sort;
  merge_sort.MergeSortArraySlice(&slice1, &slice2);
  textsort::SortArray* sort_array = merge_sort.GetMergeArray();
  std::cout << sort_array->DebugString() << std::endl;

  merge_sort.MergeSortArraySlice(&slice3);
  sort_array = merge_sort.GetMergeArray();
  std::cout << sort_array->DebugString() << std::endl;

  std::cout << "test_get_merge_sort " << "Time: " << time.Total()
            << " file size: " << file->GetFileSize() << std::endl;
  file->Close();
}

class T1 : public textsort::QueueItem<T1> {
public:
  T1(int i) : i_(i) {};
  void cout(size_t i) {
    std::cout << i << " T1 i: " << i_ << std::endl;
  };
  std::string DumpString() const {
    return "T1";
  }
  int i_;
};

class TestTask : public textsort::Task {
public:
  TestTask(int i, textsort::FIFOQueue<T1>* queue) 
    : Task("TestTask", false), i_(i), queue_ptr_(queue) {};
  void RunTask() {
    for (int i = 0; i < 10; i++) {
      T1* t1 = new T1(i_);
      queue_ptr_->Enqueue(t1);
      textsort::Time::Sleep(500);
    }
    T1* t1 = new T1(-1);
    queue_ptr_->Enqueue(t1);
  };
private:
  textsort::FIFOQueue<T1>* queue_ptr_;
  int i_;
};

void test_queue() {
  textsort::ThreadPool pool;
  textsort::FIFOQueue<T1> queue;
  T1 t1(88);
  t1.cout(0);
  queue.Enqueue(&t1);
  for (int i = 100; i < 150; i++) {
    textsort::Task* task = new TestTask(i, &queue);
    pool.RunTask(task);
  }
  size_t c = 0;
  int t = 0;
  while (1) {
    T1* t1_p = queue.Dequeue();
    t1_p->cout(c);
    std::cout << "queue size: " << queue.Count() << std::endl;
    c++;
    if (t1_p->i_ < 0) {
      t += t1_p->i_;
      if (t <= -50) {
        return;
      }
    }
  }
}

//
// test: 1. one recored write 2. batch recored write 
//

int main() {
  // mmap_read2();
  // mmap_read();
  // sysapi_read();
  // ifstream_read();
  // mmap_read2();
  // sysapi_read();
  // ifstream_read();
  // mmap_read();
  // mmap_read2();
  // mmap_partition();
  //test_topk_heapsort();
  //test_get_top_k();
  //test_get_merge_sort();
  test_queue();
  return 0;
}
