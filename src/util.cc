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
#include <thread.h>

namespace textsort {

static ThreadLock LOGLOCK;

void Loglock() {
  LOGLOCK.Lock();
}

void UnLoglock() {
  LOGLOCK.Unlock();
}

void Time::Start() {
  gettimeofday(&start_, NULL);
  snap_ = start_;
}

void Time::ResetSnap() {
  gettimeofday(&snap_, NULL);
}

std::string Time::Snap() {
  struct timeval snap;
  gettimeofday(&snap, NULL);
  std::string msg = GetMsg(snap_, snap);
  snap_ = snap;
  return msg;
}

std::string Time::Total() {
  struct timeval end;
  gettimeofday(&end, NULL);
  return GetMsg(start_, end);
}

std::string Time::GetMsg(struct timeval& start, struct timeval& end) {
  // microsecond time difference
  time_t ms = 
    (end.tv_sec*1000000 + end.tv_usec) - (start.tv_sec*1000000 + start.tv_usec);

  /// BUG: direct use msg_, coredump (__memmove_avx_unaligned_erms)
  /// return reference, memory destruction
  std::string str = "Time: " + std::to_string(ms / 1000000) + 
        "." + std::to_string(ms % 1000000 / 1000) + 
        "." + std::to_string(ms % 1000) + "sec";
  return str;
}

void Time::Sleep(int milliseconds) {
  usleep(milliseconds * 1000);
}

std::string PtrToHex(ptr_t num) {
  char buff[128];
  sprintf(buff, "0x%lx", num);
  return std::string(buff);
}

struct Config* GetConfig() {
  static struct Config config = {.debug=0};
  return &config;
}

} // namespace textsort
