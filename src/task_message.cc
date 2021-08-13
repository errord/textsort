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

#include <task_message.h>

namespace textsort {

const char* TaskMessage::GetTaskTypeName() const {
  switch (type_) {
    case RegisterTaskThread:
      return "RegisterTaskThread";
    case TopkTaskStart:
      return "TopkTaskStart";
    case Topk:
      return "Topk";
    case TopkTaskDone:
      return "TopkTaskDone";
    case Merge:
      return "Merge";
    case MergeDone:
      return "MergeDone";
    case TaskThreadExit:
      return "TaskThreadExit";
    case Combine:
      return "Combine";
    case CombineExit:
      return "CombineExit";
    case DiskWrite:
      return "DiskWrite";
    case DiskWriteExit:
      return "DiskWriteExit";
    default:
      return "UnknowType";
  }
}

std::string TaskMessage::DumpString() const {
  return std::string(" type: " + std::string(GetTaskTypeName()));
}

const char* MergeMessageData::GetTaskTypeName() const {
  switch (type)
  {
  case Init:
    return "Init";
  case SliceAndSlice:
    return "SliceAndSlice";
  case ArrayAndArray:
    return "ArrayAndArray";
  case SliceAndArray:
    return "SliceAndArray";
  case ArrayAndSlice:
    return "ArrayAndSlice";
  default:
    return "UnknowType";
  }
}

} // namespace textsort
