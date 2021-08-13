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

#include <disk_writer.h>
#include <task_message.h>
#include <file_io.h>

namespace textsort {

void DiskWriter::PutDiskWriteMessage(int batch_id, SortArraySlice slice) {
  DiskWriteMessageData* msg_data = new DiskWriteMessageData(batch_id, slice);
  TaskMessage* msg = new TaskMessage(
    TaskMessage::TaskType::DiskWrite, static_cast<void*>(msg_data));
  diskwrite_queue_.Enqueue(msg);
}

void DiskWriter::PutExitMessage() {
  TaskMessage* msg = new TaskMessage(TaskMessage::TaskType::DiskWriteExit);
  diskwrite_queue_.Enqueue(msg);
}

TaskMessage* DiskWriter::GetDiskWriteMessage() {
  return diskwrite_queue_.Dequeue();
}

void DiskWriter::WriteBuffAndSync(size_t count) {
  file_->WriteFile(write_buff_ptr_, count);
  file_->Sync();
}

void DiskWriter::WriteBuff(SortArraySlice& slice) {
  size_t pos = 0;
  SortItem* start = slice.start;
  const char* line_ptr = nullptr;
  uint8_t length = 0;
  size_t item_count = slice.count;
  for (size_t idx = 0; idx < item_count; idx++) {
    line_ptr = start->line.Ptr();
    length = start->line.length;
    if (pos + length > buff_size_) {
      WriteBuffAndSync(pos);
      pos = 0;
    }
    // Prevent buff overflow 
    assert(pos + length <= buff_size_);
    memcpy(write_buff_ptr_ + pos, line_ptr, sizeof(char)*length);
    pos += length;
    write_buff_ptr_[pos++] = '\n';
    start++;
  }
  // Write the rest  
  WriteBuffAndSync(pos);
}

void DiskWriter::DirectWrite(SortArraySlice& slice) {
  char* line_ptr = nullptr;
  SortItem* start = slice.start;
  uint8_t length = 0;
  size_t item_count = slice.count;
  for (size_t idx = 0; idx < item_count; idx++) {
    line_ptr = (char*)start->line.Ptr();
    length = start->line.length;
    char t = line_ptr[length];
    line_ptr[length] = '\n';
    file_->WriteFile(line_ptr, length+1);
    line_ptr[length] = t;
    start++;
  }
  file_->Sync();
}

void DiskWriter::RunDiskWriter() {
  while (1) {
    TaskMessage* msg = GetDiskWriteMessage();
    if (msg->GetTaskType() == TaskMessage::DiskWriteExit) {
      Debug_log_2(std::cout << "DiskWriter Exit" << std::endl;);
      delete msg;
      break;
    }
    auto* data = msg->GetMessageData<DiskWriteMessageData>();
    size_t count = data->slice.count;
    WriteBuff(data->slice);
    // slow
    //DirectWrite(data->slice);
    delete data->slice.sort_array_ptr;
    Debug_log_2({
      std::cout << "DiskWriter process message: DiskWrite"
                << " batch: " << data->batch_id 
                << " count: " << count << std::endl;
    });
    delete msg;
  }
}

} // namespace textsort
