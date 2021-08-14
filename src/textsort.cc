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

#include <iostream>
#include <text_sort.h>

int main(int argc, char* argv[]) {
  if (argc < 3) {
    std::cout << "usage: textsort input_file output_file" << std::endl;
    return 0;
  }

  textsort::Config* config = textsort::GetConfig();
  config->debug = 2;
  config->input_file_path = std::string(argv[1]);
  config->output_file_path = std::string(argv[2]);
  config->block_num = 13;
  config->init_merge_thread_num = 3;
  config->fileblock_line_num = 20000000;
  config->top_k = 100000;
  config->load_data_mode = textsort::Config::LoadDataMode::Sequence;
  config->sequence_load_immediate = true;
  config->write_buff_size = 1024*100000; // 100m

  textsort::TextSort* tsort = new textsort::TextSort();
  std::cout << "TextSort Running..." << std::endl;
  textsort::Time time(true);
  tsort->Start();
  std::cout << "TextSort Done time: " << time.Total() << std::endl;
  delete tsort;
  return 0;
}
