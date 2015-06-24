/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by Cloudius Systems.
 * Copyright 2015 Cloudius Systems.
 */

#include "streaming/session_info.hh"

namespace streaming {

void session_info::update_progress(progress_info new_progress) {
    assert(peer == new_progress.peer);
    auto& current_files = new_progress.dir == progress_info::direction::IN
        ? receiving_files : sending_files;
    current_files[new_progress.file_name] = new_progress;
}

std::vector<progress_info> session_info::get_receiving_files() {
    std::vector<progress_info> ret;
    for (auto& x : receiving_files) {
        ret.push_back(x.second);
    }
    return ret;
}

std::vector<progress_info> session_info::get_sending_files() {
    std::vector<progress_info> ret;
    for (auto& x : sending_files) {
        ret.push_back(x.second);
    }
    return ret;
}

long session_info::get_total_size_in_progress(std::vector<progress_info> files) {
    long total = 0;
    for (auto& file : files) {
        total += file.current_bytes;
    }
    return total;
}

long session_info::get_total_files(std::vector<stream_summary>& summaries) {
    long total = 0;
    for (auto& summary : summaries) {
        total += summary.files;
    }
    return total;
}

long session_info::get_total_sizes(std::vector<stream_summary>& summaries) {
    long total = 0;
    for (auto& summary : summaries)
        total += summary.total_size;
    return total;
}

long session_info::get_total_files_completed(std::vector<progress_info> files) {
    long size = 0;
    for (auto& x : files) {
        if (x.is_completed()) {
            size++;
        }
    }
    return size;
}

} // namespace streaming
