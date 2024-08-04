/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "utils/assert.hh"
#include "streaming/session_info.hh"

namespace streaming {

void session_info::update_progress(progress_info new_progress) {
    SCYLLA_ASSERT(peer == new_progress.peer);
    auto& current_files = new_progress.dir == progress_info::direction::IN
        ? receiving_files : sending_files;
    current_files[new_progress.file_name] = new_progress;
}

std::vector<progress_info> session_info::get_receiving_files() const {
    std::vector<progress_info> ret;
    for (auto const& x : receiving_files) {
        ret.push_back(x.second);
    }
    return ret;
}

std::vector<progress_info> session_info::get_sending_files() const {
    std::vector<progress_info> ret;
    for (auto const& x : sending_files) {
        ret.push_back(x.second);
    }
    return ret;
}

long session_info::get_total_size_in_progress(std::vector<progress_info> files) const {
    long total = 0;
    for (auto const& file : files) {
        total += file.current_bytes;
    }
    return total;
}

long session_info::get_total_files(std::vector<stream_summary> const& summaries) const {
    long total = 0;
    for (auto const& summary : summaries) {
        total += summary.files;
    }
    return total;
}

long session_info::get_total_sizes(std::vector<stream_summary> const& summaries) const {
    long total = 0;
    for (auto const& summary : summaries)
        total += summary.total_size;
    return total;
}

long session_info::get_total_files_completed(std::vector<progress_info> files) const {
    long size = 0;
    for (auto const& x : files) {
        if (x.is_completed()) {
            size++;
        }
    }
    return size;
}

} // namespace streaming
