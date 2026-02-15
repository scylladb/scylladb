/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include <cstdint>
#include <filesystem>
#include <seastar/core/shared_future.hh>
#include <seastar/core/file.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/shared_ptr.hh>
#include "bytes_fwd.hh"
#include "replica/logstor/write_buffer.hh"
#include "types.hh"

namespace replica::logstor {

static constexpr size_t default_segment_size = 128 * 1024;
static constexpr size_t default_file_size = 32 * 1024 * 1024;

/// Configuration for the segment manager
struct segment_manager_config {
    std::filesystem::path base_dir;
    size_t segment_size = default_segment_size;
    size_t file_size = default_file_size;
    size_t disk_size;
    bool compaction_enabled = true;
    size_t max_segments_per_compaction = 8;
    seastar::scheduling_group compaction_sg;
    seastar::scheduling_group separator_sg;
    uint32_t separator_delay_limit_ms;
};

class segment_manager_impl;
class log_index;

class segment_manager {
    std::unique_ptr<segment_manager_impl> _impl;
private:
    segment_manager_impl& get_impl() noexcept;
    const segment_manager_impl& get_impl() const noexcept;
public:
    static constexpr size_t block_alignment = 4096;

    explicit segment_manager(segment_manager_config config, log_index&);
    ~segment_manager();

    segment_manager(const segment_manager&) = delete;
    segment_manager& operator=(const segment_manager&) = delete;

    future<> start();
    future<> stop();

    future<log_location> write(write_buffer& wb);

    future<log_record> read(log_location location);

    void free_record(log_location location);

    future<> for_each_record(const std::vector<log_segment_id>& segments,
                            std::function<future<>(log_location, log_record)> callback);

    void enable_auto_compaction();
    future<> disable_auto_compaction();
    future<> trigger_compaction(bool major = false);

    size_t get_segment_size() const noexcept;

    friend class segment_manager_impl;

};

}
