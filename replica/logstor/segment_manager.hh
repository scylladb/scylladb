/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <cstdint>
#include <filesystem>
#include <seastar/core/shared_future.hh>
#include <seastar/core/file.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/bool_class.hh>
#include "bytes_fwd.hh"
#include "mutation_writer/token_group_based_splitting_writer.hh"
#include "replica/logstor/write_buffer.hh"
#include "replica/logstor/compaction.hh"
#include "types.hh"
#include "utils/updateable_value.hh"

namespace replica {

class database;
class table;

namespace logstor {

using want_data = seastar::bool_class<class want_data_tag>;

class compaction_manager;
class segment_set;
class primary_index;

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
    utils::updateable_value<float> compaction_static_shares;
    seastar::scheduling_group separator_sg;
    uint32_t separator_delay_limit_ms;
    size_t max_separator_memory = 1 * 1024 * 1024;
};

struct table_segment_histogram_bucket {
    size_t count;
    size_t max_data_size;

    table_segment_histogram_bucket& operator+=(table_segment_histogram_bucket& other) {
        count += other.count;
        max_data_size = std::max(max_data_size, other.max_data_size);
        return *this;
    }
};

struct table_segment_stats {
    size_t compaction_group_count{0};
    size_t segment_count{0};
    std::vector<table_segment_histogram_bucket> histogram;

    table_segment_stats& operator+=(table_segment_stats& other) {
        compaction_group_count += other.compaction_group_count;
        segment_count += other.segment_count;
        histogram.resize(std::max(histogram.size(), other.histogram.size()));
        for (size_t i = 0; i < other.histogram.size(); i++) {
            histogram[i] += other.histogram[i];
        }
        return *this;
    }
};

struct segment_snapshot {
    log_segment_id segment_id;
    segment_ref seg_ref;
    noncopyable_function<future<seastar::input_stream<char>>(const file_input_stream_options&)> source;
};

class segment_stream_sink {
public:
    virtual ~segment_stream_sink() = default;
    virtual log_segment_id segment_id() const noexcept = 0;
    virtual future<output_stream<char>> output() = 0;
    virtual future<> close() = 0;
    virtual future<> abort() = 0;
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

    explicit segment_manager(segment_manager_config config);
    ~segment_manager();

    segment_manager(const segment_manager&) = delete;
    segment_manager& operator=(const segment_manager&) = delete;

    future<> do_recovery(replica::database&);

    future<> start();
    future<> stop();

    future<> write(write_buffer& wb);

    future<log_record> read(log_location location);

    void free_record(log_location location);

    compaction_manager& get_compaction_manager() noexcept;
    const compaction_manager& get_compaction_manager() const noexcept;

    void set_trigger_compaction_hook(std::function<void()> fn);
    void set_trigger_separator_flush_hook(std::function<void(size_t)> fn);

    size_t get_segment_size() const noexcept;

    future<> discard_segments(segment_set&);

    size_t get_memory_usage() const;

    future<> await_pending_writes();

    future<utils::chunked_vector<segment_snapshot>> make_snapshot(compaction_group& cg);

    // Create an output stream to write a segment (for receiving from remote node)
    // Allocates a new local segment and returns an output stream for writing to the segment.
    future<std::unique_ptr<segment_stream_sink>> create_segment_output_stream(replica::database&);

    friend class segment_manager_impl;

};

}
}