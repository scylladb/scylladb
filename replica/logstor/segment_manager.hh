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
#include "replica/logstor/segment_io.hh"
#include "replica/logstor/write_buffer.hh"
#include "replica/logstor/compaction.hh"
#include "types.hh"
#include "utils/updateable_value.hh"

namespace replica {

class database;

namespace logstor {

class compaction_manager;
class segment_set;
class primary_index;

static constexpr uint64_t default_segment_size = 128 * 1024;
static constexpr uint64_t default_file_size = 32 * 1024 * 1024;

/// Configuration for the segment manager
struct segment_manager_config {
    std::filesystem::path base_dir;
    uint64_t segment_size = default_segment_size;
    uint64_t file_size = default_file_size;
    uint64_t disk_size;
    bool format_on_startup = true;
    // Create files sparse: extend them to their nominal size without
    // preallocating or zero-filling. Saves disk space and I/O, at the cost of
    // fragmentation and of not reserving space up front.
    bool sparse_files = false;
    bool compaction_enabled = true;
    size_t max_segments_per_compaction = 32;
    utils::updateable_value<double> trigger_compaction_threshold{0.05};
    seastar::scheduling_group compaction_sg;
    utils::updateable_value<float> compaction_static_shares;
    utils::updateable_value<float> compaction_max_shares;
    seastar::scheduling_group separator_sg;
    seastar::scheduling_group split_compaction_sg;
};

// What the logstor of one shard is using. Every field is shard wide and covers every table, unlike
// the statistics of the segments a compaction group owns. Kept in one struct, read through a single
// accessor, so that the shard wide usage numbers stay in one place and another one can join them
// without an accessor of its own.
struct segment_manager_usage {
    // Bytes of the files allocated for segments. This is the space logstor holds on disk, which is
    // more than the space its data takes: it also covers the segments that are free, and files are
    // never given back during normal operation, only retired on recovery.
    uint64_t disk_usage{0};
    // Bytes of the segments that hold records, which is the space the data of this shard takes:
    // the segments the compaction groups own, the one being written and the ones a read still
    // refers to. Less than disk_usage by the free segments and by the room in the files holding
    // them, and the logstor part of the load a node reports.
    uint64_t segment_bytes_in_use{0};
    // Memory the segment manager holds for its own bookkeeping. The indexes of the tables are not
    // part of it.
    size_t memory_usage{0};
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
    uint64_t live_record_bytes{0};
    std::vector<table_segment_histogram_bucket> histogram;

    table_segment_stats& operator+=(table_segment_stats& other) {
        compaction_group_count += other.compaction_group_count;
        segment_count += other.segment_count;
        live_record_bytes += other.live_record_bytes;
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

class segment_manager : public space_accounting_subscriber {
    std::unique_ptr<segment_manager_impl> _impl;
private:
    segment_manager_impl& get_impl() noexcept;
    const segment_manager_impl& get_impl() const noexcept;
public:

    explicit segment_manager(segment_manager_config config);
    ~segment_manager();

    segment_manager(const segment_manager&) = delete;
    segment_manager& operator=(const segment_manager&) = delete;

    future<> do_recovery(replica::database&);
    future<> do_recovery_for_test();

    future<> start();
    future<> stop();

    future<> write(write_buffer& wb);

    future<log_record> read(log_location location);

    void on_add_record(log_location location) noexcept override;
    void on_free_record(log_location location) noexcept override;

    compaction_manager& get_compaction_manager() noexcept;
    const compaction_manager& get_compaction_manager() const noexcept;

    uint64_t get_segment_size() const noexcept;

    // Returns the path of the file holding the given segment (for debug/introspection).
    // The path is computed from the segment id, the segment is not looked up, so the file
    // does not have to hold a live segment. The file names are shard local, so this has to
    // be called on the shard owning the segment.
    sstring get_segment_file_path(log_segment_id) const;

    // Removes all the segments of the group and frees them. Waits for an ongoing compaction of
    // the group and keeps compaction disabled while discarding, so the caller doesn't have to.
    // The index must be cleared first, so that no record of the group is reachable.
    future<> discard_segments(logstor_group&);

    segment_manager_usage get_usage() const noexcept;

    future<> await_pending_writes();

    future<utils::chunked_vector<segment_snapshot>> make_snapshot(logstor_group& cg);

    // Create an output stream to write a segment (for receiving from remote node)
    // Allocates a new local segment and returns an output stream for writing to the segment.
    future<std::unique_ptr<segment_stream_sink>> create_segment_output_stream(replica::database&);

    friend class segment_manager_impl;

};

}
}