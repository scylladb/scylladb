/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <ranges>
#include <string_view>
#include <vector>

#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/log.hh>

#include <fmt/format.h>

#include "bytes_fwd.hh"
#include "replica/logstor/segment_ref.hh"
#include "replica/logstor/segment_types.hh"
#include "seastarx.hh"

namespace replica::logstor {

extern seastar::logger logstor_logger;

using file_id_t = uint64_t;

// Where a segment is on disk.
struct segment_location {
    file_id_t file_id;
    // The offset in the file where the segment starts.
    uint64_t file_offset;
};

// The layout of the segments in the files of one shard. Each file holds the same count of
// segments, one after the other, and the segment ids continue from one file to the next. The
// configured disk size gives the file count and the segment count. Recovery can find more files,
// from an earlier, larger configuration, and their segments have the same layout.
class segment_geometry {
public:
    // Throws exceptions::configuration_exception when a size is zero, when a segment is larger than
    // a file, or when a file is larger than the disk size.
    segment_geometry(uint64_t segment_size, uint64_t file_size, uint64_t disk_size);

    uint64_t segments_per_file() const noexcept { return _segments_per_file; }

    // The file count of the configuration.
    uint64_t file_count() const noexcept { return _file_count; }

    // The segment count of the configuration. New segment ids are allocated only below it.
    // Recovery can find more segments, in files from an earlier, larger configuration. Those
    // segments stay usable until they are freed, and a freed segment at or above this count is
    // retired and not allocated again.
    uint64_t segment_count() const noexcept { return _file_count * _segments_per_file; }

    segment_location location_of(log_segment_id segment_id) const noexcept {
        return segment_location{
            .file_id = segment_id.value / _segments_per_file,
            .file_offset = (segment_id.value % _segments_per_file) * _segment_size,
        };
    }

    auto segments_in_file(file_id_t file_id) const noexcept {
        return std::views::iota(file_id * _segments_per_file, (file_id + 1) * _segments_per_file)
            | std::views::transform([] (uint64_t i) {
                return log_segment_id(i);
            });
    }

private:
    // Checks the sizes before the members are computed from them, and returns the segment size.
    static uint64_t checked_segment_size(uint64_t segment_size, uint64_t file_size, uint64_t disk_size);

    const uint64_t _segment_size;
    const uint64_t _segments_per_file;
    const uint64_t _file_count;
};

// The configuration of the segment files of one shard.
struct file_manager_config {
    std::filesystem::path base_dir;
    uint64_t file_size;
    // The file count of the configuration. A new file is allocated only below it. Recovery can
    // find more files, from an earlier, larger configuration.
    uint64_t file_count;
    // The scheduling group of the file formats.
    seastar::scheduling_group format_sg;
    bool format_on_startup;
    bool sparse_files;
};

// The segment files of one shard. A file holds a fixed count of segments, and is named
// ls_{shard_id}-{file_id}-Data.db in the base directory. The segment manager owns one file manager
// for the life of the shard.
//
// A file goes through these states, in this order:
// - formatted: the file exists on disk with its full size, zero filled or sparse. The format of
//   file N starts in the background when file N-1 is allocated, so that it is ready before its
//   first segment is taken. With format_on_startup, all the configured files are formatted at the
//   end of recovery.
// - allocated: the segments of the file can be used, the file counts in the disk usage, and the
//   file is open. Recovery allocates the files that it finds, and with format_on_startup also the
//   files that it formats. A file that is formatted in the background is allocated by the first
//   segment taken from it.
// - retired: recovery removes a file from the end when the configured disk size became smaller
//   and the file holds no used segment.
//
// Invariants:
// I1. Files are allocated in order. File N is allocated if and only if N < allocated_file_count().
// I2. An allocated file is formatted and open. Only allocated files are open.
// I3. After recovery, while N files are allocated and N < configured, _next_file_format is the
//     format of file N: pending, done or failed.
// I4. The files at or above configured exist from an earlier, larger configuration. They are
//     allocated, but a new file is never allocated at or above configured. Recovery removes them
//     from the end when they hold no used segment.
// I5. _open_files has one entry per file id below max(configured, allocated).
// I6. Recovery opens the files it found before it reads them. Every file a reader asks for is
//     allocated, and open.
// I7. Recovery drives the file manager through one recovery object, in three steps: open the found
//     files, retire the unused files, finish. Recovery reads the found files between the first two
//     steps. The file manager checks the order of the steps, and allocates no file before the
//     recovery finished.
class file_manager {
public:
    explicit file_manager(file_manager_config cfg);

    future<> stop();

    std::filesystem::path file_path(file_id_t file_id) const {
        auto fname = fmt::format("{}{}-Data.db", file_name_prefix(), file_id);
        return _cfg.base_dir / fname;
    }

    // The recovery of the files of the shard. start_recovery() makes one, and its three steps run
    // in this order. A step out of order is a programming error. The file manager keeps the step,
    // and the object cannot be copied or moved, so there is one object per recovery.
    class recovery {
        file_manager& _files;

        explicit recovery(file_manager& files) noexcept : _files(files) {}
        friend class file_manager;
    public:
        recovery(const recovery&) = delete;
        recovery(recovery&&) = delete;

        // Scans the base directory, removes the temporary files of formats that did not finish,
        // checks that the found files are 0..N-1, and opens them as allocated. Returns N.
        future<file_id_t> open_found_files();

        // Removes the found files at or above the configured count that hold no used segment, from
        // the end. used_file_count is the count of the files that hold a used segment.
        future<> retire_unused_files(file_id_t used_file_count);

        // Formats the rest of the configured files, or starts the background format of the next
        // one. After it, the file manager allocates files.
        future<> finish();
    };

    // Starts the recovery of the files. Call it once.
    recovery start_recovery();

    // Returns the open file for a new segment. Recovery allocates the files that it finds, and with
    // format_on_startup also the files that it formats. A file that is formatted in the background
    // is allocated here, by the first segment taken from it. Each later segment finds the file
    // allocated and open. The caller must have finished the recovery. The caller must not call
    // this again before the call returns, because a second call for a file that is in allocation is
    // an internal error.
    future<seastar::file> get_file_for_new_segment(file_id_t);

    uint64_t allocated_file_count() const noexcept { return _allocated_file_count; }

    // The open file. The caller must ask for an allocated file, which is open.
    seastar::file get_open_file(file_id_t file_id) const {
        if (file_id >= _allocated_file_count) [[unlikely]] {
            on_internal_error(logstor_logger, fmt::format("Attempted to access file {} that is not allocated, {} files allocated", file_id, _allocated_file_count));
        }
        return _open_files[file_id];
    }

    // Writes zeros over a region of an allocated file. The region must be in the file.
    future<> zero_region(file_id_t file_id, uint64_t offset, uint64_t size);

private:
    // The zero buffer is written in one request over as much of a region as the disk takes in one,
    // see write_zeros(). 128 KiB is large enough that the vector of a request is short, and
    // small enough that the buffer costs nothing.
    static constexpr size_t zero_buffer_size = 128 * 1024;

    const file_manager_config _cfg;

    file_id_t _allocated_file_count{0};

    // The step of the recovery. start_recovery() and the steps of the recovery object advance
    // it, each after its work is done.
    enum class recovery_step { not_started, started, opened, retired, finished };
    recovery_step _recovery_step{recovery_step::not_started};

    seastar::gate _async_gate;

    shared_future<> _next_file_format{make_ready_future<>()};

    // One handle per file, opened read-write, kept open for the life of the shard and shared by
    // the read and the write paths. Both go through it, so it is always opened for writing: a file
    // first touched by a read would otherwise hold a read-only handle the write path cannot use.
    // A file is opened when it is allocated, thus only the allocated files are open.
    std::vector<seastar::file> _open_files;

    const std::unique_ptr<char[], seastar::free_deleter> _zero_buf;

    // The file names are ls_{shard_id}-{file_id}-Data.db
    static sstring file_name_prefix() {
        return fmt::format("ls_{}-", this_shard_id());
    }

    // The file id in a file name of this shard, or none if the name is not one.
    static std::optional<file_id_t> file_id_of(std::string_view file_name);

    // Fails with an internal error when the recovery is not at the expected step.
    void check_recovery_step(recovery_step expected, std::string_view call) const;

    future<> open_allocated_files();

    // Removes a retired file. The caller must have checked that the file is the last one on disk,
    // at or above the configured count, and that it holds no used segment.
    future<> remove_file(file_id_t);

    // Formats and opens the configured files that are not allocated, in order. Each file counts
    // as allocated after it is open.
    future<> format_remaining_files();

    // Starts the background format of the next file.
    void start_next_file_format();

    future<> format_file(file_id_t);

    // Writes zeros over the region. The file can be a file that is not allocated, such as the
    // temporary file of a format.
    future<> write_zeros(seastar::file file, uint64_t offset, uint64_t size);

    // Allocates the next file: waits for its format, opens it, and starts the format of the file
    // after it in the background. The caller must give the next file, below the configured
    // count, and must not call this again before it returns.
    future<> allocate_file(file_id_t);

    // Opens the file and puts it in the cache. An open of a file that is open is an internal error.
    future<seastar::file> open_file(file_id_t);
};

// A segment that the store writes to. The segment is a fixed-size region of a file of the file
// manager, and writes append to it from the start of the region to its end. The segment manager
// makes the object when it allocates the segment. When a writer takes the segment, start() gives
// the segment its reference and its sequence number. A writer that shares the segment with other
// writers holds the write gate while it uses the segment, and stop() waits for these writers. The
// segment writes through the open file of the file manager, and file_manager::stop() closes that
// file. Thus the writes to the segment must end before the file manager stops.
//
// An append has two steps: reserve() takes the next region of the segment, and write_reserved()
// writes the data to it. _append_sem has one unit, and a reservation holds it until its write
// completes. Thus the writes go to the disk one at a time, in the order of their offsets, and a
// segment has no hole before the data that it holds. Only write_reserved() moves the append offset,
// thus a reservation that is dropped before its write leaves no hole either. When a write fails,
// the segment fails: it accepts no more appends, and the callers write to a new segment.
class segment {
public:
    struct append_reservation {
        segment_position pos;
        size_t size;
        seastar::semaphore_units<> units;
    };

    segment(log_segment_id id, seastar::file file, uint64_t file_offset, uint64_t size);

    void start(segment_ref, segment_sequence);

    future<> stop();

    seastar::gate::holder hold() {
        return _write_gate.hold();
    }

    // Reserves the next region of the segment for an append of this size, which must be a positive
    // multiple of ondisk::block_alignment. The reservation holds the unit of _append_sem until its write
    // completes. Returns nullopt if the data does not fit in the segment, or if the segment failed.
    // In both cases nothing is written, and the caller must write to another segment.
    future<std::optional<append_reservation>> reserve(size_t size);

    // Writes the data to the region of the reservation. The data must have the size of the
    // reservation. If the write fails, the segment fails.
    future<segment_position> write_reserved(append_reservation reservation, bytes_view data);

    // Reserves and writes in one call, for a caller that does not need the two steps. Fails if
    // the data does not fit in the segment, or if the segment failed.
    future<segment_position> append(bytes_view data);

    log_segment_id id() const noexcept { return _id; }

    segment_ref ref() const {
        return _seg_ref;
    }

    segment_sequence seq_num() const noexcept {
        return _seq_num;
    }

private:
    const log_segment_id _id;
    seastar::file _file;
    // The offset in the file where the segment starts.
    const uint64_t _file_offset;
    // The size of the region of the segment in the file.
    const uint64_t _size;

    seastar::gate _write_gate;
    seastar::semaphore _append_sem{1};
    segment_ref _seg_ref;
    segment_sequence _seq_num;

    // The offset in the segment of the next append.
    uint32_t _append_offset = 0;

    // Set when a write to the segment fails. Then the segment accepts no more appends, and
    // reserve() returns nullopt, so that the caller writes to a new segment.
    bool _failed = false;

    future<> do_write(segment_position, bytes_view data);

    bool can_fit(size_t size) const noexcept {
        return _append_offset + size <= _size;
    }

    uint64_t absolute_offset(uint64_t relative_offset) const noexcept {
        return _file_offset + relative_offset;
    }

    size_t bytes_remaining() const noexcept {
        return _size - _append_offset;
    }
};

}
