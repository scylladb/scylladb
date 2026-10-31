/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/bool_class.hh>
#include <seastar/util/log.hh>

#include <fmt/format.h>

#include "seastarx.hh"

namespace replica::logstor {

extern seastar::logger logstor_logger;

using file_id_t = uint64_t;

using wait_for_format = seastar::bool_class<class wait_for_format_tag>;

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
    // After the format of the next file fails, an allocation of the file starts the format again
    // only when this interval has passed.
    std::chrono::milliseconds format_retry_interval;
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
    // allocated and open. If the format of the file is not done and `wait` is no, the call fails at
    // once and does not wait for the format. The caller must have finished the recovery. The caller
    // must not call this again before the call returns, because a second call for a file that is in
    // allocation is an internal error.
    future<seastar::file> get_file_for_new_segment(file_id_t, wait_for_format wait = wait_for_format::yes);

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

    // The background format of the next file. A failed format keeps its error until
    // allocate_file() starts the format again, when _cfg.format_retry_interval has passed.
    shared_future<> _next_file_format{make_ready_future<>()};
    lowres_clock::time_point _next_file_format_failed_at{};

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

    // Allocates the next file: waits for its format (or fails, if `wait` is no and the format is not
    // done), opens it, and starts the format of the file after it in the background. The caller
    // must give the next file, below the configured count, and must not call this again before it
    // returns.
    future<> allocate_file(file_id_t, wait_for_format);

    // Opens the file and puts it in the cache. An open of a file that is open is an internal error.
    future<seastar::file> open_file(file_id_t);
};

}
