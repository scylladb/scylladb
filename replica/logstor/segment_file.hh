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
#include <vector>

#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/log.hh>

#include <fmt/format.h>

#include "seastarx.hh"

namespace replica::logstor {

extern seastar::logger logstor_logger;

using file_id_t = uint64_t;

// The configuration of the segment files of one shard.
struct file_manager_config {
    std::filesystem::path base_dir;
    uint64_t file_size;
    // The file count of the configuration. A new file is allocated only below it.
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
// I2. An allocated file is formatted and open. Outside recovery, only allocated files are open.
// I3. After recovery, while N files are allocated and N < configured, _next_file_formatter is the
//     format of file N: pending, done or failed.
// I4. configured <= actual. The files in [configured, actual) exist from an earlier, larger
//     configuration. They are allocated, but a new file is never allocated at or above configured.
//     Recovery removes them from the end when they hold no used segment.
// I5. _open_files has one entry per file on disk, actual in total.
// I6. Recovery reads the files it found before it sets the allocated count. After recovery, every
//     file a reader asks for is allocated, and open.
// I7. Recovery drives the file manager in this order: start(), set_actual_max_files(), reads of
//     the found files, remove_file() for each retired file, recover_next_file(). After that, only
//     get_file_for_new_segment() allocates files.
class file_manager {
public:
    file_manager(file_manager_config cfg)
        : _max_files{cfg.file_count, cfg.file_count}
        , _file_size(cfg.file_size)
        , _base_dir(std::move(cfg.base_dir))
        , _sched_group(cfg.format_sg)
        , _format_on_startup(cfg.format_on_startup)
        , _sparse_files(cfg.sparse_files)
        , _open_files(static_cast<size_t>(_max_files.actual))
    {}

    future<> start();

    future<> stop();

    // The file names are ls_{shard_id}-{file_id}-Data.db
    static const sstring get_file_name_prefix() {
        return fmt::format("ls_{}-", this_shard_id());
    }

    std::filesystem::path get_file_path(file_id_t file_id) const {
        auto fname = fmt::format("{}{}-Data.db", get_file_name_prefix(), file_id);
        return _base_dir / fname;
    }

    // The file id in a file name of this shard, or none if the name is not one.
    std::optional<file_id_t> file_name_to_file_id(const std::string& fname) const;

    // Recovery calls this with the count of the files it found on disk, which can be above the
    // configured count.
    void set_actual_max_files(uint64_t actual_max_files);

    // Removes a retired file. The caller must have checked that the file is the last one on disk,
    // at or above the configured count, and that it holds no used segment.
    future<> remove_file(file_id_t);

    // Recovery calls this at its end, with the count of the files it keeps. It opens them, and
    // formats the files that are not allocated, or starts the background format of the next file.
    future<> recover_next_file(file_id_t);

    // Returns the open file for a new segment. Recovery allocates the files that it finds, and with
    // format_on_startup also the files that it formats. A file that is formatted in the background
    // is allocated here, by the first segment taken from it. Each later segment finds the file
    // allocated and open.
    future<seastar::file> get_file_for_new_segment(file_id_t);

    uint64_t allocated_file_count() const noexcept { return _next_file_id; }

    uint64_t configured_max_files() const noexcept { return _max_files.configured; }

    // The file, if it is already open, which after startup it always is. A caller that finds it here
    // gets at it without the call and the future that get_file() costs even when it waits for
    // nothing. Otherwise it returns an unset file, which tells the caller that get_file() must open
    // the file.
    seastar::file opened_file(file_id_t file_id) const {
        check_file_id(file_id);
        return _open_files[file_id];
    }

    // The file, opened if it is not open.
    future<seastar::file> get_file(file_id_t);

    // Writes zeros over the region.
    future<> format_file_region(seastar::file file, uint64_t offset, uint64_t size);

private:
    // The zero buffer is written in one request over as much of a region as the disk takes in one,
    // see format_file_region(). 128 KiB is large enough that the vector of a request is short, and
    // small enough that the buffer costs nothing.
    static constexpr size_t zero_buffer_size = 128 * 1024;

    // configured: file count allowed by the current configuration; new file ids
    // are allocated only below this limit.
    // actual: file count currently present on disk. Recovery may raise this
    // above configured if it finds existing files beyond the configured limit.
    // Such files remain readable and their live segments remain usable until
    // freed, but new files are never allocated beyond configured. Fully retired
    // files may be deleted from the end.
    struct {
        const uint64_t configured;
        uint64_t actual;
    } _max_files;

    const uint64_t _file_size;
    const std::filesystem::path _base_dir;
    const seastar::scheduling_group _sched_group;
    const bool _format_on_startup;
    const bool _sparse_files;

    file_id_t _next_file_id{0};

    seastar::gate _async_gate;

    shared_future<> _next_file_formatter{make_ready_future<>()};

    // One handle per file, opened read-write, kept open for the life of the shard and shared by
    // the read and the write paths. Both go through it, so it is always opened for writing: a file
    // first touched by a read would otherwise hold a read-only handle the write path cannot use.
    // A file is opened when it is allocated, thus only the allocated files are open.
    std::vector<seastar::file> _open_files;

    std::unique_ptr<char[], seastar::free_deleter> _zero_buf;

    future<> open_allocated_files();

    future<> format_file(file_id_t);

    // Allocates the file if it is the next one, which waits for it to be formatted, opens it and
    // starts formatting the one after it in the background.
    future<> allocate_file(file_id_t);

    // Opens the file and puts it in the cache. The caller must have checked that it is not open.
    future<seastar::file> do_open_file(file_id_t);

    // A file id is valid if the file is on disk.
    void check_file_id(file_id_t file_id) const {
        if (file_id >= _open_files.size()) [[unlikely]] {
            on_internal_error(logstor_logger, fmt::format("Attempted to access file {} beyond the {} files on disk", file_id, _open_files.size()));
        }
    }
};

}
