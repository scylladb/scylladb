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
#include <string_view>
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

    // the file names are ls_{shard_id}-{file_id}-Data.db
    static const sstring get_file_name_prefix() {
        return fmt::format("ls_{}-", this_shard_id());
    }

    std::filesystem::path get_file_path(file_id_t file_id) const {
        auto fname = fmt::format("{}{}-Data.db", get_file_name_prefix(), file_id);
        return _base_dir / fname;
    }

    std::optional<file_id_t> file_name_to_file_id(const std::string& fname) const;

    void set_actual_max_files(uint64_t actual_max_files);
    future<> remove_file(file_id_t);
    future<> recover_next_file(file_id_t);

    // Returns the open file for a new segment. Recovery allocates the files that it finds, and with
    // format_on_startup also the files that it formats. A file that is formatted in the background
    // is allocated here, by the first segment taken from it. Each later segment finds the file
    // allocated and open.
    future<seastar::file> get_file_for_new_segment(file_id_t);

    // The file, if it is already open, which after startup it always is. A caller that finds it here
    // gets at it without the call and the future that get_file() costs even when it waits for
    // nothing. Otherwise it returns an unset file, which tells the caller that get_file() must open
    // the file.
    seastar::file opened_file(file_id_t file_id) const {
        if (file_id >= _open_files.size()) [[unlikely]] {
            on_internal_error(logstor_logger, "Attempted to access file beyond actual disk capacity");
        }
        return _open_files[file_id];
    }

    future<seastar::file> get_file(file_id_t);

    // Opens the file and puts it in the cache. The caller must have checked that it is not open.
    future<seastar::file> do_open_file(file_id_t);

    future<> format_file_region(seastar::file file, uint64_t offset, uint64_t size);
    future<> format_file(file_id_t);
    future<> open_allocated_files();

    uint64_t allocated_file_count() const noexcept { return _next_file_id; }

    uint64_t configured_max_files() const noexcept { return _max_files.configured; }
    uint64_t actual_max_files() const noexcept { return _max_files.actual; }

private:
    // configured: file count allowed by the current configuration; new file ids
    // are allocated only below this limit.
    // actual: file count currently present on disk. Recovery may raise this
    // above configured if it finds existing files beyond the configured limit.
    // Such files remain readable and their live segments remain usable until
    // freed, but new files are never allocated beyond configured. Fully retired
    // files may be deleted from the end.
    struct {
        uint64_t configured;
        uint64_t actual;
    } _max_files;

    uint64_t _file_size;
    std::filesystem::path _base_dir;
    seastar::scheduling_group _sched_group;
    bool _format_on_startup;
    bool _sparse_files;

    file_id_t _next_file_id{0};

    seastar::gate _async_gate;
    shared_future<> _next_file_formatter{make_ready_future<>()};

    // One handle per file, opened read-write, kept open for the life of the shard and shared by
    // the read and the write paths. Both go through it, so it is always opened for writing: a file
    // first touched by a read would otherwise hold a read-only handle the write path cannot use.
    // A file is opened when it is allocated, thus only the allocated files are open.
    std::vector<seastar::file> _open_files;

    std::unique_ptr<char[], seastar::free_deleter> _zero_buf;
    size_t _zero_buf_size{0};

    // Allocates the file if it is the next one, which waits for it to be formatted, opens it and
    // starts formatting the one after it in the background.
    future<> allocate_file(file_id_t);
};

}
