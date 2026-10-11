/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/segment_file.hh"

#include <algorithm>
#include <cstring>
#include <ranges>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/coroutine/as_future.hh>

#include "replica/logstor/ondisk.hh"
#include "utils/checked-file-impl.hh"
#include "utils/disk-error-handler.hh"
#include "utils/error_injection.hh"

namespace replica::logstor {

// The most a single write of the zero fill may ask the disk for. Larger requests keep saturating
// the disk with less of the queue, but a format that runs while the shard is serving shares the
// disk with the reads of that traffic, which wait behind whatever request is in flight.
static constexpr uint64_t max_format_write_size = 1024 * 1024;

future<> file_manager::start() {
    co_await seastar::recursive_touch_directory(_base_dir.string());
    _zero_buf = allocate_aligned_buffer<char>(zero_buffer_size, ondisk::block_alignment);
    std::memset(_zero_buf.get(), 0, zero_buffer_size);
}

future<> file_manager::stop() {
    if (!_async_gate.is_closed()) {
        co_await _async_gate.close();
    }

    // Closing a file opened for writing goes through the syscall thread pool, and a shard configured
    // for a large disk holds thousands of them, so they are not closed one after the other. A close
    // that fails is only reported: the store is going away and there is nothing left to do about it.
    co_await max_concurrent_for_each(std::views::iota(size_t(0), _open_files.size()), 32,
            [this] (size_t file_id) -> future<> {
        auto file = std::exchange(_open_files[file_id], seastar::file());
        if (!file) {
            co_return;
        }
        auto result = co_await coroutine::as_future(file.close());
        if (result.failed()) {
            logstor_logger.warn("Failed to close logstor file {}: {}", get_file_path(file_id).string(), result.get_exception());
        }
    });
}

std::optional<file_id_t> file_manager::file_name_to_file_id(const std::string& fname) const {
    std::string prefix = get_file_name_prefix();
    std::string suffix = "-Data.db";
    if (fname.starts_with(prefix) && fname.ends_with(suffix)) {
        // Extract file_id between prefix and suffix
        size_t start = prefix.size();
        size_t end = fname.size() - suffix.size();
        std::string file_id_str = fname.substr(start, end - start);
        try {
            return static_cast<file_id_t>(std::stoull(file_id_str));
        } catch (...) {
            return std::nullopt;
        }
    }
    return std::nullopt;
}

void file_manager::set_actual_max_files(uint64_t actual_max_files) {
    if (actual_max_files < _max_files.configured) {
        on_internal_error(logstor_logger, fmt::format("Attempted to set actual max files {} below configured max files {}", actual_max_files, _max_files.configured));
    }
    if (actual_max_files < _max_files.actual) {
        on_internal_error(logstor_logger, fmt::format("Attempted to reduce actual max files from {} to {}", _max_files.actual, actual_max_files));
    }
    _max_files.actual = actual_max_files;
    _open_files.resize(static_cast<size_t>(_max_files.actual));
}

future<> file_manager::remove_file(file_id_t file_id) {
    if (file_id != _max_files.actual - 1) {
        on_internal_error(logstor_logger, fmt::format("Attempted to remove file {} that is not the last file on disk, actual max files {}", file_id, _max_files.actual));
    }
    if (file_id + 1 != _next_file_id) {
        on_internal_error(logstor_logger, fmt::format("Attempted to remove file {} that is not the last allocated file {}", file_id, _next_file_id));
    }
    if (_max_files.actual <= _max_files.configured) {
        on_internal_error(logstor_logger, fmt::format("Attempted to remove file {} while actual max files {} is not above configured max files {}", file_id, _max_files.actual, _max_files.configured));
    }
    if (file_id < _open_files.size()) {
        if (file_id + 1 != _open_files.size()) {
            on_internal_error(logstor_logger, fmt::format("Attempted to remove file {} while higher file {} is still allocated", file_id, file_id + 1));
        }
        if (_open_files[file_id]) {
            co_await _open_files[file_id].close();
        }
        _open_files.pop_back();
    }
    co_await do_io_check(logstor_error_handler, [this, file_id] {
        return seastar::remove_file(get_file_path(file_id).string());
    });
    _max_files.actual--;
    _next_file_id--;
}

future<> file_manager::recover_files(file_id_t file_count) {
    _next_file_id = file_count;
    co_await open_allocated_files();
}

future<> file_manager::recover_next_file() {
    if (_format_on_startup) {
        auto holder = _async_gate.hold();
        file_id_t next_file_id = _next_file_id;
        co_await with_scheduling_group(_sched_group, [this, next_file_id] -> future<> {
            logstor_logger.info("Formatting {} logstor files", _max_files.configured);
            for (file_id_t file_id = next_file_id; file_id < _max_files.configured; ++file_id) {
                co_await format_file(file_id);
                // Open the file before it counts as allocated (I2).
                co_await do_open_file(file_id);
                _next_file_id = file_id + 1;
            }
        });
        co_return;
    }

    if (_next_file_id < _max_files.configured) {
        _next_file_formatter = with_gate(_async_gate, [this] {
            return with_scheduling_group(_sched_group, [this] {
                return format_file(_next_file_id);
            });
        });
    }
}

future<seastar::file> file_manager::get_file_for_new_segment(file_id_t file_id) {
    if (file_id >= _next_file_id) [[unlikely]] {
        return allocate_file(file_id).then([this, file_id] {
            return get_file(file_id);
        });
    }
    return get_file(file_id);
}

// Not a coroutine: every file is opened at startup and held for the life of the shard, so this ends
// in a value it already has, and in continuation style it hands that value back without allocating
// a frame for a wait that does not happen.
future<seastar::file> file_manager::get_file(file_id_t file_id) {
    check_file_id(file_id);

    if (auto& cached_file = _open_files[file_id]) {
        return make_ready_future<seastar::file>(cached_file);
    }

    return do_open_file(file_id);
}

future<> file_manager::format_file_region(seastar::file file, uint64_t offset, uint64_t size) {
    // One request covers as much as the disk takes in one, up to max_format_write_size, which the
    // zero buffer is repeated across rather than held in memory: a request an order of magnitude
    // larger than the buffer is worth most of the difference between a stream of small writes and
    // one the device is saturated by, and the cap keeps a format that runs while the shard serves
    // traffic - the background format of the next file - from putting a request in the disk's way
    // that a read then waits behind.
    const auto max_write_size = std::min<uint64_t>(file.disk_write_max_length(), max_format_write_size);

    uint64_t remaining = size;
    uint64_t current_offset = offset;

    while (remaining > 0) {
        auto write_size = std::min(remaining, max_write_size);

        size_t written;
        if (write_size <= zero_buffer_size) {
            // A region smaller than the buffer needs no vector, which is every write of the block a
            // discarded segment has its header invalidated with.
            written = co_await file.dma_write(current_offset, _zero_buf.get(), write_size);
        } else {
            std::vector<iovec> iov;
            iov.reserve(write_size / zero_buffer_size + 1);
            for (uint64_t covered = 0; covered < write_size; covered += zero_buffer_size) {
                iov.emplace_back(iovec{_zero_buf.get(), std::min<size_t>(zero_buffer_size, write_size - covered)});
            }
            written = co_await file.dma_write(current_offset, std::move(iov));
        }

        current_offset += written;
        remaining -= written;
    }
}

// Formatting a new file grows the store, so its I/O deliberately does not go through
// logstor_error_handler: failing to grow (e.g. ENOSPC) only stops further allocation and
// must not isolate the node, while writes to already formatted files keep working.
future<> file_manager::format_file(file_id_t file_id) {
    auto file_path = get_file_path(file_id).string();
    bool file_exists = co_await seastar::file_exists(file_path);
    if (!file_exists) {
        // Create and format a temporary file, then move it to the final location.
        //
        // The temporary file is deliberately not opened with O_DSYNC, unlike the file the segments
        // are then written through: a segment write is acknowledged when it completes and has to be
        // durable by then, while formatting only has to be durable before the rename publishes the
        // file. Flushing once at the end rather than on every one of the writes below is worth more
        // than an order of magnitude, and the durability of the published file is the same either
        // way. The commitlog re-opens its segments without O_DSYNC to pre-write them for the same
        // reason, see segment_manager::allocate_segment_ex().
        auto tmp_path = file_path + ".tmp";
        auto tmp_file = co_await seastar::open_file_dma(tmp_path,
                seastar::open_flags::rw | seastar::open_flags::create | seastar::open_flags::truncate);
        if (_sparse_files) {
            // Reading a hole yields zeros, so the file is already formatted as
            // far as the reader is concerned.
            co_await tmp_file.truncate(_file_size);
        } else {
            co_await tmp_file.allocate(0, _file_size);
            co_await format_file_region(tmp_file, 0, _file_size);
        }
        co_await tmp_file.flush();
        co_await tmp_file.close();

        // move the temp file to the final location
        co_await seastar::rename_file(tmp_path, file_path);
    }
}

// Opens every file that recovery found, so that no read opens one. A file that is allocated later
// is opened by allocate_file(). Files are formatted before they are allocated, so they all exist.
future<> file_manager::open_allocated_files() {
    return max_concurrent_for_each(std::views::iota(file_id_t(0), _next_file_id), 32,
            [this] (file_id_t file_id) {
        return do_open_file(file_id).discard_result();
    });
}

future<> file_manager::allocate_file(file_id_t file_id) {
    check_file_id(file_id);

    if (file_id == _next_file_id && file_id < _max_files.configured) {
        // allocate file_id and wait for it to be formatted, and start formatting
        // the next file in background
        co_await _next_file_formatter.get_future();

        if (file_id == _next_file_id) {
            // The format does not open the file, thus only the allocated files are open. If the
            // open fails, the file stays the next one, and the next attempt opens it again.
            co_await do_open_file(file_id);
            _next_file_id++;

            if (_next_file_id < _max_files.configured) {
                _next_file_formatter = with_gate(_async_gate, [this] {
                    return with_scheduling_group(_sched_group, [this] {
                        return format_file(_next_file_id);
                    });
                });
            }
        }
    } else if (file_id >= _max_files.configured && file_id >= _next_file_id) {
        on_internal_error(logstor_logger, "Disk size limit reached, cannot allocate more files");
    } else if (file_id > _next_file_id) {
        on_internal_error(logstor_logger, "files must be allocated in sequential order");
    }
}

// The handle is shared by the read and the write paths, so it is opened read-write even when a read
// is what asked for it, and with O_DSYNC, which is what makes a completed segment write durable.
// It is not opened with O_CREAT: files are created by format_file() alone, and a read of a file that
// is missing must fail rather than quietly create an empty one.
future<seastar::file> file_manager::do_open_file(file_id_t file_id) {
    utils::get_local_injector().inject("logstor_open_file_fail", [] {
        throw std::runtime_error("file open failed by injection");
    });

    auto file = co_await open_checked_file_dma(logstor_error_handler,
        get_file_path(file_id).string(),
        seastar::open_flags::rw | seastar::open_flags::dsync
    );

    // Each file is opened once. The file is open now if the caller opened an open file, or if
    // another caller opened it at the same time.
    if (_open_files[file_id]) [[unlikely]] {
        co_await file.close();
        on_internal_error(logstor_logger, fmt::format("Attempted to open logstor file {} that is already open", file_id));
    }
    _open_files[file_id] = file;

    co_return file;
}

}
