/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/segment_file.hh"

#include <algorithm>
#include <charconv>
#include <cstring>
#include <ranges>
#include <set>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include <seastar/core/align.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/coroutine/parallel_for_each.hh>

#include "replica/logstor/ondisk.hh"
#include "utils/checked-file-impl.hh"
#include "utils/disk-error-handler.hh"
#include "utils/error_injection.hh"
#include "utils/lister.hh"

namespace replica::logstor {

file_manager::file_manager(file_manager_config cfg)
    : _cfg(std::move(cfg))
    , _open_files(static_cast<size_t>(_cfg.file_count))
    , _zero_buf(allocate_aligned_buffer<char>(zero_buffer_size, ondisk::block_alignment))
{
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
            logstor_logger.warn("Failed to close logstor file {}: {}", file_path(file_id).string(), result.get_exception());
        }
    });
}

std::optional<file_id_t> file_manager::file_id_of(std::string_view file_name) {
    const std::string prefix = file_name_prefix();
    const std::string_view suffix = "-Data.db";
    if (!file_name.starts_with(prefix) || !file_name.ends_with(suffix) || file_name.size() <= prefix.size() + suffix.size()) {
        return std::nullopt;
    }
    const auto digits = file_name.substr(prefix.size(), file_name.size() - prefix.size() - suffix.size());
    file_id_t file_id;
    const auto [end, ec] = std::from_chars(digits.data(), digits.data() + digits.size(), file_id);
    if (ec != std::errc() || end != digits.data() + digits.size()) {
        return std::nullopt;
    }
    return file_id;
}

file_manager::recovery file_manager::start_recovery() {
    check_recovery_step(recovery_step::not_started, "start_recovery");
    _recovery_step = recovery_step::started;
    return recovery(*this);
}

void file_manager::check_recovery_step(recovery_step expected, std::string_view call) const {
    if (_recovery_step != expected) [[unlikely]] {
        auto name = [] (recovery_step step) -> std::string_view {
            switch (step) {
            case recovery_step::not_started: return "not_started";
            case recovery_step::started: return "started";
            case recovery_step::opened: return "opened";
            case recovery_step::retired: return "retired";
            case recovery_step::finished: return "finished";
            }
            return "unknown";
        };
        on_internal_error(logstor_logger, fmt::format("File recovery: {} called at step {}, expected step {}",
                call, name(_recovery_step), name(expected)));
    }
}

future<file_id_t> file_manager::recovery::open_found_files() {
    auto& files = _files;
    files.check_recovery_step(recovery_step::started, "open_found_files");
    co_await seastar::recursive_touch_directory(files._cfg.base_dir.string());

    std::set<file_id_t> found_file_ids;
    std::vector<sstring> temporary_files;
    const std::string prefix = file_name_prefix();
    co_await lister::scan_dir(files._cfg.base_dir, lister::dir_entry_types::of<directory_entry_type::regular>(),
            [&] (fs::path dir, directory_entry de) {
        if (de.name.starts_with(prefix)) {
            if (auto file_id = file_id_of(de.name)) {
                found_file_ids.insert(*file_id);
            } else if (de.name.ends_with(".tmp")) {
                temporary_files.push_back((dir / de.name).string());
            }
        }
        return make_ready_future<>();
    });
    logstor_logger.info("Recovery: found {} files for shard {} in {}", found_file_ids.size(), this_shard_id(), files._cfg.base_dir.string());

    // A temporary file is a format that did not finish. format_file() makes a new one.
    co_await coroutine::parallel_for_each(temporary_files.begin(), temporary_files.end(), [] (const sstring& path) {
        logstor_logger.info("Recovery: removing leftover temp file {}", path);
        return seastar::remove_file(path);
    });

    file_id_t found_file_count = 0;
    for (auto file_id : found_file_ids) {
        if (file_id != found_file_count) {
            throw std::runtime_error(fmt::format("Missing log segment file(s) detected during recovery: file {} missing", files.file_path(found_file_count).string()));
        }
        found_file_count++;
    }

    files._open_files.resize(static_cast<size_t>(std::max(files._cfg.file_count, found_file_count)));
    files._allocated_file_count = found_file_count;
    co_await files.open_allocated_files();
    files._recovery_step = recovery_step::opened;
    co_return found_file_count;
}

// Opens every file that recovery found, so that no read opens one. A file that is allocated later
// is opened by allocate_file(). Files are formatted before they are allocated, so they all exist.
future<> file_manager::open_allocated_files() {
    return max_concurrent_for_each(std::views::iota(file_id_t(0), _allocated_file_count), 32,
            [this] (file_id_t file_id) {
        return open_file(file_id).discard_result();
    });
}

future<> file_manager::recovery::retire_unused_files(file_id_t used_file_count) {
    auto& files = _files;
    files.check_recovery_step(recovery_step::opened, "retire_unused_files");
    const auto kept_file_count = std::max(used_file_count, files._cfg.file_count);
    while (files._allocated_file_count > kept_file_count) {
        const auto file_id = files._allocated_file_count - 1;
        logstor_logger.info("Recovery: removing retired file {}", files.file_path(file_id).string());
        co_await files.remove_file(file_id);
    }
    files._recovery_step = recovery_step::retired;
}

future<> file_manager::remove_file(file_id_t file_id) {
    // The file is the last allocated one, at or above the configured count, thus the
    // last entry of _open_files.
    if (file_id + 1 != _allocated_file_count || _allocated_file_count <= _cfg.file_count) {
        on_internal_error(logstor_logger, fmt::format("Attempted to remove file {}: {} files allocated, {} configured",
                file_id, _allocated_file_count, _cfg.file_count));
    }
    // The file is open. The handle leaves _open_files before the close, so that stop() does
    // not close it again when the close or the removal fails.
    co_await std::exchange(_open_files[file_id], seastar::file()).close();
    co_await do_io_check(logstor_error_handler, [this, file_id] {
        return seastar::remove_file(file_path(file_id).string());
    });
    _open_files.pop_back();
    _allocated_file_count--;
}

future<> file_manager::recovery::finish() {
    auto& files = _files;
    files.check_recovery_step(recovery_step::retired, "finish");
    if (files._cfg.format_on_startup) {
        auto holder = files._async_gate.hold();
        co_await with_scheduling_group(files._cfg.format_sg, [&files] { return files.format_remaining_files(); });
    } else if (files._allocated_file_count < files._cfg.file_count) {
        files.start_next_file_format();
    }
    files._recovery_step = recovery_step::finished;
}

future<> file_manager::format_remaining_files() {
    if (_allocated_file_count >= _cfg.file_count) {
        co_return;
    }
    logstor_logger.info("Formatting {} logstor files", _cfg.file_count - _allocated_file_count);
    for (file_id_t file_id = _allocated_file_count; file_id < _cfg.file_count; ++file_id) {
        co_await format_file(file_id);
        co_await open_file(file_id);
        _allocated_file_count = file_id + 1;
    }
}

void file_manager::start_next_file_format() {
    _next_file_format = with_gate(_async_gate, [this, file_id = _allocated_file_count] {
        return with_scheduling_group(_cfg.format_sg, [this, file_id] {
            return format_file(file_id);
        });
    });
}

// Formatting a new file grows the store, so its I/O deliberately does not go through
// logstor_error_handler: failing to grow (e.g. ENOSPC) only stops further allocation and
// must not isolate the node, while writes to already formatted files keep working.
future<> file_manager::format_file(file_id_t file_id) {
    auto path = file_path(file_id).string();
    bool file_exists = co_await seastar::file_exists(path);
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
        auto tmp_path = path + ".tmp";
        auto tmp_file = co_await seastar::open_file_dma(tmp_path,
                seastar::open_flags::rw | seastar::open_flags::create | seastar::open_flags::truncate);
        if (_cfg.sparse_files) {
            // Reading a hole yields zeros, so the file is already formatted as
            // far as the reader is concerned.
            co_await tmp_file.truncate(_cfg.file_size);
        } else {
            co_await tmp_file.allocate(0, _cfg.file_size);
            co_await write_zeros(tmp_file, 0, _cfg.file_size);
        }
        co_await tmp_file.flush();
        co_await tmp_file.close();

        // move the temp file to the final location
        co_await seastar::rename_file(tmp_path, path);
    }
}

// The most a single write of the zero fill may ask the disk for. Larger requests keep saturating
// the disk with less of the queue, but a format that runs while the shard is serving shares the
// disk with the reads of that traffic, which wait behind whatever request is in flight.
static constexpr uint64_t max_format_write_size = 1024 * 1024;

future<> file_manager::write_zeros(seastar::file file, uint64_t offset, uint64_t size) {
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

future<seastar::file> file_manager::get_file_for_new_segment(file_id_t file_id) {
    check_recovery_step(recovery_step::finished, "get_file_for_new_segment");
    if (file_id == _allocated_file_count) [[unlikely]] {
        co_await allocate_file(file_id);
    }
    // A file after the next one fails here.
    co_return get_open_file(file_id);
}

future<> file_manager::allocate_file(file_id_t file_id) {
    if (file_id != _allocated_file_count || file_id >= _cfg.file_count) [[unlikely]] {
        on_internal_error(logstor_logger, fmt::format("Attempted to allocate file {}: {} files allocated, {} configured", file_id, _allocated_file_count, _cfg.file_count));
    }
    co_await _next_file_format.get_future();
    if (file_id != _allocated_file_count) [[unlikely]] {
        on_internal_error(logstor_logger, fmt::format("Attempted to allocate file {} twice", file_id));
    }

    // The format does not open the file, thus only the allocated files are open. If the open
    // fails, the file stays the next one, and the next attempt opens it again.
    co_await open_file(file_id);
    _allocated_file_count++;

    if (_allocated_file_count < _cfg.file_count) {
        start_next_file_format();
    }
}

// The handle is shared by the read and the write paths, so it is opened read-write even when a read
// is what asked for it, and with O_DSYNC, which is what makes a completed segment write durable.
// It is not opened with O_CREAT: files are created by format_file() alone, and a read of a file that
// is missing must fail rather than quietly create an empty one.
future<seastar::file> file_manager::open_file(file_id_t file_id) {
    utils::get_local_injector().inject("logstor_open_file_fail", [] {
        throw std::runtime_error("file open failed by injection");
    });

    auto file = co_await open_checked_file_dma(logstor_error_handler,
        file_path(file_id).string(),
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

future<> file_manager::zero_region(file_id_t file_id, uint64_t offset, uint64_t size) {
    if (offset > _cfg.file_size || size > _cfg.file_size - offset) [[unlikely]] {
        on_internal_error(logstor_logger, fmt::format("Attempted to zero bytes {} to {} of file {}, which has {} bytes",
                offset, offset + size, file_id, _cfg.file_size));
    }
    co_await write_zeros(get_open_file(file_id), offset, size);
}

segment::segment(log_segment_id id, seastar::file file, uint64_t file_offset, uint64_t max_size)
    : _id(id)
    , _file(std::move(file))
    , _file_offset(file_offset)
    , _max_size(max_size) {
}

void writeable_segment::start(segment_ref seg_ref, segment_sequence seq_num) {
    _seg_ref = std::move(seg_ref);
    _seq_num = seq_num;
}

future<> writeable_segment::stop() {
    if (_write_gate.is_closed()) {
        co_return;
    }
    co_await _write_gate.close();
}

future<std::optional<writeable_segment::append_reservation>> writeable_segment::reserve(size_t size) {
    if (_failed) [[unlikely]] {
        co_return std::nullopt;
    }

    std::optional<seastar::semaphore_units<>> units;
    try {
        units = co_await get_units(_append_sem, 1);
    } catch (...) {
        // The only way _append_sem breaks is a failed write retiring the segment, which
        // may have happened while we were waiting for the permit. Nothing was reserved,
        // so the caller can retry on another segment.
        co_return std::nullopt;
    }

    if (!can_fit(size)) {
        co_return std::nullopt;
    }

    append_reservation reservation {
        .pos = segment_position {
            .segment = _id,
            .offset = _current_offset,
        },
        .units = std::move(*units),
    };

    _current_offset += size;

    co_return std::move(reservation);
}

future<segment_position> writeable_segment::write_reserved(append_reservation reservation, bytes_view data) {
    auto write_result = co_await coroutine::as_future(do_write(reservation.pos, data));
    if (write_result.failed()) {
        auto ex = write_result.get_exception();

        // if a write fails, fail also all later writes to the segment, since we assume no holes in a segment.
        // the segment is retired, so that callers switch to a new segment instead of failing forever.
        _failed = true;
        _append_sem.broken(ex);
        logstor_logger.warn("Write to segment {} seq {} failed: {}, retiring segment", _id, _seq_num, ex);

        co_await coroutine::return_exception_ptr(std::move(ex));
    }
    co_return reservation.pos;
}

future<segment_position> writeable_segment::append(bytes_view data) {
    auto reservation = co_await reserve(data.size());
    if (!reservation) {
        co_return coroutine::return_exception(std::runtime_error(_failed
                ? fmt::format("Can't append write of size {} to segment {} retired by a failed write", data.size(), _id)
                : fmt::format("Can't append write of size {} to segment with {} bytes remaining", data.size(), bytes_remaining())));
    }
    co_return co_await write_reserved(std::move(*reservation), data);
}

future<> writeable_segment::do_write(segment_position pos, bytes_view data) {
    utils::get_local_injector().inject("logstor_fail_segment_write", [] {
        throw std::runtime_error("segment write failed by injection");
    });

    utils::get_local_injector().inject("logstor_segment_write_io_error", [] {
        // Simulates a device error reported by the file layer: the handler fires the logstor
        // disk error signal, which isolates the node, and turns it into a storage_io_error.
        logstor_error_handler(std::make_exception_ptr(std::system_error(EIO, std::system_category(),
                "segment write I/O error by injection")));
    });

    const auto alignment = _file.disk_write_dma_alignment();
    const uint64_t total = data.size();
    auto base_offset = absolute_offset(pos.offset);
    uint64_t written = 0;

    while (written < total) {
        auto new_written = co_await _file.dma_write(
                base_offset + written, data.data() + written, total - written);

        written += new_written;
        if (written == total) {
            break;
        }

        written = align_down(written, alignment);
    }
}

}
