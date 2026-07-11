/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string_view>

#include <seastar/core/align.hh>
#include <seastar/core/deleter.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>

#include "seastarx.hh"
#include "utils/assert.hh"
#include "reader_permit.hh"
#include "sstables/exceptions.hh"

namespace sstables {

// Reads a file at a movable position, forwards and backwards. Each I/O loads
// one interval of the file into a buffer. Reads inside that interval are served
// from the buffer. The next I/O replaces the buffer.
//
// set_bounds() limits I/O to an interval of the file. It widens the interval to
// the DMA alignment of the file, but not past the end of the file. Thus,
// upper_bound() can be greater than the upper bound passed to set_bounds().
// Reads of the widened part are valid.
//
// A forward I/O loads max_read_size bytes, but not past the `limit` that the
// caller passes, unless the read itself needs more. A backward I/O loads
// initial_backward_read_size bytes the first time, or more if the read needs
// more. The size doubles with each backward I/O, up to max_read_size. seek()
// doesn't reset it.
//
// No I/O is larger than max_read_size, including the bytes added by aligning
// it. If a read needs more, the I/O loads part of it. The *_exactly() functions
// then load the rest with more I/O.
//
// The cursor doesn't own the file. The caller closes it. Thus, the file can be
// shared, such as the sstable's data file.
//
// The cursor doesn't charge the permit for the I/O buffers. The caller should
// pass a file from make_tracked_file(), which charges them.
class file_cursor {
    seastar::file _file;
    uint64_t _position = 0;
    reader_permit _permit;
    uint64_t _lower_bound = 0;
    uint64_t _upper_bound;
    uint64_t _file_size;

    temporary_buffer<char> _current;
    uint64_t _current_end = 0;

    // Backward reads start small and double up to the maximum. This matches
    // the logical reversing path, which also never shrinks its read size
    // within a partition.
    constexpr static uint64_t initial_backward_read_size = 4096;
    constexpr static uint64_t max_read_size = 128 * 1024;
    uint64_t _backward_read_size = initial_backward_read_size;

public:
    file_cursor(seastar::file f, uint64_t file_size, reader_permit permit)
        : _file(std::move(f))
        , _permit(std::move(permit))
        , _upper_bound(file_size)
        , _file_size(file_size)
    {}
    void set_bounds(uint64_t lower, uint64_t upper) {
        SCYLLA_ASSERT(lower <= upper);
        SCYLLA_ASSERT(upper <= _file_size);
        size_t align = _file.disk_read_dma_alignment();
        _lower_bound = align_down(lower, align);
        _upper_bound = std::min<uint64_t>(_file_size, align_up(upper, align));
        _position = std::clamp(_position, _lower_bound, _upper_bound);
    }
    void seek(uint64_t pos) {
        parse_assert(pos >= _lower_bound && pos <= _upper_bound);
        _position = std::clamp(pos, _lower_bound, _upper_bound);
    }
    // The loaded bytes after the position, up to the upper bound. Doesn't do
    // I/O, and doesn't move the position. Empty if the buffer doesn't hold the
    // byte at the position. Valid until the next read.
    std::string_view peek_forwards() const noexcept {
        const uint64_t buf_start = _current_end - _current.size();
        const uint64_t end = std::min(_current_end, _upper_bound);
        if (buf_start <= _position && _position < end) {
            return std::string_view(_current.get() + (_position - buf_start), end - _position);
        }
        return {};
    }
    // The loaded bytes before the position, down to the lower bound. Doesn't do
    // I/O, and doesn't move the position. Empty if the buffer doesn't hold the
    // byte before the position. Valid until the next read.
    std::string_view peek_backwards() const noexcept {
        const uint64_t buf_start = _current_end - _current.size();
        const uint64_t start = std::max(buf_start, _lower_bound);
        if (start < _position && _position <= _current_end) {
            return std::string_view(_current.get() + (start - buf_start), _position - start);
        }
        return {};
    }
    static std::optional<temporary_buffer<char>> maybe_share_buffer_forwards(uint64_t position, uint64_t n, uint64_t buf_end, temporary_buffer<char>& buf) {
        uint64_t buf_start = buf_end - buf.size();
        if (buf_start <= position && position < buf_end) {
            n = std::min(buf_end - position, n);
            return buf.share(position - buf_start, n);
        }
        return std::nullopt;
    }
    static std::optional<temporary_buffer<char>> maybe_share_buffer_backwards(uint64_t position, uint64_t n, uint64_t buf_end, temporary_buffer<char>& buf) {
        uint64_t buf_start = buf_end - buf.size();
        if (buf_start < position && position <= buf_end) {
            n = std::min(position - buf_start, n);
            return buf.share(position - buf_start - n, n);
        }
        return std::nullopt;
    }
    // The size of the largest I/O. It is aligned, so an aligned I/O of this
    // size is not larger than max_read_size.
    uint64_t max_io_size() const {
        return align_down(max_read_size, _file.disk_read_dma_alignment());
    }
    // Loads [begin, end). Both must be aligned.
    future<> load_interval(uint64_t begin, uint64_t end) {
        _current = co_await _file.dma_read<char>(begin, end - begin);
        _current_end = begin + _current.size();
    }
    // Forward reads use the maximum size from the start, as a bounded
    // input_stream does. They don't read ahead past the file offset `limit`:
    // the end of the data that the reader needs.
    future<temporary_buffer<char>> read_forwards_up_to(size_t n, uint64_t limit) {
        if (auto buf = maybe_share_buffer_forwards(_position, n, _current_end, _current)) {
            _position += buf->size();
            co_return std::move(*buf);
        }
        const uint64_t align = _file.disk_read_dma_alignment();
        // _lower_bound is aligned, so `begin` is not below it.
        const uint64_t begin = align_down(_position, align);
        uint64_t end = std::max(_position + n, std::min(_position + max_read_size, limit));
        end = std::min(align_up(std::min(end, _upper_bound), align), begin + max_io_size());
        co_await load_interval(begin, end);

        if (auto buf = maybe_share_buffer_forwards(_position, n, _current_end, _current)) {
            _position += buf->size();
            co_return std::move(*buf);
        }

        co_return temporary_buffer<char>();
    }
    future<temporary_buffer<char>> read_backwards_up_to(size_t n) {
        n = std::min(_position, n);
        if (auto buf = maybe_share_buffer_backwards(_position, n, _current_end, _current)) {
            _position -= buf->size();
            co_return std::move(*buf);
        }
        const uint64_t align = _file.disk_read_dma_alignment();
        const uint64_t end = align_up(_position, align);
        // The I/O loads the `n` bytes before the position, and at least
        // _backward_read_size bytes, but not more than max_io_size().
        const uint64_t size = std::min(align_up(std::max(_backward_read_size, end - _position + n), align), max_io_size());
        const uint64_t begin = std::max(_lower_bound, end - std::min(end, size));
        co_await load_interval(begin, end);
        _backward_read_size = std::min<uint64_t>(_backward_read_size * 2, max_read_size);

        if (auto buf = maybe_share_buffer_backwards(_position, n, _current_end, _current)) {
            _position -= buf->size();
            co_return std::move(*buf);
        }

        co_return temporary_buffer<char>();
    }
    future<temporary_buffer<char>> read_forwards_exactly(size_t n, uint64_t limit) {
        auto units = _permit.consume_memory(n);
        auto out = temporary_buffer<char>(n);
        while (n) {
            auto frag = co_await read_forwards_up_to(n, limit);
            if (frag.empty()) {
                break;
            }
            std::memcpy(out.get_write() + (out.size() - n), frag.get(), frag.size());
            n -= frag.size();
        }
        out = temporary_buffer<char>(out.get_write(), out.size(), make_object_deleter(out.release(), std::move(units)));
        out.trim(out.size() - n);
        co_return out;
    }
    future<temporary_buffer<char>> read_backwards_exactly(size_t n) {
        auto units = _permit.consume_memory(n);
        auto out = temporary_buffer<char>(n);
        while (n) {
            auto frag = co_await read_backwards_up_to(n);
            if (frag.empty()) {
                break;
            }
            std::memcpy(out.get_write() + (n - frag.size()), frag.get(), frag.size());
            n -= frag.size();
        }
        out = temporary_buffer<char>(out.get_write(), out.size(), make_object_deleter(out.release(), std::move(units)));
        out.trim_front(n);
        co_return out;
    }
    // Doesn't close the file, which the cursor doesn't own. Each I/O finishes
    // before the call that started it returns, so no I/O is left to wait for.
    future<> close() {
        return make_ready_future<>();
    }
    uint64_t position() const {
        return _position;
    }
    uint64_t file_size() const {
        return _file_size;
    }
    uint64_t upper_bound() const {
        return _upper_bound;
    }
};

} // namespace sstables
