/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// A read-only seastar::file over bytes in memory, which records every read.
// Tests use it to read files that they built in memory, and to check which
// byte ranges a reader asked for. dma_read_bulk() allocates its buffer as
// the posix file does: the request aligned to the read alignment.

#include <cstring>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include <sys/stat.h>

#include <seastar/core/align.hh>
#include <seastar/core/file.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/util/later.hh>
#include <seastar/core/shared_ptr.hh>

#include "seastarx.hh"

namespace tests {

class memory_file_impl final : public file_impl {
public:
    struct read_record {
        uint64_t offset;
        uint64_t size;
        /// The buffer that dma_read_bulk() allocated, or 0.
        uint64_t allocated = 0;
    };
private:
    std::string _data;
    std::vector<read_record> _reads;
    bool _yield = false;
    std::optional<shared_promise<>> _hold;
    uint64_t _hold_from = 0;
    size_t _held = 0;

    size_t available(uint64_t pos, size_t len) const noexcept {
        return pos >= _data.size() ? 0 : std::min<uint64_t>(len, _data.size() - pos);
    }
    [[noreturn]] static void read_only() {
        throw std::runtime_error("memory_file_impl is read-only");
    }
public:
    explicit memory_file_impl(std::string data) : _data(std::move(data)) {}

    const std::string& data() const noexcept { return _data; }
    /// The reads so far, in the order of the calls.
    const std::vector<read_record>& reads() const noexcept { return _reads; }
    void clear_reads() noexcept { _reads.clear(); }
    /// Makes dma_read_bulk() yield before it reads, so that concurrent
    /// readers interleave.
    void set_yield(bool yield) noexcept { _yield = yield; }
    /// Sets the read alignment, a power of two.
    void set_read_alignment(unsigned alignment) noexcept {
        _disk_read_dma_alignment = alignment;
        _memory_dma_alignment = alignment;
    }
    /// Makes dma_read_bulk() at `from` or beyond wait, after it records
    /// the read, until release_reads().
    void hold_reads(uint64_t from = 0) {
        _hold.emplace();
        _hold_from = from;
    }
    void release_reads() {
        if (_hold) {
            _hold->set_value();
            _hold.reset();
        }
    }
    /// The calls of dma_read_bulk() that wait for release_reads().
    size_t held_reads() const noexcept { return _held; }

    future<size_t> write_dma(uint64_t, const void*, size_t, io_intent*) override { read_only(); }
    future<size_t> write_dma(uint64_t, std::vector<iovec>, io_intent*) override { read_only(); }

    future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, io_intent*) override {
        _reads.push_back({pos, len});
        const size_t n = available(pos, len);
        std::memcpy(buffer, _data.data() + pos, n);
        return make_ready_future<size_t>(n);
    }

    future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, io_intent*) override {
        size_t total = 0;
        for (const auto& v : iov) {
            total += v.iov_len;
        }
        _reads.push_back({pos, total});
        size_t done = 0;
        for (const auto& v : iov) {
            const size_t n = available(pos + done, v.iov_len);
            std::memcpy(v.iov_base, _data.data() + pos + done, n);
            done += n;
            if (n < v.iov_len) {
                break;
            }
        }
        return make_ready_future<size_t>(done);
    }

    future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, io_intent* intent) override {
        if (_hold && offset >= _hold_from) {
            const size_t record = _reads.size();
            _reads.push_back({offset, range_size});
            ++_held;
            return _hold->get_shared_future().then([this, offset, range_size, record] {
                --_held;
                return read_bulk(offset, range_size, record);
            });
        }
        if (_yield) {
            return seastar::yield().then([this, offset, range_size] {
                _reads.push_back({offset, range_size});
                return read_bulk(offset, range_size, _reads.size() - 1);
            });
        }
        _reads.push_back({offset, range_size});
        return read_bulk(offset, range_size, _reads.size() - 1);
    }

private:
    future<temporary_buffer<uint8_t>> read_bulk(uint64_t offset, size_t range_size, size_t record) {
        const uint64_t alignment = _disk_read_dma_alignment;
        const uint64_t front = offset % alignment;
        const uint64_t start = offset - front;
        auto buf = temporary_buffer<uint8_t>::aligned(_memory_dma_alignment, align_up<uint64_t>(front + range_size, alignment));
        _reads[record].allocated = buf.size();
        const size_t n = available(start, buf.size());
        std::memcpy(buf.get_write(), _data.data() + start, n);
        buf.trim(std::max<uint64_t>(n, front));
        buf.trim_front(front);
        buf.trim(std::min<uint64_t>(buf.size(), range_size));
        return make_ready_future<temporary_buffer<uint8_t>>(std::move(buf));
    }

public:

    future<> flush() override { return make_ready_future<>(); }
    future<struct stat> stat() override {
        struct stat st{};
        st.st_size = _data.size();
        return make_ready_future<struct stat>(st);
    }
    future<> truncate(uint64_t) override { read_only(); }
    future<> discard(uint64_t, uint64_t) override { read_only(); }
    future<> allocate(uint64_t, uint64_t) override { read_only(); }
    future<uint64_t> size() override { return make_ready_future<uint64_t>(_data.size()); }
    future<> close() override { return make_ready_future<>(); }
    subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)>) override {
        throw std::runtime_error("memory_file_impl is not a directory");
    }
};

/// A file over `data`, and its implementation for inspecting the reads.
inline std::pair<file, shared_ptr<memory_file_impl>> make_memory_file(std::string data) {
    auto impl = seastar::make_shared<memory_file_impl>(std::move(data));
    return {file(impl), impl};
}

} // namespace tests
