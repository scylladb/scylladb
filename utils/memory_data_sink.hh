/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include "utils/small_vector.hh"

#include "seastarx.hh"

#pragma once

// Accumulates data sent to the memory_data_sink allowing it
// to be examined later.
class memory_data_sink_buffers {
    using buffers_type = utils::small_vector<temporary_buffer<char>, 1>;
    buffers_type _bufs;
    size_t _size = 0;
public:
    size_t size() const { return _size; }
    buffers_type& buffers() { return _bufs; }

    // Strong exception guarantees
    void put(temporary_buffer<char>&& buf) {
        auto size = buf.size();
        _bufs.emplace_back(std::move(buf));
        _size += size;
    }

    void clear() {
        _bufs.clear();
        _size = 0;
    }

    /// split current memory_data_sink_buffers into two at the given offset.
    /// the buffers at the right side of the split point are left in current
    /// instance.
    ///
    /// @param pos the split point
    /// @return a new instance of memory_data_sink_buffers with given size
    memory_data_sink_buffers split_at(size_t pos) {
        assert(_size >= pos);
        memory_data_sink_buffers slice;
        auto split_point = _bufs.begin();
        while (pos > 0) {
            size_t buf_size = split_point->size();
            if (buf_size > pos) {
                slice.put(split_point->share().prefix(pos));
                split_point->trim_front(pos);
                break;
            } else {
                slice.put(std::move(*split_point));
                pos -= buf_size;
            }
            ++split_point;
        }
        std::move(split_point, _bufs.end(), _bufs.begin());
        _bufs.resize(std::distance(split_point, _bufs.end()));
        _size -= slice.size();
        return slice;
    }

    memory_data_sink_buffers() = default;

    memory_data_sink_buffers(memory_data_sink_buffers&& other)
            : _bufs(std::move(other._bufs))
            , _size(std::exchange(other._size, 0))
    {
    }
};

class memory_data_sink : public data_sink_impl {
    memory_data_sink_buffers& _bufs;
public:
    memory_data_sink(memory_data_sink_buffers& b) : _bufs(b) {}
    virtual future<> put(net::packet data)  override {
        abort();
        return make_ready_future<>();
    }
    virtual future<> put(temporary_buffer<char> buf) override {
        _bufs.put(std::move(buf));
        return make_ready_future<>();
    }
    virtual future<> flush() override {
        return make_ready_future<>();
    }
    virtual future<> close() override {
        return make_ready_future<>();
    }
    size_t buffer_size() const noexcept override {
        return 128*1024;
    }
};
