/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
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
};
