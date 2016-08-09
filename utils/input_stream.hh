/*
 * Copyright (C) 2016 ScyllaDB
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

#pragma once

#include "bytes_ostream.hh"

namespace utils {

class fragmented_input_stream {
    bytes_ostream::fragment_iterator _it;
    bytes_view _current;
    size_t _size;
private:
    template<typename Func>
    //requires requires(Func f, bytes_view bv) { { f(bv) } -> void; }
    void for_each_fragment(size_t size, Func&& func) {
        if (size > _size) {
            throw std::out_of_range("deserialization buffer underflow");
        }
        _size -= size;
        while (size) {
            if (_current.empty()) {
                _current = *_it++;
            }
            auto this_size = std::min(_current.size(), size);
            func(_current.substr(0, this_size));
            _current.remove_prefix(this_size);
            size -= this_size;
        }
    }
public:
    explicit fragmented_input_stream(bytes_view bv)
        : _it(nullptr), _current(bv), _size(_current.size()) { }
    fragmented_input_stream(bytes_ostream::fragment_iterator it, size_t size)
        : _it(it), _current(size ? *_it++ : bytes_view()), _size(size) { }
    fragmented_input_stream(bytes_ostream::fragment_iterator it, bytes_view bv, size_t size)
        : _it(it), _current(bv), _size(size) { }

    void skip(size_t size) {
        for_each_fragment(size, [] (auto) { });
    }
    fragmented_input_stream read_substream(size_t size) {
        if (size > _size) {
            throw std::out_of_range("deserialization buffer underflow");
        }
        fragmented_input_stream substream(_it, _current, size);
        skip(size);
        return substream;
    }
    void read(char* p, size_t size) {
        for_each_fragment(size, [&p] (auto bv) {
            p = std::copy_n(bv.data(), bv.size(), p);
        });
    }
    template<typename Output>
    void copy_to(Output& out) {
        for_each_fragment(_size, [&out] (auto bv) {
            out.write(reinterpret_cast<const char*>(bv.data()), bv.size());
        });
    }
    const size_t size() const {
        return _size;
    }
};

}