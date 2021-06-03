/*
 * Copyright (C) 2019-present ScyllaDB
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

#include <list>
#include <fmt/format.h>
#include <seastar/net/byteorder.hh>
#include <seastar/util/backtrace.hh>

#include "utils/fragment_range.hh"
#include "utils/bit_cast.hh"

namespace utils {

// Facilitates transparently reading from a fragmented range.
template<typename T, typename Exception = std::runtime_error>
requires FragmentRange<T>
class linearizing_input_stream {
    using iterator = typename T::iterator;
    using fragment_type = typename T::fragment_type;

private:
    iterator _it;
    iterator _end;
    fragment_type _current;
    size_t _size;
    // We need stable addresses for the `bytes`, which, due to the small
    // value optimization, can invalidate any attached bytes_view on move.
    std::list<bytes> _linearized_values;

private:
    size_t remove_current_prefix(size_t size) {
        if (size < _current.size()) {
            _current.remove_prefix(size);
            _size -= size;
            return size;
        }
        const auto ret = _current.size();
        _size -= ret;
        ++_it;
        _current = (_it == _end) ? fragment_type{} : *_it;
        return ret;
    }

    void check_size(size_t size) const {
        if (size > _size) {
            seastar::throw_with_backtrace<Exception>(
                    fmt::format("linearizing_input_stream::check_size() - not enough bytes (requested {:d}, got {:d})", size, _size));
        }
    }

    std::pair<bytes_view, bool> do_read(size_t size) {
        check_size(size);

        if (size <= _current.size()) {
            bytes_view ret(_current.begin(), size);
            remove_current_prefix(size);
            return {ret, false};
        }

        auto out = _linearized_values.emplace_back(bytes::initialized_later{}, size).begin();
        while (size) {
            out = std::copy_n(_current.begin(), std::min(size, _current.size()), out);
            size -= remove_current_prefix(size);
        }

        return {_linearized_values.back(), true};
    }

public:
    explicit linearizing_input_stream(const T& fr)
        : _it(fr.begin())
        , _end(fr.end())
        , _current(*_it)
        , _size(fr.size_bytes()) {
    }
    // Not cheap to copy, would copy all linearized values.
    linearizing_input_stream(const linearizing_input_stream&) = delete;

    size_t size() const {
        return _size;
    }

    bool empty() const {
        return _size == 0;
    }

    // The returned view is only valid as long as the stream is alive.
    bytes_view read(size_t size) {
        return do_read(size).first;
    }

    template <typename Type>
    requires std::is_trivial_v<Type>
    Type read_trivial() {
        auto [bv, linearized] = do_read(sizeof(Type));
        auto ret = net::ntoh(::read_unaligned<net::packed<Type>>(bv.begin()));
        if (linearized) {
            _linearized_values.pop_back();
        }
        return ret;
    }

    void skip(size_t size) {
        check_size(size);
        while (size) {
            size -= remove_current_prefix(size);
        }
    }
};

} // namespace utils
