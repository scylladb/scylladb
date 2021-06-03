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

#include <iterator>
#include "cql_serialization_format.hh"
#include "utils/fragment_range.hh"
#include "utils/managed_bytes.hh"
#include "types.hh"

int read_collection_size(bytes_view& in, cql_serialization_format sf);
bytes_view read_collection_value(bytes_view& in, cql_serialization_format sf);

template <FragmentedView View>
int read_collection_size(View& in, cql_serialization_format sf) {
    if (sf.using_32_bits_for_collections()) {
        return read_simple<int32_t>(in);
    } else {
        return read_simple<uint16_t>(in);
    }
}

template <FragmentedView View>
View read_collection_value(View& in, cql_serialization_format sf) {
    auto size = sf.using_32_bits_for_collections() ? read_simple<int32_t>(in) : read_simple<uint16_t>(in);
    return read_simple_bytes(in, size);
}

// iterator that takes a set or list in serialized form, and emits
// each element, still in serialized form
class listlike_partial_deserializing_iterator {
public:
    using iterator_category = std::input_iterator_tag;
    using value_type = managed_bytes_view;
    using difference_type = std::ptrdiff_t;
    using pointer = managed_bytes_view*;
    using reference = managed_bytes_view&;
private:
    managed_bytes_view* _in;
    int _remain;
    managed_bytes_view _cur;
    cql_serialization_format _sf;
private:
    struct end_tag {};
    listlike_partial_deserializing_iterator(managed_bytes_view& in, cql_serialization_format sf)
            : _in(&in), _sf(sf) {
        _remain = read_collection_size(*_in, _sf);
        parse();
    }
    listlike_partial_deserializing_iterator(end_tag)
            : _remain(0), _sf(cql_serialization_format::internal()) {  // _sf is bogus, but doesn't matter
    }
public:
    managed_bytes_view operator*() const { return _cur; }
    listlike_partial_deserializing_iterator& operator++() {
        --_remain;
        parse();
        return *this;
    }
    void operator++(int) {
        --_remain;
        parse();
    }
    bool operator==(const listlike_partial_deserializing_iterator& x) const {
        return _remain == x._remain;
    }
    bool operator!=(const listlike_partial_deserializing_iterator& x) const {
        return _remain != x._remain;
    }
    static listlike_partial_deserializing_iterator begin(managed_bytes_view& in, cql_serialization_format sf) {
        return { in, sf };
    }
    static listlike_partial_deserializing_iterator end(managed_bytes_view in, cql_serialization_format sf) {
        return { end_tag() };
    }
private:
    void parse() {
        if (_remain) {
            _cur = read_collection_value(*_in, _sf);
        } else {
            _cur = {};
        }
    }
};
