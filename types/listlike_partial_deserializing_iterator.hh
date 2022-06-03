/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <iterator>
#include "cql_serialization_format.hh"
#include "utils/fragment_range.hh"
#include "utils/managed_bytes.hh"
#include "exceptions/exceptions.hh"
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
    if (size == -2) {
        throw exceptions::invalid_request_exception("unset value is not supported inside collections");
    }
    if (size < 0) {
        throw exceptions::invalid_request_exception("null is not supported inside collections");
    }
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
