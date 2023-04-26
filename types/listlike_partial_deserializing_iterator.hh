/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <iterator>

#include "utils/fragment_range.hh"
#include "utils/managed_bytes.hh"
#include "exceptions/exceptions.hh"
#include "types/types.hh"

int read_collection_size(bytes_view& in);
bytes_view read_collection_key(bytes_view& in);
bytes_view read_collection_value_nonnull(bytes_view& in);

template <FragmentedView View>
int read_collection_size(View& in) {
    return read_simple<int32_t>(in);
}

template <FragmentedView View>
View read_collection_key(View& in) {
    auto size = read_simple<int32_t>(in);
    if (size == -2) {
        throw exceptions::invalid_request_exception("unset value is not supported inside collections");
    }
    if (size < 0) {
        throw exceptions::invalid_request_exception("null is not supported inside collections");
    }
    return read_simple_bytes(in, size);
}

template <FragmentedView View>
std::optional<View> read_collection_value(View& in) {
    auto size = read_simple<int32_t>(in);
    if (size == -1) {
        return std::nullopt;
    }
    if (size < 0) {
        throw exceptions::invalid_request_exception("unset value is not supported inside collections");
    }
    return read_simple_bytes(in, size);
}

template <FragmentedView View>
View read_collection_value_nonnull(View& in) {
    auto size = read_simple<int32_t>(in);
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
    using pointer = managed_bytes_view_opt*;
    using reference = managed_bytes_view_opt&;
private:
    managed_bytes_view* _in;
    int _remain;
    managed_bytes_view_opt _cur;
private:
    struct end_tag {};
    listlike_partial_deserializing_iterator(managed_bytes_view& in)
            : _in(&in) {
        _remain = read_collection_size(*_in);
        parse();
    }
    listlike_partial_deserializing_iterator(end_tag)
            : _remain(0) {
    }
public:
    managed_bytes_view_opt operator*() const { return _cur; }
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
    static listlike_partial_deserializing_iterator begin(managed_bytes_view& in) {
        return { in };
    }
    static listlike_partial_deserializing_iterator end(managed_bytes_view in) {
        return { end_tag() };
    }
private:
    void parse() {
        if (_remain) {
            _cur = read_collection_value(*_in);
        } else {
            _cur = {};
        }
    }
};
