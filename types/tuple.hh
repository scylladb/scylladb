/*
 * Copyright (C) 2014-present ScyllaDB
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
#include <vector>
#include <string>

#include <boost/range/numeric.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/for_each.hpp>

#include "types.hh"

struct tuple_deserializing_iterator {
public:
    using iterator_category = std::input_iterator_tag;
    using value_type = const managed_bytes_view_opt;
    using difference_type = std::ptrdiff_t;
    using pointer = const managed_bytes_view_opt*;
    using reference = const managed_bytes_view_opt&;
private:
    managed_bytes_view _v;
    managed_bytes_view_opt _current;
public:
    struct end_tag {};
    tuple_deserializing_iterator(managed_bytes_view v) : _v(v) {
        parse();
    }
    tuple_deserializing_iterator(end_tag, managed_bytes_view v) : _v(v) {
        _v.remove_prefix(_v.size());
    }
    static tuple_deserializing_iterator start(managed_bytes_view v) {
        return tuple_deserializing_iterator(v);
    }
    static tuple_deserializing_iterator finish(managed_bytes_view v) {
        return tuple_deserializing_iterator(end_tag(), v);
    }
    const managed_bytes_view_opt& operator*() const {
        return _current;
    }
    const managed_bytes_view_opt* operator->() const {
        return &_current;
    }
    tuple_deserializing_iterator& operator++() {
        skip();
        parse();
        return *this;
    }
    void operator++(int) {
        skip();
        parse();
    }
    bool operator==(const tuple_deserializing_iterator& x) const {
        return _v == x._v;
    }
    bool operator!=(const tuple_deserializing_iterator& x) const {
        return !operator==(x);
    }
private:
    void parse() {
        _current = std::nullopt;
        if (_v.empty()) {
            return;
        }
        // we don't consume _v, otherwise operator==
        // or the copy constructor immediately after
        // parse() yields the wrong results.
        auto tmp = _v;
        auto s = read_simple<int32_t>(tmp);
        if (s < 0) {
            return;
        }
        _current = read_simple_bytes(tmp, s);
    }
    void skip() {
        _v.remove_prefix(4 + (_current ? _current->size() : 0));
    }
};

template <FragmentedView View>
std::optional<View> read_tuple_element(View& v) {
    auto s = read_simple<int32_t>(v);
    if (s < 0) {
        return std::nullopt;
    }
    return read_simple_bytes(v, s);
}

template <FragmentedView View>
bytes_opt get_nth_tuple_element(View v, size_t n) {
    for (size_t i = 0; i < n; ++i) {
        if (v.empty()) {
            return std::nullopt;
        }
        read_tuple_element(v);
    }
    if (v.empty()) {
        return std::nullopt;
    }
    auto el = read_tuple_element(v);
    if (el) {
        return linearized(*el);
    }
    return std::nullopt;
}

class tuple_type_impl : public concrete_type<std::vector<data_value>> {
    using intern = type_interning_helper<tuple_type_impl, std::vector<data_type>>;
protected:
    std::vector<data_type> _types;
    static boost::iterator_range<tuple_deserializing_iterator> make_range(managed_bytes_view v) {
        return { tuple_deserializing_iterator::start(v), tuple_deserializing_iterator::finish(v) };
    }
    tuple_type_impl(kind k, sstring name, std::vector<data_type> types, bool freeze_inner);
    tuple_type_impl(std::vector<data_type> types, bool freze_inner);
public:
    tuple_type_impl(std::vector<data_type> types);
    static shared_ptr<const tuple_type_impl> get_instance(std::vector<data_type> types);
    data_type type(size_t i) const {
        return _types[i];
    }
    size_t size() const {
        return _types.size();
    }
    const std::vector<data_type>& all_types() const {
        return _types;
    }
    std::vector<bytes_opt> split(FragmentedView auto v) const {
        std::vector<bytes_opt> elements;
        while (!v.empty()) {
            auto fragmented_element_optional = read_tuple_element(v);
            if (fragmented_element_optional) {
                elements.push_back(linearized(*fragmented_element_optional));
            } else {
                elements.push_back(std::nullopt);
            }
        }
        return elements;
    }
    std::vector<managed_bytes_opt> split_fragmented(FragmentedView auto v) const {
        std::vector<managed_bytes_opt> elements;
        while (!v.empty()) {
            auto fragmented_element_optional = read_tuple_element(v);
            if (fragmented_element_optional) {
                elements.push_back(managed_bytes(*fragmented_element_optional));
            } else {
                elements.push_back(std::nullopt);
            }
        }
        return elements;
    }
    template <typename RangeOf_bytes_opt>  // also accepts bytes_view_opt
    static bytes build_value(RangeOf_bytes_opt&& range) {
        auto item_size = [] (auto&& v) { return 4 + (v ? v->size() : 0); };
        auto size = boost::accumulate(range | boost::adaptors::transformed(item_size), 0);
        auto ret = bytes(bytes::initialized_later(), size);
        auto out = ret.begin();
        auto put = [&out] (auto&& v) {
            if (v) {
                using val_type = std::remove_cvref_t<decltype(*v)>;
                if constexpr (FragmentedView<val_type>) {
                    int32_t size = v->size_bytes();
                    write(out, size);
                    read_fragmented(*v, size, out);
                    out += size;
                } else {
                    write(out, int32_t(v->size()));
                    out = std::copy(v->begin(), v->end(), out);
                }
            } else {
                write(out, int32_t(-1));
            }
        };
        boost::range::for_each(range, put);
        return ret;
    }
    template <typename Range> // range of managed_bytes_opt or managed_bytes_view_opt
    requires requires (Range it) { {it.begin()->value()} -> std::convertible_to<managed_bytes_view>; }
    static managed_bytes build_value_fragmented(Range&& range) {
        size_t size = 0;
        for (auto&& v : range) {
            size += 4 + (v ? v->size() : 0);
        }
        auto ret = managed_bytes(managed_bytes::initialized_later(), size);
        auto out = managed_bytes_mutable_view(ret);
        for (auto&& v : range) {
            if (v) {
                write<int32_t>(out, v->size());
                write_fragmented(out, managed_bytes_view(*v));
            } else {
                write<int32_t>(out, -1);
            }
        }
        return ret;
    }
private:
    static sstring make_name(const std::vector<data_type>& types);
    friend abstract_type;
};

data_value make_tuple_value(data_type tuple_type, tuple_type_impl::native_type value);


