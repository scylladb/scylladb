/*
 * Copyright (C) 2014 ScyllaDB
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

#include "types.hh"

struct tuple_deserializing_iterator : public std::iterator<std::input_iterator_tag, const bytes_view_opt> {
    bytes_view _v;
    bytes_view_opt _current;
public:
    struct end_tag {};
    tuple_deserializing_iterator(bytes_view v) : _v(v) {
        parse();
    }
    tuple_deserializing_iterator(end_tag, bytes_view v) : _v(v) {
        _v.remove_prefix(_v.size());
    }
    static tuple_deserializing_iterator start(bytes_view v) {
        return tuple_deserializing_iterator(v);
    }
    static tuple_deserializing_iterator finish(bytes_view v) {
        return tuple_deserializing_iterator(end_tag(), v);
    }
    const bytes_view_opt& operator*() const {
        return _current;
    }
    const bytes_view_opt* operator->() const {
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

class tuple_type_impl : public concrete_type<std::vector<data_value>> {
    using intern = type_interning_helper<tuple_type_impl, std::vector<data_type>>;
protected:
    std::vector<data_type> _types;
    static boost::iterator_range<tuple_deserializing_iterator> make_range(bytes_view v) {
        return { tuple_deserializing_iterator::start(v), tuple_deserializing_iterator::finish(v) };
    }
    tuple_type_impl(sstring name, std::vector<data_type> types);
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
    virtual int32_t compare(bytes_view v1, bytes_view v2) const override;
    virtual bool less(bytes_view v1, bytes_view v2) const override;
    virtual void validate(bytes_view v, cql_serialization_format sf) const;
    virtual size_t serialized_size(const void* value) const override;
    virtual void serialize(const void* value, bytes::iterator& out) const override;
    virtual data_value deserialize(bytes_view v) const override;
    std::vector<bytes_view_opt> split(bytes_view v) const;
    template <typename RangeOf_bytes_opt>  // also accepts bytes_view_opt
    static bytes build_value(RangeOf_bytes_opt&& range) {
        auto item_size = [] (auto&& v) { return 4 + (v ? v->size() : 0); };
        auto size = boost::accumulate(range | boost::adaptors::transformed(item_size), 0);
        auto ret = bytes(bytes::initialized_later(), size);
        auto out = ret.begin();
        auto put = [&out] (auto&& v) {
            if (v) {
                write(out, int32_t(v->size()));
                out = std::copy(v->begin(), v->end(), out);
            } else {
                write(out, int32_t(-1));
            }
        };
        boost::range::for_each(range, put);
        return ret;
    }
    virtual size_t hash(bytes_view v) const override;
    virtual bytes from_string(sstring_view s) const override;
    virtual sstring to_string_impl(const data_value& v) const override;
    virtual sstring to_json_string(bytes_view bv) const override;
    virtual bytes from_json_object(const Json::Value& value, cql_serialization_format sf) const override;
    virtual bool equals(const abstract_type& other) const override;
    virtual bool is_compatible_with(const abstract_type& previous) const override;
    virtual bool is_value_compatible_with_internal(const abstract_type& previous) const override;
    virtual sstring cql3_type_name_impl() const override;
    virtual bool is_native() const override { return false; }
    virtual bool is_tuple() const override { return true; }
    virtual bool references_user_type(const sstring& keyspace, const bytes& name) const override;
    virtual std::optional<data_type> update_user_type(const shared_ptr<const user_type_impl> updated) const override;
    virtual bool references_duration() const override;
private:
    bool check_compatibility(const abstract_type& previous, bool (abstract_type::*predicate)(const abstract_type&) const) const;
    static sstring make_name(const std::vector<data_type>& types);
};

data_value make_tuple_value(data_type tuple_type, tuple_type_impl::native_type value);


