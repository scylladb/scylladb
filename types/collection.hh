/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include "types/types.hh"
#include "collection_mutation.hh"
#include "log.hh"

namespace cql3 {

class column_specification;

}

class collection_type_impl : public abstract_type {
    static logging::logger _logger;
public:
    static constexpr size_t max_elements = 65535;

protected:
    bool _is_multi_cell;
    explicit collection_type_impl(kind k, sstring name, bool is_multi_cell)
            : abstract_type(k, std::move(name), {}), _is_multi_cell(is_multi_cell) {
                _contains_collection = true;
            }
public:
    bool is_multi_cell() const { return _is_multi_cell; }
    virtual data_type name_comparator() const = 0;
    virtual data_type value_comparator() const = 0;
    lw_shared_ptr<cql3::column_specification> make_collection_receiver(const cql3::column_specification& collection, bool is_key) const;
    virtual bool is_compatible_with_frozen(const collection_type_impl& previous) const = 0;
    virtual bool is_value_compatible_with_frozen(const collection_type_impl& previous) const = 0;

    template <typename Iterator>
    requires requires (Iterator it) { {*it} -> std::convertible_to<bytes_view_opt>; }
    static bytes pack(Iterator start, Iterator finish, int elements);

    template <typename Iterator>
    requires requires (Iterator it) { {*it} -> std::convertible_to<managed_bytes_view_opt>; }
    static managed_bytes pack_fragmented(Iterator start, Iterator finish, int elements);

private:
    // Explicitly instantiated in types.cc
    template <FragmentedView View> data_value deserialize_impl(View v) const;
public:
    template <FragmentedView View> data_value deserialize_value(View v) const {
        return deserialize(v);
    }
    data_value deserialize_value(bytes_view v) const {
        return deserialize_impl(single_fragmented_view(v));
    }
};

// a list or a set
class listlike_collection_type_impl : public collection_type_impl {
protected:
    data_type _elements;
    explicit listlike_collection_type_impl(kind k, sstring name, data_type elements,bool is_multi_cell);
public:
    const data_type& get_elements_type() const { return _elements; }
    // A list or set value can be serialized as a vector<pair<timeuuid, data_value>> or
    // vector<pair<data_value, empty>> respectively. Compare this representation with
    // vector<data_value> without transforming either of the arguments. Since Cassandra doesn't
    // allow nested multi-cell collections this representation does not transcend to values, and we
    // don't need to worry about recursing.
    // @param this          type of the listlike value represented as vector<data_value>
    // @param map_type      type of the listlike value represented as vector<pair<data_value, data_value>>
    // @param list          listlike value, represented as vector<data_value>
    // @param map           listlike value represented as vector<pair<data_value, data_value>>
    //
    // This function is used to compare receiver with a literal or parameter marker during condition
    // evaluation.
    std::strong_ordering compare_with_map(const map_type_impl& map_type, bytes_view list, bytes_view map) const;
    // A list or set value can be represented as a vector<pair<timeuuid, data_value>> or
    // vector<pair<data_value, empty>> respectively. Serialize this representation
    // as a vector of values, not as a vector of pairs.
    bytes serialize_map(const map_type_impl& map_type, const data_value& value) const;

    // Verify that there are no NULL elements. Throws if there are.
    void validate_for_storage(const FragmentedView auto& value) const;
};

template <typename Iterator>
requires requires (Iterator it) { {*it} -> std::convertible_to<bytes_view_opt>; }
bytes
collection_type_impl::pack(Iterator start, Iterator finish, int elements) {
    size_t len = collection_size_len();
    size_t psz = collection_value_len();
    for (auto j = start; j != finish; j++) {
        auto v = bytes_view_opt(*j);
        len += (v ? v->size() : 0) + psz;
    }
    bytes out(bytes::initialized_later(), len);
    bytes::iterator i = out.begin();
    write_collection_size(i, elements);
    while (start != finish) {
        write_collection_value(i, *start++);
    }
    return out;
}

template <typename Iterator>
requires requires (Iterator it) { {*it} -> std::convertible_to<managed_bytes_view_opt>; }
managed_bytes
collection_type_impl::pack_fragmented(Iterator start, Iterator finish, int elements) {
    size_t len = collection_size_len();
    size_t psz = collection_value_len();
    for (auto j = start; j != finish; j++) {
        auto v = managed_bytes_view_opt(*j);
        len += (v ? v->size() : 0) + psz;
    }
    managed_bytes out(managed_bytes::initialized_later(), len);
    managed_bytes_mutable_view v(out);
    write_collection_size(v, elements);
    while (start != finish) {
        write_collection_value(v, *start++);
    }
    return out;
}

extern
template
void listlike_collection_type_impl::validate_for_storage(const managed_bytes_view& value) const;

extern
template
void listlike_collection_type_impl::validate_for_storage(const fragmented_temporary_buffer::view& value) const;
