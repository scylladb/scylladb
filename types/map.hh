/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <vector>
#include <utility>

#include "exceptions/exceptions.hh"
#include "types/types.hh"
#include "types/collection.hh"

class user_type_impl;

namespace Json {
class Value;
}

class map_type_impl final : public concrete_type<std::vector<std::pair<data_value, data_value>>, collection_type_impl> {
    using map_type = shared_ptr<const map_type_impl>;
    using intern = type_interning_helper<map_type_impl, data_type, data_type, bool>;
    data_type _keys;
    data_type _values;
    data_type _key_value_pair_type;
public:
    static shared_ptr<const map_type_impl> get_instance(data_type keys, data_type values, bool is_multi_cell);
    map_type_impl(data_type keys, data_type values, bool is_multi_cell);
    const data_type& get_keys_type() const { return _keys; }
    const data_type& get_values_type() const { return _values; }
    virtual data_type name_comparator() const override { return _keys; }
    virtual data_type value_comparator() const override { return _values; }
    virtual data_type freeze() const override;
    virtual bool is_compatible_with_frozen(const collection_type_impl& previous) const override;
    virtual bool is_value_compatible_with_frozen(const collection_type_impl& previous) const override;
    static std::strong_ordering compare_maps(data_type keys_comparator, data_type values_comparator,
                        managed_bytes_view o1, managed_bytes_view o2);
    using abstract_type::deserialize;
    using collection_type_impl::deserialize;
    template <FragmentedView View> data_value deserialize(View v) const;
    static bytes serialize_partially_deserialized_form(const std::vector<std::pair<bytes_view, bytes_view>>& v);
    static managed_bytes serialize_partially_deserialized_form_fragmented(const std::vector<std::pair<managed_bytes_view, managed_bytes_view>>& v);

    // Serializes a map using the internal cql serialization format
    // Takes a range of pair<const bytes, bytes>
    template <std::ranges::range Range>
    requires std::convertible_to<std::ranges::range_value_t<Range>, std::pair<const bytes, bytes>>
    static bytes serialize_to_bytes(const Range& map_range);

    // Serializes a map using the internal cql serialization format
    // Takes a range of pair<const managed_bytes, managed_bytes>
    template <std::ranges::range Range>
    requires std::convertible_to<std::ranges::range_value_t<Range>, std::pair<const managed_bytes, managed_bytes>>
    static managed_bytes serialize_to_managed_bytes(const Range& map_range);
};

data_value make_map_value(data_type tuple_type, map_type_impl::native_type value);

template <std::ranges::range Range>
requires std::convertible_to<std::ranges::range_value_t<Range>, std::pair<const bytes, bytes>>
bytes map_type_impl::serialize_to_bytes(const Range& map_range) {
    size_t serialized_len = 4;
    size_t map_size = 0;
    for (const std::pair<const bytes, bytes>& elem : map_range) {
        serialized_len += 4 + elem.first.size() + 4 + elem.second.size();
        map_size += 1;
    }

    if (map_size > std::numeric_limits<int32_t>::max()) {
        throw exceptions::invalid_request_exception(
            fmt::format("Map size too large: {} > {}", map_size, std::numeric_limits<int32_t>::max()));
    }

    bytes result(bytes::initialized_later(), serialized_len);
    bytes::iterator out = result.begin();

    write_collection_size(out, map_size);
    for (const std::pair<const bytes, bytes>& elem : map_range) {
        if (elem.first.size() > std::numeric_limits<int32_t>::max()) {
            throw exceptions::invalid_request_exception(
                fmt::format("Map key size too large: {} bytes > {}", map_size, std::numeric_limits<int32_t>::max()));
        }

        if (elem.second.size() > std::numeric_limits<int32_t>::max()) {
            throw exceptions::invalid_request_exception(
                fmt::format("Map value size too large: {} bytes > {}", map_size, std::numeric_limits<int32_t>::max()));
        }

        write_collection_value(out, elem.first);
        write_collection_value(out, elem.second);
    }

    return result;
}

template <std::ranges::range Range>
requires std::convertible_to<std::ranges::range_value_t<Range>, std::pair<const managed_bytes, managed_bytes>>
managed_bytes map_type_impl::serialize_to_managed_bytes(const Range& map_range) {
    size_t serialized_len = 4;
    size_t map_size = 0;
    for (const std::pair<const managed_bytes, managed_bytes>& elem : map_range) {
        serialized_len += 4 + elem.first.size() + 4 + elem.second.size();
        map_size += 1;
    }

    if (map_size > std::numeric_limits<int32_t>::max()) {
        throw exceptions::invalid_request_exception(
            fmt::format("Map size too large: {} > {}", map_size, std::numeric_limits<int32_t>::max()));
    }

    managed_bytes result(managed_bytes::initialized_later(), serialized_len);
    managed_bytes_mutable_view out(result);

    write_collection_size(out, map_size);
    for (const std::pair<const managed_bytes, managed_bytes>& elem : map_range) {
        if (elem.first.size() > std::numeric_limits<int32_t>::max()) {
            throw exceptions::invalid_request_exception(
                fmt::format("Map key size too large: {} bytes > {}", map_size, std::numeric_limits<int32_t>::max()));
        }

        if (elem.second.size() > std::numeric_limits<int32_t>::max()) {
            throw exceptions::invalid_request_exception(
                fmt::format("Map value size too large: {} bytes > {}", map_size, std::numeric_limits<int32_t>::max()));
        }

        write_collection_value(out, elem.first);
        write_collection_value(out, elem.second);
    }

    return result;
}
