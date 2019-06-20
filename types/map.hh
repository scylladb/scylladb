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

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <vector>
#include <utility>

#include "types.hh"
#include "types/collection.hh"

class user_type_impl;
class cql_serialization_format;

namespace Json {
class Value;
}

class map_type_impl final : public concrete_collection_type<std::vector<std::pair<data_value, data_value>>> {
    using map_type = shared_ptr<const map_type_impl>;
    using intern = type_interning_helper<map_type_impl, data_type, data_type, bool>;
    data_type _keys;
    data_type _values;
    data_type _key_value_pair_type;
    bool _is_multi_cell;
protected:
    virtual sstring cql3_type_name_impl() const override;
public:
    static shared_ptr<const map_type_impl> get_instance(data_type keys, data_type values, bool is_multi_cell);
    map_type_impl(data_type keys, data_type values, bool is_multi_cell);
    data_type get_keys_type() const { return _keys; }
    data_type get_values_type() const { return _values; }
    virtual data_type name_comparator() const override { return _keys; }
    virtual data_type value_comparator() const override { return _values; }
    virtual bool is_multi_cell() const override { return _is_multi_cell; }
    virtual data_type freeze() const override;
    virtual bool is_compatible_with_frozen(const collection_type_impl& previous) const override;
    virtual bool is_value_compatible_with_frozen(const collection_type_impl& previous) const override;
    virtual bool less(bytes_view o1, bytes_view o2) const override;
    static int32_t compare_maps(data_type keys_comparator, data_type values_comparator,
                        bytes_view o1, bytes_view o2);
    virtual bool is_byte_order_comparable() const override { return false; }
    virtual void validate(bytes_view v, cql_serialization_format sf) const override;
    virtual void serialize(const void* value, bytes::iterator& out) const override;
    virtual void serialize(const void* value, bytes::iterator& out, cql_serialization_format sf) const override;
    virtual size_t serialized_size(const void* value) const;
    virtual data_value deserialize(bytes_view v) const override;
    virtual data_value deserialize(bytes_view v, cql_serialization_format sf) const override;
    virtual sstring to_string_impl(const data_value& v) const override;
    virtual sstring to_json_string(bytes_view bv) const override;
    virtual bytes from_json_object(const Json::Value& value, cql_serialization_format sf) const override;
    virtual size_t hash(bytes_view v) const override;
    virtual bytes from_string(sstring_view text) const override;
    virtual std::vector<bytes> serialized_values(std::vector<atomic_cell> cells) const override;
    static bytes serialize_partially_deserialized_form(const std::vector<std::pair<bytes_view, bytes_view>>& v,
            cql_serialization_format sf);
    virtual bytes to_value(mutation_view mut, cql_serialization_format sf) const override;
    virtual bool references_user_type(const sstring& keyspace, const bytes& name) const override;
    virtual std::optional<data_type> update_user_type(const shared_ptr<const user_type_impl> updated) const override;
    virtual bool references_duration() const override;
};

data_value make_map_value(data_type tuple_type, map_type_impl::native_type value);
