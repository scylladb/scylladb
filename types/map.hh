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
    static int32_t compare_maps(data_type keys_comparator, data_type values_comparator,
                        managed_bytes_view o1, managed_bytes_view o2);
    using abstract_type::deserialize;
    using collection_type_impl::deserialize;
    template <FragmentedView View> data_value deserialize(View v, cql_serialization_format sf) const;
    static bytes serialize_partially_deserialized_form(const std::vector<std::pair<bytes_view, bytes_view>>& v,
            cql_serialization_format sf);
    static managed_bytes serialize_partially_deserialized_form_fragmented(const std::vector<std::pair<managed_bytes_view, managed_bytes_view>>& v,
            cql_serialization_format sf);
};

data_value make_map_value(data_type tuple_type, map_type_impl::native_type value);
