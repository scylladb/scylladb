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

#include "types.hh"
#include "types/collection.hh"

class user_type_impl;
class cql_serialization_format;

namespace Json {
class Value;
}

class set_type_impl final : public concrete_type<std::vector<data_value>, listlike_collection_type_impl> {
    using set_type = shared_ptr<const set_type_impl>;
    using intern = type_interning_helper<set_type_impl, data_type, bool>;
public:
    static set_type get_instance(data_type elements, bool is_multi_cell);
    set_type_impl(data_type elements, bool is_multi_cell);
    virtual data_type name_comparator() const override { return _elements; }
    virtual data_type value_comparator() const override;
    virtual data_type freeze() const override;
    virtual bool is_compatible_with_frozen(const collection_type_impl& previous) const override;
    virtual bool is_value_compatible_with_frozen(const collection_type_impl& previous) const override;
    using abstract_type::deserialize;
    using collection_type_impl::deserialize;
    template <FragmentedView View> data_value deserialize(View v, cql_serialization_format sf) const;
    static bytes serialize_partially_deserialized_form(
            const std::vector<bytes_view>& v, cql_serialization_format sf);
    static managed_bytes serialize_partially_deserialized_form_fragmented(
            const std::vector<managed_bytes_view>& v, cql_serialization_format sf);
};

data_value make_set_value(data_type tuple_type, set_type_impl::native_type value);

template <typename NativeType>
data_value::data_value(const std::unordered_set<NativeType>& v)
    : data_value(new set_type_impl::native_type(v.begin(), v.end()), set_type_impl::get_instance(data_type_for<NativeType>(), true))
{}
