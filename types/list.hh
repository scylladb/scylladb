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

class list_type_impl final : public concrete_type<std::vector<data_value>, listlike_collection_type_impl> {
    using list_type = shared_ptr<const list_type_impl>;
    using intern = type_interning_helper<list_type_impl, data_type, bool>;
public:
    static list_type get_instance(data_type elements, bool is_multi_cell);
    list_type_impl(data_type elements, bool is_multi_cell);
    virtual data_type name_comparator() const override;
    virtual data_type value_comparator() const override;
    virtual data_type freeze() const override;
    virtual bool is_compatible_with_frozen(const collection_type_impl& previous) const override;
    virtual bool is_value_compatible_with_frozen(const collection_type_impl& previous) const override;
    using abstract_type::deserialize;
    using collection_type_impl::deserialize;
    template <FragmentedView View> data_value deserialize(View v, cql_serialization_format sf) const;
};

data_value make_list_value(data_type type, list_type_impl::native_type value);

