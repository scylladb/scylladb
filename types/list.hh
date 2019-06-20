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

#include "types.hh"
#include "types/collection.hh"

class user_type_impl;
class cql_serialization_format;

namespace Json {
class Value;
}

class list_type_impl final : public concrete_collection_type<std::vector<data_value>> {
    using list_type = shared_ptr<const list_type_impl>;
    using intern = type_interning_helper<list_type_impl, data_type, bool>;
    data_type _elements;
    bool _is_multi_cell;
protected:
    virtual sstring cql3_type_name_impl() const override;
public:
    static list_type get_instance(data_type elements, bool is_multi_cell);
    list_type_impl(data_type elements, bool is_multi_cell);
    data_type get_elements_type() const { return _elements; }
    virtual data_type name_comparator() const override;
    virtual data_type value_comparator() const override;
    virtual bool is_multi_cell() const override { return _is_multi_cell; }
    virtual data_type freeze() const override;
    virtual bool is_compatible_with_frozen(const collection_type_impl& previous) const override;
    virtual bool is_value_compatible_with_frozen(const collection_type_impl& previous) const override;
    virtual bool less(bytes_view o1, bytes_view o2) const override;
    // FIXME: origin doesn't override is_byte_order_comparable().  Why?
    virtual void validate(bytes_view v, cql_serialization_format sf) const override;
    virtual void serialize(const void* value, bytes::iterator& out) const override;
    virtual void serialize(const void* value, bytes::iterator& out, cql_serialization_format sf) const override;
    virtual size_t serialized_size(const void* value) const override;
    virtual data_value deserialize(bytes_view v) const override;
    virtual data_value deserialize(bytes_view v, cql_serialization_format sf) const override;
    virtual sstring to_string_impl(const data_value& v) const override;
    virtual sstring to_json_string(bytes_view bv) const override;
    virtual bytes from_json_object(const Json::Value& value, cql_serialization_format sf) const override;
    virtual size_t hash(bytes_view v) const override;
    virtual bytes from_string(sstring_view text) const override;
    virtual std::vector<bytes> serialized_values(std::vector<atomic_cell> cells) const override;
    virtual bytes to_value(mutation_view mut, cql_serialization_format sf) const override;
    virtual bool references_user_type(const sstring& keyspace, const bytes& name) const override;
    virtual std::optional<data_type> update_user_type(const shared_ptr<const user_type_impl> updated) const override;
    virtual bool references_duration() const override;
};

data_value make_list_value(data_type type, list_type_impl::native_type value);

