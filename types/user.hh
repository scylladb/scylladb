/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "cql3/description.hh"
#include "types/types.hh"
#include "types/tuple.hh"

#include <ranges>
#include <set>

class user_type_impl : public tuple_type_impl {
    using intern = type_interning_helper<user_type_impl, sstring, bytes, std::vector<bytes>, std::vector<data_type>, bool>;
public:
    const sstring _keyspace;
    const bytes _name;
private:
    const std::vector<bytes> _field_names;
    const std::vector<sstring> _string_field_names;
    const bool _is_multi_cell;
public:
    using native_type = std::vector<data_value>;
    user_type_impl(sstring keyspace, bytes name, std::vector<bytes> field_names, std::vector<data_type> field_types, bool is_multi_cell)
            : tuple_type_impl(kind::user, make_name(keyspace, name, field_names, field_types, is_multi_cell), field_types, false /* don't freeze inner */)
            , _keyspace(std::move(keyspace))
            , _name(std::move(name))
            , _field_names(std::move(field_names))
            , _string_field_names(_field_names
                | std::views::transform(
                    [] (const bytes& field_name) { return utf8_type->to_string(field_name); })
                | std::ranges::to<std::vector<sstring>>())
            , _is_multi_cell(is_multi_cell) {
    }
    static shared_ptr<const user_type_impl> get_instance(sstring keyspace, bytes name,
            std::vector<bytes> field_names, std::vector<data_type> field_types, bool multi_cell);
    data_type field_type(size_t i) const { return type(i); }
    const std::vector<data_type>& field_types() const { return _types; }
    bytes_view field_name(size_t i) const { return _field_names[i]; }
    sstring field_name_as_string(size_t i) const { return _string_field_names[i]; }
    const std::vector<bytes>& field_names() const { return _field_names; }
    const std::vector<sstring>& string_field_names() const { return _string_field_names; }
    std::optional<size_t> idx_of_field(const bytes_view& name) const;
    bool is_multi_cell() const { return _is_multi_cell; }
    virtual data_type freeze() const override;
    bytes get_name() const { return _name; }
    sstring get_name_as_string() const;
    sstring get_name_as_cql_string() const;

    /* Returns set of user-defined types referenced by this UDT
     * 
     * Example:
     * create type some_udt {
     *   a frozen<udt_a>,
     *   m frozen<map<udt_a, udt_b>,
     *   t frozen<tuple<int, list<udt_c>>   
     * }
     * get_all_referenced_user_types() will return {udt_a, udt_b, udt_c}.
     */
    std::set<user_type> get_all_referenced_user_types() const;

    cql3::description describe(cql3::with_create_statement) const;

private:
    static sstring make_name(sstring keyspace,
                             bytes name,
                             std::vector<bytes> field_names,
                             std::vector<data_type> field_types,
                             bool is_multi_cell);
};

data_value make_user_value(data_type tuple_type, user_type_impl::native_type value);

constexpr size_t max_udt_fields = std::numeric_limits<int16_t>::max();

// The following two functions are used to translate field indices (used to identify fields inside non-frozen UDTs)
// from/to a serialized bytes representation to be stored in mutations and sstables.
// Refer to collection_mutation.hh for a detailed description on how the serialized indices are used inside mutations.
bytes serialize_field_index(size_t);
size_t deserialize_field_index(const bytes_view&);
size_t deserialize_field_index(managed_bytes_view);
