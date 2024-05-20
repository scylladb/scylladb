/*
 * Copyright (C) 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "schema/schema.hh"

#include "data_dictionary/data_dictionary.hh"
#include "cql3/statements/index_target.hh"

#include <vector>

namespace cql3::expr {

enum class oper_t;

}

namespace secondary_index {

sstring index_table_name(const sstring& index_name);

/*!
 * \brief a reverse of index_table_name
 * It gets a table_name and return the index name that was used
 * to create that table.
 */
sstring index_name_from_table_name(const sstring& table_name);

/// Given a list of base-table schemas, return all their secondary indexes, except that specified in cf_to_exclude.
std::set<sstring>
existing_index_names(const std::vector<schema_ptr>& tables, std::string_view cf_to_exclude);

/// Given a base-table keyspace and table name, return the first available index
/// name (containing index_name_root if specified).
/// If needed, a running counder is appended to the index name, if it is already
/// taken (existing_names contains it).
sstring get_available_index_name(
        std::string_view ks_name,
        std::string_view cf_name,
        std::optional<sstring> index_name_root,
        const std::set<sstring>& existing_names,
        std::function<bool(std::string_view, std::string_view)> has_schema);

class index {
    index_metadata _im;
    cql3::statements::index_target::target_type _target_type;
    sstring _target_column;
public:
    index(const sstring& target_column, const index_metadata& im);
    bool depends_on(const column_definition& cdef) const;
    struct supports_expression_v {
        enum class value_type {
            UsualYes,
            CollectionYes,
            No,
        };
        value_type value;
        operator bool() const {
            return value != value_type::No;
        }
        static constexpr supports_expression_v from_bool(bool b) {
            return {b ? value_type::UsualYes : value_type::No};
        }
        static constexpr supports_expression_v from_bool_collection(bool b) {
            return {b ? value_type::CollectionYes : value_type::No};
        }
        friend bool operator==(supports_expression_v, supports_expression_v) = default;
    };

    supports_expression_v supports_expression(const column_definition& cdef, const cql3::expr::oper_t op) const;
    supports_expression_v supports_subscript_expression(const column_definition& cdef, const cql3::expr::oper_t op) const;
    const index_metadata& metadata() const;
    const sstring& target_column() const {
        return _target_column;
    }
    cql3::statements::index_target::target_type target_type() const {
        return _target_type;
    }
};

class secondary_index_manager {
    data_dictionary::table _cf;
    /// The key of the map is the name of the index as stored in system tables.
    std::unordered_map<sstring, index> _indices;
public:
    secondary_index_manager(data_dictionary::table cf);
    void reload();
    view_ptr create_view_for_index(const index_metadata& index) const;
    std::vector<index_metadata> get_dependent_indices(const column_definition& cdef) const;
    std::vector<index> list_indexes() const;
    bool is_index(view_ptr) const;
    bool is_index(const schema& s) const;
    bool is_global_index(const schema& s) const;
private:
    void add_index(const index_metadata& im);
};

}
