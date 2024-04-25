/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <optional>
#include <set>
#include <string_view>
#include <vector>
#include <seastar/core/shared_ptr.hh>
#include "seastarx.hh"
#include "schema/schema_fwd.hh"

namespace replica {
class database; // For transition; remove
}

class schema;
using schema_ptr = lw_shared_ptr<const schema>;
class view_ptr;

namespace db {
class config;
class extensions;
}

namespace secondary_index {
class secondary_index_manager;
}

namespace gms {
class feature_service;
}

namespace locator {
class abstract_replication_strategy;
}

// Classes representing the overall schema, but without access to data.
// Useful on the coordinator side (where access to data is via storage_proxy).
//
// Everything here is type-erased to reduce dependencies. No references are
// kept, so lower-level objects like keyspaces and tables must not be held
// across continuations.
namespace data_dictionary {

// Used to forward all operations to the underlying backing store.
class impl;

class user_types_metadata;
class keyspace_metadata;

class no_such_keyspace : public std::runtime_error {
public:
    no_such_keyspace(std::string_view ks_name);
};

class no_such_column_family : public std::runtime_error {
public:
    no_such_column_family(const table_id& uuid);
    no_such_column_family(std::string_view ks_name, std::string_view cf_name);
    no_such_column_family(std::string_view ks_name, const table_id& uuid);
};

class table {
    const impl* _ops;
    const void* _table;
private:
    friend class impl;
    table(const impl* ops, const void* table);
public:
    schema_ptr schema() const;
    const std::vector<view_ptr>& views() const;
    const secondary_index::secondary_index_manager& get_index_manager() const;
};

class keyspace {
    const impl* _ops;
    const void* _keyspace;
private:
    friend class impl;
    keyspace(const impl* ops, const void* keyspace);
public:
    bool is_internal() const;
    lw_shared_ptr<keyspace_metadata> metadata() const;
    const user_types_metadata& user_types() const;
    const locator::abstract_replication_strategy& get_replication_strategy() const;
};

class database {
    const impl* _ops;
    const void* _database;
private:
    friend class impl;
    database(const impl* ops, const void* database);
public:
    const table_schema_version& get_version() const;
    keyspace find_keyspace(std::string_view name) const;
    std::optional<keyspace> try_find_keyspace(std::string_view name) const;
    bool has_keyspace(std::string_view name) const;  // throws no_keyspace
    std::vector<keyspace> get_keyspaces() const;
    std::vector<sstring> get_user_keyspaces() const;
    std::vector<sstring> get_all_keyspaces() const;
    std::vector<table> get_tables() const;
    table find_table(std::string_view ks, std::string_view table) const;  // throws no_such_column_family
    table find_column_family(table_id uuid) const;  // throws no_such_column_family
    schema_ptr find_schema(std::string_view ks, std::string_view table) const;  // throws no_such_column_family
    schema_ptr find_schema(table_id uuid) const;  // throws no_such_column_family
    table find_column_family(schema_ptr s) const;
    bool has_schema(std::string_view ks_name, std::string_view cf_name) const;
    std::optional<table> try_find_table(std::string_view ks, std::string_view table) const;
    std::optional<table> try_find_table(table_id id) const;
    const db::config& get_config() const;
    std::set<sstring> existing_index_names(std::string_view ks_name, std::string_view cf_to_exclude = sstring()) const;
    schema_ptr find_indexed_table(std::string_view ks_name, std::string_view index_name) const;
    sstring get_available_index_name(std::string_view ks_name, std::string_view table_name,
                                               std::optional<sstring> index_name_root) const;
    schema_ptr get_cdc_base_table(sstring_view ks_name, std::string_view table_name) const;
    schema_ptr get_cdc_base_table(const schema&) const;
    const db::extensions& extensions() const;
    const gms::feature_service& features() const;
    replica::database& real_database() const; // For transition; remove
    replica::database* real_database_ptr() const;
};

}

#if FMT_VERSION < 100000
// fmt v10 introduced formatter for std::exception
template <>
struct fmt::formatter<data_dictionary::no_such_keyspace> : fmt::formatter<string_view> {
    auto format(const data_dictionary::no_such_keyspace& e, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", e.what());
    }
};

template <>
struct fmt::formatter<data_dictionary::no_such_column_family> : fmt::formatter<string_view> {
    auto format(const data_dictionary::no_such_column_family& e, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", e.what());
    }
};
#endif
