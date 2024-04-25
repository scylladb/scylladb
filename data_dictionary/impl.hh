/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "data_dictionary.hh"

// Interface for implementing data_dictionary classes

namespace data_dictionary {

class impl {
public:
    virtual ~impl();
    virtual const table_schema_version& get_version(database) const = 0;
    virtual std::optional<keyspace> try_find_keyspace(database db, std::string_view name) const = 0;
    virtual std::vector<keyspace> get_keyspaces(database db) const = 0;
    virtual std::vector<sstring> get_user_keyspaces(database db) const = 0;
    virtual std::vector<sstring> get_all_keyspaces(database db) const = 0;
    virtual std::vector<table> get_tables(database db) const = 0;
    virtual std::optional<table> try_find_table(database db, std::string_view ks, std::string_view tab) const = 0;
    virtual std::optional<table> try_find_table(database db, table_id id) const = 0;
    virtual const secondary_index::secondary_index_manager& get_index_manager(table t) const = 0;
    virtual schema_ptr get_table_schema(table t) const = 0;
    virtual lw_shared_ptr<keyspace_metadata> get_keyspace_metadata(keyspace ks) const = 0;
    virtual bool is_internal(keyspace ks) const = 0;
    virtual const locator::abstract_replication_strategy& get_replication_strategy(keyspace ks) const = 0;
    virtual const std::vector<view_ptr>& get_table_views(table t) const = 0;
    virtual sstring get_available_index_name(database db, std::string_view ks_name, std::string_view table_name,
            std::optional<sstring> index_name_root) const = 0;
    virtual std::set<sstring> existing_index_names(database db, std::string_view ks_name, std::string_view cf_to_exclude = {}) const = 0;
    virtual schema_ptr find_indexed_table(database db, std::string_view ks_name, std::string_view index_name) const = 0;
    virtual schema_ptr get_cdc_base_table(database db, const schema&) const = 0;
    virtual const db::config& get_config(database db) const = 0;
    virtual const db::extensions& get_extensions(database db) const = 0;
    virtual const gms::feature_service& get_features(database db) const = 0;
    virtual replica::database& real_database(database db) const = 0;
    virtual replica::database* real_database_ptr(database db) const = 0;
protected:
    // Tools for type erasing an unerasing
    static database make_database(const impl* i, const void* db) {
        return database(i, db);
    }
    static keyspace make_keyspace(const impl* i, const void* k) {
        return keyspace(i, k);
    }
    static table make_table(const impl* i, const void* db) {
        return table(i, db);
    }
    static const void* extract(database db) {
        return db._database;
    }
    static const void* extract(keyspace ks) {
        return ks._keyspace;
    }
    static const void* extract(table t) {
        return t._table;
    }
};

inline
table::table(const impl* ops, const void* table)
        : _ops(ops)
        , _table(table) {
}

inline
keyspace::keyspace(const impl* ops, const void* keyspace)
        : _ops(ops)
        , _keyspace(keyspace) {
}

inline
database::database(const impl* ops, const void* database)
        : _ops(ops)
        , _database(database) {
}

}
