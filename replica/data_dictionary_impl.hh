/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "data_dictionary/impl.hh"
#include "database.hh"

namespace cdc {
schema_ptr get_base_table(const replica::database& db, const schema& s);
}

namespace replica {

class data_dictionary_impl : public data_dictionary::impl {
public:
    const data_dictionary::database wrap(const replica::database& db) const {
        return make_database(this, &db);
    }
    data_dictionary::keyspace wrap(const replica::keyspace& ks) const {
        return make_keyspace(this, &ks);
    }
    data_dictionary::table wrap(const replica::table& t) const {
        return make_table(this, &t);
    }
    static const replica::database& unwrap(data_dictionary::database db) {
        return *static_cast<const replica::database*>(extract(db));
    }
    static const replica::keyspace& unwrap(data_dictionary::keyspace ks) {
        return *static_cast<const replica::keyspace*>(extract(ks));
    }
    static const replica::table& unwrap(data_dictionary::table t) {
        return *static_cast<const replica::table*>(extract(t));
    }
public:
    virtual const table_schema_version& get_version(data_dictionary::database db) const override {
        return unwrap(db).get_version();
    }

    virtual std::optional<data_dictionary::keyspace> try_find_keyspace(data_dictionary::database db, std::string_view name) const override {
        try {
            return wrap(unwrap(db).find_keyspace(name));
        } catch (no_such_keyspace&) {
            return std::nullopt;
        }
    }
    virtual std::vector<data_dictionary::keyspace> get_keyspaces(data_dictionary::database db) const override {
        std::vector<data_dictionary::keyspace> ret;
        const auto& keyspaces = unwrap(db).get_keyspaces();
        ret.reserve(keyspaces.size());
        for (auto& ks : keyspaces) {
            ret.push_back(wrap(ks.second));
        }
        return ret;
    }
    virtual std::vector<sstring> get_user_keyspaces(data_dictionary::database db) const override {
        return unwrap(db).get_user_keyspaces();
    }
    virtual std::vector<sstring> get_all_keyspaces(data_dictionary::database db) const override {
        return unwrap(db).get_all_keyspaces();
    }
    virtual std::vector<data_dictionary::table> get_tables(data_dictionary::database db) const override {
        std::vector<data_dictionary::table> ret;
        auto& tmd = unwrap(db).get_tables_metadata();
        ret.reserve(tmd.size());
        tmd.for_each_table([&] (table_id, const lw_shared_ptr<table> table) {
            ret.push_back(wrap(*table));
        });
        return ret;
    }
    virtual std::optional<data_dictionary::table> try_find_table(data_dictionary::database db, std::string_view ks, std::string_view table) const override {
        try {
            return wrap(unwrap(db).find_column_family(ks, table));
        } catch (no_such_column_family&) {
            return std::nullopt;
        }
    }
    virtual std::optional<data_dictionary::table> try_find_table(data_dictionary::database db, table_id id) const override {
        try {
            return wrap(unwrap(db).find_column_family(id));
        } catch (no_such_column_family&) {
            return std::nullopt;
        }
    }
    virtual schema_ptr get_table_schema(data_dictionary::table t) const override {
        return unwrap(t).schema();
    }
    virtual const std::vector<view_ptr>& get_table_views(data_dictionary::table t) const override {
        return unwrap(t).views();
    }
    virtual const secondary_index::secondary_index_manager& get_index_manager(data_dictionary::table t) const override {
        return const_cast<replica::table&>(unwrap(t)).get_index_manager();
    }
    virtual lw_shared_ptr<keyspace_metadata> get_keyspace_metadata(data_dictionary::keyspace ks) const override {
        return unwrap(ks).metadata();
    }
    virtual const locator::abstract_replication_strategy& get_replication_strategy(data_dictionary::keyspace ks) const override {
        return unwrap(ks).get_replication_strategy();
    }
    virtual bool is_internal(data_dictionary::keyspace ks) const override {
        return is_internal_keyspace(unwrap(ks).metadata()->name());
    }
    virtual sstring get_available_index_name(data_dictionary::database db, std::string_view ks_name, std::string_view table_name,
            std::optional<sstring> index_name_root) const override {
        return unwrap(db).get_available_index_name(sstring(ks_name), sstring(table_name), index_name_root);
    }
    virtual std::set<sstring> existing_index_names(data_dictionary::database db, std::string_view ks_name, std::string_view cf_to_exclude = {}) const override {
        return unwrap(db).existing_index_names(sstring(ks_name), sstring(cf_to_exclude));
    }
    virtual schema_ptr find_indexed_table(data_dictionary::database db, std::string_view ks_name, std::string_view index_name) const override {
        return unwrap(db).find_indexed_table(sstring(ks_name), sstring(index_name));
    }
    virtual const db::config& get_config(data_dictionary::database db) const override {
        return unwrap(db).get_config();
    }
    virtual const db::extensions& get_extensions(data_dictionary::database db) const override {
        return unwrap(db).extensions();
    }
    virtual const gms::feature_service& get_features(data_dictionary::database db) const override {
        return unwrap(db).features();
    }
    virtual replica::database& real_database(data_dictionary::database db) const override {
        return const_cast<replica::database&>(unwrap(db));
    }
    virtual replica::database* real_database_ptr(data_dictionary::database db) const override {
        return &real_database(db);
    }
    virtual schema_ptr get_cdc_base_table(data_dictionary::database db, const schema& s) const override {
        return cdc::get_base_table(unwrap(db), s);
    }
};

} // namespace replica
