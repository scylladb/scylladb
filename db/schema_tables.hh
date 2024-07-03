/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/functions/functions.hh"
#include "mutation/mutation.hh"
#include "schema/schema_fwd.hh"
#include "schema_features.hh"
#include "utils/hashing.hh"
#include "schema_mutations.hh"
#include "types/map.hh"
#include "query-result-set.hh"

#include <seastar/core/distributed.hh>

#include <vector>
#include <map>

namespace data_dictionary {
class keyspace_metadata;
class user_types_storage;
}

using keyspace_metadata = data_dictionary::keyspace_metadata;

namespace replica {
class database;
}

namespace query {
class result_set;
}

namespace service {

class storage_service;
class storage_proxy;

}

namespace gms {

class feature_service;

}

namespace cql3::functions {

class user_function;
class user_aggregate;

}

namespace db {

class system_keyspace;
class extensions;
class config;

class schema_ctxt {
public:
    schema_ctxt(const config&, std::shared_ptr<data_dictionary::user_types_storage> uts, const gms::feature_service&,
                replica::database* = nullptr);
    schema_ctxt(replica::database&);
    schema_ctxt(distributed<replica::database>&);
    schema_ctxt(distributed<service::storage_proxy>&);

    const db::extensions& extensions() const {
        return _extensions;
    }

    unsigned murmur3_partitioner_ignore_msb_bits() const {
        return _murmur3_partitioner_ignore_msb_bits;
    }

    uint32_t schema_registry_grace_period() const {
        return _schema_registry_grace_period;
    }

    const data_dictionary::user_types_storage& user_types() const noexcept {
        return *_user_types;
    }

    const gms::feature_service& features() const {
        return _features;
    }

    replica::database* get_db() const {
        return _db;
    }
private:
    replica::database* _db;
    const gms::feature_service& _features;
    const db::extensions& _extensions;
    const unsigned _murmur3_partitioner_ignore_msb_bits;
    const uint32_t _schema_registry_grace_period;
    const std::shared_ptr<data_dictionary::user_types_storage> _user_types;
};

namespace schema_tables {

using schema_result = std::map<sstring, lw_shared_ptr<query::result_set>>;
using schema_result_value_type = std::pair<sstring, lw_shared_ptr<query::result_set>>;

const std::string COMMITLOG_FILENAME_PREFIX("SchemaLog-");

namespace v3 {

static constexpr auto NAME = "system_schema";
static constexpr auto KEYSPACES = "keyspaces";
static constexpr auto SCYLLA_KEYSPACES = "scylla_keyspaces";
static constexpr auto TABLES = "tables";
static constexpr auto SCYLLA_TABLES = "scylla_tables";
static constexpr auto COLUMNS = "columns";
static constexpr auto DROPPED_COLUMNS = "dropped_columns";
static constexpr auto TRIGGERS = "triggers";
static constexpr auto VIEWS = "views";
static constexpr auto TYPES = "types";
static constexpr auto FUNCTIONS = "functions";
static constexpr auto AGGREGATES = "aggregates";
static constexpr auto SCYLLA_AGGREGATES = "scylla_aggregates";
static constexpr auto INDEXES = "indexes";
static constexpr auto VIEW_VIRTUAL_COLUMNS = "view_virtual_columns"; // Scylla specific
static constexpr auto COMPUTED_COLUMNS = "computed_columns"; // Scylla specific
static constexpr auto SCYLLA_TABLE_SCHEMA_HISTORY = "scylla_table_schema_history"; // Scylla specific;

schema_ptr columns();
schema_ptr view_virtual_columns();
schema_ptr dropped_columns();
schema_ptr indexes();
schema_ptr tables();
schema_ptr scylla_tables(schema_features features = schema_features::full());
schema_ptr views();
schema_ptr types();
schema_ptr computed_columns();
// Belongs to the "system" keyspace
schema_ptr scylla_table_schema_history();

}

namespace legacy {

class schema_mutations {
    mutation _columnfamilies;
    mutation _columns;
public:
    schema_mutations(mutation columnfamilies, mutation columns)
        : _columnfamilies(std::move(columnfamilies))
        , _columns(std::move(columns))
    { }
    table_schema_version digest() const;
};

future<schema_mutations> read_table_mutations(distributed<service::storage_proxy>& proxy,
    sstring keyspace_name, sstring table_name, schema_ptr s);

}

using namespace v3;

// Change on non-backwards compatible changes of schema mutations.
// Replication of schema between nodes with different version is inhibited.
extern const sstring version;

// Returns schema_ptrs for all schema tables supported by given schema_features.
std::vector<schema_ptr> all_tables(schema_features);

// Like all_tables(), but returns schema::cf_name() of each table.
std::vector<sstring> all_table_names(schema_features);

// saves/creates all the system objects in the appropriate keyspaces;
// deletes them first, so they will be effectively overwritten.
future<> save_system_schema(cql3::query_processor& qp);

future<table_schema_version> calculate_schema_digest(distributed<service::storage_proxy>& proxy, schema_features, noncopyable_function<bool(std::string_view)> accept_keyspace);
// Calculates schema digest for all non-system keyspaces
future<table_schema_version> calculate_schema_digest(distributed<service::storage_proxy>& proxy, schema_features);

future<std::vector<canonical_mutation>> convert_schema_to_mutations(distributed<service::storage_proxy>& proxy, schema_features);
std::vector<mutation> adjust_schema_for_schema_features(std::vector<mutation> schema, schema_features features);

future<schema_result_value_type>
read_schema_partition_for_keyspace(distributed<service::storage_proxy>& proxy, sstring schema_table_name, sstring keyspace_name);
future<mutation> read_keyspace_mutation(distributed<service::storage_proxy>&, const sstring& keyspace_name);

// Must be called on shard 0.
future<semaphore_units<>> hold_merge_lock() noexcept;

future<> merge_schema(sharded<db::system_keyspace>& sys_ks, distributed<service::storage_proxy>& proxy, gms::feature_service& feat, std::vector<mutation> mutations, bool reload = false);

// Recalculates the local schema version.
//
// It is safe to call concurrently with recalculate_schema_version() and merge_schema() in which case it
// is guaranteed that the schema version we end up with after all calls will reflect the most recent state
// of feature_service and schema tables.
future<> recalculate_schema_version(sharded<db::system_keyspace>& sys_ks, distributed<service::storage_proxy>& proxy, gms::feature_service& feat);

future<std::set<sstring>> merge_keyspaces(distributed<service::storage_proxy>& proxy, schema_result&& before, schema_result&& after);

std::vector<mutation> make_create_keyspace_mutations(schema_features features, lw_shared_ptr<keyspace_metadata> keyspace, api::timestamp_type timestamp, bool with_tables_and_types_and_functions = true);

std::vector<mutation> make_drop_keyspace_mutations(schema_features features, lw_shared_ptr<keyspace_metadata> keyspace, api::timestamp_type timestamp);

lw_shared_ptr<keyspace_metadata> create_keyspace_from_schema_partition(const schema_result_value_type& partition, lw_shared_ptr<query::result_set> scylla_specific);

future<lw_shared_ptr<query::result_set>> extract_scylla_specific_keyspace_info(distributed<service::storage_proxy>& proxy, const schema_result_value_type& partition);

std::vector<mutation> make_create_type_mutations(lw_shared_ptr<keyspace_metadata> keyspace, user_type type, api::timestamp_type timestamp);

future<std::vector<user_type>> create_types_from_schema_partition(keyspace_metadata& ks, lw_shared_ptr<query::result_set> result);

seastar::future<std::vector<shared_ptr<cql3::functions::user_function>>> create_functions_from_schema_partition(replica::database& db, lw_shared_ptr<query::result_set> result);

std::vector<shared_ptr<cql3::functions::user_aggregate>> create_aggregates_from_schema_partition(replica::database& db, lw_shared_ptr<query::result_set> result, lw_shared_ptr<query::result_set> scylla_result, cql3::functions::change_batch& batch);

std::vector<mutation> make_create_function_mutations(shared_ptr<cql3::functions::user_function> func, api::timestamp_type timestamp);

std::vector<mutation> make_drop_function_mutations(shared_ptr<cql3::functions::user_function> func, api::timestamp_type timestamp);

std::vector<mutation> make_create_aggregate_mutations(schema_features features, shared_ptr<cql3::functions::user_aggregate> func, api::timestamp_type timestamp);

std::vector<mutation> make_drop_aggregate_mutations(schema_features features, shared_ptr<cql3::functions::user_aggregate> aggregate, api::timestamp_type timestamp);

std::vector<mutation> make_drop_type_mutations(lw_shared_ptr<keyspace_metadata> keyspace, user_type type, api::timestamp_type timestamp);

void add_type_to_schema_mutation(user_type type, api::timestamp_type timestamp, std::vector<mutation>& mutations);

std::vector<mutation> make_create_table_mutations(schema_ptr table, api::timestamp_type timestamp);

std::vector<mutation> make_update_table_mutations(
    replica::database& db,
    lw_shared_ptr<keyspace_metadata> keyspace,
    schema_ptr old_table,
    schema_ptr new_table,
    api::timestamp_type timestamp);

future<std::map<sstring, schema_ptr>> create_tables_from_tables_partition(distributed<service::storage_proxy>& proxy, const schema_result::mapped_type& result);

std::vector<mutation> make_drop_table_mutations(lw_shared_ptr<keyspace_metadata> keyspace, schema_ptr table, api::timestamp_type timestamp);

schema_ptr create_table_from_mutations(const schema_ctxt&, schema_mutations, std::optional<table_schema_version> version = {});

view_ptr create_view_from_mutations(const schema_ctxt&, schema_mutations, std::optional<table_schema_version> version = {});

future<std::vector<view_ptr>> create_views_from_schema_partition(distributed<service::storage_proxy>& proxy, const schema_result::mapped_type& result);

schema_mutations make_schema_mutations(schema_ptr s, api::timestamp_type timestamp, bool with_columns);
mutation make_scylla_tables_mutation(schema_ptr, api::timestamp_type timestamp);

void add_table_or_view_to_schema_mutation(schema_ptr view, api::timestamp_type timestamp, bool with_columns, std::vector<mutation>& mutations);

std::vector<mutation> make_create_view_mutations(lw_shared_ptr<keyspace_metadata> keyspace, view_ptr view, api::timestamp_type timestamp);

std::vector<mutation> make_update_view_mutations(lw_shared_ptr<keyspace_metadata> keyspace, view_ptr old_view, view_ptr new_view, api::timestamp_type timestamp, bool include_base);

std::vector<mutation> make_drop_view_mutations(lw_shared_ptr<keyspace_metadata> keyspace, view_ptr view, api::timestamp_type timestamp);

void check_no_legacy_secondary_index_mv_schema(replica::database& db, const view_ptr& v, schema_ptr base_schema);

sstring serialize_kind(column_kind kind);
column_kind deserialize_kind(sstring kind);
data_type parse_type(sstring str);

sstring serialize_index_kind(index_metadata_kind kind);
index_metadata_kind deserialize_index_kind(sstring kind);

mutation compact_for_schema_digest(const mutation& m);

void feed_hash_for_schema_digest(hasher&, const mutation&, schema_features);

template<typename K, typename V>
std::optional<std::map<K, V>> get_map(const query::result_set_row& row, const sstring& name) {
    if (auto values = row.get<map_type_impl::native_type>(name)) {
        std::map<K, V> map;
        for (auto&& entry : *values) {
            map.emplace(value_cast<K>(entry.first), value_cast<V>(entry.second));
        };
        return map;
    }
    return std::nullopt;
}

/// Stores the column mapping for the table being created or altered in the system table
/// which holds a history of schema versions alongside with their column mappings.
/// Can be used to insert entries with TTL (equal to DEFAULT_GC_GRACE_SECONDS) in case we are
/// overwriting an existing column mapping to garbage collect obsolete entries.
future<> store_column_mapping(distributed<service::storage_proxy>& proxy, schema_ptr s, bool with_ttl);
/// Query column mapping for a given version of the table locally.
future<column_mapping> get_column_mapping(db::system_keyspace& sys_ks, table_id table_id, table_schema_version version);
/// Check that column mapping exists for a given version of the table
future<bool> column_mapping_exists(db::system_keyspace& sys_ks, table_id table_id, table_schema_version version);
/// Delete matching column mapping entries from the `system.scylla_table_schema_history` table
future<> drop_column_mapping(db::system_keyspace& sys_ks, table_id table_id, table_schema_version version);

} // namespace schema_tables
} // namespace db
