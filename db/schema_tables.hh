/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
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

#include "mutation.hh"
#include "cql3/functions/user_function.hh"
#include "cql3/functions/user_aggregate.hh"
#include "schema_fwd.hh"
#include "schema_features.hh"
#include "hashing.hh"
#include "schema_mutations.hh"
#include "types/map.hh"
#include "query-result-set.hh"

#include <seastar/core/distributed.hh>

#include <vector>
#include <map>

class keyspace_metadata;
class database;

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

namespace db {

class extensions;
class config;

class schema_ctxt {
public:
    schema_ctxt(const config&);
    schema_ctxt(const database&);
    schema_ctxt(distributed<database>&);
    schema_ctxt(distributed<service::storage_proxy>&);

    // For tests *only*.
    class for_tests {};
    schema_ctxt(for_tests, db::extensions&);

    const db::extensions& extensions() const {
        return _extensions;
    }

    const unsigned murmur3_partitioner_ignore_msb_bits() const {
        return _murmur3_partitioner_ignore_msb_bits;
    }

    uint32_t schema_registry_grace_period() const {
        return _schema_registry_grace_period;
    }

private:
    const db::extensions& _extensions;
    const unsigned _murmur3_partitioner_ignore_msb_bits;
    const uint32_t _schema_registry_grace_period;
};

namespace schema_tables {

using schema_result = std::map<sstring, lw_shared_ptr<query::result_set>>;
using schema_result_value_type = std::pair<sstring, lw_shared_ptr<query::result_set>>;

namespace v3 {

static constexpr auto NAME = "system_schema";
static constexpr auto KEYSPACES = "keyspaces";
static constexpr auto TABLES = "tables";
static constexpr auto SCYLLA_TABLES = "scylla_tables";
static constexpr auto COLUMNS = "columns";
static constexpr auto DROPPED_COLUMNS = "dropped_columns";
static constexpr auto TRIGGERS = "triggers";
static constexpr auto VIEWS = "views";
static constexpr auto TYPES = "types";
static constexpr auto FUNCTIONS = "functions";
static constexpr auto AGGREGATES = "aggregates";
static constexpr auto INDEXES = "indexes";
static constexpr auto VIEW_VIRTUAL_COLUMNS = "view_virtual_columns"; // Scylla specific
static constexpr auto COMPUTED_COLUMNS = "computed_columns"; // Scylla specific
static constexpr auto SCYLLA_TABLE_SCHEMA_HISTORY = "scylla_table_schema_history"; // Scylla specific;

schema_ptr columns(schema_registry& registry);
schema_ptr view_virtual_columns(schema_registry& registry);
schema_ptr dropped_columns(schema_registry& registry);
schema_ptr indexes(schema_registry& registry);
schema_ptr tables(schema_registry& registry);
schema_ptr scylla_tables(schema_registry& registry, schema_features features = schema_features::full());
schema_ptr views(schema_registry& registry);
schema_ptr computed_columns(schema_registry& registry);
// Belongs to the "system" keyspace
schema_ptr scylla_table_schema_history(schema_registry& registry);

utils::UUID views_id();

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
std::vector<schema_ptr> all_tables(schema_registry& registry, schema_features);

// Like all_tables(), but returns schema::cf_name() of each table.
std::vector<sstring> all_table_names(schema_registry& registry, schema_features);

// saves/creates "ks" + all tables etc, while first deleting all old schema entries (will be rewritten)
future<> save_system_schema(cql3::query_processor& qp, const sstring & ks);

// saves/creates "system_schema" keyspace
future<> save_system_keyspace_schema(cql3::query_processor& qp);

future<utils::UUID> calculate_schema_digest(distributed<service::storage_proxy>& proxy, schema_features, noncopyable_function<bool(std::string_view)> accept_keyspace);
// Calculates schema digest for all non-system keyspaces
future<utils::UUID> calculate_schema_digest(distributed<service::storage_proxy>& proxy, schema_features);

future<std::vector<canonical_mutation>> convert_schema_to_mutations(distributed<service::storage_proxy>& proxy, schema_features);
std::vector<mutation> adjust_schema_for_schema_features(std::vector<mutation> schema, schema_registry& registry, schema_features features);

future<schema_result_value_type>
read_schema_partition_for_keyspace(distributed<service::storage_proxy>& proxy, sstring schema_table_name, sstring keyspace_name);
future<mutation> read_keyspace_mutation(distributed<service::storage_proxy>&, const sstring& keyspace_name);

// Must be called on shard 0.
future<semaphore_units<>> hold_merge_lock() noexcept;

future<> merge_schema(distributed<service::storage_proxy>& proxy, gms::feature_service& feat, std::vector<mutation> mutations);

// Recalculates the local schema version.
//
// It is safe to call concurrently with recalculate_schema_version() and merge_schema() in which case it
// is guaranteed that the schema version we end up with after all calls will reflect the most recent state
// of feature_service and schema tables.
future<> recalculate_schema_version(distributed<service::storage_proxy>& proxy, gms::feature_service& feat);

future<std::set<sstring>> merge_keyspaces(distributed<service::storage_proxy>& proxy, schema_result&& before, schema_result&& after);

std::vector<mutation> make_create_keyspace_mutations(schema_registry& registry, lw_shared_ptr<keyspace_metadata> keyspace, api::timestamp_type timestamp, bool with_tables_and_types_and_functions = true);

std::vector<mutation> make_drop_keyspace_mutations(schema_registry& registry, lw_shared_ptr<keyspace_metadata> keyspace, api::timestamp_type timestamp);

lw_shared_ptr<keyspace_metadata> create_keyspace_from_schema_partition(const schema_result_value_type& partition);

std::vector<mutation> make_create_type_mutations(schema_registry& registry, lw_shared_ptr<keyspace_metadata> keyspace, user_type type, api::timestamp_type timestamp);

std::vector<user_type> create_types_from_schema_partition(keyspace_metadata& ks, lw_shared_ptr<query::result_set> result);

std::vector<shared_ptr<cql3::functions::user_function>> create_functions_from_schema_partition(database& db, lw_shared_ptr<query::result_set> result);

std::vector<mutation> make_create_function_mutations(schema_registry& registry, shared_ptr<cql3::functions::user_function> func, api::timestamp_type timestamp);

std::vector<mutation> make_drop_function_mutations(schema_registry& registry, shared_ptr<cql3::functions::user_function> func, api::timestamp_type timestamp);

std::vector<mutation> make_create_aggregate_mutations(schema_registry& registry, shared_ptr<cql3::functions::user_aggregate> func, api::timestamp_type timestamp);

std::vector<mutation> make_drop_aggregate_mutations(schema_registry& registry, shared_ptr<cql3::functions::user_aggregate> aggregate, api::timestamp_type timestamp);

std::vector<mutation> make_drop_type_mutations(schema_registry& registry, lw_shared_ptr<keyspace_metadata> keyspace, user_type type, api::timestamp_type timestamp);

void add_type_to_schema_mutation(schema_registry& registry, user_type type, api::timestamp_type timestamp, std::vector<mutation>& mutations);

std::vector<mutation> make_create_table_mutations(lw_shared_ptr<keyspace_metadata> keyspace, schema_ptr table, api::timestamp_type timestamp);

std::vector<mutation> make_update_table_mutations(
    database& db,
    lw_shared_ptr<keyspace_metadata> keyspace,
    schema_ptr old_table,
    schema_ptr new_table,
    api::timestamp_type timestamp,
    bool from_thrift);

future<std::map<sstring, schema_ptr>> create_tables_from_tables_partition(distributed<service::storage_proxy>& proxy, const schema_result::mapped_type& result);

std::vector<mutation> make_drop_table_mutations(schema_registry& registry, lw_shared_ptr<keyspace_metadata> keyspace, schema_ptr table, api::timestamp_type timestamp);

schema_ptr create_table_from_mutations(schema_registry&, schema_mutations, std::optional<table_schema_version> version = {});

view_ptr create_view_from_mutations(schema_registry&, schema_mutations, std::optional<table_schema_version> version = {});

future<std::vector<view_ptr>> create_views_from_schema_partition(distributed<service::storage_proxy>& proxy, const schema_result::mapped_type& result);

schema_mutations make_schema_mutations(schema_ptr s, api::timestamp_type timestamp, bool with_columns);
mutation make_scylla_tables_mutation(schema_ptr, api::timestamp_type timestamp);

void add_table_or_view_to_schema_mutation(schema_ptr view, api::timestamp_type timestamp, bool with_columns, std::vector<mutation>& mutations);

std::vector<mutation> make_create_view_mutations(lw_shared_ptr<keyspace_metadata> keyspace, view_ptr view, api::timestamp_type timestamp);

std::vector<mutation> make_update_view_mutations(lw_shared_ptr<keyspace_metadata> keyspace, view_ptr old_view, view_ptr new_view, api::timestamp_type timestamp, bool include_base);

std::vector<mutation> make_drop_view_mutations(schema_registry& registry, lw_shared_ptr<keyspace_metadata> keyspace, view_ptr view, api::timestamp_type timestamp);

class preserve_version_tag {};
using preserve_version = bool_class<preserve_version_tag>;
view_ptr maybe_fix_legacy_secondary_index_mv_schema(database& db, const view_ptr& v, schema_ptr base_schema, preserve_version preserve_version);

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
future<column_mapping> get_column_mapping(utils::UUID table_id, table_schema_version version);
/// Check that column mapping exists for a given version of the table
future<bool> column_mapping_exists(utils::UUID table_id, table_schema_version version);
/// Delete matching column mapping entries from the `system.scylla_table_schema_history` table
future<> drop_column_mapping(utils::UUID table_id, table_schema_version version);

} // namespace schema_tables
} // namespace db
