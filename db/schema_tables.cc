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
 * Copyright (C) 2015 ScyllaDB
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

#include "db/schema_tables.hh"

#include "service/migration_manager.hh"
#include "partition_slice_builder.hh"
#include "dht/i_partitioner.hh"
#include "system_keyspace.hh"
#include "query_context.hh"
#include "query-result-set.hh"
#include "query-result-writer.hh"
#include "schema_builder.hh"
#include "map_difference.hh"
#include "utils/UUID_gen.hh"
#include "core/do_with.hh"
#include "core/thread.hh"
#include "json.hh"
#include "log.hh"
#include "frozen_schema.hh"
#include "schema_registry.hh"
#include "mutation_query.hh"
#include "system_keyspace.hh"
#include "cql3/cql3_type.hh"

#include "db/marshal/type_parser.hh"
#include "db/config.hh"
#include "db/extensions.hh"
#include "md5_hasher.hh"

#include <seastar/util/noncopyable_function.hh>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm/transform.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/join.hpp>

#include "compaction_strategy.hh"
#include "utils/joinpoint.hh"
#include "view_info.hh"
#include "cql_type_parser.hh"
#include "db/timeout_clock.hh"

using namespace db::system_keyspace;
using namespace std::chrono_literals;

/** system.schema_* tables used to store keyspace/table/type attributes prior to C* 3.0 */
namespace db {

schema_ctxt::schema_ctxt(const db::config& cfg)
    : _extensions(cfg.extensions())
{}

schema_ctxt::schema_ctxt(const database& db)
    : schema_ctxt(db.get_config())
{}

schema_ctxt::schema_ctxt(distributed<database>& db)
    : schema_ctxt(db.local())
{}

schema_ctxt::schema_ctxt(distributed<service::storage_proxy>& proxy)
    : schema_ctxt(proxy.local().get_db())
{}

namespace schema_tables {

logging::logger slogger("schema_tables");

const sstring version = "3";

struct push_back_and_return {
    std::vector<mutation> muts;

    std::vector<mutation> operator()(mutation&& m) {
        muts.emplace_back(std::move(m));
        return std::move(muts);
    }
};

struct qualified_name {
    sstring keyspace_name;
    sstring table_name;

    qualified_name(sstring keyspace_name, sstring table_name)
            : keyspace_name(std::move(keyspace_name))
            , table_name(std::move(table_name))
    { }

    qualified_name(const schema_ptr& s)
            : keyspace_name(s->ks_name())
            , table_name(s->cf_name())
    { }

    bool operator<(const qualified_name& o) const {
        return keyspace_name < o.keyspace_name
               || (keyspace_name == o.keyspace_name && table_name < o.table_name);
    }

    bool operator==(const qualified_name& o) const {
        return keyspace_name == o.keyspace_name && table_name == o.table_name;
    }
};

static future<schema_mutations> read_table_mutations(distributed<service::storage_proxy>& proxy, const qualified_name& table, schema_ptr s);

static void merge_tables_and_views(distributed<service::storage_proxy>& proxy,
    std::map<qualified_name, schema_mutations>&& tables_before,
    std::map<qualified_name, schema_mutations>&& tables_after,
    std::map<qualified_name, schema_mutations>&& views_before,
    std::map<qualified_name, schema_mutations>&& views_after);

struct user_types_to_drop final {
    seastar::noncopyable_function<void()> drop;
};

[[nodiscard]] static user_types_to_drop merge_types(distributed<service::storage_proxy>& proxy,
    schema_result&& before,
    schema_result&& after);

static future<> do_merge_schema(distributed<service::storage_proxy>&, std::vector<mutation>, bool do_flush);

static std::vector<column_definition> create_columns_from_column_rows(
                const query::result_set& rows, const sstring& keyspace,
                const sstring& table, bool is_super, column_view_virtual is_view_virtual);


static std::vector<index_metadata> create_indices_from_index_rows(const query::result_set& rows,
                                const sstring& keyspace,
                                const sstring& table);

static index_metadata create_index_from_index_row(const query::result_set_row& row,
                     sstring keyspace,
                     sstring table);

static void add_column_to_schema_mutation(schema_ptr, const column_definition&,
                api::timestamp_type, mutation&);

static void add_index_to_schema_mutation(schema_ptr table,
                const index_metadata& index, api::timestamp_type timestamp,
                mutation& mutation);

static void drop_column_from_schema_mutation(schema_ptr schema_table, schema_ptr table,
                const sstring& column_name, long timestamp,
                std::vector<mutation>&);

static void drop_index_from_schema_mutation(schema_ptr table,
                const index_metadata& column, long timestamp,
                std::vector<mutation>& mutations);

static future<schema_ptr> create_table_from_table_row(
                distributed<service::storage_proxy>&,
                const query::result_set_row&);

static void prepare_builder_from_table_row(const schema_ctxt&, schema_builder&, const query::result_set_row&);

using namespace v3;

std::vector<const char*> ALL { KEYSPACES, TABLES, SCYLLA_TABLES, COLUMNS, DROPPED_COLUMNS, TRIGGERS, VIEWS, TYPES, FUNCTIONS, AGGREGATES, INDEXES };

using days = std::chrono::duration<int, std::ratio<24 * 3600>>;

future<> save_system_schema(const sstring & ksname) {
    auto& ks = db::qctx->db().find_keyspace(ksname);
    auto ksm = ks.metadata();

    // delete old, possibly obsolete entries in schema tables
    return parallel_for_each(ALL, [ksm] (sstring cf) {
        auto deletion_timestamp = schema_creation_timestamp() - 1;
        return db::execute_cql(sprint("DELETE FROM %s.%s USING TIMESTAMP %s WHERE keyspace_name = ?", NAME, cf,
            deletion_timestamp), ksm->name()).discard_result();
    }).then([ksm] {
        auto mvec  = make_create_keyspace_mutations(ksm, schema_creation_timestamp(), true);
        return qctx->proxy().mutate_locally(std::move(mvec));
    });
}

/** add entries to system_schema.* for the hardcoded system definitions */
future<> save_system_keyspace_schema() {
    return save_system_schema(NAME);
}

namespace v3 {

static constexpr auto schema_gc_grace = std::chrono::duration_cast<std::chrono::seconds>(days(7)).count();

schema_ptr keyspaces() {
    static thread_local auto schema = [] {
        schema_builder builder(make_lw_shared(::schema(generate_legacy_id(NAME, KEYSPACES), NAME, KEYSPACES,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {},
        // regular columns
        {
            {"durable_writes", boolean_type},
            {"replication", map_type_impl::get_instance(utf8_type, utf8_type, false)},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "keyspace definitions"
        )));
        builder.set_gc_grace_seconds(schema_gc_grace);
        builder.with_version(generate_schema_version(builder.uuid()));
        return builder.build();
    }();
    return schema;
}

schema_ptr tables() {
    static thread_local auto schema = [] {
        schema_builder builder(make_lw_shared(::schema(generate_legacy_id(NAME, TABLES), NAME, TABLES,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"table_name", utf8_type}},
        // regular columns
        {
         {"bloom_filter_fp_chance", double_type},
         {"caching", map_type_impl::get_instance(utf8_type, utf8_type, false)},
         {"comment", utf8_type},
         {"compaction", map_type_impl::get_instance(utf8_type, utf8_type, false)},
         {"compression", map_type_impl::get_instance(utf8_type, utf8_type, false)},
         {"crc_check_chance", double_type},
         {"dclocal_read_repair_chance", double_type},
         {"default_time_to_live", int32_type},
         {"extensions", map_type_impl::get_instance(utf8_type, bytes_type, false)},
         {"flags", set_type_impl::get_instance(utf8_type, false)}, // SUPER, COUNTER, DENSE, COMPOUND
         {"gc_grace_seconds", int32_type},
         {"id", uuid_type},
         {"max_index_interval", int32_type},
         {"memtable_flush_period_in_ms", int32_type},
         {"min_index_interval", int32_type},
         {"read_repair_chance", double_type},
         {"speculative_retry", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "table definitions"
        )));
        builder.set_gc_grace_seconds(schema_gc_grace);
        builder.with_version(generate_schema_version(builder.uuid()));
        return builder.build();
    }();
    return schema;
}

// Holds Scylla-specific table metadata.
schema_ptr scylla_tables() {
    static thread_local auto schema = [] {
        auto id = generate_legacy_id(NAME, SCYLLA_TABLES);
        return schema_builder(NAME, SCYLLA_TABLES, stdx::make_optional(id))
            .with_column("keyspace_name", utf8_type, column_kind::partition_key)
            .with_column("table_name", utf8_type, column_kind::clustering_key)
            .with_column("version", uuid_type)
            .set_gc_grace_seconds(schema_gc_grace)
            .with_version(generate_schema_version(id))
            .build();
    }();
    return schema;
}

// The "columns" table lists the definitions of all columns in all tables
// and views. Its schema needs to be identical to the one in Cassandra because
// it is the API through which drivers inspect the list of columns in a table
// (e.g., cqlsh's "DESCRIBE TABLE" and "DESCRIBE MATERIALIZED VIEW" get their
// information from the columns table).
// The "view_virtual_columns" table is an additional table with exactly the
// same schema (both are created by columns_schema()), but has a separate
// list of "virtual" columns. Those are used in materialized views for keeping
// rows without data alive (see issue #3362). These virtual columns cannot be
// listed in the regular "columns" table, otherwise the "DESCRIBE MATERIALIZED
// VIEW" would list them - while it should only list real, selected, columns.

static schema_ptr columns_schema(const char* columns_table_name) {
    schema_builder builder(make_lw_shared(::schema(generate_legacy_id(NAME, columns_table_name), NAME, columns_table_name,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"table_name", utf8_type},{"column_name", utf8_type}},
        // regular columns
        {
         {"clustering_order", utf8_type},
         {"column_name_bytes", bytes_type},
         {"kind", utf8_type},
         {"position", int32_type},
         {"type", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "column definitions"
        )));
    builder.set_gc_grace_seconds(schema_gc_grace);
    builder.with_version(generate_schema_version(builder.uuid()));
    return builder.build();
}
schema_ptr columns() {
    static thread_local auto schema = columns_schema(COLUMNS);
    return schema;
}
schema_ptr view_virtual_columns() {
    static thread_local auto schema = columns_schema(VIEW_VIRTUAL_COLUMNS);
    return schema;
}

schema_ptr dropped_columns() {
    static thread_local auto schema = [] {
        schema_builder builder(make_lw_shared(::schema(generate_legacy_id(NAME, DROPPED_COLUMNS), NAME, DROPPED_COLUMNS,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"table_name", utf8_type},{"column_name", utf8_type}},
        // regular columns
        {
         {"dropped_time", timestamp_type},
         {"type", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "dropped column registry"
        )));
        builder.set_gc_grace_seconds(schema_gc_grace);
        builder.with_version(generate_schema_version(builder.uuid()));
        return builder.build();
    }();
    return schema;
}

schema_ptr triggers() {
    static thread_local auto schema = [] {
        schema_builder builder(make_lw_shared(::schema(generate_legacy_id(NAME, TRIGGERS), NAME, TRIGGERS,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"table_name", utf8_type},{"trigger_name", utf8_type}},
        // regular columns
        {
         {"options", map_type_impl::get_instance(utf8_type, utf8_type, false)},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "trigger definitions"
        )));
        builder.set_gc_grace_seconds(schema_gc_grace);
        builder.with_version(generate_schema_version(builder.uuid()));
        return builder.build();
    }();
    return schema;
}

schema_ptr views() {
    static thread_local auto schema = [] {
        schema_builder builder(make_lw_shared(::schema(generate_legacy_id(NAME, VIEWS), NAME, VIEWS,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"view_name", utf8_type}},
        // regular columns
        {
         {"base_table_id", uuid_type},
         {"base_table_name", utf8_type},
         {"where_clause", utf8_type},
         {"bloom_filter_fp_chance", double_type},
         {"caching", map_type_impl::get_instance(utf8_type, utf8_type, false)},
         {"comment", utf8_type},
         {"compaction", map_type_impl::get_instance(utf8_type, utf8_type, false)},
         {"compression", map_type_impl::get_instance(utf8_type, utf8_type, false)},
         {"crc_check_chance", double_type},
         {"dclocal_read_repair_chance", double_type},
         {"default_time_to_live", int32_type},
         {"extensions", map_type_impl::get_instance(utf8_type, bytes_type, false)},
         {"gc_grace_seconds", int32_type},
         {"id", uuid_type},
         {"include_all_columns", boolean_type},
         {"max_index_interval", int32_type},
         {"memtable_flush_period_in_ms", int32_type},
         {"min_index_interval", int32_type},
         {"read_repair_chance", double_type},
         {"speculative_retry", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "view definitions"
        )));
        builder.set_gc_grace_seconds(schema_gc_grace);
        builder.with_version(generate_schema_version(builder.uuid()));
        return builder.build();
    }();
    return schema;
}

schema_ptr indexes() {
    static thread_local auto schema = [] {
        schema_builder builder(make_lw_shared(::schema(generate_legacy_id(NAME, INDEXES), NAME, INDEXES,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"table_name", utf8_type},{"index_name", utf8_type}},
        // regular columns
        {
         {"kind", utf8_type},
         {"options", map_type_impl::get_instance(utf8_type, utf8_type, false)},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "secondary index definitions"
        )));
        builder.set_gc_grace_seconds(schema_gc_grace);
        builder.with_version(generate_schema_version(builder.uuid()));
        return builder.build();
    }();
    return schema;
}

schema_ptr types() {
    static thread_local auto schema = [] {
        schema_builder builder(make_lw_shared(::schema(generate_legacy_id(NAME, TYPES), NAME, TYPES,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"type_name", utf8_type}},
        // regular columns
        {
         {"field_names", list_type_impl::get_instance(utf8_type, false)},
         {"field_types", list_type_impl::get_instance(utf8_type, false)},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "user defined type definitions"
        )));
        builder.set_gc_grace_seconds(schema_gc_grace);
        builder.with_version(generate_schema_version(builder.uuid()));
        return builder.build();
    }();
    return schema;
}

schema_ptr functions() {
    static thread_local auto schema = [] {
        schema_builder builder(make_lw_shared(::schema(generate_legacy_id(NAME, FUNCTIONS), NAME, FUNCTIONS,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"function_name", utf8_type}, {"argument_types", list_type_impl::get_instance(utf8_type, false)}},
        // regular columns
        {
         {"argument_names", list_type_impl::get_instance(utf8_type, false)},
         {"body", utf8_type},
         {"language", utf8_type},
         {"return_type", utf8_type},
         {"called_on_null_input", boolean_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "user defined function definitions"
        )));
        builder.set_gc_grace_seconds(schema_gc_grace);
        builder.with_version(generate_schema_version(builder.uuid()));
        return builder.build();
    }();
    return schema;
}

schema_ptr aggregates() {
    static thread_local auto schema = [] {
        schema_builder builder(make_lw_shared(::schema(generate_legacy_id(NAME, AGGREGATES), NAME, AGGREGATES,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"aggregate_name", utf8_type}, {"argument_types", list_type_impl::get_instance(utf8_type, false)}},
        // regular columns
        {
         {"final_func", utf8_type},
         {"initcond", utf8_type},
         {"return_type", utf8_type},
         {"state_func", utf8_type},
         {"state_type", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "user defined aggregate definitions"
        )));
        builder.set_gc_grace_seconds(schema_gc_grace);
        builder.with_version(generate_schema_version(builder.uuid()));
        return builder.build();
    }();
    return schema;
}

}

#if 0
    public static void truncateSchemaTables()
    {
        for (String table : ALL)
            getSchemaCFS(table).truncateBlocking();
    }

    private static void flushSchemaTables()
    {
        for (String table : ALL)
            SystemKeyspace.forceBlockingFlush(table);
    }
#endif

/**
 * Read schema from system keyspace and calculate MD5 digest of every row, resulting digest
 * will be converted into UUID which would act as content-based version of the schema.
 */
future<utils::UUID> calculate_schema_digest(distributed<service::storage_proxy>& proxy)
{
    auto map = [&proxy] (sstring table) {
        return db::system_keyspace::query_mutations(proxy, NAME, table).then([&proxy, table] (auto rs) {
            auto s = proxy.local().get_db().local().find_schema(NAME, table);
            std::vector<mutation> mutations;
            for (auto&& p : rs->partitions()) {
                auto mut = p.mut().unfreeze(s);
                auto partition_key = value_cast<sstring>(utf8_type->deserialize(mut.key().get_component(*s, 0)));
                if (is_system_keyspace(partition_key)) {
                    continue;
                }
                mutations.emplace_back(std::move(mut));
            }
            return mutations;
        });
    };
    auto reduce = [] (auto& hash, auto&& mutations) {
        for (const mutation& m : mutations) {
            feed_hash_for_schema_digest(hash, m);
        }
    };
    return do_with(md5_hasher(), [map, reduce] (auto& hash) {
        return do_for_each(ALL.begin(), ALL.end(), [&hash, map, reduce] (auto& table) {
            return map(table).then([&hash, reduce] (auto&& mutations) {
                reduce(hash, mutations);
            });
        }).then([&hash] {
            return make_ready_future<utils::UUID>(utils::UUID_gen::get_name_UUID(hash.finalize()));
        });
    });
}

future<std::vector<frozen_mutation>> convert_schema_to_mutations(distributed<service::storage_proxy>& proxy)
{
    auto map = [&proxy] (sstring table) {
        return db::system_keyspace::query_mutations(proxy, NAME, table).then([&proxy, table] (auto rs) {
            auto s = proxy.local().get_db().local().find_schema(NAME, table);
            std::vector<frozen_mutation> results;
            for (auto&& p : rs->partitions()) {
                auto mut = p.mut().unfreeze(s);
                auto partition_key = value_cast<sstring>(utf8_type->deserialize(mut.key().get_component(*s, 0)));
                if (is_system_keyspace(partition_key)) {
                    continue;
                }
                results.emplace_back(std::move(p.mut()));
            }
            return results;
        });
    };
    auto reduce = [] (auto&& result, auto&& mutations) {
        std::move(mutations.begin(), mutations.end(), std::back_inserter(result));
        return std::move(result);
    };
    return map_reduce(ALL.begin(), ALL.end(), map, std::vector<frozen_mutation>{}, reduce);
}

future<schema_result>
read_schema_for_keyspaces(distributed<service::storage_proxy>& proxy, const sstring& schema_table_name, const std::set<sstring>& keyspace_names)
{
    auto schema = proxy.local().get_db().local().find_schema(NAME, schema_table_name);
    auto map = [&proxy, schema_table_name] (const sstring& keyspace_name) { return read_schema_partition_for_keyspace(proxy, schema_table_name, keyspace_name); };
    auto insert = [] (schema_result&& result, auto&& schema_entity) {
        if (!schema_entity.second->empty()) {
            result.insert(std::move(schema_entity));
        }
        return std::move(result);
    };
    return map_reduce(keyspace_names.begin(), keyspace_names.end(), map, schema_result{}, insert);
}

static
future<mutation> query_partition_mutation(service::storage_proxy& proxy,
    schema_ptr s,
    lw_shared_ptr<query::read_command> cmd,
    partition_key pkey)
{
    auto dk = dht::global_partitioner().decorate_key(*s, pkey);
    return do_with(dht::partition_range::make_singular(dk), [&proxy, dk, s = std::move(s), cmd = std::move(cmd)] (auto& range) {
        return proxy.query_mutations_locally(s, std::move(cmd), range, db::no_timeout)
                .then([dk = std::move(dk), s](foreign_ptr<lw_shared_ptr<reconcilable_result>> res, cache_temperature hit_rate) {
                    auto&& partitions = res->partitions();
                    if (partitions.size() == 0) {
                        return mutation(s, std::move(dk));
                    } else if (partitions.size() == 1) {
                        return partitions[0].mut().unfreeze(s);
                    } else {
                        throw std::invalid_argument("Results must have at most one partition");
                    }
                });
    });
}

future<schema_result_value_type>
read_schema_partition_for_keyspace(distributed<service::storage_proxy>& proxy, const sstring& schema_table_name, const sstring& keyspace_name)
{
    auto schema = proxy.local().get_db().local().find_schema(NAME, schema_table_name);
    auto keyspace_key = dht::global_partitioner().decorate_key(*schema,
        partition_key::from_singular(*schema, keyspace_name));
    return db::system_keyspace::query(proxy, NAME, schema_table_name, keyspace_key).then([keyspace_name] (auto&& rs) {
        return schema_result_value_type{keyspace_name, std::move(rs)};
    });
}

future<mutation>
read_schema_partition_for_table(distributed<service::storage_proxy>& proxy, schema_ptr schema, const sstring& keyspace_name, const sstring& table_name)
{
    auto keyspace_key = partition_key::from_singular(*schema, keyspace_name);
    auto clustering_range = query::clustering_range(clustering_key_prefix::from_clustering_prefix(
            *schema, exploded_clustering_prefix({utf8_type->decompose(table_name)})));
    auto slice = partition_slice_builder(*schema)
            .with_range(std::move(clustering_range))
            .build();
    auto cmd = make_lw_shared<query::read_command>(schema->id(), schema->version(), std::move(slice), query::max_rows);
    return query_partition_mutation(proxy.local(), std::move(schema), std::move(cmd), std::move(keyspace_key));
}

future<mutation>
read_keyspace_mutation(distributed<service::storage_proxy>& proxy, const sstring& keyspace_name) {
    schema_ptr s = keyspaces();
    auto key = partition_key::from_singular(*s, keyspace_name);
    auto cmd = make_lw_shared<query::read_command>(s->id(), s->version(), s->full_slice());
    return query_partition_mutation(proxy.local(), std::move(s), std::move(cmd), std::move(key));
}

static semaphore the_merge_lock {1};

future<> merge_lock() {
    // ref:  #1088
    // to avoid deadlocks, we don't want long-standing calls to the shard 0
    // as they can cause a deadlock:
    //
    //   fiber1                fiber2
    //   merge_lock()                         (succeeds)
    //                         merge_lock()   (waits)
    //   invoke_on_all()                      (waits on merge_lock to relinquish smp::submit_to slot)
    //
    // so we issue the lock calls with a timeout; the slot will be relinquished, and invoke_on_all()
    // can complete
    return repeat([] () mutable {
        return smp::submit_to(0, [] {
            return the_merge_lock.try_wait();
        }).then([] (bool result) {
            if (result) {
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            } else {
                static thread_local auto rand_engine = std::default_random_engine();
                auto dist = std::uniform_int_distribution<int>(0, 100);
                auto to = std::chrono::microseconds(dist(rand_engine));
                return sleep(to).then([] {
                    return make_ready_future<stop_iteration>(stop_iteration::no);
                });
            }
        });
    });
}

future<> merge_unlock() {
    return smp::submit_to(0, [] { the_merge_lock.signal(); });
}

/**
 * Merge remote schema in form of mutations with local and mutate ks/cf metadata objects
 * (which also involves fs operations on add/drop ks/cf)
 *
 * @param mutations the schema changes to apply
 *
 * @throws ConfigurationException If one of metadata attributes has invalid value
 * @throws IOException If data was corrupted during transportation or failed to apply fs operations
 */
future<> merge_schema(distributed<service::storage_proxy>& proxy, std::vector<mutation> mutations)
{
    return merge_lock().then([&proxy, mutations = std::move(mutations)] () mutable {
        return do_merge_schema(proxy, std::move(mutations), true).then([&proxy] {
            return update_schema_version_and_announce(proxy);
        });
    }).finally([] {
        return merge_unlock();
    });
}

future<> merge_schema(distributed<service::storage_proxy>& proxy, std::vector<mutation> mutations, bool do_flush)
{
    return merge_lock().then([&proxy, mutations = std::move(mutations), do_flush] () mutable {
        return do_merge_schema(proxy, std::move(mutations), do_flush);
    }).finally([] {
        return merge_unlock();
    });
}

// Returns names of live table definitions of given keyspace
future<std::vector<sstring>>
static read_table_names_of_keyspace(distributed<service::storage_proxy>& proxy, const sstring& keyspace_name, schema_ptr schema_table) {
    auto pkey = dht::global_partitioner().decorate_key(*schema_table, partition_key::from_singular(*schema_table, keyspace_name));
    return db::system_keyspace::query(proxy, schema_table->ks_name(), schema_table->cf_name(), pkey).then([schema_table] (auto&& rs) {
        return boost::copy_range<std::vector<sstring>>(rs->rows() | boost::adaptors::transformed([schema_table] (const query::result_set_row& row) {
            const sstring name = schema_table->clustering_key_columns().begin()->name_as_text();
            return row.get_nonnull<sstring>(name);
        }));
    });
}

// Call inside a seastar thread
static
std::map<qualified_name, schema_mutations>
read_tables_for_keyspaces(distributed<service::storage_proxy>& proxy, const std::set<sstring>& keyspace_names, schema_ptr s)
{
    std::map<qualified_name, schema_mutations> result;
    for (auto&& keyspace_name : keyspace_names) {
        for (auto&& table_name : read_table_names_of_keyspace(proxy, keyspace_name, s).get0()) {
            auto qn = qualified_name(keyspace_name, table_name);
            result.emplace(qn, read_table_mutations(proxy, qn, s).get0());
        }
    }
    return result;
}

mutation compact_for_schema_digest(const mutation& m) {
    // Cassandra is skipping tombstones from digest calculation
    // to avoid disagreements due to tombstone GC.
    // See https://issues.apache.org/jira/browse/CASSANDRA-6862.
    // We achieve similar effect with compact_for_compaction().
    mutation m_compacted(m);
    m_compacted.partition().compact_for_compaction(*m.schema(), always_gc, gc_clock::time_point::max());
    return m_compacted;
}

// Applies deletion of the "version" column to a system_schema.scylla_tables mutation.
static void delete_schema_version(mutation& m) {
    if (m.column_family_id() != scylla_tables()->id()) {
        return;
    }
    const column_definition& version_col = *scylla_tables()->get_column_definition(to_bytes("version"));
    for (auto&& row : m.partition().clustered_rows()) {
        auto&& cells = row.row().cells();
        auto&& cell = cells.find_cell(version_col.id);
        api::timestamp_type t = api::new_timestamp();
        if (cell) {
            t = std::max(t, cell->as_atomic_cell(version_col).timestamp());
        }
        cells.apply(version_col, atomic_cell::make_dead(t, gc_clock::now()));
    }
}

static future<> do_merge_schema(distributed<service::storage_proxy>& proxy, std::vector<mutation> mutations, bool do_flush)
{
   return seastar::async([&proxy, mutations = std::move(mutations), do_flush] () mutable {
       schema_ptr s = keyspaces();
       // compare before/after schemas of the affected keyspaces only
       std::set<sstring> keyspaces;
       std::set<utils::UUID> column_families;
       for (auto&& mutation : mutations) {
           keyspaces.emplace(value_cast<sstring>(utf8_type->deserialize(mutation.key().get_component(*s, 0))));
           column_families.emplace(mutation.column_family_id());
           // We must force recalculation of schema version after the merge, since the resulting
           // schema may be a mix of the old and new schemas.
           delete_schema_version(mutation);
       }

       // current state of the schema
       auto&& old_keyspaces = read_schema_for_keyspaces(proxy, KEYSPACES, keyspaces).get0();
       auto&& old_column_families = read_tables_for_keyspaces(proxy, keyspaces, tables());
       auto&& old_types = read_schema_for_keyspaces(proxy, TYPES, keyspaces).get0();
       auto&& old_views = read_tables_for_keyspaces(proxy, keyspaces, views());
#if 0 // not in 2.1.8
       /*auto& old_functions = */read_schema_for_keyspaces(proxy, FUNCTIONS, keyspaces).get0();
       /*auto& old_aggregates = */read_schema_for_keyspaces(proxy, AGGREGATES, keyspaces).get0();
#endif

       proxy.local().mutate_locally(std::move(mutations)).get0();

       if (do_flush) {
           proxy.local().get_db().invoke_on_all([s, cfs = std::move(column_families)] (database& db) {
               return parallel_for_each(cfs.begin(), cfs.end(), [&db] (auto& id) {
                   auto& cf = db.find_column_family(id);
                   return cf.flush();
               });
           }).get();
       }

       // with new data applied
       auto&& new_keyspaces = read_schema_for_keyspaces(proxy, KEYSPACES, keyspaces).get0();
       auto&& new_column_families = read_tables_for_keyspaces(proxy, keyspaces, tables());
       auto&& new_types = read_schema_for_keyspaces(proxy, TYPES, keyspaces).get0();
       auto&& new_views = read_tables_for_keyspaces(proxy, keyspaces, views());
#if 0 // not in 2.1.8
       /*auto& new_functions = */read_schema_for_keyspaces(proxy, FUNCTIONS, keyspaces).get0();
       /*auto& new_aggregates = */read_schema_for_keyspaces(proxy, AGGREGATES, keyspaces).get0();
#endif

       std::set<sstring> keyspaces_to_drop = merge_keyspaces(proxy, std::move(old_keyspaces), std::move(new_keyspaces)).get0();
       auto types_to_drop = merge_types(proxy, std::move(old_types), std::move(new_types));
       merge_tables_and_views(proxy,
            std::move(old_column_families), std::move(new_column_families),
            std::move(old_views), std::move(new_views));
#if 0
       mergeFunctions(oldFunctions, newFunctions);
       mergeAggregates(oldAggregates, newAggregates);
#endif
       types_to_drop.drop();

       proxy.local().get_db().invoke_on_all([keyspaces_to_drop = std::move(keyspaces_to_drop)] (database& db) {
           // it is safe to drop a keyspace only when all nested ColumnFamilies where deleted
           return do_for_each(keyspaces_to_drop, [&db] (auto keyspace_to_drop) {
               db.drop_keyspace(keyspace_to_drop);
               return service::get_local_migration_manager().notify_drop_keyspace(keyspace_to_drop);
            });
       }).get0();
   });
}

future<std::set<sstring>> merge_keyspaces(distributed<service::storage_proxy>& proxy, schema_result&& before, schema_result&& after)
{
    std::vector<schema_result_value_type> created;
    std::vector<sstring> altered;
    std::set<sstring> dropped;

    /*
     * - we don't care about entriesOnlyOnLeft() or entriesInCommon(), because only the changes are of interest to us
     * - of all entriesOnlyOnRight(), we only care about ones that have live columns; it's possible to have a ColumnFamily
     *   there that only has the top-level deletion, if:
     *      a) a pushed DROP KEYSPACE change for a keyspace hadn't ever made it to this node in the first place
     *      b) a pulled dropped keyspace that got dropped before it could find a way to this node
     * - of entriesDiffering(), we don't care about the scenario where both pre and post-values have zero live columns:
     *   that means that a keyspace had been recreated and dropped, and the recreated keyspace had never found a way
     *   to this node
     */
    auto diff = difference(before, after, indirect_equal_to<lw_shared_ptr<query::result_set>>());

    for (auto&& key : diff.entries_only_on_left) {
        slogger.info("Dropping keyspace {}", key);
        dropped.emplace(key);
    }
    for (auto&& key : diff.entries_only_on_right) {
        auto&& value = after[key];
        slogger.info("Creating keyspace {}", key);
        created.emplace_back(schema_result_value_type{key, std::move(value)});
    }
    for (auto&& key : diff.entries_differing) {
        slogger.info("Altering keyspace {}", key);
        altered.emplace_back(key);
    }
    return do_with(std::move(created), [&proxy, altered = std::move(altered)] (auto& created) mutable {
        return do_with(std::move(altered), [&proxy, &created](auto& altered) {
            return proxy.local().get_db().invoke_on_all([&created, &altered] (database& db) {
                return do_for_each(created, [&db](auto&& val) {
                    auto ksm = create_keyspace_from_schema_partition(val);
                    return db.create_keyspace(ksm).then([ksm] {
                        return service::get_local_migration_manager().notify_create_keyspace(ksm);
                    });
                }).then([&altered, &db]() {
                    return do_for_each(altered, [&db](auto& name) {
                        return db.update_keyspace(name);
                    });
                });
            });
        });
    }).then([dropped = std::move(dropped)] () {
        return make_ready_future<std::set<sstring>>(dropped);
    });
}

struct schema_diff {
    struct dropped_schema {
        global_schema_ptr schema;
        utils::joinpoint<db_clock::time_point> jp{[] {
            return make_ready_future<db_clock::time_point>(db_clock::now());
        }};
    };

    std::vector<global_schema_ptr> created;
    std::vector<global_schema_ptr> altered;
    std::vector<dropped_schema> dropped;

    size_t size() const {
        return created.size() + altered.size() + dropped.size();
    }
};

template<typename CreateSchema>
static schema_diff diff_table_or_view(distributed<service::storage_proxy>& proxy,
    std::map<qualified_name, schema_mutations>&& before,
    std::map<qualified_name, schema_mutations>&& after,
    CreateSchema&& create_schema)
{
    schema_diff d;
    auto diff = difference(before, after);
    for (auto&& key : diff.entries_only_on_left) {
        auto&& s = proxy.local().get_db().local().find_schema(key.keyspace_name, key.table_name);
        slogger.info("Dropping {}.{} id={} version={}", s->ks_name(), s->cf_name(), s->id(), s->version());
        d.dropped.emplace_back(schema_diff::dropped_schema{s});
    }
    for (auto&& key : diff.entries_only_on_right) {
        auto s = create_schema(std::move(after.at(key)));
        slogger.info("Creating {}.{} id={} version={}", s->ks_name(), s->cf_name(), s->id(), s->version());
        d.created.emplace_back(s);
    }
    for (auto&& key : diff.entries_differing) {
        auto s = create_schema(std::move(after.at(key)));
        slogger.info("Altering {}.{} id={} version={}", s->ks_name(), s->cf_name(), s->id(), s->version());
        d.altered.emplace_back(s);
    }
    return d;
}

// see the comments for merge_keyspaces()
// Atomically publishes schema changes. In particular, this function ensures
// that when a base schema and a subset of its views are modified together (i.e.,
// upon an alter table or alter type statement), then they are published together
// as well, without any deferring in-between.
static void merge_tables_and_views(distributed<service::storage_proxy>& proxy,
    std::map<qualified_name, schema_mutations>&& tables_before,
    std::map<qualified_name, schema_mutations>&& tables_after,
    std::map<qualified_name, schema_mutations>&& views_before,
    std::map<qualified_name, schema_mutations>&& views_after)
{
    auto tables_diff = diff_table_or_view(proxy, std::move(tables_before), std::move(tables_after), [&] (auto&& sm) {
        return create_table_from_mutations(proxy, std::move(sm));
    });
    auto views_diff = diff_table_or_view(proxy, std::move(views_before), std::move(views_after), [&] (auto&& sm) {
        return create_view_from_mutations(proxy, std::move(sm));
    });

    proxy.local().get_db().invoke_on_all([&] (database& db) {
        return seastar::async([&] {
            parallel_for_each(boost::range::join(tables_diff.created, views_diff.created), [&] (global_schema_ptr& gs) {
                return db.add_column_family_and_make_directory(gs);
            }).get();
            for (auto&& gs : boost::range::join(tables_diff.created, views_diff.created)) {
                db.find_column_family(gs).mark_ready_for_writes();
            }
            std::vector<bool> columns_changed;
            columns_changed.reserve(tables_diff.altered.size() + views_diff.altered.size());
            for (auto&& gs : boost::range::join(tables_diff.altered, views_diff.altered)) {
                columns_changed.push_back(db.update_column_family(gs));
            }
            parallel_for_each(boost::range::join(tables_diff.dropped, views_diff.dropped), [&] (schema_diff::dropped_schema& dt) {
                auto& s = *dt.schema.get();
                return db.drop_column_family(s.ks_name(), s.cf_name(), [&] { return dt.jp.value(); });
            }).get();

            auto& mm = service::get_local_migration_manager();
            auto it = columns_changed.begin();
            std::vector<future<>> notifications;
            notifications.reserve(tables_diff.size() + views_diff.size());
            auto notify = [&] (auto& r, auto&& f) { boost::range::transform(r, std::back_inserter(notifications), f); };
            notify(tables_diff.created, [&] (auto&& gs) { return mm.notify_create_column_family(gs); });
            notify(tables_diff.altered, [&] (auto&& gs) { return mm.notify_update_column_family(gs, *it++); });
            notify(tables_diff.dropped, [&] (auto&& dt) { return mm.notify_drop_column_family(dt.schema); });
            notify(views_diff.created, [&] (auto&& gs) { return mm.notify_create_view(view_ptr(gs)); });
            notify(views_diff.altered, [&] (auto&& gs) { return mm.notify_update_view(view_ptr(gs), *it++); });
            notify(views_diff.dropped, [&] (auto&& dt) { return mm.notify_drop_view(view_ptr(dt.schema)); });
            when_all(notifications.rbegin(), notifications.rend()).get();
        });
    }).get();
}

struct naked_user_type {
    const sstring keyspace;
    const sstring qualified_name;
};

static inline void collect_types(std::set<sstring>& keys, schema_result& result, std::vector<naked_user_type>& to)
{
    for (auto&& key : keys) {
        auto&& value = result[key];
        auto types = create_types_from_schema_partition(schema_result_value_type{key, std::move(value)});
        boost::transform(types, std::back_inserter(to), [] (user_type type) {
            return naked_user_type{std::move(type->_keyspace), std::move(type->name())};
        });
    }
}

// see the comments for merge_keyspaces()
[[nodiscard]] static user_types_to_drop merge_types(distributed<service::storage_proxy>& proxy, schema_result&& before, schema_result&& after)
{
    std::vector<naked_user_type> created, altered, dropped;

    auto diff = difference(before, after, indirect_equal_to<lw_shared_ptr<query::result_set>>());

    collect_types(diff.entries_only_on_left, before, dropped); // Keyspaces with no more types
    collect_types(diff.entries_only_on_right, after, created); // New keyspaces with types

    for (auto&& keyspace : diff.entries_differing) {
        // The user types of this keyspace differ, so diff the current types with the updated ones
        auto current_types = proxy.local().get_db().local().find_keyspace(keyspace).metadata()->user_types()->get_all_types();
        decltype(current_types) updated_types;
        auto ts = create_types_from_schema_partition(schema_result_value_type{keyspace, std::move(after[keyspace])});
        updated_types.reserve(ts.size());
        for (auto&& type : ts) {
            updated_types[type->_name] = std::move(type);
        }

        auto delta = difference(current_types, updated_types, indirect_equal_to<user_type>());

        for (auto&& type_name : delta.entries_only_on_left) {
            dropped.emplace_back(naked_user_type{keyspace, current_types[type_name]->name()});
        }
        for (auto&& type_name : delta.entries_only_on_right) {
            created.emplace_back(naked_user_type{keyspace, updated_types[type_name]->name()});
        }
        for (auto&& type_name : delta.entries_differing) {
            altered.emplace_back(naked_user_type{keyspace, updated_types[type_name]->name()});
        }
    }

    // Create and update user types before any tables/views are created that potentially
    // use those types. Similarly, defer dropping until after tables/views that may use
    // some of these user types are dropped.

    proxy.local().get_db().invoke_on_all([&created, &altered] (database& db) {
        return seastar::async([&] {
            for (auto&& type : created) {
                auto user_type = dynamic_pointer_cast<const user_type_impl>(parse_type(type.qualified_name));
                db.find_keyspace(user_type->_keyspace).add_user_type(user_type);
                service::get_local_migration_manager().notify_create_user_type(user_type).get();
            }
            for (auto&& type : altered) {
                auto user_type = dynamic_pointer_cast<const user_type_impl>(parse_type(type.qualified_name));
                db.find_keyspace(user_type->_keyspace).add_user_type(user_type);
                service::get_local_migration_manager().notify_update_user_type(user_type).get();
            }
        });
    }).get();

    return user_types_to_drop{[&proxy, dropped = std::move(dropped)] {
        proxy.local().get_db().invoke_on_all([dropped = std::move(dropped)](database& db) {
            return do_for_each(dropped, [&db](auto& user_type_to_drop) {
                auto user_type = dynamic_pointer_cast<const user_type_impl>(
                        parse_type(std::move(user_type_to_drop.qualified_name)));
                db.find_keyspace(user_type->_keyspace).remove_user_type(user_type);
                return service::get_local_migration_manager().notify_drop_user_type(user_type);
            });
        }).get();
    }};
}

#if 0
    // see the comments for mergeKeyspaces()
    private static void mergeFunctions(Map<DecoratedKey, ColumnFamily> before, Map<DecoratedKey, ColumnFamily> after)
    {
        List<UDFunction> created = new ArrayList<>();
        List<UDFunction> altered = new ArrayList<>();
        List<UDFunction> dropped = new ArrayList<>();

        MapDifference<DecoratedKey, ColumnFamily> diff = Maps.difference(before, after);

        // New keyspace with functions
        for (Map.Entry<DecoratedKey, ColumnFamily> entry : diff.entriesOnlyOnRight().entrySet())
            if (entry.getValue().hasColumns())
                created.addAll(createFunctionsFromFunctionsPartition(new Row(entry.getKey(), entry.getValue())).values());

        for (Map.Entry<DecoratedKey, MapDifference.ValueDifference<ColumnFamily>> entry : diff.entriesDiffering().entrySet())
        {
            ColumnFamily pre = entry.getValue().leftValue();
            ColumnFamily post = entry.getValue().rightValue();

            if (pre.hasColumns() && post.hasColumns())
            {
                MapDifference<ByteBuffer, UDFunction> delta =
                    Maps.difference(createFunctionsFromFunctionsPartition(new Row(entry.getKey(), pre)),
                                    createFunctionsFromFunctionsPartition(new Row(entry.getKey(), post)));

                dropped.addAll(delta.entriesOnlyOnLeft().values());
                created.addAll(delta.entriesOnlyOnRight().values());
                Iterables.addAll(altered, Iterables.transform(delta.entriesDiffering().values(), new Function<MapDifference.ValueDifference<UDFunction>, UDFunction>()
                {
                    public UDFunction apply(MapDifference.ValueDifference<UDFunction> pair)
                    {
                        return pair.rightValue();
                    }
                }));
            }
            else if (pre.hasColumns())
            {
                dropped.addAll(createFunctionsFromFunctionsPartition(new Row(entry.getKey(), pre)).values());
            }
            else if (post.hasColumns())
            {
                created.addAll(createFunctionsFromFunctionsPartition(new Row(entry.getKey(), post)).values());
            }
        }

        for (UDFunction udf : created)
            Schema.instance.addFunction(udf);
        for (UDFunction udf : altered)
            Schema.instance.updateFunction(udf);
        for (UDFunction udf : dropped)
            Schema.instance.dropFunction(udf);
    }

    // see the comments for mergeKeyspaces()
    private static void mergeAggregates(Map<DecoratedKey, ColumnFamily> before, Map<DecoratedKey, ColumnFamily> after)
    {
        List<UDAggregate> created = new ArrayList<>();
        List<UDAggregate> altered = new ArrayList<>();
        List<UDAggregate> dropped = new ArrayList<>();

        MapDifference<DecoratedKey, ColumnFamily> diff = Maps.difference(before, after);

        // New keyspace with functions
        for (Map.Entry<DecoratedKey, ColumnFamily> entry : diff.entriesOnlyOnRight().entrySet())
            if (entry.getValue().hasColumns())
                created.addAll(createAggregatesFromAggregatesPartition(new Row(entry.getKey(), entry.getValue())).values());

        for (Map.Entry<DecoratedKey, MapDifference.ValueDifference<ColumnFamily>> entry : diff.entriesDiffering().entrySet())
        {
            ColumnFamily pre = entry.getValue().leftValue();
            ColumnFamily post = entry.getValue().rightValue();

            if (pre.hasColumns() && post.hasColumns())
            {
                MapDifference<ByteBuffer, UDAggregate> delta =
                    Maps.difference(createAggregatesFromAggregatesPartition(new Row(entry.getKey(), pre)),
                                    createAggregatesFromAggregatesPartition(new Row(entry.getKey(), post)));

                dropped.addAll(delta.entriesOnlyOnLeft().values());
                created.addAll(delta.entriesOnlyOnRight().values());
                Iterables.addAll(altered, Iterables.transform(delta.entriesDiffering().values(), new Function<MapDifference.ValueDifference<UDAggregate>, UDAggregate>()
                {
                    public UDAggregate apply(MapDifference.ValueDifference<UDAggregate> pair)
                    {
                        return pair.rightValue();
                    }
                }));
            }
            else if (pre.hasColumns())
            {
                dropped.addAll(createAggregatesFromAggregatesPartition(new Row(entry.getKey(), pre)).values());
            }
            else if (post.hasColumns())
            {
                created.addAll(createAggregatesFromAggregatesPartition(new Row(entry.getKey(), post)).values());
            }
        }

        for (UDAggregate udf : created)
            Schema.instance.addAggregate(udf);
        for (UDAggregate udf : altered)
            Schema.instance.updateAggregate(udf);
        for (UDAggregate udf : dropped)
            Schema.instance.dropAggregate(udf);
    }
#endif

template<typename... Args>
void set_cell_or_clustered(mutation& m, const clustering_key & ckey, Args && ...args) {
    m.set_clustered_cell(ckey, std::forward<Args>(args)...);
}

template<typename... Args>
void set_cell_or_clustered(mutation& m, const exploded_clustering_prefix & ckey, Args && ...args) {
    m.set_cell(ckey, std::forward<Args>(args)...);
}

template<typename Func, typename Map>
static atomic_cell_or_collection
make_map_mutation(const Map& map,
                  const column_definition& column,
                  api::timestamp_type timestamp,
                  Func&& f)
{
    auto column_type = static_pointer_cast<const map_type_impl>(column.type);
    auto ktyp = column_type->get_keys_type();
    auto vtyp = column_type->get_values_type();

    if (column_type->is_multi_cell()) {
        map_type_impl::mutation mut;

        for (auto&& entry : map) {
            auto te = f(entry);
            mut.cells.emplace_back(ktyp->decompose(data_value(te.first)), atomic_cell::make_live(*vtyp, timestamp, vtyp->decompose(data_value(te.second)), atomic_cell::collection_member::yes));
        }

        auto col_mut = column_type->serialize_mutation_form(std::move(mut));
        return atomic_cell_or_collection::from_collection_mutation(std::move(col_mut));
    } else {
        map_type_impl::native_type tmp;
        tmp.reserve(map.size());
        std::transform(map.begin(), map.end(), std::inserter(tmp, tmp.end()), f);
        return atomic_cell::make_live(*column.type, timestamp, column_type->decompose(make_map_value(column_type, std::move(tmp))));
    }
}

template<typename Map>
static atomic_cell_or_collection
make_map_mutation(const Map& map,
                  const column_definition& column,
                  api::timestamp_type timestamp)
{
    return make_map_mutation(map, column, timestamp, [](auto&& p) {
        return std::make_pair(data_value(p.first), data_value(p.second));
    });
}

template<typename K, typename Map>
static void store_map(mutation& m, const K& ckey, const bytes& name, api::timestamp_type timestamp, const Map& map) {
    auto s = m.schema();
    auto column = s->get_column_definition(name);
    assert(column);
    set_cell_or_clustered(m, ckey, *column, make_map_mutation(map, *column, timestamp));
}

/*
 * Keyspace metadata serialization/deserialization.
 */

std::vector<mutation> make_create_keyspace_mutations(lw_shared_ptr<keyspace_metadata> keyspace, api::timestamp_type timestamp, bool with_tables_and_types_and_functions)
{
    std::vector<mutation> mutations;
    schema_ptr s = keyspaces();
    auto pkey = partition_key::from_singular(*s, keyspace->name());
    mutation m(s, pkey);
    auto ckey = clustering_key_prefix::make_empty();
    m.set_cell(ckey, "durable_writes", keyspace->durable_writes(), timestamp);

    {
        auto map = keyspace->strategy_options();
        map["class"] = keyspace->strategy_name();
        store_map(m, ckey, "replication", timestamp, map);
    }

    mutations.emplace_back(std::move(m));

    if (with_tables_and_types_and_functions) {
        for (auto&& kv : keyspace->user_types()->get_all_types()) {
            add_type_to_schema_mutation(kv.second, timestamp, mutations);
        }
        for (auto&& s : keyspace->cf_meta_data() | boost::adaptors::map_values) {
            add_table_or_view_to_schema_mutation(s, timestamp, true, mutations);
        }
    }
    return mutations;
}

std::vector<mutation> make_drop_keyspace_mutations(lw_shared_ptr<keyspace_metadata> keyspace, api::timestamp_type timestamp)
{
    std::vector<mutation> mutations;
    for (auto&& schema_table : all_tables()) {
        auto pkey = partition_key::from_exploded(*schema_table, {utf8_type->decompose(keyspace->name())});
        mutation m{schema_table, pkey};
        m.partition().apply(tombstone{timestamp, gc_clock::now()});
        mutations.emplace_back(std::move(m));
    }
    auto&& schema = db::system_keyspace::built_indexes();
    auto pkey = partition_key::from_exploded(*schema, {utf8_type->decompose(keyspace->name())});
    mutation m{schema, pkey};
    m.partition().apply(tombstone{timestamp, gc_clock::now()});
    mutations.emplace_back(std::move(m));
    return mutations;
}

/**
 * Deserialize only Keyspace attributes without nested tables or types
 *
 * @param partition Keyspace attributes in serialized form
 */
lw_shared_ptr<keyspace_metadata> create_keyspace_from_schema_partition(const schema_result_value_type& result)
{
    auto&& rs = result.second;
    if (rs->empty()) {
        throw std::runtime_error("query result has no rows");
    }
    auto&& row = rs->row(0);
    auto keyspace_name = row.get_nonnull<sstring>("keyspace_name");
    // We get called from multiple shards with result set originating on only one of them.
    // Cannot use copying accessors for "deep" types like map, because we will hit shared_ptr asserts
    // (or screw up shared pointers)
    const auto& replication = value_cast<map_type_impl::native_type>(row.get_data_value("replication"));

    std::map<sstring, sstring> strategy_options;
    for (auto& p : replication) {
        strategy_options.emplace(value_cast<sstring>(p.first), value_cast<sstring>(p.second));
    }
    auto strategy_name = strategy_options["class"];
    strategy_options.erase("class");
    bool durable_writes = row.get_nonnull<bool>("durable_writes");
    return make_lw_shared<keyspace_metadata>(keyspace_name, strategy_name, strategy_options, durable_writes);
}

template<typename K, typename V>
static std::map<K, V> get_map(const query::result_set_row& row, const sstring& name) {
    std::map<K, V> map;

    auto values = row.get_nonnull<map_type_impl::native_type>(name);
    for (auto&& entry : values) {
        map.emplace(value_cast<K>(entry.first), value_cast<V>(entry.second));
    };

    return map;
}

template<typename V>
static std::vector<V> get_list(const query::result_set_row& row, const sstring& name) {
    std::vector<V> list;

    auto values = row.get_nonnull<list_type_impl::native_type>(name);
    for (auto&& v : values) {
        list.emplace_back(value_cast<V>(v));
    };

    return list;
}

std::vector<user_type> create_types_from_schema_partition(const schema_result_value_type& result)
{
    cql_type_parser::raw_builder builder(result.first);
    for (auto&& row : result.second->rows()) {
        builder.add(row.get_nonnull<sstring>("type_name"),
                        get_list<sstring>(row, "field_names"),
                        get_list<sstring>(row, "field_types"));
    }
    return builder.build();
}

/*
 * User type metadata serialization/deserialization
 */

template<typename Func, typename T, typename... Args>
static atomic_cell_or_collection
make_list_mutation(const std::vector<T, Args...>& values,
                const column_definition& column,
                api::timestamp_type timestamp,
                Func&& f)
{
    auto column_type = static_pointer_cast<const list_type_impl>(column.type);
    auto vtyp = column_type->get_elements_type();

    if (column_type->is_multi_cell()) {
        list_type_impl::mutation m;
        m.cells.reserve(values.size());
        m.tomb.timestamp = timestamp - 1;
        m.tomb.deletion_time = gc_clock::now();

        for (auto&& value : values) {
            auto dv = f(value);
            auto uuid = utils::UUID_gen::get_time_UUID_bytes();
            m.cells.emplace_back(
                bytes(reinterpret_cast<const int8_t*>(uuid.data()), uuid.size()),
                atomic_cell::make_live(*vtyp, timestamp, vtyp->decompose(std::move(dv)), atomic_cell::collection_member::yes));
        }

        auto list_mut = column_type->serialize_mutation_form(std::move(m));
        return atomic_cell_or_collection::from_collection_mutation(std::move(list_mut));
    } else {
        list_type_impl::native_type tmp;
        tmp.reserve(values.size());
        std::transform(values.begin(), values.end(), std::back_inserter(tmp), f);
        return atomic_cell::make_live(*column.type, timestamp, column_type->decompose(make_list_value(column_type, std::move(tmp))));
    }
}

void add_type_to_schema_mutation(user_type type, api::timestamp_type timestamp, std::vector<mutation>& mutations)
{
    schema_ptr s = types();
    auto pkey = partition_key::from_singular(*s, type->_keyspace);
    auto ckey = clustering_key::from_singular(*s, type->get_name_as_string());
    mutation m{s, pkey};

    auto field_names_column = s->get_column_definition("field_names");
    auto field_names = make_list_mutation(type->field_names(), *field_names_column, timestamp, [](auto&& name) {
        return utf8_type->deserialize(name);
    });
    m.set_clustered_cell(ckey, *field_names_column, std::move(field_names));

    auto field_types_column = s->get_column_definition("field_types");
    auto field_types = make_list_mutation(type->field_types(), *field_types_column, timestamp, [](auto&& type) {
        return data_value(type->as_cql3_type()->to_string());
    });
    m.set_clustered_cell(ckey, *field_types_column, std::move(field_types));

    mutations.emplace_back(std::move(m));
}

future<std::vector<mutation>> make_create_type_mutations(lw_shared_ptr<keyspace_metadata> keyspace, user_type type, api::timestamp_type timestamp)
{
    std::vector<mutation> mutations;
    add_type_to_schema_mutation(type, timestamp, mutations);

    // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
    return read_keyspace_mutation(service::get_storage_proxy(), keyspace->name()).then(push_back_and_return{std::move(mutations)});
}

future<std::vector<mutation>> make_drop_type_mutations(lw_shared_ptr<keyspace_metadata> keyspace, user_type type, api::timestamp_type timestamp)
{
    std::vector<mutation> mutations;
    schema_ptr s = types();
    auto pkey = partition_key::from_singular(*s, type->_keyspace);
    auto ckey = clustering_key::from_singular(*s, type->get_name_as_string());
    mutation m{s, pkey};
    m.partition().apply_delete(*s, ckey, tombstone(timestamp, gc_clock::now()));
    mutations.emplace_back(std::move(m));

    // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
    return read_keyspace_mutation(service::get_storage_proxy(), keyspace->name()).then(push_back_and_return{std::move(mutations)});
}

/*
 * Table metadata serialization/deserialization.
 */

future<std::vector<mutation>> make_create_table_mutations(lw_shared_ptr<keyspace_metadata> keyspace, schema_ptr table, api::timestamp_type timestamp)
{
    std::vector<mutation> mutations;
    add_table_or_view_to_schema_mutation(table, timestamp, true, mutations);

    // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
    return read_keyspace_mutation(service::get_storage_proxy(), keyspace->name()).then(push_back_and_return{std::move(mutations)});
}

static void add_table_params_to_mutations(mutation& m, const clustering_key& ckey, schema_ptr table, api::timestamp_type timestamp) {
    m.set_clustered_cell(ckey, "bloom_filter_fp_chance", table->bloom_filter_fp_chance(), timestamp);
    m.set_clustered_cell(ckey, "comment", table->comment(), timestamp);
    m.set_clustered_cell(ckey, "dclocal_read_repair_chance", table->dc_local_read_repair_chance(), timestamp);
    m.set_clustered_cell(ckey, "default_time_to_live", table->default_time_to_live().count(), timestamp);
    m.set_clustered_cell(ckey, "gc_grace_seconds", table->gc_grace_seconds().count(), timestamp);
    m.set_clustered_cell(ckey, "max_index_interval", table->max_index_interval(), timestamp);
    m.set_clustered_cell(ckey, "memtable_flush_period_in_ms", table->memtable_flush_period(), timestamp);
    m.set_clustered_cell(ckey, "min_index_interval", table->min_index_interval(), timestamp);
    m.set_clustered_cell(ckey, "read_repair_chance", table->read_repair_chance(), timestamp);
    m.set_clustered_cell(ckey, "speculative_retry", table->speculative_retry().to_sstring(), timestamp);
    m.set_clustered_cell(ckey, "crc_check_chance", table->crc_check_chance(), timestamp);

    store_map(m, ckey, "caching", timestamp, table->caching_options().to_map());

    {
        auto map = table->compaction_strategy_options();
        map["class"] = sstables::compaction_strategy::name(table->configured_compaction_strategy());
        store_map(m, ckey, "compaction", timestamp, map);
    }

    store_map(m, ckey, "compression", timestamp, table->get_compressor_params().get_options());

    std::map<sstring, bytes> map;

    if (!table->extensions().empty()) {
        for (auto& p : table->extensions()) {
            map.emplace(p.first, p.second->serialize());
        }
    }

    store_map(m, ckey, "extensions", timestamp, map);
}

static data_type expand_user_type(data_type);

static std::vector<data_type> expand_user_types(const std::vector<data_type>& types) {
    std::vector<data_type> result;
    result.reserve(types.size());
    std::transform(types.begin(), types.end(), std::back_inserter(result), &expand_user_type);
    return result;
}

static data_type expand_user_type(data_type original) {
    if (original->is_user_type()) {
        return tuple_type_impl::get_instance(
                        expand_user_types(
                                        static_pointer_cast<const user_type_impl>(
                                                        original)->field_types()));
    }
    if (original->is_tuple()) {
        return tuple_type_impl::get_instance(
                        expand_user_types(
                                        static_pointer_cast<
                                                        const tuple_type_impl>(
                                                        original)->all_types()));
    }
    if (original->is_reversed()) {
        return reversed_type_impl::get_instance(
                        expand_user_type(original->underlying_type()));
    }

    if (original->is_collection()) {

        auto ct = static_pointer_cast<const collection_type_impl>(original);

        if (ct->is_list()) {
            return list_type_impl::get_instance(
                            expand_user_type(ct->value_comparator()),
                            ct->is_multi_cell());
        }
        if (ct->is_map()) {
            return map_type_impl::get_instance(
                            expand_user_type(ct->name_comparator()),
                            expand_user_type(ct->value_comparator()),
                            ct->is_multi_cell());
        }
        if (ct->is_set()) {
            return set_type_impl::get_instance(
                            expand_user_type(ct->name_comparator()),
                            ct->is_multi_cell());
        }
    }

    return original;
}

static void add_dropped_column_to_schema_mutation(schema_ptr table, const sstring& name, const schema::dropped_column& column, api::timestamp_type timestamp, mutation& m) {
    auto ckey = clustering_key::from_exploded(*dropped_columns(), {utf8_type->decompose(table->cf_name()), utf8_type->decompose(name)});
    m.set_clustered_cell(ckey, "dropped_time", column.timestamp, timestamp);

    /*
     * From origin:
     * we never store actual UDT names in dropped column types (so that we can safely drop types if nothing refers to
     * them anymore), so before storing dropped columns in schema we expand UDTs to tuples. See expandUserTypes method.
     * Because of that, we can safely pass Types.none() to parse()
     */
    m.set_clustered_cell(ckey, "type", expand_user_type(column.type)->as_cql3_type()->to_string(), timestamp);
}

mutation make_scylla_tables_mutation(schema_ptr table, api::timestamp_type timestamp) {
    schema_ptr s = tables();
    auto pkey = partition_key::from_singular(*s, table->ks_name());
    auto ckey = clustering_key::from_singular(*s, table->cf_name());
    mutation m(scylla_tables(), pkey);
    m.set_clustered_cell(ckey, "version", utils::UUID(table->version()), timestamp);
    return m;
}

static schema_mutations make_table_mutations(schema_ptr table, api::timestamp_type timestamp, bool with_columns_and_triggers)
{
    // When adding new schema properties, don't set cells for default values so that
    // both old and new nodes will see the same version during rolling upgrades.

    // For property that can be null (and can be changed), we insert tombstones, to make sure
    // we don't keep a property the user has removed
    schema_ptr s = tables();
    auto pkey = partition_key::from_singular(*s, table->ks_name());
    mutation m{s, pkey};
    auto ckey = clustering_key::from_singular(*s, table->cf_name());
    m.set_clustered_cell(ckey, "id", table->id(), timestamp);

    auto scylla_tables_mutation = make_scylla_tables_mutation(table, timestamp);

    {
        list_type_impl::native_type flags;
        if (table->is_super()) {
            flags.emplace_back("super");
        }
        if (table->is_dense()) {
            flags.emplace_back("dense");
        }
        if (table->is_compound()) {
            flags.emplace_back("compound");
        }
        if (table->is_counter()) {
            flags.emplace_back("counter");
        }

        m.set_clustered_cell(ckey, "flags", make_list_value(s->get_column_definition("flags")->type, flags), timestamp);
    }

    add_table_params_to_mutations(m, ckey, table, timestamp);

    mutation columns_mutation(columns(), pkey);
    mutation dropped_columns_mutation(dropped_columns(), pkey);
    mutation indices_mutation(indexes(), pkey);

    if (with_columns_and_triggers) {
        for (auto&& column : table->v3().all_columns()) {
            if (column.is_view_virtual()) {
                throw std::logic_error("view_virtual column found in non-view table");
            }
            add_column_to_schema_mutation(table, column, timestamp, columns_mutation);
        }
        for (auto&& index : table->indices()) {
            add_index_to_schema_mutation(table, index, timestamp, indices_mutation);
        }
        // TODO: triggers

        for (auto&& e : table->dropped_columns()) {
            add_dropped_column_to_schema_mutation(table, e.first, e.second, timestamp, dropped_columns_mutation);
        }
    }

    return schema_mutations{std::move(m), std::move(columns_mutation), stdx::nullopt,
                            std::move(indices_mutation), std::move(dropped_columns_mutation),
                            std::move(scylla_tables_mutation)};
}

void add_table_or_view_to_schema_mutation(schema_ptr s, api::timestamp_type timestamp, bool with_columns, std::vector<mutation>& mutations)
{
    make_schema_mutations(s, timestamp, with_columns).copy_to(mutations);
}

static schema_mutations make_view_mutations(view_ptr view, api::timestamp_type timestamp, bool with_columns);
static void make_drop_table_or_view_mutations(schema_ptr schema_table, schema_ptr table_or_view, api::timestamp_type timestamp, std::vector<mutation>& mutations);

static void make_update_indices_mutations(
        schema_ptr old_table,
        schema_ptr new_table,
        api::timestamp_type timestamp,
        std::vector<mutation>& mutations)
{
    mutation indices_mutation(indexes(), partition_key::from_singular(*indexes(), old_table->ks_name()));

    auto diff = difference(old_table->all_indices(), new_table->all_indices());

    // indices that are no longer needed
    for (auto&& name : diff.entries_only_on_left) {
        const index_metadata& index = old_table->all_indices().at(name);
        drop_index_from_schema_mutation(old_table, index, timestamp, mutations);
        auto& cf = service::get_storage_proxy().local().get_db().local().find_column_family(old_table);
        auto view = cf.get_index_manager().create_view_for_index(index);
        make_drop_table_or_view_mutations(views(), view, timestamp, mutations);
    }

    // newly added indices and old indices with updated attributes
    for (auto&& name : boost::range::join(diff.entries_differing, diff.entries_only_on_right)) {
        const index_metadata& index = new_table->all_indices().at(name);
        add_index_to_schema_mutation(new_table, index, timestamp, indices_mutation);
        auto& cf = service::get_storage_proxy().local().get_db().local().find_column_family(new_table);
        auto view = cf.get_index_manager().create_view_for_index(index);
        auto view_mutations = make_view_mutations(view, timestamp, true);
        view_mutations.copy_to(mutations);
    }

    mutations.emplace_back(std::move(indices_mutation));
}

static void add_drop_column_to_mutations(schema_ptr table, const sstring& name, const schema::dropped_column& dc, api::timestamp_type timestamp, std::vector<mutation>& mutations) {
    schema_ptr s = dropped_columns();
    auto pkey = partition_key::from_singular(*s, table->ks_name());
    auto ckey = clustering_key::from_exploded(*s, {utf8_type->decompose(table->cf_name()), utf8_type->decompose(name)});
    mutation m(s, pkey);
    add_dropped_column_to_schema_mutation(table, name, dc, timestamp, m);
    mutations.emplace_back(std::move(m));
}

static void make_update_columns_mutations(schema_ptr old_table,
        schema_ptr new_table,
        api::timestamp_type timestamp,
        bool from_thrift,
        std::vector<mutation>& mutations) {
    mutation columns_mutation(columns(), partition_key::from_singular(*columns(), old_table->ks_name()));
    mutation view_virtual_columns_mutation(view_virtual_columns(), partition_key::from_singular(*columns(), old_table->ks_name()));

    auto diff = difference(old_table->v3().columns_by_name(), new_table->v3().columns_by_name());

    // columns that are no longer needed
    for (auto&& name : diff.entries_only_on_left) {
        // Thrift only knows about the REGULAR ColumnDefinition type, so don't consider other type
        // are being deleted just because they are not here.
        const column_definition& column = *old_table->v3().columns_by_name().at(name);
        if (from_thrift && !column.is_regular()) {
            continue;
        }
        if (column.is_view_virtual()) {
            drop_column_from_schema_mutation(view_virtual_columns(), old_table, column.name_as_text(), timestamp, mutations);
        } else {
            drop_column_from_schema_mutation(columns(), old_table, column.name_as_text(), timestamp, mutations);
        }
    }

    // newly added columns and old columns with updated attributes
    for (auto&& name : boost::range::join(diff.entries_differing, diff.entries_only_on_right)) {
        const column_definition& column = *new_table->v3().columns_by_name().at(name);
        if (column.is_view_virtual()) {
            add_column_to_schema_mutation(new_table, column, timestamp, view_virtual_columns_mutation);
        } else {
            add_column_to_schema_mutation(new_table, column, timestamp, columns_mutation);
        }
    }

    mutations.emplace_back(std::move(columns_mutation));
    mutations.emplace_back(std::move(view_virtual_columns_mutation));

    // dropped columns
    auto dc_diff = difference(old_table->dropped_columns(), new_table->dropped_columns());

    // newly dropped columns
    // columns added then dropped again
    for (auto& name : boost::range::join(dc_diff.entries_differing, dc_diff.entries_only_on_right)) {
        add_drop_column_to_mutations(new_table, name, new_table->dropped_columns().at(name), timestamp, mutations);
    }
}

future<std::vector<mutation>> make_update_table_mutations(lw_shared_ptr<keyspace_metadata> keyspace,
    schema_ptr old_table,
    schema_ptr new_table,
    api::timestamp_type timestamp,
    bool from_thrift)
{
    std::vector<mutation> mutations;
    add_table_or_view_to_schema_mutation(new_table, timestamp, false, mutations);
    make_update_indices_mutations(old_table, new_table, timestamp, mutations);
    make_update_columns_mutations(std::move(old_table), std::move(new_table), timestamp, from_thrift, mutations);

    warn(unimplemented::cause::TRIGGERS);
#if 0
        MapDifference<String, TriggerDefinition> triggerDiff = Maps.difference(oldTable.getTriggers(), newTable.getTriggers());

        // dropped triggers
        for (TriggerDefinition trigger : triggerDiff.entriesOnlyOnLeft().values())
            dropTriggerFromSchemaMutation(oldTable, trigger, timestamp, mutation);

        // newly created triggers
        for (TriggerDefinition trigger : triggerDiff.entriesOnlyOnRight().values())
            addTriggerToSchemaMutation(newTable, trigger, timestamp, mutation);

#endif
    // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
    return read_keyspace_mutation(service::get_storage_proxy(), keyspace->name()).then(push_back_and_return{std::move(mutations)});
}

static void make_drop_table_or_view_mutations(schema_ptr schema_table,
            schema_ptr table_or_view,
            api::timestamp_type timestamp,
            std::vector<mutation>& mutations) {
    auto pkey = partition_key::from_singular(*schema_table, table_or_view->ks_name());
    mutation m{schema_table, pkey};
    auto ckey = clustering_key::from_singular(*schema_table, table_or_view->cf_name());
    m.partition().apply_delete(*schema_table, ckey, tombstone(timestamp, gc_clock::now()));
    mutations.emplace_back(m);
    for (auto& column : table_or_view->v3().all_columns()) {
        if (column.is_view_virtual()) {
            drop_column_from_schema_mutation(view_virtual_columns(), table_or_view, column.name_as_text(), timestamp, mutations);
        } else {
            drop_column_from_schema_mutation(columns(), table_or_view, column.name_as_text(), timestamp, mutations);
        }
    }
    for (auto& column : table_or_view->dropped_columns() | boost::adaptors::map_keys) {
        drop_column_from_schema_mutation(dropped_columns(), table_or_view, column, timestamp, mutations);
    }
    {
        mutation m{scylla_tables(), pkey};
        m.partition().apply_delete(*scylla_tables(), ckey, tombstone(timestamp, gc_clock::now()));
        mutations.emplace_back(m);
    }
}

future<std::vector<mutation>> make_drop_table_mutations(lw_shared_ptr<keyspace_metadata> keyspace, schema_ptr table, api::timestamp_type timestamp)
{
    std::vector<mutation> mutations;
    make_drop_table_or_view_mutations(tables(), std::move(table), timestamp, mutations);

#if 0
    for (TriggerDefinition trigger : table.getTriggers().values())
        dropTriggerFromSchemaMutation(table, trigger, timestamp, mutation);

    // TODO: get rid of in #6717
    ColumnFamily indexCells = mutation.addOrGet(SystemKeyspace.BuiltIndexes);
    for (String indexName : Keyspace.open(keyspace.name).getColumnFamilyStore(table.cfName).getBuiltIndexes())
        indexCells.addTombstone(indexCells.getComparator().makeCellName(indexName), ldt, timestamp);
#endif
    // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
    return read_keyspace_mutation(service::get_storage_proxy(), keyspace->name()).then(push_back_and_return{std::move(mutations)});
}

static future<schema_mutations> read_table_mutations(distributed<service::storage_proxy>& proxy, const qualified_name& table, schema_ptr s)
{
    return when_all_succeed(
        read_schema_partition_for_table(proxy, s, table.keyspace_name, table.table_name),
        read_schema_partition_for_table(proxy, columns(), table.keyspace_name, table.table_name),
        read_schema_partition_for_table(proxy, view_virtual_columns(), table.keyspace_name, table.table_name),
        read_schema_partition_for_table(proxy, dropped_columns(), table.keyspace_name, table.table_name),
        read_schema_partition_for_table(proxy, indexes(), table.keyspace_name, table.table_name),
        read_schema_partition_for_table(proxy, scylla_tables(), table.keyspace_name, table.table_name)).then(
            [] (mutation cf_m, mutation col_m, mutation vv_col_m, mutation dropped_m, mutation idx_m, mutation st_m) {
                return schema_mutations{std::move(cf_m), std::move(col_m), std::move(vv_col_m), std::move(idx_m), std::move(dropped_m), std::move(st_m)};
            });
#if 0
        // FIXME:
    Row serializedTriggers = readSchemaPartitionForTable(TRIGGERS, ksName, cfName);
    try
    {
        for (TriggerDefinition trigger : createTriggersFromTriggersPartition(serializedTriggers))
            cfm.addTriggerDefinition(trigger);
    }
    catch (InvalidRequestException e)
    {
        throw new RuntimeException(e);
    }
#endif
}

future<schema_ptr> create_table_from_name(distributed<service::storage_proxy>& proxy, const sstring& keyspace, const sstring& table)
{
    return do_with(qualified_name(keyspace, table), [&proxy] (auto&& qn) {
        return read_table_mutations(proxy, qn, tables()).then([qn, &proxy] (schema_mutations sm) {
            if (!sm.live()) {
               throw std::runtime_error(sprint("%s:%s not found in the schema definitions keyspace.", qn.keyspace_name, qn.table_name));
            }
            return create_table_from_mutations(proxy, std::move(sm));
        });
    });
}

/**
 * Deserialize tables from low-level schema representation, all of them belong to the same keyspace
 *
 * @return map containing name of the table and its metadata for faster lookup
 */
future<std::map<sstring, schema_ptr>> create_tables_from_tables_partition(distributed<service::storage_proxy>& proxy, const schema_result::mapped_type& result)
{
    auto tables = make_lw_shared<std::map<sstring, schema_ptr>>();
    return parallel_for_each(result->rows().begin(), result->rows().end(), [&proxy, tables] (auto&& row) {
        return create_table_from_table_row(proxy, row).then([tables] (schema_ptr&& cfm) {
            tables->emplace(cfm->cf_name(), std::move(cfm));
        });
    }).then([tables] {
        return std::move(*tables);
    });
}

#if 0
    public static CFMetaData createTableFromTablePartitionAndColumnsPartition(Row serializedTable, Row serializedColumns)
    {
        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, COLUMNFAMILIES);
        return createTableFromTableRowAndColumnsPartition(QueryProcessor.resultify(query, serializedTable).one(), serializedColumns);
    }
#endif

/**
 * Deserialize table metadata from low-level representation
 *
 * @return Metadata deserialized from schema
 */
static future<schema_ptr> create_table_from_table_row(distributed<service::storage_proxy>& proxy, const query::result_set_row& row)
{
    auto ks_name = row.get_nonnull<sstring>("keyspace_name");
    auto cf_name = row.get_nonnull<sstring>("table_name");
    return create_table_from_name(proxy, ks_name, cf_name);
}

static void prepare_builder_from_table_row(const schema_ctxt& ctxt, schema_builder& builder, const query::result_set_row& table_row)
{
    // These row reads have been purposefully reordered to match the origin counterpart. For easier matching.
    if (table_row.has("bloom_filter_fp_chance")) {
        builder.set_bloom_filter_fp_chance(table_row.get_nonnull<double>("bloom_filter_fp_chance"));
    } else {
        builder.set_bloom_filter_fp_chance(builder.get_bloom_filter_fp_chance());
    }

    if (table_row.has("caching")) {
        auto map = get_map<sstring, sstring>(table_row, "caching");
        builder.set_caching_options(caching_options::from_map(map));
    }

    if (table_row.has("comment")) {
        builder.set_comment(table_row.get_nonnull<sstring>("comment"));
    }

    if (table_row.has("compaction")) {
        auto map = get_map<sstring, sstring>(table_row, "compaction");

        auto i = map.find("class");
        if (i != map.end()) {
            try {
                builder.set_compaction_strategy(sstables::compaction_strategy::type(i->second));
                map.erase(i);
            } catch (const exceptions::configuration_exception& e) {
                // If compaction strategy class isn't supported, fallback to size tiered.
                slogger.warn("Falling back to size-tiered compaction strategy after the problem: {}", e.what());
                builder.set_compaction_strategy(sstables::compaction_strategy_type::size_tiered);
            }
        }
        if (map.count("max_threshold")) {
            builder.set_max_compaction_threshold(std::stoi(map["max_threshold"]));
        }
        if (map.count("min_threshold")) {
            builder.set_min_compaction_threshold(std::stoi(map["min_threshold"]));
        }
        if (map.count("enabled")) {
            builder.set_compaction_enabled(boost::algorithm::iequals(map["enabled"], "true"));
        }

        builder.set_compaction_strategy_options(map);
    }

    if (table_row.has("compression")) {
        auto map = get_map<sstring, sstring>(table_row, "compression");
        compression_parameters cp(map);
        builder.set_compressor_params(cp);
    }

    if (table_row.has("dclocal_read_repair_chance")) {
        builder.set_dc_local_read_repair_chance(table_row.get_nonnull<double>("dclocal_read_repair_chance"));
    }

    if (table_row.has("default_time_to_live")) {
        builder.set_default_time_to_live(gc_clock::duration(table_row.get_nonnull<gc_clock::rep>("default_time_to_live")));
    }

    if (table_row.has("extensions")) {
        auto map = get_map<sstring, bytes>(table_row, "extensions");
        schema::extensions_map result;
        auto& exts = ctxt.extensions().schema_extensions();
        for (auto&p : map) {
            auto i = exts.find(p.first);
            if (i != exts.end()) {
                try {
                    result.emplace(p.first, i->second(p.second));
                    continue;
                } catch (...) {
                    slogger.warn("Error parsing extension {}: {}", p.first, std::current_exception());
                }
            }

            // unknown. we should still preserve it.
            class placeholder : public schema_extension {
                bytes _bytes;
            public:
                placeholder(bytes bytes)
                                : _bytes(std::move(bytes)) {
                }
                bytes serialize() const override {
                    return _bytes;
                }
                bool is_placeholder() const {
                    return true;
                }
            };

            result.emplace(p.first, ::make_shared<placeholder>(p.second));
        }
        builder.set_extensions(std::move(result));
    }

    if (table_row.has("gc_grace_seconds")) {
        builder.set_gc_grace_seconds(table_row.get_nonnull<int32_t>("gc_grace_seconds"));
    }

    if (table_row.has("min_index_interval")) {
        builder.set_min_index_interval(table_row.get_nonnull<int>("min_index_interval"));
    }

    if (table_row.has("memtable_flush_period_in_ms")) {
        builder.set_memtable_flush_period(table_row.get_nonnull<int32_t>("memtable_flush_period_in_ms"));
    }

    if (table_row.has("max_index_interval")) {
        builder.set_max_index_interval(table_row.get_nonnull<int>("max_index_interval"));
    }

    if (table_row.has("read_repair_chance")) {
        builder.set_read_repair_chance(table_row.get_nonnull<double>("read_repair_chance"));
    }

    if (table_row.has("crc_check_chance")) {
        builder.set_crc_check_chance(table_row.get_nonnull<double>("crc_check_chance"));
    }

    if (table_row.has("speculative_retry")) {
        builder.set_speculative_retry(table_row.get_nonnull<sstring>("speculative_retry"));
    }
}

schema_ptr create_table_from_mutations(const schema_ctxt& ctxt, schema_mutations sm, std::experimental::optional<table_schema_version> version)
{
    auto table_rs = query::result_set(sm.columnfamilies_mutation());
    query::result_set_row table_row = table_rs.row(0);

    auto ks_name = table_row.get_nonnull<sstring>("keyspace_name");
    auto cf_name = table_row.get_nonnull<sstring>("table_name");
    auto id = table_row.get_nonnull<utils::UUID>("id");
    schema_builder builder{ks_name, cf_name, id};

    auto cf = cf_type::standard;
    auto is_dense = false;
    auto is_counter = false;
    auto is_compound = false;
    auto flags = table_row.get<set_type_impl::native_type>("flags");

    if (flags) {
        for (auto& s : *flags) {
            if (s == "super") {
                // cf = cf_type::super;
                fail(unimplemented::cause::SUPER);
            } else if (s == "dense") {
                is_dense = true;
            } else if (s == "compound") {
                is_compound = true;
            } else if (s == "counter") {
                is_counter = true;
            }
        }
    }

    std::vector<column_definition> column_defs = create_columns_from_column_rows(
            query::result_set(sm.columns_mutation()),
            ks_name,
            cf_name,/*,
            fullRawComparator, */
            cf == cf_type::super,
            column_view_virtual::no);


    builder.set_is_dense(is_dense);
    builder.set_is_compound(is_compound);
    builder.set_is_counter(is_counter);

    prepare_builder_from_table_row(ctxt, builder, table_row);

    v3_columns columns(std::move(column_defs), is_dense, is_compound);
    columns.apply_to(builder);

    std::vector<index_metadata> index_defs;
    if (sm.indices_mutation()) {
        index_defs = create_indices_from_index_rows(query::result_set(*sm.indices_mutation()), ks_name, cf_name);
    }
    for (auto&& index : index_defs) {
        builder.with_index(index);
    }

    if (sm.dropped_columns_mutation()) {
        query::result_set dcr(*sm.dropped_columns_mutation());
        for (auto& row : dcr.rows()) {
            auto name = row.get_nonnull<sstring>("column_name");
            auto type = cql_type_parser::parse(ks_name, row.get_nonnull<sstring>("type"));
            auto time = row.get_nonnull<db_clock::time_point>("dropped_time");
            builder.without_column(name, type, time.time_since_epoch().count());
        }
    }

    if (version) {
        builder.with_version(*version);
    } else {
        builder.with_version(sm.digest());
    }
    return builder.build();
}

/*
 * Column metadata serialization/deserialization.
 */

static void add_column_to_schema_mutation(schema_ptr table,
                                   const column_definition& column,
                                   api::timestamp_type timestamp,
                                   mutation& m)
{
    auto ckey = clustering_key::from_exploded(*m.schema(), {utf8_type->decompose(table->cf_name()),
                                                            utf8_type->decompose(column.name_as_text())});

    auto order = "NONE";
    if (column.is_clustering_key()) {
        order = "ASC";
    }
    auto type = column.type;
    if (type->is_reversed()) {
        type = type->underlying_type();
        if (column.is_clustering_key()) {
            order = "DESC";
        }
    }
    auto pos = -1;
    if (column.is_primary_key()) {
        pos = int32_t(table->position(column));
    }

    m.set_clustered_cell(ckey, "column_name_bytes", data_value(column.name()), timestamp);
    m.set_clustered_cell(ckey, "kind", serialize_kind(column.kind), timestamp);
    m.set_clustered_cell(ckey, "position", pos, timestamp);
    m.set_clustered_cell(ckey, "clustering_order", sstring(order), timestamp);
    m.set_clustered_cell(ckey, "type", type->as_cql3_type()->to_string(), timestamp);
}

sstring serialize_kind(column_kind kind)
{
    switch (kind) {
    case column_kind::partition_key:  return "partition_key";
    case column_kind::clustering_key: return "clustering";
    case column_kind::static_column:  return "static";
    case column_kind::regular_column: return "regular";
    default:                          throw std::invalid_argument("unknown column kind");
    }
}

column_kind deserialize_kind(sstring kind) {
    if (kind == "partition_key") {
        return column_kind::partition_key;
    } else if (kind == "clustering_key" || kind == "clustering") {
        return column_kind::clustering_key;
    } else if (kind == "static") {
        return column_kind::static_column;
    } else if (kind == "regular") {
        return column_kind::regular_column;
    } else if (kind == "compact_value") { // backward compatibility
        return column_kind::regular_column;
    } else {
        throw std::invalid_argument("unknown column kind: " + kind);
    }
}

sstring serialize_index_kind(index_metadata_kind kind)
{
    switch (kind) {
    case index_metadata_kind::keys:       return "KEYS";
    case index_metadata_kind::composites: return "COMPOSITES";
    case index_metadata_kind::custom:     return "CUSTOM";
    }
    throw std::invalid_argument("unknown index kind");
}

index_metadata_kind deserialize_index_kind(sstring kind) {
    if (kind == "KEYS") {
        return index_metadata_kind::keys;
    } else if (kind == "COMPOSITES") {
        return index_metadata_kind::composites;
    } else if (kind == "CUSTOM") {
        return index_metadata_kind::custom;
    } else {
        throw std::invalid_argument("unknown column kind: " + kind);
    }
}

static void add_index_to_schema_mutation(schema_ptr table,
                                  const index_metadata& index,
                                  api::timestamp_type timestamp,
                                  mutation& m)
{
    auto ckey = clustering_key::from_exploded(*m.schema(), {utf8_type->decompose(table->cf_name()), utf8_type->decompose(index.name())});
    m.set_clustered_cell(ckey, "kind", serialize_index_kind(index.kind()), timestamp);
    store_map(m, ckey, "options", timestamp, index.options());
}

static void drop_index_from_schema_mutation(schema_ptr table, const index_metadata& index, long timestamp, std::vector<mutation>& mutations)
{
    schema_ptr s = indexes();
    auto pkey = partition_key::from_singular(*s, table->ks_name());
    auto ckey = clustering_key::from_exploded(*s, {utf8_type->decompose(table->cf_name()), utf8_type->decompose(index.name())});
    mutation m{s, pkey};
    m.partition().apply_delete(*s, ckey, tombstone(timestamp, gc_clock::now()));
    mutations.push_back(std::move(m));
}

static void drop_column_from_schema_mutation(
        schema_ptr schema_table,
        schema_ptr table,
        const sstring& column_name,
        long timestamp,
        std::vector<mutation>& mutations)
{
    auto pkey = partition_key::from_singular(*schema_table, table->ks_name());
    auto ckey = clustering_key::from_exploded(*schema_table, {utf8_type->decompose(table->cf_name()),
                                                              utf8_type->decompose(column_name)});

    mutation m{schema_table, pkey};
    m.partition().apply_delete(*schema_table, ckey, tombstone(timestamp, gc_clock::now()));
    mutations.emplace_back(m);
}

static std::vector<column_definition> create_columns_from_column_rows(const query::result_set& rows,
                                                               const sstring& keyspace,
                                                               const sstring& table, /*,
                                                               AbstractType<?> rawComparator, */
                                                               bool is_super,
                                                               column_view_virtual is_view_virtual)
{
    std::vector<column_definition> columns;
    for (auto&& row : rows.rows()) {
        auto kind = deserialize_kind(row.get_nonnull<sstring>("kind"));
        auto type = cql_type_parser::parse(keyspace, row.get_nonnull<sstring>("type"));
        auto name_bytes = row.get_nonnull<bytes>("column_name_bytes");
        column_id position = row.get_nonnull<int32_t>("position");

        if (row.has("clustering_order")) {
            auto order = row.get_nonnull<sstring>("clustering_order");
            std::transform(order.begin(), order.end(), order.begin(), ::toupper);
            if (order == "DESC") {
                type = reversed_type_impl::get_instance(type);
            }
        }

        columns.emplace_back(name_bytes, type, kind, position, is_view_virtual);
    }
    return columns;
}

static std::vector<index_metadata> create_indices_from_index_rows(const query::result_set& rows,
                                                           const sstring& keyspace,
                                                           const sstring& table)
{
    return boost::copy_range<std::vector<index_metadata>>(rows.rows() | boost::adaptors::transformed([&keyspace, &table] (auto&& row) {
        return create_index_from_index_row(row, keyspace, table);
    }));
}

static index_metadata create_index_from_index_row(const query::result_set_row& row,
                                           sstring keyspace,
                                           sstring table)
{
    auto index_name = row.get_nonnull<sstring>("index_name");
    index_options_map options;
    auto map = row.get_nonnull<map_type_impl::native_type>("options");
    for (auto&& entry : map) {
        options.emplace(value_cast<sstring>(entry.first), value_cast<sstring>(entry.second));
    }
    index_metadata_kind kind = deserialize_index_kind(row.get_nonnull<sstring>("kind"));
    return index_metadata{index_name, options, kind};
}

/*
 * View metadata serialization/deserialization.
 */

view_ptr create_view_from_mutations(const schema_ctxt& ctxt, schema_mutations sm, std::experimental::optional<table_schema_version> version)  {
    auto table_rs = query::result_set(sm.columnfamilies_mutation());
    query::result_set_row row = table_rs.row(0);

    auto ks_name = row.get_nonnull<sstring>("keyspace_name");
    auto cf_name = row.get_nonnull<sstring>("view_name");
    auto id = row.get_nonnull<utils::UUID>("id");

    schema_builder builder{ks_name, cf_name, id};
    prepare_builder_from_table_row(ctxt, builder, row);

    auto column_defs = create_columns_from_column_rows(query::result_set(sm.columns_mutation()), ks_name, cf_name, false, column_view_virtual::no);
    for (auto&& cdef : column_defs) {
        builder.with_column(cdef);
    }
    if (sm.view_virtual_columns_mutation()) {
        column_defs = create_columns_from_column_rows(query::result_set(*sm.view_virtual_columns_mutation()), ks_name, cf_name, false, column_view_virtual::yes);
        for (auto&& cdef : column_defs) {
            builder.with_column(cdef);
        }
    }

    if (version) {
        builder.with_version(*version);
    } else {
        builder.with_version(sm.digest());
    }

    auto base_id = row.get_nonnull<utils::UUID>("base_table_id");
    auto base_name = row.get_nonnull<sstring>("base_table_name");
    auto include_all_columns = row.get_nonnull<bool>("include_all_columns");
    auto where_clause = row.get_nonnull<sstring>("where_clause");

    builder.with_view_info(std::move(base_id), std::move(base_name), include_all_columns, std::move(where_clause));
    return view_ptr(builder.build());
}

static future<view_ptr> create_view_from_table_row(distributed<service::storage_proxy>& proxy, const query::result_set_row& row) {
    qualified_name qn(row.get_nonnull<sstring>("keyspace_name"), row.get_nonnull<sstring>("view_name"));
    return do_with(std::move(qn), [&proxy] (auto&& qn) {
        return read_table_mutations(proxy, qn, views()).then([&] (schema_mutations sm) {
            if (!sm.live()) {
                throw std::runtime_error(sprint("%s:%s not found in the view definitions keyspace.", qn.keyspace_name, qn.table_name));
            }
            return create_view_from_mutations(proxy, std::move(sm));
        });
    });
}

/**
 * Deserialize views from low-level schema representation, all of them belong to the same keyspace
 *
 * @return vector containing the view definitions
 */
future<std::vector<view_ptr>> create_views_from_schema_partition(distributed<service::storage_proxy>& proxy, const schema_result::mapped_type& result)
{
    return do_with(std::vector<view_ptr>(), [&] (auto& views) {
        return parallel_for_each(result->rows().begin(), result->rows().end(), [&proxy, &views] (auto&& row) {
            return create_view_from_table_row(proxy, row).then([&views] (auto&& v) {
                views.push_back(std::move(v));
            });
        }).then([&views] {
            return std::move(views);
        });
    });
}

static schema_mutations make_view_mutations(view_ptr view, api::timestamp_type timestamp, bool with_columns)
{
    // When adding new schema properties, don't set cells for default values so that
    // both old and new nodes will see the same version during rolling upgrades.

    // For properties that can be null (and can be changed), we insert tombstones, to make sure
    // we don't keep a property the user has removed
    schema_ptr s = views();
    auto pkey = partition_key::from_singular(*s, view->ks_name());
    mutation m{s, pkey};
    auto ckey = clustering_key::from_singular(*s, view->cf_name());

    m.set_clustered_cell(ckey, "base_table_id", view->view_info()->base_id(), timestamp);
    m.set_clustered_cell(ckey, "base_table_name", view->view_info()->base_name(), timestamp);
    m.set_clustered_cell(ckey, "where_clause", view->view_info()->where_clause(), timestamp);
    m.set_clustered_cell(ckey, "bloom_filter_fp_chance", view->bloom_filter_fp_chance(), timestamp);
    m.set_clustered_cell(ckey, "include_all_columns", view->view_info()->include_all_columns(), timestamp);
    m.set_clustered_cell(ckey, "id", view->id(), timestamp);

    add_table_params_to_mutations(m, ckey, view, timestamp);


    mutation columns_mutation(columns(), pkey);
    mutation view_virtual_columns_mutation(view_virtual_columns(), pkey);
    mutation dropped_columns_mutation(dropped_columns(), pkey);
    mutation indices_mutation(indexes(), pkey);

    if (with_columns) {
        for (auto&& column : view->v3().all_columns()) {
            if (column.is_view_virtual()) {
                add_column_to_schema_mutation(view, column, timestamp, view_virtual_columns_mutation);
            } else {
                add_column_to_schema_mutation(view, column, timestamp, columns_mutation);
            }
        }

        for (auto&& e : view->dropped_columns()) {
            add_dropped_column_to_schema_mutation(view, e.first, e.second, timestamp, dropped_columns_mutation);
        }
        for (auto&& index : view->indices()) {
            add_index_to_schema_mutation(view, index, timestamp, indices_mutation);
        }
    }

    auto scylla_tables_mutation = make_scylla_tables_mutation(view, timestamp);

    return schema_mutations{std::move(m), std::move(columns_mutation), std::move(view_virtual_columns_mutation),
                            std::move(indices_mutation), std::move(dropped_columns_mutation),
                            std::move(scylla_tables_mutation)};
}

schema_mutations make_schema_mutations(schema_ptr s, api::timestamp_type timestamp, bool with_columns)
{
    return s->is_view() ? make_view_mutations(view_ptr(s), timestamp, with_columns) : make_table_mutations(s, timestamp, with_columns);
}

future<std::vector<mutation>> make_create_view_mutations(lw_shared_ptr<keyspace_metadata> keyspace, view_ptr view, api::timestamp_type timestamp)
{
    std::vector<mutation> mutations;
    // And also the serialized base table.
    auto base = keyspace->cf_meta_data().at(view->view_info()->base_name());
    add_table_or_view_to_schema_mutation(base, timestamp, true, mutations);
    add_table_or_view_to_schema_mutation(view, timestamp, true, mutations);

    // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
    return read_keyspace_mutation(service::get_storage_proxy(), keyspace->name()).then(push_back_and_return{std::move(mutations)});
}

/**
 * Note: new_view can be generated due to an ALTER on its base table; in that
 * case, the new base schema isn't yet loaded, thus can't be accessed from this
 * function.
 */
future<std::vector<mutation>> make_update_view_mutations(lw_shared_ptr<keyspace_metadata> keyspace,
                                                 view_ptr old_view,
                                                 view_ptr new_view,
                                                 api::timestamp_type timestamp,
                                                 bool include_base)
{
    std::vector<mutation> mutations;
    if (include_base) {
        // Include the serialized base table mutations in case the target node is missing them.
        auto base = keyspace->cf_meta_data().at(new_view->view_info()->base_name());
        add_table_or_view_to_schema_mutation(base, timestamp, true, mutations);
    }
    add_table_or_view_to_schema_mutation(new_view, timestamp, false, mutations);
    make_update_columns_mutations(old_view, new_view, timestamp, false, mutations);

    // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
    return read_keyspace_mutation(service::get_storage_proxy(), keyspace->name()).then(push_back_and_return{std::move(mutations)});
}

future<std::vector<mutation>> make_drop_view_mutations(lw_shared_ptr<keyspace_metadata> keyspace, view_ptr view, api::timestamp_type timestamp) {
    std::vector<mutation> mutations;
    make_drop_table_or_view_mutations(views(), view, timestamp, mutations);
    // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
    return read_keyspace_mutation(service::get_storage_proxy(), keyspace->name()).then(push_back_and_return{std::move(mutations)});
}

#if 0
    private static AbstractType<?> getComponentComparator(AbstractType<?> rawComparator, Integer componentIndex)
    {
        return (componentIndex == null || (componentIndex == 0 && !(rawComparator instanceof CompositeType)))
               ? rawComparator
               : ((CompositeType)rawComparator).types.get(componentIndex);
    }

    /*
     * Trigger metadata serialization/deserialization.
     */

    private static void addTriggerToSchemaMutation(CFMetaData table, TriggerDefinition trigger, long timestamp, Mutation mutation)
    {
        ColumnFamily cells = mutation.addOrGet(Triggers);
        Composite prefix = Triggers.comparator.make(table.cfName, trigger.name);
        CFRowAdder adder = new CFRowAdder(cells, prefix, timestamp);
        adder.addMapEntry("trigger_options", "class", trigger.classOption);
    }

    private static void dropTriggerFromSchemaMutation(CFMetaData table, TriggerDefinition trigger, long timestamp, Mutation mutation)
    {
        ColumnFamily cells = mutation.addOrGet(Triggers);
        int ldt = (int) (System.currentTimeMillis() / 1000);

        Composite prefix = Triggers.comparator.make(table.cfName, trigger.name);
        cells.addAtom(new RangeTombstone(prefix, prefix.end(), timestamp, ldt));
    }

    /**
     * Deserialize triggers from storage-level representation.
     *
     * @param partition storage-level partition containing the trigger definitions
     * @return the list of processed TriggerDefinitions
     */
    private static List<TriggerDefinition> createTriggersFromTriggersPartition(Row partition)
    {
        List<TriggerDefinition> triggers = new ArrayList<>();
        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, TRIGGERS);
        for (UntypedResultSet.Row row : QueryProcessor.resultify(query, partition))
        {
            String name = row.getString("trigger_name");
            String classOption = row.getMap("trigger_options", UTF8Type.instance, UTF8Type.instance).get("class");
            triggers.add(new TriggerDefinition(name, classOption));
        }
        return triggers;
    }

    /*
     * UDF metadata serialization/deserialization.
     */

    public static Mutation makeCreateFunctionMutation(KSMetaData keyspace, UDFunction function, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace, timestamp, false);
        addFunctionToSchemaMutation(function, timestamp, mutation);
        return mutation;
    }

    private static void addFunctionToSchemaMutation(UDFunction function, long timestamp, Mutation mutation)
    {
        ColumnFamily cells = mutation.addOrGet(Functions);
        Composite prefix = Functions.comparator.make(function.name().name, UDHelper.calculateSignature(function));
        CFRowAdder adder = new CFRowAdder(cells, prefix, timestamp);

        adder.resetCollection("argument_names");
        adder.resetCollection("argument_types");

        for (int i = 0; i < function.argNames().size(); i++)
        {
            adder.addListEntry("argument_names", function.argNames().get(i).bytes);
            adder.addListEntry("argument_types", function.argTypes().get(i).toString());
        }

        adder.add("body", function.body());
        adder.add("is_deterministic", function.isDeterministic());
        adder.add("language", function.language());
        adder.add("return_type", function.returnType().toString());
    }

    public static Mutation makeDropFunctionMutation(KSMetaData keyspace, UDFunction function, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace, timestamp, false);

        ColumnFamily cells = mutation.addOrGet(Functions);
        int ldt = (int) (System.currentTimeMillis() / 1000);

        Composite prefix = Functions.comparator.make(function.name().name, UDHelper.calculateSignature(function));
        cells.addAtom(new RangeTombstone(prefix, prefix.end(), timestamp, ldt));

        return mutation;
    }

    private static Map<ByteBuffer, UDFunction> createFunctionsFromFunctionsPartition(Row partition)
    {
        Map<ByteBuffer, UDFunction> functions = new HashMap<>();
        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, FUNCTIONS);
        for (UntypedResultSet.Row row : QueryProcessor.resultify(query, partition))
        {
            UDFunction function = createFunctionFromFunctionRow(row);
            functions.put(UDHelper.calculateSignature(function), function);
        }
        return functions;
    }

    private static UDFunction createFunctionFromFunctionRow(UntypedResultSet.Row row)
    {
        String ksName = row.getString("keyspace_name");
        String functionName = row.getString("function_name");
        FunctionName name = new FunctionName(ksName, functionName);

        List<ColumnIdentifier> argNames = new ArrayList<>();
        if (row.has("argument_names"))
            for (String arg : row.getList("argument_names", UTF8Type.instance))
                argNames.add(new ColumnIdentifier(arg, true));

        List<AbstractType<?>> argTypes = new ArrayList<>();
        if (row.has("argument_types"))
            for (String type : row.getList("argument_types", UTF8Type.instance))
                argTypes.add(parseType(type));

        AbstractType<?> returnType = parseType(row.getString("return_type"));

        boolean isDeterministic = row.getBoolean("is_deterministic");
        String language = row.getString("language");
        String body = row.getString("body");

        try
        {
            return UDFunction.create(name, argNames, argTypes, returnType, language, body, isDeterministic);
        }
        catch (InvalidRequestException e)
        {
            slogger.error(String.format("Cannot load function '%s' from schema: this function won't be available (on this node)", name), e);
            return UDFunction.createBrokenFunction(name, argNames, argTypes, returnType, language, body, e);
        }
    }

    /*
     * Aggregate UDF metadata serialization/deserialization.
     */

    public static Mutation makeCreateAggregateMutation(KSMetaData keyspace, UDAggregate aggregate, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace, timestamp, false);
        addAggregateToSchemaMutation(aggregate, timestamp, mutation);
        return mutation;
    }

    private static void addAggregateToSchemaMutation(UDAggregate aggregate, long timestamp, Mutation mutation)
    {
        ColumnFamily cells = mutation.addOrGet(Aggregates);
        Composite prefix = Aggregates.comparator.make(aggregate.name().name, UDHelper.calculateSignature(aggregate));
        CFRowAdder adder = new CFRowAdder(cells, prefix, timestamp);

        adder.resetCollection("argument_types");
        adder.add("return_type", aggregate.returnType().toString());
        adder.add("state_func", aggregate.stateFunction().name().name);
        if (aggregate.stateType() != null)
            adder.add("state_type", aggregate.stateType().toString());
        if (aggregate.finalFunction() != null)
            adder.add("final_func", aggregate.finalFunction().name().name);
        if (aggregate.initialCondition() != null)
            adder.add("initcond", aggregate.initialCondition());

        for (AbstractType<?> argType : aggregate.argTypes())
            adder.addListEntry("argument_types", argType.toString());
    }

    private static Map<ByteBuffer, UDAggregate> createAggregatesFromAggregatesPartition(Row partition)
    {
        Map<ByteBuffer, UDAggregate> aggregates = new HashMap<>();
        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, AGGREGATES);
        for (UntypedResultSet.Row row : QueryProcessor.resultify(query, partition))
        {
            UDAggregate aggregate = createAggregateFromAggregateRow(row);
            aggregates.put(UDHelper.calculateSignature(aggregate), aggregate);
        }
        return aggregates;
    }

    private static UDAggregate createAggregateFromAggregateRow(UntypedResultSet.Row row)
    {
        String ksName = row.getString("keyspace_name");
        String functionName = row.getString("aggregate_name");
        FunctionName name = new FunctionName(ksName, functionName);

        List<String> types = row.getList("argument_types", UTF8Type.instance);

        List<AbstractType<?>> argTypes;
        if (types == null)
        {
            argTypes = Collections.emptyList();
        }
        else
        {
            argTypes = new ArrayList<>(types.size());
            for (String type : types)
                argTypes.add(parseType(type));
        }

        AbstractType<?> returnType = parseType(row.getString("return_type"));

        FunctionName stateFunc = new FunctionName(ksName, row.getString("state_func"));
        FunctionName finalFunc = row.has("final_func") ? new FunctionName(ksName, row.getString("final_func")) : null;
        AbstractType<?> stateType = row.has("state_type") ? parseType(row.getString("state_type")) : null;
        ByteBuffer initcond = row.has("initcond") ? row.getBytes("initcond") : null;

        try
        {
            return UDAggregate.create(name, argTypes, returnType, stateFunc, finalFunc, stateType, initcond);
        }
        catch (InvalidRequestException reason)
        {
            return UDAggregate.createBroken(name, argTypes, returnType, initcond, reason);
        }
    }

    public static Mutation makeDropAggregateMutation(KSMetaData keyspace, UDAggregate aggregate, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace, timestamp, false);

        ColumnFamily cells = mutation.addOrGet(Aggregates);
        int ldt = (int) (System.currentTimeMillis() / 1000);

        Composite prefix = Aggregates.comparator.make(aggregate.name().name, UDHelper.calculateSignature(aggregate));
        cells.addAtom(new RangeTombstone(prefix, prefix.end(), timestamp, ldt));

        return mutation;
    }
#endif

data_type parse_type(sstring str)
{
    return db::marshal::type_parser::parse(str);
}

std::vector<schema_ptr> all_tables() {
    return {
        keyspaces(), tables(), scylla_tables(), columns(), dropped_columns(), triggers(),
        views(), indexes(), types(), functions(), aggregates(), view_virtual_columns()
    };
}

namespace legacy {

table_schema_version schema_mutations::digest() const {
    md5_hasher h;
    db::schema_tables::feed_hash_for_schema_digest(h, _columnfamilies);
    db::schema_tables::feed_hash_for_schema_digest(h, _columns);
    return utils::UUID_gen::get_name_UUID(h.finalize());
}

future<schema_mutations> read_table_mutations(distributed<service::storage_proxy>& proxy,
    sstring keyspace_name, sstring table_name, schema_ptr s)
{
    return read_schema_partition_for_table(proxy, s, keyspace_name, table_name)
        .then([&proxy, keyspace_name, table_name] (mutation cf_m) {
            return read_schema_partition_for_table(proxy, db::system_keyspace::legacy::columns(), keyspace_name, table_name)
                .then([cf_m = std::move(cf_m)] (mutation col_m) {
                    return schema_mutations{std::move(cf_m), std::move(col_m)};
                });
        });
}

} // namespace legacy

} // namespace schema_tables
} // namespace schema
