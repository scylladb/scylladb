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
 * Modified by Cloudius Systems
 * Copyright 2015 Cloudius Systems
 */

#include "db/schema_tables.hh"

#include "service/migration_manager.hh"
#include "partition_slice_builder.hh"
#include "dht/i_partitioner.hh"
#include "system_keyspace.hh"
#include "query_context.hh"
#include "query-result-set.hh"
#include "schema_builder.hh"
#include "map_difference.hh"
#include "utils/UUID_gen.hh"
#include "core/do_with.hh"
#include "core/thread.hh"
#include "json.hh"

#include "db/marshal/type_parser.hh"
#include "db/config.hh"

#include <boost/range/algorithm/copy.hpp>
#include <boost/range/adaptor/map.hpp>

#include "compaction_strategy.hh"

using namespace db::system_keyspace;

/** system.schema_* tables used to store keyspace/table/type attributes prior to C* 3.0 */
namespace db {
namespace schema_tables {

std::vector<const char*> ALL { KEYSPACES, COLUMNFAMILIES, COLUMNS, TRIGGERS, USERTYPES, FUNCTIONS, AGGREGATES };

using days = std::chrono::duration<int, std::ratio<24 * 3600>>;

#if 0
    private static final Logger logger = LoggerFactory.getLogger(LegacySchemaTables.class);
#endif

/* static */ schema_ptr keyspaces() {
    static thread_local auto keyspaces = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, KEYSPACES), NAME, KEYSPACES,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {},
        // regular columns
        {
            {"durable_writes", boolean_type},
            {"strategy_class", utf8_type},
            {"strategy_options", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "keyspace definitions"
        )));
        builder.set_gc_grace_seconds(std::chrono::duration_cast<std::chrono::seconds>(days(7)).count());
        return builder.build(schema_builder::compact_storage::yes);
    }();
    return keyspaces;
}

/* static */ schema_ptr columnfamilies() {
    static thread_local auto columnfamilies = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, COLUMNFAMILIES), NAME, COLUMNFAMILIES,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"columnfamily_name", utf8_type}},
        // regular columns
        {
            {"bloom_filter_fp_chance", double_type},
            {"caching", utf8_type},
            {"cf_id", uuid_type},
            {"comment", utf8_type},
            {"compaction_strategy_class", utf8_type},
            {"compaction_strategy_options", utf8_type},
            {"comparator", utf8_type},
            {"compression_parameters", utf8_type},
            {"default_time_to_live", int32_type},
            {"default_validator", utf8_type},
            {"dropped_columns",  map_type_impl::get_instance(utf8_type, long_type, true)},
            {"gc_grace_seconds", int32_type},
            {"is_dense", boolean_type},
            {"key_validator", utf8_type},
            {"local_read_repair_chance", double_type},
            {"max_compaction_threshold", int32_type},
            {"max_index_interval", int32_type},
            {"memtable_flush_period_in_ms", int32_type},
            {"min_compaction_threshold", int32_type},
            {"min_index_interval", int32_type},
            {"read_repair_chance", double_type},
            {"speculative_retry", utf8_type},
            {"subcomparator", utf8_type},
            {"type", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "table definitions"
        )));
        builder.set_gc_grace_seconds(std::chrono::duration_cast<std::chrono::seconds>(days(7)).count());
        return builder.build();
    }();
    return columnfamilies;
}

/* static */ schema_ptr columns() {
    static thread_local auto columns = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, COLUMNS), NAME, COLUMNS,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"columnfamily_name", utf8_type}, {"column_name", utf8_type}},
        // regular columns
        {
            {"component_index", int32_type},
            {"index_name", utf8_type},
            {"index_options", utf8_type},
            {"index_type", utf8_type},
            {"type", utf8_type},
            {"validator", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "column definitions"
        )));
        builder.set_gc_grace_seconds(std::chrono::duration_cast<std::chrono::seconds>(days(7)).count());
        return builder.build();
    }();
    return columns;
}

/* static */ schema_ptr triggers() {
    static thread_local auto triggers = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, TRIGGERS), NAME, TRIGGERS,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"columnfamily_name", utf8_type}, {"trigger_name", utf8_type}},
        // regular columns
        {
            {"trigger_options",  map_type_impl::get_instance(utf8_type, utf8_type, true)},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "trigger definitions"
        )));
        builder.set_gc_grace_seconds(std::chrono::duration_cast<std::chrono::seconds>(days(7)).count());
        return builder.build();
    }();
    return triggers;
}

/* static */ schema_ptr usertypes() {
    static thread_local auto usertypes = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, USERTYPES), NAME, USERTYPES,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"type_name", utf8_type}},
        // regular columns
        {
            {"field_names", list_type_impl::get_instance(utf8_type, true)},
            {"field_types", list_type_impl::get_instance(utf8_type, true)},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "user defined type definitions"
        )));
        builder.set_gc_grace_seconds(std::chrono::duration_cast<std::chrono::seconds>(days(7)).count());
        return builder.build();
    }();
    return usertypes;
}

/* static */ schema_ptr functions() {
    static thread_local auto functions = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, FUNCTIONS), NAME, FUNCTIONS,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"function_name", utf8_type}, {"signature", bytes_type}},
        // regular columns
        {
            {"argument_names", list_type_impl::get_instance(utf8_type, true)},
            {"argument_types", list_type_impl::get_instance(utf8_type, true)},
            {"body", utf8_type},
            {"is_deterministic", boolean_type},
            {"language", utf8_type},
            {"return_type", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "user defined type definitions"
        )));
        builder.set_gc_grace_seconds(std::chrono::duration_cast<std::chrono::seconds>(days(7)).count());
        return builder.build();
    }();
    return functions;
}

/* static */ schema_ptr aggregates() {
    static thread_local auto aggregates = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, AGGREGATES), NAME, AGGREGATES,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"aggregate_name", utf8_type}, {"signature", bytes_type}},
        // regular columns
        {
            {"argument_types", list_type_impl::get_instance(utf8_type, true)},
            {"final_func", utf8_type},
            {"intercond", bytes_type},
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
        builder.set_gc_grace_seconds(std::chrono::duration_cast<std::chrono::seconds>(days(7)).count());
        return builder.build();
    }();
    return aggregates;
}

#if 0
    public static final List<CFMetaData> All = Arrays.asList(Keyspaces, Columnfamilies, Columns, Triggers, Usertypes, Functions, Aggregates);

    private static CFMetaData compile(String name, String description, String schema)
    {
        return CFMetaData.compile(String.format(schema, name), SystemKeyspace.NAME)
                         .comment(description)
                         .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(7));
    }
#endif

/** add entries to system.schema_* for the hardcoded system definitions */
future<> save_system_keyspace_schema() {
    auto& ks = db::qctx->db().find_keyspace(db::system_keyspace::NAME);
    auto ksm = ks.metadata();

    // delete old, possibly obsolete entries in schema tables
    return parallel_for_each(ALL, [ksm] (sstring cf) {
        return db::execute_cql("DELETE FROM system.%s WHERE keyspace_name = ?", cf, ksm->name()).discard_result();
    }).then([ksm] {
        // (+1 to timestamp to make sure we don't get shadowed by the tombstones we just added)
        auto mvec  = make_create_keyspace_mutations(ksm, qctx->next_timestamp(), true);
        return qctx->proxy().mutate_locally(mvec);
    });
}

#if 0

    public static Collection<KSMetaData> readSchemaFromSystemTables()
    {
        List<Row> serializedSchema = getSchemaPartitionsForTable(KEYSPACES);

        List<KSMetaData> keyspaces = new ArrayList<>(serializedSchema.size());

        for (Row partition : serializedSchema)
        {
            if (isEmptySchemaPartition(partition) || isSystemKeyspaceSchemaPartition(partition))
                continue;

            keyspaces.add(createKeyspaceFromSchemaPartitions(partition,
                                                             readSchemaPartitionForKeyspace(COLUMNFAMILIES, partition.key),
                                                             readSchemaPartitionForKeyspace(USERTYPES, partition.key)));

            // Will be moved away in #6717
            for (UDFunction function : createFunctionsFromFunctionsPartition(readSchemaPartitionForKeyspace(FUNCTIONS, partition.key)).values())
                org.apache.cassandra.cql3.functions.Functions.addFunction(function);

            // Will be moved away in #6717
            for (UDAggregate aggregate : createAggregatesFromAggregatesPartition(readSchemaPartitionForKeyspace(AGGREGATES, partition.key)).values())
                org.apache.cassandra.cql3.functions.Functions.addFunction(aggregate);
        }

        return keyspaces;
    }

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
    future<utils::UUID> calculate_schema_digest(service::storage_proxy& proxy)
    {
        auto map = [&proxy] (sstring table) {
            return db::system_keyspace::query_mutations(proxy, table).then([&proxy, table] (auto rs) {
                auto s = proxy.get_db().local().find_schema(system_keyspace::NAME, table);
                std::vector<query::result> results;
                for (auto&& p : rs->partitions()) {
                    auto mut = p.mut().unfreeze(s);
                    auto partition_key = boost::any_cast<sstring>(utf8_type->deserialize(mut.key().get_component(*s, 0)));
                    if (partition_key == system_keyspace::NAME) {
                        continue;
                    }
                    auto slice = partition_slice_builder(*s).build();
                    results.emplace_back(mut.query(slice));
                }
                return results;
            });
        };
        auto reduce = [] (auto&& hash, auto&& results) {
            for (auto&& rs : results) {
                for (auto&& f : rs.buf().fragments()) {
                    hash->Update(reinterpret_cast<const unsigned char*>(f.begin()), f.size());
                }
            }
            return std::move(hash);
        };
        return map_reduce(ALL.begin(), ALL.end(), map, std::move(std::make_unique<CryptoPP::Weak::MD5>()), reduce).then([] (auto&& hash) {
            bytes digest{bytes::initialized_later(), CryptoPP::Weak::MD5::DIGESTSIZE};
            hash->Final(reinterpret_cast<unsigned char*>(digest.begin()));
            return utils::UUID_gen::get_name_UUID(digest);
        });
    }

#if 0
    /**
     * @param schemaTableName The name of the table responsible for part of the schema
     * @return CFS responsible to hold low-level serialized schema
     */
    private static ColumnFamilyStore getSchemaCFS(String schemaTableName)
    {
        return Keyspace.open(SystemKeyspace.NAME).getColumnFamilyStore(schemaTableName);
    }

    /**
     * @param schemaTableName The name of the table responsible for part of the schema.
     * @return low-level schema representation
     */
    private static List<Row> getSchemaPartitionsForTable(String schemaTableName)
    {
        Token minToken = StorageService.getPartitioner().getMinimumToken();
        return getSchemaCFS(schemaTableName).getRangeSlice(new Range<RowPosition>(minToken.minKeyBound(), minToken.maxKeyBound()),
                                                           null,
                                                           new IdentityQueryFilter(),
                                                           Integer.MAX_VALUE,
                                                           System.currentTimeMillis());
    }
#endif

    future<std::vector<frozen_mutation>> convert_schema_to_mutations(service::storage_proxy& proxy)
    {
        auto map = [&proxy] (sstring table) {
            return db::system_keyspace::query_mutations(proxy, table).then([&proxy, table] (auto rs) {
                auto s = proxy.get_db().local().find_schema(system_keyspace::NAME, table);
                std::vector<frozen_mutation> results;
                for (auto&& p : rs->partitions()) {
                    auto mut = p.mut().unfreeze(s);
                    auto partition_key = boost::any_cast<sstring>(utf8_type->deserialize(mut.key().get_component(*s, 0)));
                    if (partition_key == system_keyspace::NAME) {
                        continue;
                    }
                    results.emplace_back(p.mut());
                }
                return results;
            });
        };
        auto reduce = [] (auto&& result, auto&& mutations) {
            std::copy(mutations.begin(), mutations.end(), std::back_inserter(result));
            return std::move(result);
        };
        return map_reduce(ALL.begin(), ALL.end(), map, std::move(std::vector<frozen_mutation>{}), reduce);
    }

    future<schema_result>
    read_schema_for_keyspaces(service::storage_proxy& proxy, const sstring& schema_table_name, const std::set<sstring>& keyspace_names)
    {
        auto schema = proxy.get_db().local().find_schema(system_keyspace::NAME, schema_table_name);
        auto map = [&proxy, schema_table_name] (sstring keyspace_name) { return read_schema_partition_for_keyspace(proxy, schema_table_name, keyspace_name); };
        auto insert = [] (schema_result&& result, auto&& schema_entity) {
            if (!schema_entity.second->empty()) {
                result.insert(std::move(schema_entity));
            }
            return std::move(result);
        };
        return map_reduce(keyspace_names.begin(), keyspace_names.end(), map, schema_result{}, insert);
    }

#if 0
    private static ByteBuffer getSchemaKSKey(String ksName)
    {
        return AsciiType.instance.fromString(ksName);
    }
#endif

    future<schema_result::value_type>
    read_schema_partition_for_keyspace(service::storage_proxy& proxy, const sstring& schema_table_name, const sstring& keyspace_name)
    {
        auto schema = proxy.get_db().local().find_schema(system_keyspace::NAME, schema_table_name);
        auto keyspace_key = dht::global_partitioner().decorate_key(*schema,
            partition_key::from_single_value(*schema, to_bytes(keyspace_name)));
        return db::system_keyspace::query(proxy, schema_table_name, keyspace_key).then([keyspace_name] (auto&& rs) {
            return schema_result::value_type{keyspace_name, std::move(rs)};
        });
    }

    future<schema_result::value_type>
    read_schema_partition_for_table(service::storage_proxy& proxy, const sstring& schema_table_name, const sstring& keyspace_name, const sstring& table_name)
    {
        auto schema = proxy.get_db().local().find_schema(system_keyspace::NAME, schema_table_name);
        auto keyspace_key = dht::global_partitioner().decorate_key(*schema,
            partition_key::from_single_value(*schema, to_bytes(keyspace_name)));
        auto clustering_range = query::clustering_range(clustering_key_prefix::from_clustering_prefix(*schema, exploded_clustering_prefix({to_bytes(table_name)})));
        return db::system_keyspace::query(proxy, schema_table_name, keyspace_key, clustering_range).then([keyspace_name] (auto&& rs) {
            return schema_result::value_type{keyspace_name, std::move(rs)};
        });
    }

#if 0
    private static boolean isEmptySchemaPartition(Row partition)
    {
        return partition.cf == null || (partition.cf.isMarkedForDelete() && !partition.cf.hasColumns());
    }

    private static boolean isSystemKeyspaceSchemaPartition(Row partition)
    {
        return getSchemaKSKey(SystemKeyspace.NAME).equals(partition.key.getKey());
    }
#endif

    static semaphore the_merge_lock;

    future<> merge_lock() {
        return smp::submit_to(0, [] { return the_merge_lock.wait(); });
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
    future<> merge_schema(service::storage_proxy& proxy, std::vector<mutation> mutations)
    {
        return merge_lock().then([&proxy, mutations = std::move(mutations)] {
            return do_merge_schema(proxy, std::move(mutations), true).then([&proxy] {
                return update_schema_version_and_announce(proxy);
            });
        }).finally([] {
            return merge_unlock();
        });
    }

    future<> merge_schema(service::storage_proxy& proxy, std::vector<mutation> mutations, bool do_flush)
    { 
        return merge_lock().then([&proxy, mutations = std::move(mutations), do_flush] {
            return do_merge_schema(proxy, std::move(mutations), do_flush);
        }).finally([] {
            return merge_unlock();
        });
    }

    future<> do_merge_schema(service::storage_proxy& proxy, std::vector<mutation> mutations, bool do_flush)
    {
       return seastar::async([&proxy, mutations = std::move(mutations), do_flush] {
           schema_ptr s = keyspaces();
           // compare before/after schemas of the affected keyspaces only
           std::set<sstring> keyspaces;
           std::set<utils::UUID> column_families;
           for (auto&& mutation : mutations) {
               keyspaces.emplace(boost::any_cast<sstring>(utf8_type->deserialize(mutation.key().get_component(*s, 0))));
               column_families.emplace(mutation.column_family_id());
           }

           // current state of the schema
           auto&& old_keyspaces = read_schema_for_keyspaces(proxy, KEYSPACES, keyspaces).get0();
           auto&& old_column_families = read_schema_for_keyspaces(proxy, COLUMNFAMILIES, keyspaces).get0();
           /*auto& old_types = */read_schema_for_keyspaces(proxy, USERTYPES, keyspaces).get0();
           /*auto& old_functions = */read_schema_for_keyspaces(proxy, FUNCTIONS, keyspaces).get0();
           /*auto& old_aggregates = */read_schema_for_keyspaces(proxy, AGGREGATES, keyspaces).get0();

           proxy.mutate_locally(std::move(mutations)).get0();

           if (do_flush) {
               proxy.get_db().invoke_on_all([s, cfs = std::move(column_families)] (database& db) {
                   return parallel_for_each(cfs.begin(), cfs.end(), [&db] (auto& id) {
                       auto& cf = db.find_column_family(id);
                       return cf.flush();
                   });
               }).get();
           }

          // with new data applied
           auto&& new_keyspaces = read_schema_for_keyspaces(proxy, KEYSPACES, keyspaces).get0();
           auto&& new_column_families = read_schema_for_keyspaces(proxy, COLUMNFAMILIES, keyspaces).get0();
           /*auto& new_types = */read_schema_for_keyspaces(proxy, USERTYPES, keyspaces).get0();
           /*auto& new_functions = */read_schema_for_keyspaces(proxy, FUNCTIONS, keyspaces).get0();
           /*auto& new_aggregates = */read_schema_for_keyspaces(proxy, AGGREGATES, keyspaces).get0();

           std::set<sstring> keyspaces_to_drop = merge_keyspaces(proxy, std::move(old_keyspaces), std::move(new_keyspaces)).get0();
           merge_tables(proxy, std::move(old_column_families), std::move(new_column_families)).get0();
#if 0
           mergeTypes(oldTypes, newTypes);
           mergeFunctions(oldFunctions, newFunctions);
           mergeAggregates(oldAggregates, newAggregates);
#endif
           proxy.get_db().invoke_on_all([keyspaces_to_drop = std::move(keyspaces_to_drop)] (database& db) {
               // it is safe to drop a keyspace only when all nested ColumnFamilies where deleted
               for (auto&& keyspace_to_drop : keyspaces_to_drop) {
                   db.drop_keyspace(keyspace_to_drop);
               }
           }).get0();
       });
    }

    future<std::set<sstring>> merge_keyspaces(service::storage_proxy& proxy, schema_result&& before, schema_result&& after)
    {
        std::vector<schema_result::value_type> created;
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
        auto diff = difference(before, after, [](const auto& x, const auto& y) -> bool {
            return *x == *y;
        });

        for (auto&& key : diff.entries_only_on_right) {
            auto&& value = after[key];
            if (!value->empty()) {
                created.emplace_back(schema_result::value_type{key, std::move(value)});
            }
        }
        for (auto&& key : diff.entries_differing) {
            sstring keyspace_name = key;

            auto&& pre  = before[key];
            auto&& post = after[key];

            if (!pre->empty() && !post->empty()) {
                altered.emplace_back(keyspace_name);
            } else if (!pre->empty()) {
                dropped.emplace(keyspace_name);
            } else if (!post->empty()) { // a (re)created keyspace
                created.emplace_back(schema_result::value_type{key, std::move(post)});
            }
        }
        return do_with(std::move(created), [&proxy, altered = std::move(altered)] (auto& created) {
            return do_for_each(created, [&proxy] (auto&& val) {
                auto make_ksm = [val] () {
                    return create_keyspace_from_schema_partition(val);
                };
                return database::create_keyspace_on_all(proxy.get_db(), make_ksm);
            }).then([&proxy, &altered] () mutable {
                return proxy.get_db().invoke_on_all([&proxy, altered = std::move(altered)] (database& db) {
                    for (auto&& name : altered) {
                        db.update_keyspace(name);
                    }

                    return make_ready_future<>();
                });
            });
        }).then([dropped = std::move(dropped)] () {
            return make_ready_future<std::set<sstring>>(dropped);
        });
    }

    // see the comments for merge_keyspaces()
    future<> merge_tables(service::storage_proxy& proxy, schema_result&& before, schema_result&& after)
    {
        return do_with(std::make_pair(std::move(after), std::move(before)), [&proxy] (auto& pair) {
            auto& after = pair.first;
            auto& before = pair.second;
            return proxy.get_db().invoke_on_all([&proxy, &before, &after] (database& db) {
                return seastar::async([&proxy, &db, &before, &after] {
                    std::vector<schema_ptr> created;
                    std::vector<schema_ptr> altered;
                    std::vector<schema_ptr> dropped;
                    auto diff = difference(before, after, [](const auto& x, const auto& y) -> bool {
                        return *x == *y;
                    });
                    for (auto&& key : diff.entries_only_on_right) {
                        auto&& value = after[key];
                        if (!value->empty()) {
                            auto&& tables = create_tables_from_tables_partition(proxy, value).get0();
                            boost::copy(tables | boost::adaptors::map_values, std::back_inserter(created));
                        }
                    }
                    for (auto&& key : diff.entries_differing) {
                        sstring keyspace_name = key;

                        auto&& pre  = before[key];
                        auto&& post = after[key];

                        if (!pre->empty() && !post->empty()) {
                            auto before = db.find_keyspace(keyspace_name).metadata()->cf_meta_data();
                            auto after = create_tables_from_tables_partition(proxy, post).get0();
                            auto delta = difference(std::map<sstring, schema_ptr>{before.begin(), before.end()}, after, [](const schema_ptr& x, const schema_ptr& y) -> bool {
                                return *x == *y;
                            });
                            for (auto&& key : delta.entries_only_on_left) {
                                dropped.emplace_back(before[key]);
                            }
                            for (auto&& key : delta.entries_only_on_right) {
                                created.emplace_back(after[key]);
                            }
                            for (auto&& key : delta.entries_differing) {
                                altered.emplace_back(after[key]);
                            }
                        } else if (!pre->empty()) {
                            auto before = db.find_keyspace(keyspace_name).metadata()->cf_meta_data();
                            boost::copy(before | boost::adaptors::map_values, std::back_inserter(dropped));
                        } else if (!post->empty()) {
                            auto tables = create_tables_from_tables_partition(proxy, post).get0();
                            boost::copy(tables | boost::adaptors::map_values, std::back_inserter(created));
                        }
                    }
                    for (auto&& cfm : created) {
                        auto& ks = db.find_keyspace(cfm->ks_name());
                        auto cfg = ks.make_column_family_config(*cfm);
                        db.add_column_family(cfm, cfg);
                    }
                    parallel_for_each(altered.begin(), altered.end(), [&db] (auto&& cfm) {
                        return db.update_column_family(cfm->ks_name(), cfm->cf_name());
                    }).get();
                    for (auto&& cfm : dropped) {
                        db.drop_column_family(cfm->ks_name(), cfm->cf_name());
                    }
                });
            });
        });
    }

#if 0
    // see the comments for mergeKeyspaces()
    private static void mergeTypes(Map<DecoratedKey, ColumnFamily> before, Map<DecoratedKey, ColumnFamily> after)
    {
        List<UserType> created = new ArrayList<>();
        List<UserType> altered = new ArrayList<>();
        List<UserType> dropped = new ArrayList<>();

        MapDifference<DecoratedKey, ColumnFamily> diff = Maps.difference(before, after);

        // New keyspace with types
        for (Map.Entry<DecoratedKey, ColumnFamily> entry : diff.entriesOnlyOnRight().entrySet())
            if (entry.getValue().hasColumns())
                created.addAll(createTypesFromPartition(new Row(entry.getKey(), entry.getValue())).values());

        for (Map.Entry<DecoratedKey, MapDifference.ValueDifference<ColumnFamily>> entry : diff.entriesDiffering().entrySet())
        {
            String keyspaceName = AsciiType.instance.compose(entry.getKey().getKey());

            ColumnFamily pre  = entry.getValue().leftValue();
            ColumnFamily post = entry.getValue().rightValue();

            if (pre.hasColumns() && post.hasColumns())
            {
                MapDifference<ByteBuffer, UserType> delta =
                    Maps.difference(Schema.instance.getKSMetaData(keyspaceName).userTypes.getAllTypes(),
                                    createTypesFromPartition(new Row(entry.getKey(), post)));

                dropped.addAll(delta.entriesOnlyOnLeft().values());
                created.addAll(delta.entriesOnlyOnRight().values());
                Iterables.addAll(altered, Iterables.transform(delta.entriesDiffering().values(), new Function<MapDifference.ValueDifference<UserType>, UserType>()
                {
                    public UserType apply(MapDifference.ValueDifference<UserType> pair)
                    {
                        return pair.rightValue();
                    }
                }));
            }
            else if (pre.hasColumns())
            {
                dropped.addAll(Schema.instance.getKSMetaData(keyspaceName).userTypes.getAllTypes().values());
            }
            else if (post.hasColumns())
            {
                created.addAll(createTypesFromPartition(new Row(entry.getKey(), post)).values());
            }
        }

        for (UserType type : created)
            Schema.instance.addType(type);
        for (UserType type : altered)
            Schema.instance.updateType(type);
        for (UserType type : dropped)
            Schema.instance.dropType(type);
    }

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

    /*
     * Keyspace metadata serialization/deserialization.
     */

    std::vector<mutation> make_create_keyspace_mutations(lw_shared_ptr<keyspace_metadata> keyspace, api::timestamp_type timestamp, bool with_tables_and_types_and_functions)
    {
        std::vector<mutation> mutations;
        schema_ptr s = keyspaces();
        auto pkey = partition_key::from_exploded(*s, {utf8_type->decompose(keyspace->name())});
        mutation m(pkey, s);
        exploded_clustering_prefix ckey;
        m.set_cell(ckey, "durable_writes", keyspace->durable_writes(), timestamp);
        m.set_cell(ckey, "strategy_class", keyspace->strategy_name(), timestamp);
        auto raw = json::to_json(keyspace->strategy_options());
        m.set_cell(ckey, "strategy_options", raw, timestamp);
        mutations.emplace_back(std::move(m));

        if (with_tables_and_types_and_functions) {
#if 0
            for (UserType type : keyspace.userTypes.getAllTypes().values())
                addTypeToSchemaMutation(type, timestamp, mutation);
#endif
            for (auto&& kv : keyspace->cf_meta_data()) {
                add_table_to_schema_mutation(kv.second, timestamp, true, pkey, mutations);
            }
        }
        return mutations;
    }

#if 0
    public static Mutation makeDropKeyspaceMutation(KSMetaData keyspace, long timestamp)
    {
        Mutation mutation = new Mutation(SystemKeyspace.NAME, getSchemaKSKey(keyspace.name));
        for (String schemaTable : ALL)
            mutation.delete(schemaTable, timestamp);
        mutation.delete(SystemKeyspace.BUILT_INDEXES, timestamp);
        return mutation;
    }

    private static KSMetaData createKeyspaceFromSchemaPartitions(Row serializedKeyspace, Row serializedTables, Row serializedTypes)
    {
        Collection<CFMetaData> tables = createTablesFromTablesPartition(serializedTables).values();
        UTMetaData types = new UTMetaData(createTypesFromPartition(serializedTypes));
        return createKeyspaceFromSchemaPartition(serializedKeyspace).cloneWith(tables, types);
    }

    public static KSMetaData createKeyspaceFromName(String keyspace)
    {
        Row partition = readSchemaPartitionForKeyspace(KEYSPACES, keyspace);

        if (isEmptySchemaPartition(partition))
            throw new RuntimeException(String.format("%s not found in the schema definitions keyspaceName (%s).", keyspace, KEYSPACES));

        return createKeyspaceFromSchemaPartition(partition);
    }
#endif

    /**
     * Deserialize only Keyspace attributes without nested tables or types
     *
     * @param partition Keyspace attributes in serialized form
     */
    lw_shared_ptr<keyspace_metadata> create_keyspace_from_schema_partition(const schema_result::value_type& result)
    {
        auto&& rs = result.second;
        if (rs->empty()) {
            throw std::runtime_error("query result has no rows");
        }
        auto&& row = rs->row(0);
        auto keyspace_name = row.get_nonnull<sstring>("keyspace_name");
        auto strategy_name = row.get_nonnull<sstring>("strategy_class");
        auto raw = row.get_nonnull<sstring>("strategy_options");
        std::map<sstring, sstring> strategy_options = json::to_map(raw);
        bool durable_writes = row.get_nonnull<bool>("durable_writes");
        return make_lw_shared<keyspace_metadata>(keyspace_name, strategy_name, strategy_options, durable_writes);
    }

#if 0
    /*
     * User type metadata serialization/deserialization.
     */

    public static Mutation makeCreateTypeMutation(KSMetaData keyspace, UserType type, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace, timestamp, false);
        addTypeToSchemaMutation(type, timestamp, mutation);
        return mutation;
    }

    private static void addTypeToSchemaMutation(UserType type, long timestamp, Mutation mutation)
    {
        ColumnFamily cells = mutation.addOrGet(Usertypes);

        Composite prefix = Usertypes.comparator.make(type.name);
        CFRowAdder adder = new CFRowAdder(cells, prefix, timestamp);

        adder.resetCollection("field_names");
        adder.resetCollection("field_types");

        for (int i = 0; i < type.size(); i++)
        {
            adder.addListEntry("field_names", type.fieldName(i));
            adder.addListEntry("field_types", type.fieldType(i).toString());
        }
    }

    public static Mutation dropTypeFromSchemaMutation(KSMetaData keyspace, UserType type, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace, timestamp, false);

        ColumnFamily cells = mutation.addOrGet(Usertypes);
        int ldt = (int) (System.currentTimeMillis() / 1000);

        Composite prefix = Usertypes.comparator.make(type.name);
        cells.addAtom(new RangeTombstone(prefix, prefix.end(), timestamp, ldt));

        return mutation;
    }

    private static Map<ByteBuffer, UserType> createTypesFromPartition(Row partition)
    {
        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, USERTYPES);
        Map<ByteBuffer, UserType> types = new HashMap<>();
        for (UntypedResultSet.Row row : QueryProcessor.resultify(query, partition))
        {
            UserType type = createTypeFromRow(row);
            types.put(type.name, type);
        }
        return types;
    }

    private static UserType createTypeFromRow(UntypedResultSet.Row row)
    {
        String keyspace = row.getString("keyspace_name");
        ByteBuffer name = ByteBufferUtil.bytes(row.getString("type_name"));
        List<String> rawColumns = row.getList("field_names", UTF8Type.instance);
        List<String> rawTypes = row.getList("field_types", UTF8Type.instance);

        List<ByteBuffer> columns = new ArrayList<>(rawColumns.size());
        for (String rawColumn : rawColumns)
            columns.add(ByteBufferUtil.bytes(rawColumn));

        List<AbstractType<?>> types = new ArrayList<>(rawTypes.size());
        for (String rawType : rawTypes)
            types.add(parseType(rawType));

        return new UserType(keyspace, name, columns, types);
    }
#endif

    /*
     * Table metadata serialization/deserialization.
     */

    std::vector<mutation> make_create_table_mutations(lw_shared_ptr<keyspace_metadata> keyspace, schema_ptr table, api::timestamp_type timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        auto mutations = make_create_keyspace_mutations(keyspace, timestamp, false);
        schema_ptr s = keyspaces();
        auto pkey = partition_key::from_exploded(*s, {utf8_type->decompose(keyspace->name())});
        add_table_to_schema_mutation(table, timestamp, true, pkey, mutations);
        return mutations;
    }

    void add_table_to_schema_mutation(schema_ptr table, api::timestamp_type timestamp, bool with_columns_and_triggers, const partition_key& pkey, std::vector<mutation>& mutations)
    {
        // For property that can be null (and can be changed), we insert tombstones, to make sure
        // we don't keep a property the user has removed
        schema_ptr s = columnfamilies();
        mutation m{pkey, s};
        auto ckey = clustering_key::from_single_value(*s, to_bytes(table->cf_name()));
        m.set_clustered_cell(ckey, "cf_id", table->id(), timestamp);
        m.set_clustered_cell(ckey, "type", cf_type_to_sstring(table->type()), timestamp);

        if (table->is_super()) {
             warn(unimplemented::cause::SUPER);
#if 0
            // We need to continue saving the comparator and subcomparator separatly, otherwise
            // we won't know at deserialization if the subcomparator should be taken into account
            // TODO: we should implement an on-start migration if we want to get rid of that.
            adder.add("comparator", table.comparator.subtype(0).toString());
            adder.add("subcomparator", table.comparator.subtype(1).toString());
#endif
        } else {
            m.set_clustered_cell(ckey, "comparator", cell_comparator::to_sstring(*table), timestamp);
        }

        m.set_clustered_cell(ckey, "bloom_filter_fp_chance", table->bloom_filter_fp_chance(), timestamp);
#if 0
        adder.add("caching", table.getCaching().toString());
#endif
        m.set_clustered_cell(ckey, "comment", table->comment(), timestamp);

        m.set_clustered_cell(ckey, "compaction_strategy_class", sstables::compaction_strategy::name(table->compaction_strategy()), timestamp);
        m.set_clustered_cell(ckey, "compaction_strategy_options", json::to_json(table->compaction_strategy_options()), timestamp);

        const auto& compression_options = table->get_compressor_params();
        m.set_clustered_cell(ckey, "compression_parameters", json::to_json(compression_options.get_options()), timestamp);
        m.set_clustered_cell(ckey, "default_time_to_live", table->default_time_to_live().count(), timestamp);
        m.set_clustered_cell(ckey, "default_validator", table->default_validator()->name(), timestamp);
        m.set_clustered_cell(ckey, "gc_grace_seconds", table->gc_grace_seconds(), timestamp);
        m.set_clustered_cell(ckey, "key_validator", table->thrift_key_validator(), timestamp);
        m.set_clustered_cell(ckey, "local_read_repair_chance", table->dc_local_read_repair_chance(), timestamp);
        m.set_clustered_cell(ckey, "min_compaction_threshold", table->min_compaction_threshold(), timestamp);
        m.set_clustered_cell(ckey, "max_compaction_threshold", table->max_compaction_threshold(), timestamp);
        m.set_clustered_cell(ckey, "min_index_interval", table->min_index_interval(), timestamp);
        m.set_clustered_cell(ckey, "max_index_interval", table->max_index_interval(), timestamp);
        m.set_clustered_cell(ckey, "memtable_flush_period_in_ms", table->memtable_flush_period(), timestamp);
        m.set_clustered_cell(ckey, "read_repair_chance", table->read_repair_chance(), timestamp);
        m.set_clustered_cell(ckey, "speculative_retry", table->speculative_retry().to_sstring(), timestamp);

#if 0
        for (Map.Entry<ColumnIdentifier, Long> entry : table.getDroppedColumns().entrySet())
            adder.addMapEntry("dropped_columns", entry.getKey().toString(), entry.getValue());
#endif

        m.set_clustered_cell(ckey, "is_dense", table->is_dense(), timestamp);

        if (with_columns_and_triggers) {
            for (auto&& column : table->all_columns_in_select_order()) {
                add_column_to_schema_mutation(table, column, timestamp, pkey, mutations);
            }

#if 0
            for (TriggerDefinition trigger : table.getTriggers().values())
                addTriggerToSchemaMutation(table, trigger, timestamp, mutation);
#endif
        }
        mutations.emplace_back(std::move(m));
    }

#if 0
    public static Mutation makeUpdateTableMutation(KSMetaData keyspace,
                                                   CFMetaData oldTable,
                                                   CFMetaData newTable,
                                                   long timestamp,
                                                   boolean fromThrift)
    {
        Mutation mutation = makeCreateKeyspaceMutation(keyspace, timestamp, false);

        addTableToSchemaMutation(newTable, timestamp, false, mutation);

        MapDifference<ByteBuffer, ColumnDefinition> columnDiff = Maps.difference(oldTable.getColumnMetadata(),
                                                                                 newTable.getColumnMetadata());

        // columns that are no longer needed
        for (ColumnDefinition column : columnDiff.entriesOnlyOnLeft().values())
        {
            // Thrift only knows about the REGULAR ColumnDefinition type, so don't consider other type
            // are being deleted just because they are not here.
            if (fromThrift && column.kind != ColumnDefinition.Kind.REGULAR)
                continue;

            dropColumnFromSchemaMutation(oldTable, column, timestamp, mutation);
        }

        // newly added columns
        for (ColumnDefinition column : columnDiff.entriesOnlyOnRight().values())
            addColumnToSchemaMutation(newTable, column, timestamp, mutation);

        // old columns with updated attributes
        for (ByteBuffer name : columnDiff.entriesDiffering().keySet())
            addColumnToSchemaMutation(newTable, newTable.getColumnDefinition(name), timestamp, mutation);

        MapDifference<String, TriggerDefinition> triggerDiff = Maps.difference(oldTable.getTriggers(), newTable.getTriggers());

        // dropped triggers
        for (TriggerDefinition trigger : triggerDiff.entriesOnlyOnLeft().values())
            dropTriggerFromSchemaMutation(oldTable, trigger, timestamp, mutation);

        // newly created triggers
        for (TriggerDefinition trigger : triggerDiff.entriesOnlyOnRight().values())
            addTriggerToSchemaMutation(newTable, trigger, timestamp, mutation);

        return mutation;
    }

    public static Mutation makeDropTableMutation(KSMetaData keyspace, CFMetaData table, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace, timestamp, false);

        ColumnFamily cells = mutation.addOrGet(Columnfamilies);
        int ldt = (int) (System.currentTimeMillis() / 1000);

        Composite prefix = Columnfamilies.comparator.make(table.cfName);
        cells.addAtom(new RangeTombstone(prefix, prefix.end(), timestamp, ldt));

        for (ColumnDefinition column : table.allColumns())
            dropColumnFromSchemaMutation(table, column, timestamp, mutation);

        for (TriggerDefinition trigger : table.getTriggers().values())
            dropTriggerFromSchemaMutation(table, trigger, timestamp, mutation);

        // TODO: get rid of in #6717
        ColumnFamily indexCells = mutation.addOrGet(SystemKeyspace.BuiltIndexes);
        for (String indexName : Keyspace.open(keyspace.name).getColumnFamilyStore(table.cfName).getBuiltIndexes())
            indexCells.addTombstone(indexCells.getComparator().makeCellName(indexName), ldt, timestamp);

        return mutation;
    }
#endif

    future<schema_ptr> create_table_from_name(service::storage_proxy& proxy, const sstring& keyspace, const sstring& table)
    {
        return read_schema_partition_for_table(proxy, COLUMNFAMILIES, keyspace, table).then([&proxy, keyspace, table] (auto partition) {
            if (partition.second->empty()) {
                throw std::runtime_error(sprint("%s:%s not found in the schema definitions keyspace.", keyspace, table));
            }
            return create_table_from_table_partition(proxy, partition.second);
        });
    }

    /**
     * Deserialize tables from low-level schema representation, all of them belong to the same keyspace
     *
     * @return map containing name of the table and its metadata for faster lookup
     */
    future<std::map<sstring, schema_ptr>> create_tables_from_tables_partition(service::storage_proxy& proxy, const schema_result::mapped_type& result)
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

    void create_table_from_table_row_and_columns_partition(schema_builder& builder, const query::result_set_row& table_row, const schema_result::value_type& serialized_columns)
    {
        create_table_from_table_row_and_column_rows(builder, table_row, serialized_columns.second);
    }

    future<schema_ptr> create_table_from_table_partition(service::storage_proxy& proxy, const lw_shared_ptr<query::result_set>& partition)
    {
        return create_table_from_table_row(proxy, partition->row(0));
    }

    /**
     * Deserialize table metadata from low-level representation
     *
     * @return Metadata deserialized from schema
     */
    future<schema_ptr> create_table_from_table_row(service::storage_proxy& proxy, const query::result_set_row& row)
    {
        auto ks_name = row.get_nonnull<sstring>("keyspace_name");
        auto cf_name = row.get_nonnull<sstring>("columnfamily_name");
        auto id = row.get_nonnull<utils::UUID>("cf_id");
        return read_schema_partition_for_table(proxy, COLUMNS, ks_name, cf_name).then([&proxy, &row, ks_name, cf_name, id] (auto serialized_columns) {
            schema_builder builder{ks_name, cf_name, id};
            create_table_from_table_row_and_columns_partition(builder, row, serialized_columns);
            return builder.build();
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

    void create_table_from_table_row_and_column_rows(schema_builder& builder, const query::result_set_row& table_row, const schema_result::mapped_type& serialized_column_definitions)
    {
        auto ks_name = table_row.get_nonnull<sstring>("keyspace_name");
        auto cf_name = table_row.get_nonnull<sstring>("columnfamily_name");

#if 0
        AbstractType<?> rawComparator = TypeParser.parse(result.getString("comparator"));
        AbstractType<?> subComparator = result.has("subcomparator") ? TypeParser.parse(result.getString("subcomparator")) : null;
#endif

        cf_type cf = cf_type::standard;
        if (table_row.has("type")) {
            cf = sstring_to_cf_type(table_row.get_nonnull<sstring>("type"));
            if (cf == cf_type::super) {
                fail(unimplemented::cause::SUPER);
            }
        }
#if 0
        AbstractType<?> fullRawComparator = CFMetaData.makeRawAbstractType(rawComparator, subComparator);
#endif

        std::vector<column_definition> column_defs = create_columns_from_column_rows(serialized_column_definitions,
                                                                        ks_name,
                                                                        cf_name,/*,
                                                                        fullRawComparator, */
                                                                        cf == cf_type::super);

        bool is_dense;
        if (table_row.has("is_dense")) {
            is_dense = table_row.get_nonnull<bool>("is_dense");
        } else {
            // FIXME:
            // is_dense = CFMetaData.calculateIsDense(fullRawComparator, columnDefs);
            throw std::runtime_error("not implemented");
        }

        bool is_compound = cell_comparator::check_compound(table_row.get_nonnull<sstring>("comparator"));
        builder.set_is_compound(is_compound);
#if 0
        CellNameType comparator = CellNames.fromAbstractType(fullRawComparator, isDense);

        // if we are upgrading, we use id generated from names initially
        UUID cfId = result.has("cf_id")
                  ? result.getUUID("cf_id")
                  : CFMetaData.generateLegacyCfId(ksName, cfName);

        CFMetaData cfm = new CFMetaData(ksName, cfName, cfType, comparator, cfId);
#endif
        builder.set_is_dense(is_dense);

        if (table_row.has("read_repair_chance")) {
            builder.set_read_repair_chance(table_row.get_nonnull<double>("read_repair_chance"));
        }

        if (table_row.has("local_read_repair_chance")) {
            builder.set_dc_local_read_repair_chance(table_row.get_nonnull<double>("local_read_repair_chance"));
        }

        if (table_row.has("gc_grace_seconds")) {
            builder.set_gc_grace_seconds(table_row.get_nonnull<int32_t>("gc_grace_seconds"));
        }

        if (table_row.has("default_validator")) {
            builder.set_default_validator(parse_type(table_row.get_nonnull<sstring>("default_validator")));
        }

        if (table_row.has("min_compaction_threshold")) {
            builder.set_min_compaction_threshold(table_row.get_nonnull<int>("min_compaction_threshold"));
        }

        if (table_row.has("max_compaction_threshold")) {
            builder.set_max_compaction_threshold(table_row.get_nonnull<int>("max_compaction_threshold"));
        }

#if 0
        if (result.has("comment"))
            cfm.comment(result.getString("comment"));
#endif
        if (table_row.has("memtable_flush_period_in_ms")) {
            builder.set_memtable_flush_period(table_row.get_nonnull<int32_t>("memtable_flush_period_in_ms"));
        }
#if 0
        cfm.caching(CachingOptions.fromString(result.getString("caching")));
#endif
        if (table_row.has("default_time_to_live")) {
            builder.set_default_time_to_live(gc_clock::duration(table_row.get_nonnull<gc_clock::rep>("default_time_to_live")));
        }

        if (table_row.has("speculative_retry")) {
            builder.set_speculative_retry(table_row.get_nonnull<sstring>("speculative_retry"));
        }

        if (table_row.has("compaction_strategy")) {
            auto strategy = table_row.get_nonnull<sstring>("compression_strategy_class");
            builder.set_compaction_strategy(sstables::compaction_strategy::type(strategy));
        }

        if (table_row.has("compaction_strategy_options")) {
            builder.set_compaction_strategy_options(json::to_map(table_row.get_nonnull<sstring>("compaction_strategy_options")));
        }

        auto comp_param = table_row.get_nonnull<sstring>("compression_parameters");
        compression_parameters cp(json::to_map(comp_param));
        builder.set_compressor_params(cp);

        if (table_row.has("min_index_interval")) {
            builder.set_min_index_interval(table_row.get_nonnull<int>("min_index_interval"));
        }

        if (table_row.has("max_index_interval")) {
            builder.set_max_index_interval(table_row.get_nonnull<int>("max_index_interval"));
        }

        if (table_row.has("bloom_filter_fp_chance")) {
            builder.set_bloom_filter_fp_chance(table_row.get_nonnull<double>("bloom_filter_fp_chance"));
        } else {
            builder.set_bloom_filter_fp_chance(builder.get_bloom_filter_fp_chance());
        }

#if 0
        if (result.has("dropped_columns"))
            cfm.droppedColumns(convertDroppedColumns(result.getMap("dropped_columns", UTF8Type.instance, LongType.instance)));
#endif
        for (auto&& cdef : column_defs) {
            builder.with_column(cdef);
        }
    }

#if 0
    private static Map<ColumnIdentifier, Long> convertDroppedColumns(Map<String, Long> raw)
    {
        Map<ColumnIdentifier, Long> converted = Maps.newHashMap();
        for (Map.Entry<String, Long> entry : raw.entrySet())
            converted.put(new ColumnIdentifier(entry.getKey(), true), entry.getValue());
        return converted;
    }
#endif

    /*
     * Column metadata serialization/deserialization.
     */

    void add_column_to_schema_mutation(schema_ptr table,
                                       const column_definition& column,
                                       api::timestamp_type timestamp,
                                       const partition_key& pkey,
                                       std::vector<mutation>& mutations)
    {
        schema_ptr s = columns();
        mutation m{pkey, s};
        auto ckey = clustering_key::from_exploded(*s, {to_bytes(table->cf_name()), column.name()});
        m.set_clustered_cell(ckey, "validator", column.type->name(), timestamp);
        m.set_clustered_cell(ckey, "type", serialize_kind(column.kind), timestamp);
        if (!column.is_on_all_components()) {
            m.set_clustered_cell(ckey, "component_index", int32_t(column.position()), timestamp);
        }
#if 0
        adder.add("index_name", column.getIndexName());
        adder.add("index_type", column.getIndexType() == null ? null : column.getIndexType().toString());
        adder.add("index_options", json(column.getIndexOptions()));
#endif
        mutations.emplace_back(std::move(m));
    }

    sstring serialize_kind(column_kind kind)
    {
        switch (kind) {
        case column_kind::partition_key:  return "partition_key";
        case column_kind::clustering_key: return "clustering_key";
        case column_kind::static_column:  return "static";
        case column_kind::regular_column: return "regular";
        case column_kind::compact_column: return "compact_value";
        default:                          throw std::invalid_argument("unknown column kind");
        }
    }

    column_kind deserialize_kind(sstring kind) {
        if (kind == "partition_key") {
            return column_kind::partition_key;
        } else if (kind == "clustering_key") {
            return column_kind::clustering_key;
        } else if (kind == "static") {
            return column_kind::static_column;
        } else if (kind == "regular") {
            return column_kind::regular_column;
        } else if (kind == "compact_value") {
            return column_kind::compact_column;
        } else {
            throw std::invalid_argument("unknown column kind: " + kind);
        }
    }

#if 0
    private static void dropColumnFromSchemaMutation(CFMetaData table, ColumnDefinition column, long timestamp, Mutation mutation)
    {
        ColumnFamily cells = mutation.addOrGet(Columns);
        int ldt = (int) (System.currentTimeMillis() / 1000);

        // Note: we do want to use name.toString(), not name.bytes directly for backward compatibility (For CQL3, this won't make a difference).
        Composite prefix = Columns.comparator.make(table.cfName, column.name.toString());
        cells.addAtom(new RangeTombstone(prefix, prefix.end(), timestamp, ldt));
    }
#endif

    std::vector<column_definition> create_columns_from_column_rows(const schema_result::mapped_type& rows,
                                                                   const sstring& keyspace,
                                                                   const sstring& table, /*,
                                                                   AbstractType<?> rawComparator, */
                                                                   bool is_super)
    {
        std::vector<column_definition> columns;
        for (auto&& row : rows->rows()) {
            columns.emplace_back(std::move(create_column_from_column_row(row, keyspace, table, /*, rawComparator, */ is_super)));
        }
        return columns;
    }

    column_definition create_column_from_column_row(const query::result_set_row& row,
                                                sstring keyspace,
                                                sstring table, /*,
                                                AbstractType<?> rawComparator, */
                                                bool is_super)
    {
        auto kind = deserialize_kind(row.get_nonnull<sstring>("type"));

        column_id component_index = 0;
        if (row.has("component_index")) {
	    // FIXME: We need to pass component_index to schema_builder
	    // to ensure columns are instantiated in the correct order.
	    component_index = row.get_nonnull<int32_t>("component_index");
        }
#if 0
        else if (kind == ColumnDefinition.Kind.CLUSTERING_COLUMN && isSuper)
            componentIndex = 1; // A ColumnDefinition for super columns applies to the column component
#endif

#if 0
        // Note: we save the column name as string, but we should not assume that it is an UTF8 name, we
        // we need to use the comparator fromString method
        AbstractType<?> comparator = kind == ColumnDefinition.Kind.REGULAR
                                   ? getComponentComparator(rawComparator, componentIndex)
                                   : UTF8Type.instance;
#endif
        auto name_opt = row.get<sstring>("column_name");
        sstring name = name_opt ? *name_opt : sstring();

        auto validator = parse_type(row.get_nonnull<sstring>("validator"));

#if 0
        IndexType indexType = null;
        if (row.has("index_type"))
            indexType = IndexType.valueOf(row.getString("index_type"));

        Map<String, String> indexOptions = null;
        if (row.has("index_options"))
            indexOptions = fromJsonMap(row.getString("index_options"));

        String indexName = null;
        if (row.has("index_name"))
            indexName = row.getString("index_name");
#endif
        auto c = column_definition{to_bytes(name), validator, kind, component_index};
        return c;
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
            logger.error(String.format("Cannot load function '%s' from schema: this function won't be available (on this node)", name), e);
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
        keyspaces(), columnfamilies(), columns(), triggers(), usertypes(), functions(), aggregates()
    };
}

} // namespace schema_tables
} // namespace schema
