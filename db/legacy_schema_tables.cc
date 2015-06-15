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

#include "utils/UUID_gen.hh"
#include "legacy_schema_tables.hh"

#include "dht/i_partitioner.hh"
#include "system_keyspace.hh"

#include "query-result-set.hh"
#include "schema_builder.hh"
#include "map_difference.hh"

#include "core/do_with.hh"
#include "core/thread.hh"
#include "json.hh"

#include "db/config.hh"

#include <boost/range/algorithm/copy.hpp>
#include <boost/range/adaptor/map.hpp>

using namespace db::system_keyspace;

/** system.schema_* tables used to store keyspace/table/type attributes prior to C* 3.0 */
namespace db {
namespace legacy_schema_tables {

std::vector<const char*> ALL { KEYSPACES, COLUMNFAMILIES, COLUMNS, TRIGGERS, USERTYPES, FUNCTIONS, AGGREGATES };

#if 0
    private static final Logger logger = LoggerFactory.getLogger(LegacySchemaTables.class);
#endif

/* static */ schema_ptr keyspaces() {
    static thread_local auto keyspaces = make_lw_shared(schema(generate_legacy_id(NAME, KEYSPACES), NAME, KEYSPACES,
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
        // FIXME: the original Java code also had:
        // in CQL statement creating the table:
        //    "WITH COMPACT STORAGE"
        // operations on resulting CFMetaData:
        //    .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(7));
        ));
    return keyspaces;
}

/* static */ schema_ptr columnfamilies() {
    static thread_local auto columnfamilies = make_lw_shared(schema(generate_legacy_id(NAME, COLUMNFAMILIES), NAME, COLUMNFAMILIES,
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
            //    + "dropped_columns map<text, bigint>," // FIXME: add this type
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
        // FIXME: the original Java code also had:
        // operations on resulting CFMetaData:
        //    .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(7));
        ));
    return columnfamilies;
}

/* static */ schema_ptr columns() {
    static thread_local auto columns = make_lw_shared(schema(generate_legacy_id(NAME, COLUMNS), NAME, COLUMNS,
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
        // FIXME: the original Java code also had:
        // operations on resulting CFMetaData:
        //    .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(7));
        ));
    return columns;
}

/* static */ schema_ptr triggers() {
    static thread_local auto triggers = make_lw_shared(schema(generate_legacy_id(NAME, TRIGGERS), NAME, TRIGGERS,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"columnfamily_name", utf8_type}, {"trigger_name", utf8_type}},
        // regular columns
        {
            // FIXME: Cassandra had this:
            // "trigger_options map<text, text>,"
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "trigger definitions"
        // FIXME: the original Java code also had:
        // operations on resulting CFMetaData:
        //    .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(7));
        ));
    return triggers;
}

/* static */ schema_ptr usertypes() {
    static thread_local auto usertypes = make_lw_shared(schema(generate_legacy_id(NAME, USERTYPES), NAME, USERTYPES,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"type_name", utf8_type}},
        // regular columns
        {
            // FIXME: Cassandra had this:
            // "field_names list<text>,"
            // "field_types list<text>,"
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "user defined type definitions"
        // FIXME: the original Java code also had:
        // operations on resulting CFMetaData:
        //    .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(7));
        ));
    return usertypes;
}

/* static */ schema_ptr functions() {
    static thread_local auto functions = make_lw_shared(schema(generate_legacy_id(NAME, FUNCTIONS), NAME, FUNCTIONS,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"function_name", utf8_type}, {"signature", bytes_type}},
        // regular columns
        {
            // FIXME: Cassandra had this:
            // "argument_names list<text>,"
            // "argument_types list<text>,"
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
        // FIXME: the original Java code also had:
        // operations on resulting CFMetaData:
        //    .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(7));
        ));
    return functions;
}

/* static */ schema_ptr aggregates() {
    static thread_local auto aggregates = make_lw_shared(schema(generate_legacy_id(NAME, AGGREGATES), NAME, AGGREGATES,
        // partition key
        {{"keyspace_name", utf8_type}},
        // clustering key
        {{"aggregate_name", utf8_type}, {"signature", bytes_type}},
        // regular columns
        {
            // FIXME: Cassandra had this:
            // "argument_types list<text>,"
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
        // FIXME: the original Java code also had:
        // operations on resulting CFMetaData:
        //    .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(7));
        ));
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

    /** add entries to system.schema_* for the hardcoded system definitions */
    public static void saveSystemKeyspaceSchema()
    {
        KSMetaData keyspace = Schema.instance.getKSMetaData(SystemKeyspace.NAME);
        // delete old, possibly obsolete entries in schema tables
        for (String table : ALL)
            executeOnceInternal(String.format("DELETE FROM system.%s WHERE keyspace_name = ?", table), keyspace.name);
        // (+1 to timestamp to make sure we don't get shadowed by the tombstones we just added)
        makeCreateKeyspaceMutation(keyspace, FBUtilities.timestampMicros() + 1).apply();
    }

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

    /**
     * Read schema from system keyspace and calculate MD5 digest of every row, resulting digest
     * will be converted into UUID which would act as content-based version of the schema.
     */
    public static UUID calculateSchemaDigest()
    {
        MessageDigest digest;
        try
        {
            digest = MessageDigest.getInstance("MD5");
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new RuntimeException(e);
        }

        for (String table : ALL)
        {
            for (Row partition : getSchemaPartitionsForTable(table))
            {
                if (isEmptySchemaPartition(partition) || isSystemKeyspaceSchemaPartition(partition))
                    continue;

                // we want to digest only live columns
                ColumnFamilyStore.removeDeletedColumnsOnly(partition.cf, Integer.MAX_VALUE, SecondaryIndexManager.nullUpdater);
                partition.cf.purgeTombstones(Integer.MAX_VALUE);
                partition.cf.updateDigest(digest);
            }
        }

        return UUID.nameUUIDFromBytes(digest.digest());
    }

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

    public static Collection<Mutation> convertSchemaToMutations()
    {
        Map<DecoratedKey, Mutation> mutationMap = new HashMap<>();

        for (String table : ALL)
            convertSchemaToMutations(mutationMap, table);

        return mutationMap.values();
    }

    private static void convertSchemaToMutations(Map<DecoratedKey, Mutation> mutationMap, String schemaTableName)
    {
        for (Row partition : getSchemaPartitionsForTable(schemaTableName))
        {
            if (isSystemKeyspaceSchemaPartition(partition))
                continue;

            Mutation mutation = mutationMap.get(partition.key);
            if (mutation == null)
            {
                mutation = new Mutation(SystemKeyspace.NAME, partition.key.getKey());
                mutationMap.put(partition.key, mutation);
            }

            mutation.add(partition.cf);
        }
    }
#endif

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
        return proxy.query_local(system_keyspace::NAME, schema_table_name, keyspace_key).then([keyspace_name] (auto&& rs) {
            return schema_result::value_type{keyspace_name, std::move(rs)};
        });
    }

#if 0
    private static Row readSchemaPartitionForTable(String schemaTableName, String keyspaceName, String tableName)
    {
        DecoratedKey key = StorageService.getPartitioner().decorateKey(getSchemaKSKey(keyspaceName));
        ColumnFamilyStore store = getSchemaCFS(schemaTableName);
        Composite prefix = store.getComparator().make(tableName);
        ColumnFamily cells = store.getColumnFamily(key, prefix, prefix.end(), false, Integer.MAX_VALUE, System.currentTimeMillis());
        return new Row(key, cells);
    }

    private static boolean isEmptySchemaPartition(Row partition)
    {
        return partition.cf == null || (partition.cf.isMarkedForDelete() && !partition.cf.hasColumns());
    }

    private static boolean isSystemKeyspaceSchemaPartition(Row partition)
    {
        return getSchemaKSKey(SystemKeyspace.NAME).equals(partition.key.getKey());
    }
#endif

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
        return merge_schema(proxy, std::move(mutations), true).then([] {
#if 0
            Schema.instance.updateVersionAndAnnounce();
#endif
            return make_ready_future<>();
        });
    }

    future<> merge_schema(service::storage_proxy& proxy, std::vector<mutation> mutations, bool do_flush)
    {
       return seastar::async([&proxy, mutations = std::move(mutations), do_flush] {
           schema_ptr s = keyspaces();
           // compare before/after schemas of the affected keyspaces only
           std::set<sstring> keyspaces;
           for (auto&& mutation : mutations) {
               keyspaces.emplace(boost::any_cast<sstring>(utf8_type->deserialize(mutation.key().get_component(*s, 0))));
           }

           // current state of the schema
           auto&& old_keyspaces = read_schema_for_keyspaces(proxy, KEYSPACES, keyspaces).get0();
           auto&& old_column_families = read_schema_for_keyspaces(proxy, COLUMNFAMILIES, keyspaces).get0();
           /*auto& old_types = */read_schema_for_keyspaces(proxy, USERTYPES, keyspaces).get0();
           /*auto& old_functions = */read_schema_for_keyspaces(proxy, FUNCTIONS, keyspaces).get0();
           /*auto& old_aggregates = */read_schema_for_keyspaces(proxy, AGGREGATES, keyspaces).get0();

           proxy.mutate_locally(std::move(mutations)).get0();
#if 0
           if (doFlush)
                flushSchemaTables();
#endif
           // with new data applied
           auto&& new_keyspaces = read_schema_for_keyspaces(proxy, KEYSPACES, keyspaces).get0();
           auto&& new_column_families = read_schema_for_keyspaces(proxy, COLUMNFAMILIES, keyspaces).get0();
           /*auto& new_types = */read_schema_for_keyspaces(proxy, USERTYPES, keyspaces).get0();
           /*auto& new_functions = */read_schema_for_keyspaces(proxy, FUNCTIONS, keyspaces).get0();
           /*auto& new_aggregates = */read_schema_for_keyspaces(proxy, AGGREGATES, keyspaces).get0();

           // FIXME: Make the update atomic like in Origin.

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
            return proxy.get_db().invoke_on_all([&proxy, &created, altered = std::move(altered)] (database& db) {
                auto temp_vec_ptr = make_lw_shared<std::vector<std::pair<lw_shared_ptr<keyspace_metadata>, std::unique_ptr<keyspace>>>>();
                return do_for_each(created,
                        [&db, temp_vec_ptr] (auto&& val) {

                    auto ksm = create_keyspace_from_schema_partition(val);
                    std::unique_ptr<keyspace>
                        k_ptr(new keyspace(ksm, db.make_keyspace_config(*ksm)));
                    auto fu =  k_ptr->create_replication_strategy(db.get_snitch_name());
                    temp_vec_ptr->emplace_back(ksm, std::move(k_ptr));

                    return fu;
                }).then([&db, temp_vec_ptr] {
                    return do_for_each(*temp_vec_ptr, [&db] (auto&& p_val) {
                        db.add_keyspace(p_val.first->name(), std::move(*p_val.second));
                        return make_ready_future<>();
                    });
                }).then([altered = std::move(altered), &db] () mutable {
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
            return proxy.get_db().invoke_on_all([&before, &after] (database& db) {
                std::vector<schema_ptr> created;
                std::vector<schema_ptr> altered;
                std::vector<schema_ptr> dropped;
                auto diff = difference(before, after, [](const auto& x, const auto& y) -> bool {
                    return *x == *y;
                });
                for (auto&& key : diff.entries_only_on_right) {
                    auto&& value = after[key];
                    if (!value->empty()) {
                        auto&& tables = create_tables_from_tables_partition(value);
                        boost::copy(tables | boost::adaptors::map_values, std::back_inserter(created));
                    }
                }
                for (auto&& key : diff.entries_differing) {
                    sstring keyspace_name = key;

                    auto&& pre  = before[key];
                    auto&& post = after[key];

                    if (!pre->empty() && !post->empty()) {
                        auto before = db.find_keyspace(keyspace_name).metadata()->cf_meta_data();
                        auto after = create_tables_from_tables_partition(post);
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
                        auto tables = create_tables_from_tables_partition(post);
                        boost::copy(tables | boost::adaptors::map_values, std::back_inserter(created));
                    }
                }
                for (auto&& cfm : created) {
                    column_family::config cfg; // FIXME
                    db.add_column_family(std::move(column_family{cfm, cfg}));
                }
                for (auto&& cfm : altered) {
                    db.update_column_family(cfm->ks_name(), cfm->cf_name());
                }
                for (auto&& cfm : dropped) {
                    db.drop_column_family(cfm->ks_name(), cfm->cf_name());
                }
                return make_ready_future<>();
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
#if 0
        m.set_clustered_cell(ckey, "type", table.cfType.toString(), timestamp);
#endif
#if 0
        if (table.isSuper())
        {
            // We need to continue saving the comparator and subcomparator separatly, otherwise
            // we won't know at deserialization if the subcomparator should be taken into account
            // TODO: we should implement an on-start migration if we want to get rid of that.
            adder.add("comparator", table.comparator.subtype(0).toString());
            adder.add("subcomparator", table.comparator.subtype(1).toString());
        }
        else
        {
            adder.add("comparator", table.comparator.toString());
        }
#endif

#if 0
        adder.add("bloom_filter_fp_chance", table.getBloomFilterFpChance());
        adder.add("caching", table.getCaching().toString());
#endif
        m.set_clustered_cell(ckey, "comment", table->comment(), timestamp);
#if 0
        adder.add("compaction_strategy_class", table.compactionStrategyClass.getName());
        adder.add("compaction_strategy_options", json(table.compactionStrategyOptions));
        adder.add("compression_parameters", json(table.compressionParameters.asThriftOptions()));
        adder.add("default_time_to_live", table.getDefaultTimeToLive());
        adder.add("default_validator", table.getDefaultValidator().toString());
        adder.add("gc_grace_seconds", table.getGcGraceSeconds());
        adder.add("key_validator", table.getKeyValidator().toString());
        adder.add("local_read_repair_chance", table.getDcLocalReadRepairChance());
        adder.add("max_compaction_threshold", table.getMaxCompactionThreshold());
        adder.add("max_index_interval", table.getMaxIndexInterval());
        adder.add("memtable_flush_period_in_ms", table.getMemtableFlushPeriod());
        adder.add("min_compaction_threshold", table.getMinCompactionThreshold());
        adder.add("min_index_interval", table.getMinIndexInterval());
        adder.add("read_repair_chance", table.getReadRepairChance());
        adder.add("speculative_retry", table.getSpeculativeRetry().toString());

        for (Map.Entry<ColumnIdentifier, Long> entry : table.getDroppedColumns().entrySet())
            adder.addMapEntry("dropped_columns", entry.getKey().toString(), entry.getValue());

        adder.add("is_dense", table.getIsDense());

        if (withColumnsAndTriggers)
        {
            for (ColumnDefinition column : table.allColumns())
                addColumnToSchemaMutation(table, column, timestamp, mutation);

            for (TriggerDefinition trigger : table.getTriggers().values())
                addTriggerToSchemaMutation(table, trigger, timestamp, mutation);
        }
#endif
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

    public static CFMetaData createTableFromName(String keyspace, String table)
    {
        Row partition = readSchemaPartitionForTable(COLUMNFAMILIES, keyspace, table);

        if (isEmptySchemaPartition(partition))
            throw new RuntimeException(String.format("%s:%s not found in the schema definitions keyspace.", keyspace, table));

        return createTableFromTablePartition(partition);
    }
#endif

    /**
     * Deserialize tables from low-level schema representation, all of them belong to the same keyspace
     *
     * @return map containing name of the table and its metadata for faster lookup
     */
    std::map<sstring, schema_ptr> create_tables_from_tables_partition(const schema_result::mapped_type& result)
    {
        std::map<sstring, schema_ptr> tables{};
        for (auto&& row : result->rows()) {
            auto&& cfm = create_table_from_table_row(row);
            tables.emplace(cfm->cf_name(), std::move(cfm));
        }
        return tables;
    }

#if 0
    public static CFMetaData createTableFromTablePartitionAndColumnsPartition(Row serializedTable, Row serializedColumns)
    {
        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, COLUMNFAMILIES);
        return createTableFromTableRowAndColumnsPartition(QueryProcessor.resultify(query, serializedTable).one(), serializedColumns);
    }

    private static CFMetaData createTableFromTableRowAndColumnsPartition(UntypedResultSet.Row tableRow, Row serializedColumns)
    {
        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, COLUMNS);
        return createTableFromTableRowAndColumnRows(tableRow, QueryProcessor.resultify(query, serializedColumns));
    }

    private static CFMetaData createTableFromTablePartition(Row row)
    {
        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, COLUMNFAMILIES);
        return createTableFromTableRow(QueryProcessor.resultify(query, row).one());
    }
#endif

    /**
     * Deserialize table metadata from low-level representation
     *
     * @return Metadata deserialized from schema
     */
    schema_ptr create_table_from_table_row(const query::result_set_row& row)
    {
        auto ks_name = row.get_nonnull<sstring>("keyspace_name");
        auto cf_name = row.get_nonnull<sstring>("columnfamily_name");
        auto id = row.get_nonnull<utils::UUID>("cf_id");
        schema_builder builder{ks_name, cf_name, id};
#if 0
        // FIXME:
        Row serializedColumns = readSchemaPartitionForTable(COLUMNS, ksName, cfName);
        CFMetaData cfm = createTableFromTableRowAndColumnsPartition(result, serializedColumns);

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
        return builder.build();
    }

#if 0
    public static CFMetaData createTableFromTableRowAndColumnRows(UntypedResultSet.Row result,
                                                                  UntypedResultSet serializedColumnDefinitions)
    {
        try
        {
            String ksName = result.getString("keyspace_name");
            String cfName = result.getString("columnfamily_name");

            AbstractType<?> rawComparator = TypeParser.parse(result.getString("comparator"));
            AbstractType<?> subComparator = result.has("subcomparator") ? TypeParser.parse(result.getString("subcomparator")) : null;
            ColumnFamilyType cfType = ColumnFamilyType.valueOf(result.getString("type"));

            AbstractType<?> fullRawComparator = CFMetaData.makeRawAbstractType(rawComparator, subComparator);

            List<ColumnDefinition> columnDefs = createColumnsFromColumnRows(serializedColumnDefinitions,
                                                                            ksName,
                                                                            cfName,
                                                                            fullRawComparator,
                                                                            cfType == ColumnFamilyType.Super);

            boolean isDense = result.has("is_dense")
                            ? result.getBoolean("is_dense")
                            : CFMetaData.calculateIsDense(fullRawComparator, columnDefs);

            CellNameType comparator = CellNames.fromAbstractType(fullRawComparator, isDense);

            // if we are upgrading, we use id generated from names initially
            UUID cfId = result.has("cf_id")
                      ? result.getUUID("cf_id")
                      : CFMetaData.generateLegacyCfId(ksName, cfName);

            CFMetaData cfm = new CFMetaData(ksName, cfName, cfType, comparator, cfId);
            cfm.isDense(isDense);

            cfm.readRepairChance(result.getDouble("read_repair_chance"));
            cfm.dcLocalReadRepairChance(result.getDouble("local_read_repair_chance"));
            cfm.gcGraceSeconds(result.getInt("gc_grace_seconds"));
            cfm.defaultValidator(TypeParser.parse(result.getString("default_validator")));
            cfm.keyValidator(TypeParser.parse(result.getString("key_validator")));
            cfm.minCompactionThreshold(result.getInt("min_compaction_threshold"));
            cfm.maxCompactionThreshold(result.getInt("max_compaction_threshold"));
            if (result.has("comment"))
                cfm.comment(result.getString("comment"));
            if (result.has("memtable_flush_period_in_ms"))
                cfm.memtableFlushPeriod(result.getInt("memtable_flush_period_in_ms"));
            cfm.caching(CachingOptions.fromString(result.getString("caching")));
            if (result.has("default_time_to_live"))
                cfm.defaultTimeToLive(result.getInt("default_time_to_live"));
            if (result.has("speculative_retry"))
                cfm.speculativeRetry(CFMetaData.SpeculativeRetry.fromString(result.getString("speculative_retry")));
            cfm.compactionStrategyClass(CFMetaData.createCompactionStrategy(result.getString("compaction_strategy_class")));
            cfm.compressionParameters(CompressionParameters.create(fromJsonMap(result.getString("compression_parameters"))));
            cfm.compactionStrategyOptions(fromJsonMap(result.getString("compaction_strategy_options")));

            if (result.has("min_index_interval"))
                cfm.minIndexInterval(result.getInt("min_index_interval"));

            if (result.has("max_index_interval"))
                cfm.maxIndexInterval(result.getInt("max_index_interval"));

            if (result.has("bloom_filter_fp_chance"))
                cfm.bloomFilterFpChance(result.getDouble("bloom_filter_fp_chance"));
            else
                cfm.bloomFilterFpChance(cfm.getBloomFilterFpChance());

            if (result.has("dropped_columns"))
                cfm.droppedColumns(convertDroppedColumns(result.getMap("dropped_columns", UTF8Type.instance, LongType.instance)));

            for (ColumnDefinition cd : columnDefs)
                cfm.addOrReplaceColumnDefinition(cd);

            return cfm.rebuild();
        }
        catch (SyntaxException | ConfigurationException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static Map<ColumnIdentifier, Long> convertDroppedColumns(Map<String, Long> raw)
    {
        Map<ColumnIdentifier, Long> converted = Maps.newHashMap();
        for (Map.Entry<String, Long> entry : raw.entrySet())
            converted.put(new ColumnIdentifier(entry.getKey(), true), entry.getValue());
        return converted;
    }

    /*
     * Column metadata serialization/deserialization.
     */

    private static void addColumnToSchemaMutation(CFMetaData table, ColumnDefinition column, long timestamp, Mutation mutation)
    {
        ColumnFamily cells = mutation.addOrGet(Columns);
        Composite prefix = Columns.comparator.make(table.cfName, column.name.toString());
        CFRowAdder adder = new CFRowAdder(cells, prefix, timestamp);

        adder.add("validator", column.type.toString());
        adder.add("type", serializeKind(column.kind));
        adder.add("component_index", column.isOnAllComponents() ? null : column.position());
        adder.add("index_name", column.getIndexName());
        adder.add("index_type", column.getIndexType() == null ? null : column.getIndexType().toString());
        adder.add("index_options", json(column.getIndexOptions()));
    }

    private static String serializeKind(ColumnDefinition.Kind kind)
    {
        // For backward compatibility we need to special case CLUSTERING_COLUMN
        return kind == ColumnDefinition.Kind.CLUSTERING_COLUMN ? "clustering_key" : kind.toString().toLowerCase();
    }

    private static ColumnDefinition.Kind deserializeKind(String kind)
    {
        if (kind.equalsIgnoreCase("clustering_key"))
            return ColumnDefinition.Kind.CLUSTERING_COLUMN;
        return Enum.valueOf(ColumnDefinition.Kind.class, kind.toUpperCase());
    }

    private static void dropColumnFromSchemaMutation(CFMetaData table, ColumnDefinition column, long timestamp, Mutation mutation)
    {
        ColumnFamily cells = mutation.addOrGet(Columns);
        int ldt = (int) (System.currentTimeMillis() / 1000);

        // Note: we do want to use name.toString(), not name.bytes directly for backward compatibility (For CQL3, this won't make a difference).
        Composite prefix = Columns.comparator.make(table.cfName, column.name.toString());
        cells.addAtom(new RangeTombstone(prefix, prefix.end(), timestamp, ldt));
    }

    private static List<ColumnDefinition> createColumnsFromColumnRows(UntypedResultSet rows,
                                                                      String keyspace,
                                                                      String table,
                                                                      AbstractType<?> rawComparator,
                                                                      boolean isSuper)
    {
        List<ColumnDefinition> columns = new ArrayList<>();
        for (UntypedResultSet.Row row : rows)
            columns.add(createColumnFromColumnRow(row, keyspace, table, rawComparator, isSuper));
        return columns;
    }

    private static ColumnDefinition createColumnFromColumnRow(UntypedResultSet.Row row,
                                                              String keyspace,
                                                              String table,
                                                              AbstractType<?> rawComparator,
                                                              boolean isSuper)
    {
        ColumnDefinition.Kind kind = deserializeKind(row.getString("type"));

        Integer componentIndex = null;
        if (row.has("component_index"))
            componentIndex = row.getInt("component_index");
        else if (kind == ColumnDefinition.Kind.CLUSTERING_COLUMN && isSuper)
            componentIndex = 1; // A ColumnDefinition for super columns applies to the column component

        // Note: we save the column name as string, but we should not assume that it is an UTF8 name, we
        // we need to use the comparator fromString method
        AbstractType<?> comparator = kind == ColumnDefinition.Kind.REGULAR
                                   ? getComponentComparator(rawComparator, componentIndex)
                                   : UTF8Type.instance;
        ColumnIdentifier name = new ColumnIdentifier(comparator.fromString(row.getString("column_name")), comparator);

        AbstractType<?> validator = parseType(row.getString("validator"));

        IndexType indexType = null;
        if (row.has("index_type"))
            indexType = IndexType.valueOf(row.getString("index_type"));

        Map<String, String> indexOptions = null;
        if (row.has("index_options"))
            indexOptions = fromJsonMap(row.getString("index_options"));

        String indexName = null;
        if (row.has("index_name"))
            indexName = row.getString("index_name");

        return new ColumnDefinition(keyspace, table, name, validator, indexType, indexOptions, indexName, componentIndex, kind);
    }

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

    private static AbstractType<?> parseType(String str)
    {
        try
        {
            return TypeParser.parse(str);
        }
        catch (SyntaxException | ConfigurationException e)
        {
            // We only use this when reading the schema where we shouldn't get an error
            throw new RuntimeException(e);
        }
    }
}
#endif

std::vector<schema_ptr> all_tables() {
    return {
        keyspaces(), columnfamilies(), columns(), triggers(), usertypes(), functions(), aggregates()
    };
}

} // namespace legacy_schema_tables
} // namespace schema
