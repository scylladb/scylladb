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
package org.apache.cassandra.schema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.CachingOptions;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;
import static org.apache.cassandra.utils.FBUtilities.fromJsonMap;
import static org.apache.cassandra.utils.FBUtilities.json;

/** system.schema_* tables used to store keyspace/table/type attributes prior to C* 3.0 */
public class LegacySchemaTables
{
    private static final Logger logger = LoggerFactory.getLogger(LegacySchemaTables.class);

    public static final String KEYSPACES = "schema_keyspaces";
    public static final String COLUMNFAMILIES = "schema_columnfamilies";
    public static final String COLUMNS = "schema_columns";
    public static final String TRIGGERS = "schema_triggers";
    public static final String USERTYPES = "schema_usertypes";
    public static final String FUNCTIONS = "schema_functions";
    public static final String AGGREGATES = "schema_aggregates";

    public static final List<String> ALL = Arrays.asList(KEYSPACES, COLUMNFAMILIES, COLUMNS, TRIGGERS, USERTYPES, FUNCTIONS, AGGREGATES);

    private static final CFMetaData Keyspaces =
        compile(KEYSPACES,
                "keyspace definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "durable_writes boolean,"
                + "strategy_class text,"
                + "strategy_options text,"
                + "PRIMARY KEY ((keyspace_name))) "
                + "WITH COMPACT STORAGE");

    private static final CFMetaData Columnfamilies =
        compile(COLUMNFAMILIES,
                "table definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "columnfamily_name text,"
                + "bloom_filter_fp_chance double,"
                + "caching text,"
                + "cf_id uuid," // post-2.1 UUID cfid
                + "comment text,"
                + "compaction_strategy_class text,"
                + "compaction_strategy_options text,"
                + "comparator text,"
                + "compression_parameters text,"
                + "default_time_to_live int,"
                + "default_validator text,"
                + "dropped_columns map<text, bigint>,"
                + "gc_grace_seconds int,"
                + "is_dense boolean,"
                + "key_validator text,"
                + "local_read_repair_chance double,"
                + "max_compaction_threshold int,"
                + "max_index_interval int,"
                + "memtable_flush_period_in_ms int,"
                + "min_compaction_threshold int,"
                + "min_index_interval int,"
                + "read_repair_chance double,"
                + "speculative_retry text,"
                + "subcomparator text,"
                + "type text,"
                + "PRIMARY KEY ((keyspace_name), columnfamily_name))");

    private static final CFMetaData Columns =
        compile(COLUMNS,
                "column definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "columnfamily_name text,"
                + "column_name text,"
                + "component_index int,"
                + "index_name text,"
                + "index_options text,"
                + "index_type text,"
                + "type text,"
                + "validator text,"
                + "PRIMARY KEY ((keyspace_name), columnfamily_name, column_name))");

    private static final CFMetaData Triggers =
        compile(TRIGGERS,
                "trigger definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "columnfamily_name text,"
                + "trigger_name text,"
                + "trigger_options map<text, text>,"
                + "PRIMARY KEY ((keyspace_name), columnfamily_name, trigger_name))");

    private static final CFMetaData Usertypes =
        compile(USERTYPES,
                "user defined type definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "type_name text,"
                + "field_names list<text>,"
                + "field_types list<text>,"
                + "PRIMARY KEY ((keyspace_name), type_name))");

    private static final CFMetaData Functions =
        compile(FUNCTIONS,
                "user defined function definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "function_name text,"
                + "signature blob,"
                + "argument_names list<text>,"
                + "argument_types list<text>,"
                + "body text,"
                + "is_deterministic boolean,"
                + "language text,"
                + "return_type text,"
                + "PRIMARY KEY ((keyspace_name), function_name, signature))");

    private static final CFMetaData Aggregates =
        compile(AGGREGATES,
                "user defined aggregate definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "aggregate_name text,"
                + "signature blob,"
                + "argument_types list<text>,"
                + "final_func text,"
                + "initcond blob,"
                + "return_type text,"
                + "state_func text,"
                + "state_type text,"
                + "PRIMARY KEY ((keyspace_name), aggregate_name, signature))");

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

    private static Map<DecoratedKey, ColumnFamily> readSchemaForKeyspaces(String schemaTableName, Set<String> keyspaceNames)
    {
        Map<DecoratedKey, ColumnFamily> schema = new HashMap<>();

        for (String keyspaceName : keyspaceNames)
        {
            Row schemaEntity = readSchemaPartitionForKeyspace(schemaTableName, keyspaceName);
            if (schemaEntity.cf != null)
                schema.put(schemaEntity.key, schemaEntity.cf);
        }

        return schema;
    }

    private static ByteBuffer getSchemaKSKey(String ksName)
    {
        return AsciiType.instance.fromString(ksName);
    }

    private static Row readSchemaPartitionForKeyspace(String schemaTableName, String keyspaceName)
    {
        DecoratedKey keyspaceKey = StorageService.getPartitioner().decorateKey(getSchemaKSKey(keyspaceName));
        return readSchemaPartitionForKeyspace(schemaTableName, keyspaceKey);
    }

    private static Row readSchemaPartitionForKeyspace(String schemaTableName, DecoratedKey keyspaceKey)
    {
        QueryFilter filter = QueryFilter.getIdentityFilter(keyspaceKey, schemaTableName, System.currentTimeMillis());
        return new Row(keyspaceKey, getSchemaCFS(schemaTableName).getColumnFamily(filter));
    }

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

    /**
     * Merge remote schema in form of mutations with local and mutate ks/cf metadata objects
     * (which also involves fs operations on add/drop ks/cf)
     *
     * @param mutations the schema changes to apply
     *
     * @throws ConfigurationException If one of metadata attributes has invalid value
     * @throws IOException If data was corrupted during transportation or failed to apply fs operations
     */
    public static synchronized void mergeSchema(Collection<Mutation> mutations) throws ConfigurationException, IOException
    {
        mergeSchema(mutations, true);
        Schema.instance.updateVersionAndAnnounce();
    }

    public static synchronized void mergeSchema(Collection<Mutation> mutations, boolean doFlush) throws IOException
    {
        // compare before/after schemas of the affected keyspaces only
        Set<String> keyspaces = new HashSet<>(mutations.size());
        for (Mutation mutation : mutations)
            keyspaces.add(ByteBufferUtil.string(mutation.key()));

        // current state of the schema
        Map<DecoratedKey, ColumnFamily> oldKeyspaces = readSchemaForKeyspaces(KEYSPACES, keyspaces);
        Map<DecoratedKey, ColumnFamily> oldColumnFamilies = readSchemaForKeyspaces(COLUMNFAMILIES, keyspaces);
        Map<DecoratedKey, ColumnFamily> oldTypes = readSchemaForKeyspaces(USERTYPES, keyspaces);
        Map<DecoratedKey, ColumnFamily> oldFunctions = readSchemaForKeyspaces(FUNCTIONS, keyspaces);
        Map<DecoratedKey, ColumnFamily> oldAggregates = readSchemaForKeyspaces(AGGREGATES, keyspaces);

        for (Mutation mutation : mutations)
            mutation.apply();

        if (doFlush)
            flushSchemaTables();

        // with new data applied
        Map<DecoratedKey, ColumnFamily> newKeyspaces = readSchemaForKeyspaces(KEYSPACES, keyspaces);
        Map<DecoratedKey, ColumnFamily> newColumnFamilies = readSchemaForKeyspaces(COLUMNFAMILIES, keyspaces);
        Map<DecoratedKey, ColumnFamily> newTypes = readSchemaForKeyspaces(USERTYPES, keyspaces);
        Map<DecoratedKey, ColumnFamily> newFunctions = readSchemaForKeyspaces(FUNCTIONS, keyspaces);
        Map<DecoratedKey, ColumnFamily> newAggregates = readSchemaForKeyspaces(AGGREGATES, keyspaces);

        Set<String> keyspacesToDrop = mergeKeyspaces(oldKeyspaces, newKeyspaces);
        mergeTables(oldColumnFamilies, newColumnFamilies);
        mergeTypes(oldTypes, newTypes);
        mergeFunctions(oldFunctions, newFunctions);
        mergeAggregates(oldAggregates, newAggregates);

        // it is safe to drop a keyspace only when all nested ColumnFamilies where deleted
        for (String keyspaceToDrop : keyspacesToDrop)
            Schema.instance.dropKeyspace(keyspaceToDrop);
    }

    private static Set<String> mergeKeyspaces(Map<DecoratedKey, ColumnFamily> before, Map<DecoratedKey, ColumnFamily> after)
    {
        List<Row> created = new ArrayList<>();
        List<String> altered = new ArrayList<>();
        Set<String> dropped = new HashSet<>();

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
        MapDifference<DecoratedKey, ColumnFamily> diff = Maps.difference(before, after);

        for (Map.Entry<DecoratedKey, ColumnFamily> entry : diff.entriesOnlyOnRight().entrySet())
            if (entry.getValue().hasColumns())
                created.add(new Row(entry.getKey(), entry.getValue()));

        for (Map.Entry<DecoratedKey, MapDifference.ValueDifference<ColumnFamily>> entry : diff.entriesDiffering().entrySet())
        {
            String keyspaceName = AsciiType.instance.compose(entry.getKey().getKey());

            ColumnFamily pre  = entry.getValue().leftValue();
            ColumnFamily post = entry.getValue().rightValue();

            if (pre.hasColumns() && post.hasColumns())
                altered.add(keyspaceName);
            else if (pre.hasColumns())
                dropped.add(keyspaceName);
            else if (post.hasColumns()) // a (re)created keyspace
                created.add(new Row(entry.getKey(), post));
        }

        for (Row row : created)
            Schema.instance.addKeyspace(createKeyspaceFromSchemaPartition(row));
        for (String name : altered)
            Schema.instance.updateKeyspace(name);
        return dropped;
    }

    // see the comments for mergeKeyspaces()
    private static void mergeTables(Map<DecoratedKey, ColumnFamily> before, Map<DecoratedKey, ColumnFamily> after)
    {
        List<CFMetaData> created = new ArrayList<>();
        List<CFMetaData> altered = new ArrayList<>();
        List<CFMetaData> dropped = new ArrayList<>();

        MapDifference<DecoratedKey, ColumnFamily> diff = Maps.difference(before, after);

        for (Map.Entry<DecoratedKey, ColumnFamily> entry : diff.entriesOnlyOnRight().entrySet())
            if (entry.getValue().hasColumns())
                created.addAll(createTablesFromTablesPartition(new Row(entry.getKey(), entry.getValue())).values());

        for (Map.Entry<DecoratedKey, MapDifference.ValueDifference<ColumnFamily>> entry : diff.entriesDiffering().entrySet())
        {
            String keyspaceName = AsciiType.instance.compose(entry.getKey().getKey());

            ColumnFamily pre  = entry.getValue().leftValue();
            ColumnFamily post = entry.getValue().rightValue();

            if (pre.hasColumns() && post.hasColumns())
            {
                MapDifference<String, CFMetaData> delta =
                    Maps.difference(Schema.instance.getKSMetaData(keyspaceName).cfMetaData(),
                                    createTablesFromTablesPartition(new Row(entry.getKey(), post)));

                dropped.addAll(delta.entriesOnlyOnLeft().values());
                created.addAll(delta.entriesOnlyOnRight().values());
                Iterables.addAll(altered, Iterables.transform(delta.entriesDiffering().values(), new Function<MapDifference.ValueDifference<CFMetaData>, CFMetaData>()
                {
                    public CFMetaData apply(MapDifference.ValueDifference<CFMetaData> pair)
                    {
                        return pair.rightValue();
                    }
                }));
            }
            else if (pre.hasColumns())
            {
                dropped.addAll(Schema.instance.getKSMetaData(keyspaceName).cfMetaData().values());
            }
            else if (post.hasColumns())
            {
                created.addAll(createTablesFromTablesPartition(new Row(entry.getKey(), post)).values());
            }
        }

        for (CFMetaData cfm : created)
            Schema.instance.addTable(cfm);
        for (CFMetaData cfm : altered)
            Schema.instance.updateTable(cfm.ksName, cfm.cfName);
        for (CFMetaData cfm : dropped)
            Schema.instance.dropTable(cfm.ksName, cfm.cfName);
    }

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

    /*
     * Keyspace metadata serialization/deserialization.
     */

    public static Mutation makeCreateKeyspaceMutation(KSMetaData keyspace, long timestamp)
    {
        return makeCreateKeyspaceMutation(keyspace, timestamp, true);
    }

    private static Mutation makeCreateKeyspaceMutation(KSMetaData keyspace, long timestamp, boolean withTablesAndTypesAndFunctions)
    {
        Mutation mutation = new Mutation(SystemKeyspace.NAME, getSchemaKSKey(keyspace.name));
        ColumnFamily cells = mutation.addOrGet(Keyspaces);
        CFRowAdder adder = new CFRowAdder(cells, Keyspaces.comparator.builder().build(), timestamp);

        adder.add("durable_writes", keyspace.durableWrites);
        adder.add("strategy_class", keyspace.strategyClass.getName());
        adder.add("strategy_options", json(keyspace.strategyOptions));

        if (withTablesAndTypesAndFunctions)
        {
            for (UserType type : keyspace.userTypes.getAllTypes().values())
                addTypeToSchemaMutation(type, timestamp, mutation);

            for (CFMetaData table : keyspace.cfMetaData().values())
                addTableToSchemaMutation(table, timestamp, true, mutation);
        }

        return mutation;
    }

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

    /**
     * Deserialize only Keyspace attributes without nested tables or types
     *
     * @param partition Keyspace attributes in serialized form
     */
    private static KSMetaData createKeyspaceFromSchemaPartition(Row partition)
    {
        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, KEYSPACES);
        UntypedResultSet.Row row = QueryProcessor.resultify(query, partition).one();
        try
        {
            return new KSMetaData(row.getString("keyspace_name"),
                                  AbstractReplicationStrategy.getClass(row.getString("strategy_class")),
                                  fromJsonMap(row.getString("strategy_options")),
                                  row.getBoolean("durable_writes"));
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException(e);
        }
    }

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

    /*
     * Table metadata serialization/deserialization.
     */

    public static Mutation makeCreateTableMutation(KSMetaData keyspace, CFMetaData table, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace, timestamp, false);
        addTableToSchemaMutation(table, timestamp, true, mutation);
        return mutation;
    }

    private static void addTableToSchemaMutation(CFMetaData table, long timestamp, boolean withColumnsAndTriggers, Mutation mutation)
    {
        // For property that can be null (and can be changed), we insert tombstones, to make sure
        // we don't keep a property the user has removed
        ColumnFamily cells = mutation.addOrGet(Columnfamilies);
        Composite prefix = Columnfamilies.comparator.make(table.cfName);
        CFRowAdder adder = new CFRowAdder(cells, prefix, timestamp);

        adder.add("cf_id", table.cfId);
        adder.add("type", table.cfType.toString());

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

        adder.add("bloom_filter_fp_chance", table.getBloomFilterFpChance());
        adder.add("caching", table.getCaching().toString());
        adder.add("comment", table.getComment());
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
    }

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

    /**
     * Deserialize tables from low-level schema representation, all of them belong to the same keyspace
     *
     * @return map containing name of the table and its metadata for faster lookup
     */
    private static Map<String, CFMetaData> createTablesFromTablesPartition(Row partition)
    {
        if (partition.cf == null)
            return Collections.emptyMap();

        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, COLUMNFAMILIES);
        Map<String, CFMetaData> tables = new HashMap<>();
        for (UntypedResultSet.Row row : QueryProcessor.resultify(query, partition))
        {
            CFMetaData cfm = createTableFromTableRow(row);
            tables.put(cfm.cfName, cfm);
        }
        return tables;
    }

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

    /**
     * Deserialize table metadata from low-level representation
     *
     * @return Metadata deserialized from schema
     */
    private static CFMetaData createTableFromTableRow(UntypedResultSet.Row result)
    {
        String ksName = result.getString("keyspace_name");
        String cfName = result.getString("columnfamily_name");

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

        return cfm;
    }

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
