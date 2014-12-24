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
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.restrictions.Restriction;
import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CBuilder;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.Pair;

/*
 * Abstract parent class of individual modifications, i.e. INSERT, UPDATE and DELETE.
 */
public abstract class ModificationStatement implements CQLStatement
{
    private static final ColumnIdentifier CAS_RESULT_COLUMN = new ColumnIdentifier("[applied]", false);

    public static enum StatementType { INSERT, UPDATE, DELETE }
    public final StatementType type;

    private final int boundTerms;
    public final CFMetaData cfm;
    public final Attributes attrs;

    protected final Map<ColumnIdentifier, Restriction> processedKeys = new HashMap<>();
    private final List<Operation> columnOperations = new ArrayList<Operation>();

    // Separating normal and static conditions makes things somewhat easier
    private List<ColumnCondition> columnConditions;
    private List<ColumnCondition> staticConditions;
    private boolean ifNotExists;
    private boolean ifExists;

    private boolean hasNoClusteringColumns = true;

    private boolean setsStaticColumns;
    private boolean setsRegularColumns;

    private final Function<ColumnCondition, ColumnDefinition> getColumnForCondition = new Function<ColumnCondition, ColumnDefinition>()
    {
        public ColumnDefinition apply(ColumnCondition cond)
        {
            return cond.column;
        }
    };

    public ModificationStatement(StatementType type, int boundTerms, CFMetaData cfm, Attributes attrs)
    {
        this.type = type;
        this.boundTerms = boundTerms;
        this.cfm = cfm;
        this.attrs = attrs;
    }

    public boolean usesFunction(String ksName, String functionName)
    {
        if (attrs.usesFunction(ksName, functionName))
            return true;
        for (Restriction restriction : processedKeys.values())
            if (restriction != null && restriction.usesFunction(ksName, functionName))
                return true;
        for (Operation operation : columnOperations)
            if (operation != null && operation.usesFunction(ksName, functionName))
                return true;
        for (ColumnCondition condition : columnConditions)
            if (condition != null && condition.usesFunction(ksName, functionName))
                return true;
        for (ColumnCondition condition : staticConditions)
            if (condition != null && condition.usesFunction(ksName, functionName))
                return true;
        return false;
    }

    public abstract boolean requireFullClusteringKey();
    public abstract void addUpdateForKey(ColumnFamily updates, ByteBuffer key, Composite prefix, UpdateParameters params) throws InvalidRequestException;

    public int getBoundTerms()
    {
        return boundTerms;
    }

    public String keyspace()
    {
        return cfm.ksName;
    }

    public String columnFamily()
    {
        return cfm.cfName;
    }

    public boolean isCounter()
    {
        return cfm.isCounter();
    }

    public long getTimestamp(long now, QueryOptions options) throws InvalidRequestException
    {
        return attrs.getTimestamp(now, options);
    }

    public boolean isTimestampSet()
    {
        return attrs.isTimestampSet();
    }

    public int getTimeToLive(QueryOptions options) throws InvalidRequestException
    {
        return attrs.getTimeToLive(options);
    }

    public void checkAccess(ClientState state) throws InvalidRequestException, UnauthorizedException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.MODIFY);

        // CAS updates can be used to simulate a SELECT query, so should require Permission.SELECT as well.
        if (hasConditions())
            state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.SELECT);
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        if (hasConditions() && attrs.isTimestampSet())
            throw new InvalidRequestException("Cannot provide custom timestamp for conditional updates");

        if (isCounter() && attrs.isTimestampSet())
            throw new InvalidRequestException("Cannot provide custom timestamp for counter updates");

        if (isCounter() && attrs.isTimeToLiveSet())
            throw new InvalidRequestException("Cannot provide custom TTL for counter updates");
    }

    public void addOperation(Operation op)
    {
        if (op.column.isStatic())
            setsStaticColumns = true;
        else
            setsRegularColumns = true;
        columnOperations.add(op);
    }

    public List<Operation> getOperations()
    {
        return columnOperations;
    }

    public Iterable<ColumnDefinition> getColumnsWithConditions()
    {
        if (ifNotExists || ifExists)
            return null;

        return Iterables.concat(columnConditions == null ? Collections.<ColumnDefinition>emptyList() : Iterables.transform(columnConditions, getColumnForCondition),
                                staticConditions == null ? Collections.<ColumnDefinition>emptyList() : Iterables.transform(staticConditions, getColumnForCondition));
    }

    public void addCondition(ColumnCondition cond)
    {
        List<ColumnCondition> conds = null;
        if (cond.column.isStatic())
        {
            setsStaticColumns = true;
            if (staticConditions == null)
                staticConditions = new ArrayList<ColumnCondition>();
            conds = staticConditions;
        }
        else
        {
            setsRegularColumns = true;
            if (columnConditions == null)
                columnConditions = new ArrayList<ColumnCondition>();
            conds = columnConditions;
        }
        conds.add(cond);
    }

    public void setIfNotExistCondition()
    {
        ifNotExists = true;
    }

    public boolean hasIfNotExistCondition()
    {
        return ifNotExists;
    }

    public void setIfExistCondition()
    {
        ifExists = true;
    }

    public boolean hasIfExistCondition()
    {
        return ifExists;
    }

    private void addKeyValues(ColumnDefinition def, Restriction values) throws InvalidRequestException
    {
        if (def.kind == ColumnDefinition.Kind.CLUSTERING_COLUMN)
            hasNoClusteringColumns = false;
        if (processedKeys.put(def.name, values) != null)
            throw new InvalidRequestException(String.format("Multiple definitions found for PRIMARY KEY part %s", def.name));
    }

    public void addKeyValue(ColumnDefinition def, Term value) throws InvalidRequestException
    {
        addKeyValues(def, new SingleColumnRestriction.EQ(def, value));
    }

    public void processWhereClause(List<Relation> whereClause, VariableSpecifications names) throws InvalidRequestException
    {
        for (Relation relation : whereClause)
        {
            if (relation.isMultiColumn())
            {
                throw new InvalidRequestException(
                        String.format("Multi-column relations cannot be used in WHERE clauses for UPDATE and DELETE statements: %s", relation));
            }
            SingleColumnRelation rel = (SingleColumnRelation) relation;

            if (rel.onToken())
                throw new InvalidRequestException(String.format("The token function cannot be used in WHERE clauses for UPDATE and DELETE statements: %s", relation));

            ColumnIdentifier id = rel.getEntity().prepare(cfm);
            ColumnDefinition def = cfm.getColumnDefinition(id);
            if (def == null)
                throw new InvalidRequestException(String.format("Unknown key identifier %s", id));

            switch (def.kind)
            {
                case PARTITION_KEY:
                case CLUSTERING_COLUMN:
                    Restriction restriction;

                    if (rel.isEQ() || (def.isPartitionKey() && rel.isIN()))
                    {
                        restriction = rel.toRestriction(cfm, names);
                    }
                    else
                    {
                        throw new InvalidRequestException(String.format("Invalid operator %s for PRIMARY KEY part %s", rel.operator(), def.name));
                    }

                    addKeyValues(def, restriction);
                    break;
                default:
                    throw new InvalidRequestException(String.format("Non PRIMARY KEY %s found in where clause", def.name));
            }
        }
    }

    public List<ByteBuffer> buildPartitionKeyNames(QueryOptions options)
    throws InvalidRequestException
    {
        CBuilder keyBuilder = cfm.getKeyValidatorAsCType().builder();
        List<ByteBuffer> keys = new ArrayList<ByteBuffer>();
        for (ColumnDefinition def : cfm.partitionKeyColumns())
        {
            Restriction r = processedKeys.get(def.name);
            if (r == null)
                throw new InvalidRequestException(String.format("Missing mandatory PRIMARY KEY part %s", def.name));

            List<ByteBuffer> values = r.values(options);

            if (keyBuilder.remainingCount() == 1)
            {
                for (ByteBuffer val : values)
                {
                    if (val == null)
                        throw new InvalidRequestException(String.format("Invalid null value for partition key part %s", def.name));
                    ByteBuffer key = keyBuilder.buildWith(val).toByteBuffer();
                    ThriftValidation.validateKey(cfm, key);
                    keys.add(key);
                }
            }
            else
            {
                if (values.size() != 1)
                    throw new InvalidRequestException("IN is only supported on the last column of the partition key");
                ByteBuffer val = values.get(0);
                if (val == null)
                    throw new InvalidRequestException(String.format("Invalid null value for partition key part %s", def.name));
                keyBuilder.add(val);
            }
        }
        return keys;
    }

    public Composite createClusteringPrefix(QueryOptions options)
    throws InvalidRequestException
    {
        // If the only updated/deleted columns are static, then we don't need clustering columns.
        // And in fact, unless it is an INSERT, we reject if clustering colums are provided as that
        // suggest something unintended. For instance, given:
        //   CREATE TABLE t (k int, v int, s int static, PRIMARY KEY (k, v))
        // it can make sense to do:
        //   INSERT INTO t(k, v, s) VALUES (0, 1, 2)
        // but both
        //   UPDATE t SET s = 3 WHERE k = 0 AND v = 1
        //   DELETE v FROM t WHERE k = 0 AND v = 1
        // sounds like you don't really understand what your are doing.
        if (setsStaticColumns && !setsRegularColumns)
        {
            // If we set no non-static columns, then it's fine not to have clustering columns
            if (hasNoClusteringColumns)
                return cfm.comparator.staticPrefix();

            // If we do have clustering columns however, then either it's an INSERT and the query is valid
            // but we still need to build a proper prefix, or it's not an INSERT, and then we want to reject
            // (see above)
            if (type != StatementType.INSERT)
            {
                for (ColumnDefinition def : cfm.clusteringColumns())
                    if (processedKeys.get(def.name) != null)
                        throw new InvalidRequestException(String.format("Invalid restriction on clustering column %s since the %s statement modifies only static columns", def.name, type));
                // we should get there as it contradicts hasNoClusteringColumns == false
                throw new AssertionError();
            }
        }

        return createClusteringPrefixBuilderInternal(options);
    }

    private Composite createClusteringPrefixBuilderInternal(QueryOptions options)
    throws InvalidRequestException
    {
        CBuilder builder = cfm.comparator.prefixBuilder();
        ColumnDefinition firstEmptyKey = null;
        for (ColumnDefinition def : cfm.clusteringColumns())
        {
            Restriction r = processedKeys.get(def.name);
            if (r == null)
            {
                firstEmptyKey = def;
                if (requireFullClusteringKey() && !cfm.comparator.isDense() && cfm.comparator.isCompound())
                    throw new InvalidRequestException(String.format("Missing mandatory PRIMARY KEY part %s", def.name));
            }
            else if (firstEmptyKey != null)
            {
                throw new InvalidRequestException(String.format("Missing PRIMARY KEY part %s since %s is set", firstEmptyKey.name, def.name));
            }
            else
            {
                List<ByteBuffer> values = r.values(options);
                assert values.size() == 1; // We only allow IN for row keys so far
                ByteBuffer val = values.get(0);
                if (val == null)
                    throw new InvalidRequestException(String.format("Invalid null value for clustering key part %s", def.name));
                builder.add(val);
            }
        }
        return builder.build();
    }

    protected ColumnDefinition getFirstEmptyKey()
    {
        for (ColumnDefinition def : cfm.clusteringColumns())
        {
            if (processedKeys.get(def.name) == null)
                return def;
        }
        return null;
    }

    public boolean requiresRead()
    {
        // Lists SET operation incurs a read.
        for (Operation op : columnOperations)
            if (op.requiresRead())
                return true;

        return false;
    }

    protected Map<ByteBuffer, CQL3Row> readRequiredRows(Collection<ByteBuffer> partitionKeys, Composite clusteringPrefix, boolean local, ConsistencyLevel cl)
    throws RequestExecutionException, RequestValidationException
    {
        if (!requiresRead())
            return null;

        try
        {
            cl.validateForRead(keyspace());
        }
        catch (InvalidRequestException e)
        {
            throw new InvalidRequestException(String.format("Write operation require a read but consistency %s is not supported on reads", cl));
        }

        ColumnSlice[] slices = new ColumnSlice[]{ clusteringPrefix.slice() };
        List<ReadCommand> commands = new ArrayList<ReadCommand>(partitionKeys.size());
        long now = System.currentTimeMillis();
        for (ByteBuffer key : partitionKeys)
            commands.add(new SliceFromReadCommand(keyspace(),
                                                  key,
                                                  columnFamily(),
                                                  now,
                                                  new SliceQueryFilter(slices, false, Integer.MAX_VALUE)));

        List<Row> rows = local
                       ? SelectStatement.readLocally(keyspace(), commands)
                       : StorageProxy.read(commands, cl);

        Map<ByteBuffer, CQL3Row> map = new HashMap<ByteBuffer, CQL3Row>();
        for (Row row : rows)
        {
            if (row.cf == null || row.cf.isEmpty())
                continue;

            Iterator<CQL3Row> iter = cfm.comparator.CQL3RowBuilder(cfm, now).group(row.cf.getSortedColumns().iterator());
            if (iter.hasNext())
            {
                map.put(row.key.getKey(), iter.next());
                // We can only update one CQ3Row per partition key at a time (we don't allow IN for clustering key)
                assert !iter.hasNext();
            }
        }
        return map;
    }

    public boolean hasConditions()
    {
        return ifNotExists
            || ifExists
            || (columnConditions != null && !columnConditions.isEmpty())
            || (staticConditions != null && !staticConditions.isEmpty());
    }

    public ResultMessage execute(QueryState queryState, QueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        if (options.getConsistency() == null)
            throw new InvalidRequestException("Invalid empty consistency level");

        if (hasConditions() && options.getProtocolVersion() == 1)
            throw new InvalidRequestException("Conditional updates are not supported by the protocol version in use. You need to upgrade to a driver using the native protocol v2.");

        return hasConditions()
             ? executeWithCondition(queryState, options)
             : executeWithoutCondition(queryState, options);
    }

    private ResultMessage executeWithoutCondition(QueryState queryState, QueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        ConsistencyLevel cl = options.getConsistency();
        if (isCounter())
            cl.validateCounterForWrite(cfm);
        else
            cl.validateForWrite(cfm.ksName);

        Collection<? extends IMutation> mutations = getMutations(options, false, options.getTimestamp(queryState));
        if (!mutations.isEmpty())
            StorageProxy.mutateWithTriggers(mutations, cl, false);

        return null;
    }

    public ResultMessage executeWithCondition(QueryState queryState, QueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        List<ByteBuffer> keys = buildPartitionKeyNames(options);
        // We don't support IN for CAS operation so far
        if (keys.size() > 1)
            throw new InvalidRequestException("IN on the partition key is not supported with conditional updates");

        ByteBuffer key = keys.get(0);
        long now = options.getTimestamp(queryState);
        Composite prefix = createClusteringPrefix(options);

        CQL3CasRequest request = new CQL3CasRequest(cfm, key, false);
        addConditions(prefix, request, options);
        request.addRowUpdate(prefix, this, options, now);

        ColumnFamily result = StorageProxy.cas(keyspace(),
                                               columnFamily(),
                                               key,
                                               request,
                                               options.getSerialConsistency(),
                                               options.getConsistency(),
                                               queryState.getClientState());
        return new ResultMessage.Rows(buildCasResultSet(key, result, options));
    }

    public void addConditions(Composite clusteringPrefix, CQL3CasRequest request, QueryOptions options) throws InvalidRequestException
    {
        if (ifNotExists)
        {
            // If we use ifNotExists, if the statement applies to any non static columns, then the condition is on the row of the non-static
            // columns and the prefix should be the clusteringPrefix. But if only static columns are set, then the ifNotExists apply to the existence
            // of any static columns and we should use the prefix for the "static part" of the partition.
            request.addNotExist(clusteringPrefix);
        }
        else if (ifExists)
        {
            request.addExist(clusteringPrefix);
        }
        else
        {
            if (columnConditions != null)
                request.addConditions(clusteringPrefix, columnConditions, options);
            if (staticConditions != null)
                request.addConditions(cfm.comparator.staticPrefix(), staticConditions, options);
        }
    }

    private ResultSet buildCasResultSet(ByteBuffer key, ColumnFamily cf, QueryOptions options) throws InvalidRequestException
    {
        return buildCasResultSet(keyspace(), key, columnFamily(), cf, getColumnsWithConditions(), false, options);
    }

    public static ResultSet buildCasResultSet(String ksName, ByteBuffer key, String cfName, ColumnFamily cf, Iterable<ColumnDefinition> columnsWithConditions, boolean isBatch, QueryOptions options)
    throws InvalidRequestException
    {
        boolean success = cf == null;

        ColumnSpecification spec = new ColumnSpecification(ksName, cfName, CAS_RESULT_COLUMN, BooleanType.instance);
        ResultSet.Metadata metadata = new ResultSet.Metadata(Collections.singletonList(spec));
        List<List<ByteBuffer>> rows = Collections.singletonList(Collections.singletonList(BooleanType.instance.decompose(success)));

        ResultSet rs = new ResultSet(metadata, rows);
        return success ? rs : merge(rs, buildCasFailureResultSet(key, cf, columnsWithConditions, isBatch, options));
    }

    private static ResultSet merge(ResultSet left, ResultSet right)
    {
        if (left.size() == 0)
            return right;
        else if (right.size() == 0)
            return left;

        assert left.size() == 1;
        int size = left.metadata.names.size() + right.metadata.names.size();
        List<ColumnSpecification> specs = new ArrayList<ColumnSpecification>(size);
        specs.addAll(left.metadata.names);
        specs.addAll(right.metadata.names);
        List<List<ByteBuffer>> rows = new ArrayList<>(right.size());
        for (int i = 0; i < right.size(); i++)
        {
            List<ByteBuffer> row = new ArrayList<ByteBuffer>(size);
            row.addAll(left.rows.get(0));
            row.addAll(right.rows.get(i));
            rows.add(row);
        }
        return new ResultSet(new ResultSet.Metadata(specs), rows);
    }

    private static ResultSet buildCasFailureResultSet(ByteBuffer key, ColumnFamily cf, Iterable<ColumnDefinition> columnsWithConditions, boolean isBatch, QueryOptions options)
    throws InvalidRequestException
    {
        CFMetaData cfm = cf.metadata();
        Selection selection;
        if (columnsWithConditions == null)
        {
            selection = Selection.wildcard(cfm);
        }
        else
        {
            // We can have multiple conditions on the same columns (for collections) so use a set
            // to avoid duplicate, but preserve the order just to it follows the order of IF in the query in general
            Set<ColumnDefinition> defs = new LinkedHashSet<>();
            // Adding the partition key for batches to disambiguate if the conditions span multipe rows (we don't add them outside
            // of batches for compatibility sakes).
            if (isBatch)
            {
                defs.addAll(cfm.partitionKeyColumns());
                defs.addAll(cfm.clusteringColumns());
            }
            for (ColumnDefinition def : columnsWithConditions)
                defs.add(def);
            selection = Selection.forColumns(cfm, new ArrayList<>(defs));

        }

        long now = System.currentTimeMillis();
        Selection.ResultSetBuilder builder = selection.resultSetBuilder(now);
        SelectStatement.forSelection(cfm, selection).processColumnFamily(key, cf, options, now, builder);

        return builder.build(options.getProtocolVersion());
    }

    public ResultMessage executeInternal(QueryState queryState, QueryOptions options) throws RequestValidationException, RequestExecutionException
    {
        if (hasConditions())
            throw new UnsupportedOperationException();

        for (IMutation mutation : getMutations(options, true, queryState.getTimestamp()))
        {
            // We don't use counters internally.
            assert mutation instanceof Mutation;

            ((Mutation) mutation).apply();
        }
        return null;
    }

    /**
     * Convert statement into a list of mutations to apply on the server
     *
     * @param options value for prepared statement markers
     * @param local if true, any requests (for collections) performed by getMutation should be done locally only.
     * @param now the current timestamp in microseconds to use if no timestamp is user provided.
     *
     * @return list of the mutations
     * @throws InvalidRequestException on invalid requests
     */
    private Collection<? extends IMutation> getMutations(QueryOptions options, boolean local, long now)
    throws RequestExecutionException, RequestValidationException
    {
        List<ByteBuffer> keys = buildPartitionKeyNames(options);
        Composite clusteringPrefix = createClusteringPrefix(options);

        UpdateParameters params = makeUpdateParameters(keys, clusteringPrefix, options, local, now);

        Collection<IMutation> mutations = new ArrayList<IMutation>(keys.size());
        for (ByteBuffer key: keys)
        {
            ThriftValidation.validateKey(cfm, key);
            ColumnFamily cf = ArrayBackedSortedColumns.factory.create(cfm);
            addUpdateForKey(cf, key, clusteringPrefix, params);
            Mutation mut = new Mutation(cfm.ksName, key, cf);

            mutations.add(isCounter() ? new CounterMutation(mut, options.getConsistency()) : mut);
        }
        return mutations;
    }

    public UpdateParameters makeUpdateParameters(Collection<ByteBuffer> keys,
                                                 Composite prefix,
                                                 QueryOptions options,
                                                 boolean local,
                                                 long now)
    throws RequestExecutionException, RequestValidationException
    {
        // Some lists operation requires reading
        Map<ByteBuffer, CQL3Row> rows = readRequiredRows(keys, prefix, local, options.getConsistency());
        return new UpdateParameters(cfm, options, getTimestamp(now, options), getTimeToLive(options), rows);
    }

    /**
     * If there are conditions on the statement, this is called after the where clause and conditions have been
     * processed to check that they are compatible.
     * @throws InvalidRequestException
     */
    protected void validateWhereClauseForConditions() throws InvalidRequestException
    {
        //  no-op by default
    }

    public static abstract class Parsed extends CFStatement
    {
        protected final Attributes.Raw attrs;
        protected final List<Pair<ColumnIdentifier.Raw, ColumnCondition.Raw>> conditions;
        private final boolean ifNotExists;
        private final boolean ifExists;

        protected Parsed(CFName name, Attributes.Raw attrs, List<Pair<ColumnIdentifier.Raw, ColumnCondition.Raw>> conditions, boolean ifNotExists, boolean ifExists)
        {
            super(name);
            this.attrs = attrs;
            this.conditions = conditions == null ? Collections.<Pair<ColumnIdentifier.Raw, ColumnCondition.Raw>>emptyList() : conditions;
            this.ifNotExists = ifNotExists;
            this.ifExists = ifExists;
        }

        public ParsedStatement.Prepared prepare() throws InvalidRequestException
        {
            VariableSpecifications boundNames = getBoundVariables();
            ModificationStatement statement = prepare(boundNames);
            return new ParsedStatement.Prepared(statement, boundNames);
        }

        public ModificationStatement prepare(VariableSpecifications boundNames) throws InvalidRequestException
        {
            CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace(), columnFamily());

            Attributes preparedAttributes = attrs.prepare(keyspace(), columnFamily());
            preparedAttributes.collectMarkerSpecification(boundNames);

            ModificationStatement stmt = prepareInternal(metadata, boundNames, preparedAttributes);

            if (ifNotExists || ifExists || !conditions.isEmpty())
            {
                if (stmt.isCounter())
                    throw new InvalidRequestException("Conditional updates are not supported on counter tables");

                if (attrs.timestamp != null)
                    throw new InvalidRequestException("Cannot provide custom timestamp for conditional updates");

                if (ifNotExists)
                {
                    // To have both 'IF NOT EXISTS' and some other conditions doesn't make sense.
                    // So far this is enforced by the parser, but let's assert it for sanity if ever the parse changes.
                    assert conditions.isEmpty();
                    assert !ifExists;
                    stmt.setIfNotExistCondition();
                }
                else if (ifExists)
                {
                    assert conditions.isEmpty();
                    assert !ifNotExists;
                    stmt.setIfExistCondition();
                }
                else
                {
                    for (Pair<ColumnIdentifier.Raw, ColumnCondition.Raw> entry : conditions)
                    {
                        ColumnIdentifier id = entry.left.prepare(metadata);
                        ColumnDefinition def = metadata.getColumnDefinition(id);
                        if (def == null)
                            throw new InvalidRequestException(String.format("Unknown identifier %s", id));

                        ColumnCondition condition = entry.right.prepare(keyspace(), def);
                        condition.collectMarkerSpecification(boundNames);

                        switch (def.kind)
                        {
                            case PARTITION_KEY:
                            case CLUSTERING_COLUMN:
                                throw new InvalidRequestException(String.format("PRIMARY KEY column '%s' cannot have IF conditions", id));
                            default:
                                stmt.addCondition(condition);
                                break;
                        }
                    }
                }

                stmt.validateWhereClauseForConditions();
            }
            return stmt;
        }

        protected abstract ModificationStatement prepareInternal(CFMetaData cfm, VariableSpecifications boundNames, Attributes attrs) throws InvalidRequestException;
    }
}
