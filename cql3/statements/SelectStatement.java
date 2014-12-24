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

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.composites.Composites;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.pager.Pageable;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.service.pager.QueryPagers;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * Encapsulates a completely parsed SELECT query, including the target
 * column family, expression, result count, and ordering clause.
 *
 */
public class SelectStatement implements CQLStatement
{
    private static final int DEFAULT_COUNT_PAGE_SIZE = 10000;

    private final int boundTerms;
    public final CFMetaData cfm;
    public final Parameters parameters;
    private final Selection selection;
    private final Term limit;

    private final StatementRestrictions restrictions;

    private final boolean isReversed;

    /**
     * The comparator used to orders results when multiple keys are selected (using IN).
     */
    private final Comparator<List<ByteBuffer>> orderingComparator;

    // Used by forSelection below
    private static final Parameters defaultParameters = new Parameters(Collections.<ColumnIdentifier.Raw, Boolean>emptyMap(), false, false);

    public SelectStatement(CFMetaData cfm,
                           int boundTerms,
                           Parameters parameters,
                           Selection selection,
                           StatementRestrictions restrictions,
                           boolean isReversed,
                           Comparator<List<ByteBuffer>> orderingComparator,
                           Term limit)
    {
        this.cfm = cfm;
        this.boundTerms = boundTerms;
        this.selection = selection;
        this.restrictions = restrictions;
        this.isReversed = isReversed;
        this.orderingComparator = orderingComparator;
        this.parameters = parameters;
        this.limit = limit;
    }

    @Override
    public boolean usesFunction(String ksName, String functionName)
    {
        return selection.usesFunction(ksName, functionName)
                || restrictions.usesFunction(ksName, functionName)
                || (limit != null && limit.usesFunction(ksName, functionName));
    }

    // Creates a simple select based on the given selection.
    // Note that the results select statement should not be used for actual queries, but only for processing already
    // queried data through processColumnFamily.
    static SelectStatement forSelection(CFMetaData cfm, Selection selection)
    {
        return new SelectStatement(cfm,
                                   0,
                                   defaultParameters,
                                   selection,
                                   StatementRestrictions.empty(cfm),
                                   false,
                                   null,
                                   null);
    }

    public ResultSet.Metadata getResultMetadata()
    {
        return selection.getResultMetadata();
    }

    public int getBoundTerms()
    {
        return boundTerms;
    }

    public void checkAccess(ClientState state) throws InvalidRequestException, UnauthorizedException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.SELECT);
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        // Nothing to do, all validation has been done by RawStatement.prepare()
    }

    public ResultMessage.Rows execute(QueryState state, QueryOptions options) throws RequestExecutionException, RequestValidationException
    {
        ConsistencyLevel cl = options.getConsistency();
        checkNotNull(cl, "Invalid empty consistency level");

        cl.validateForRead(keyspace());

        int limit = getLimit(options);
        long now = System.currentTimeMillis();
        Pageable command = getPageableCommand(options, limit, now);

        int pageSize = options.getPageSize();

        // An aggregation query will never be paged for the user, but we always page it internally to avoid OOM.
        // If we user provided a pageSize we'll use that to page internally (because why not), otherwise we use our default
        // Note that if there are some nodes in the cluster with a version less than 2.0, we can't use paging (CASSANDRA-6707).
        if (selection.isAggregate() && pageSize <= 0)
            pageSize = DEFAULT_COUNT_PAGE_SIZE;

        if (pageSize <= 0 || command == null || !QueryPagers.mayNeedPaging(command, pageSize))
        {
            return execute(command, options, limit, now, state);
        }

        QueryPager pager = QueryPagers.pager(command, cl, state.getClientState(), options.getPagingState());

        if (selection.isAggregate())
            return pageAggregateQuery(pager, options, pageSize, now);

        // We can't properly do post-query ordering if we page (see #6722)
        checkFalse(needsPostQueryOrdering(),
                  "Cannot page queries with both ORDER BY and a IN restriction on the partition key;"
                  + " you must either remove the ORDER BY or the IN and sort client side, or disable paging for this query");

        List<Row> page = pager.fetchPage(pageSize);
        ResultMessage.Rows msg = processResults(page, options, limit, now);

        if (!pager.isExhausted())
            msg.result.metadata.setHasMorePages(pager.state());

        return msg;
    }

    private Pageable getPageableCommand(QueryOptions options, int limit, long now) throws RequestValidationException
    {
        int limitForQuery = updateLimitForQuery(limit);
        if (restrictions.isKeyRange() || restrictions.usesSecondaryIndexing())
            return getRangeCommand(options, limitForQuery, now);

        List<ReadCommand> commands = getSliceCommands(options, limitForQuery, now);
        return commands == null ? null : new Pageable.ReadCommands(commands, limitForQuery);
    }

    public Pageable getPageableCommand(QueryOptions options) throws RequestValidationException
    {
        return getPageableCommand(options, getLimit(options), System.currentTimeMillis());
    }

    private ResultMessage.Rows execute(Pageable command, QueryOptions options, int limit, long now, QueryState state) throws RequestValidationException, RequestExecutionException
    {
        List<Row> rows;
        if (command == null)
        {
            rows = Collections.<Row>emptyList();
        }
        else
        {
            rows = command instanceof Pageable.ReadCommands
                 ? StorageProxy.read(((Pageable.ReadCommands)command).commands, options.getConsistency(), state.getClientState())
                 : StorageProxy.getRangeSlice((RangeSliceCommand)command, options.getConsistency());
        }

        return processResults(rows, options, limit, now);
    }

    private ResultMessage.Rows pageAggregateQuery(QueryPager pager, QueryOptions options, int pageSize, long now)
            throws RequestValidationException, RequestExecutionException
    {
        Selection.ResultSetBuilder result = selection.resultSetBuilder(now);
        while (!pager.isExhausted())
        {
            for (org.apache.cassandra.db.Row row : pager.fetchPage(pageSize))
            {
                // Not columns match the query, skip
                if (row.cf == null)
                    continue;

                processColumnFamily(row.key.getKey(), row.cf, options, now, result);
            }
        }
        return new ResultMessage.Rows(result.build(options.getProtocolVersion()));
    }

    public ResultMessage.Rows processResults(List<Row> rows, QueryOptions options, int limit, long now) throws RequestValidationException
    {
        ResultSet rset = process(rows, options, limit, now);
        return new ResultMessage.Rows(rset);
    }

    static List<Row> readLocally(String keyspaceName, List<ReadCommand> cmds)
    {
        Keyspace keyspace = Keyspace.open(keyspaceName);
        List<Row> rows = new ArrayList<Row>(cmds.size());
        for (ReadCommand cmd : cmds)
            rows.add(cmd.getRow(keyspace));
        return rows;
    }

    public ResultMessage.Rows executeInternal(QueryState state, QueryOptions options) throws RequestExecutionException, RequestValidationException
    {
        int limit = getLimit(options);
        long now = System.currentTimeMillis();
        Pageable command = getPageableCommand(options, limit, now);
        List<Row> rows = command == null
                       ? Collections.<Row>emptyList()
                       : (command instanceof Pageable.ReadCommands
                          ? readLocally(keyspace(), ((Pageable.ReadCommands)command).commands)
                          : ((RangeSliceCommand)command).executeLocally());

        return processResults(rows, options, limit, now);
    }

    public ResultSet process(List<Row> rows) throws InvalidRequestException
    {
        QueryOptions options = QueryOptions.DEFAULT;
        return process(rows, options, getLimit(options), System.currentTimeMillis());
    }

    public String keyspace()
    {
        return cfm.ksName;
    }

    public String columnFamily()
    {
        return cfm.cfName;
    }

    private List<ReadCommand> getSliceCommands(QueryOptions options, int limit, long now) throws RequestValidationException
    {
        Collection<ByteBuffer> keys = restrictions.getPartitionKeys(options);

        List<ReadCommand> commands = new ArrayList<>(keys.size());

        IDiskAtomFilter filter = makeFilter(options, limit);
        if (filter == null)
            return null;

        // Note that we use the total limit for every key, which is potentially inefficient.
        // However, IN + LIMIT is not a very sensible choice.
        for (ByteBuffer key : keys)
        {
            QueryProcessor.validateKey(key);
            // We should not share the slice filter amongst the commands (hence the cloneShallow), due to
            // SliceQueryFilter not being immutable due to its columnCounter used by the lastCounted() method
            // (this is fairly ugly and we should change that but that's probably not a tiny refactor to do that cleanly)
            commands.add(ReadCommand.create(keyspace(), ByteBufferUtil.clone(key), columnFamily(), now, filter.cloneShallow()));
        }

        return commands;
    }

    private RangeSliceCommand getRangeCommand(QueryOptions options, int limit, long now) throws RequestValidationException
    {
        IDiskAtomFilter filter = makeFilter(options, limit);
        if (filter == null)
            return null;

        List<IndexExpression> expressions = getValidatedIndexExpressions(options);
        // The LIMIT provided by the user is the number of CQL row he wants returned.
        // We want to have getRangeSlice to count the number of columns, not the number of keys.
        AbstractBounds<RowPosition> keyBounds = restrictions.getPartitionKeyBounds(options);
        return keyBounds == null
             ? null
             : new RangeSliceCommand(keyspace(), columnFamily(), now,  filter, keyBounds, expressions, limit, !parameters.isDistinct, false);
    }

    private ColumnSlice makeStaticSlice()
    {
        // Note: we could use staticPrefix.start() for the start bound, but EMPTY gives us the
        // same effect while saving a few CPU cycles.
        return isReversed
             ? new ColumnSlice(cfm.comparator.staticPrefix().end(), Composites.EMPTY)
             : new ColumnSlice(Composites.EMPTY, cfm.comparator.staticPrefix().end());
    }

    private IDiskAtomFilter makeFilter(QueryOptions options, int limit)
    throws InvalidRequestException
    {
        int toGroup = cfm.comparator.isDense() ? -1 : cfm.clusteringColumns().size();
        if (parameters.isDistinct)
        {
            // For distinct, we only care about fetching the beginning of each partition. If we don't have
            // static columns, we in fact only care about the first cell, so we query only that (we don't "group").
            // If we do have static columns, we do need to fetch the first full group (to have the static columns values).
            return new SliceQueryFilter(ColumnSlice.ALL_COLUMNS_ARRAY, false, 1, selection.containsStaticColumns() ? toGroup : -1);
        }
        else if (restrictions.isColumnRange())
        {
            List<Composite> startBounds = restrictions.getClusteringColumnsBoundsAsComposites(Bound.START, options);
            List<Composite> endBounds = restrictions.getClusteringColumnsBoundsAsComposites(Bound.END, options);
            assert startBounds.size() == endBounds.size();

            // Handles fetching static columns. Note that for 2i, the filter is just used to restrict
            // the part of the index to query so adding the static slice would be useless and confusing.
            // For 2i, static columns are retrieve in CompositesSearcher with each index hit.
            ColumnSlice staticSlice = selection.containsStaticColumns() && !restrictions.usesSecondaryIndexing()
                                    ? makeStaticSlice()
                                    : null;

            // The case where startBounds == 1 is common enough that it's worth optimizing
            if (startBounds.size() == 1)
            {
                ColumnSlice slice = new ColumnSlice(startBounds.get(0), endBounds.get(0));
                if (slice.isAlwaysEmpty(cfm.comparator, isReversed))
                    return staticSlice == null ? null : sliceFilter(staticSlice, limit, toGroup);

                if (staticSlice == null)
                    return sliceFilter(slice, limit, toGroup);

                if (isReversed)
                    return slice.includes(cfm.comparator.reverseComparator(), staticSlice.start)
                            ? sliceFilter(new ColumnSlice(slice.start, staticSlice.finish), limit, toGroup)
                            : sliceFilter(new ColumnSlice[]{ slice, staticSlice }, limit, toGroup);
                else
                    return slice.includes(cfm.comparator, staticSlice.finish)
                            ? sliceFilter(new ColumnSlice(staticSlice.start, slice.finish), limit, toGroup)
                            : sliceFilter(new ColumnSlice[]{ staticSlice, slice }, limit, toGroup);
            }

            List<ColumnSlice> l = new ArrayList<ColumnSlice>(startBounds.size());
            for (int i = 0; i < startBounds.size(); i++)
            {
                ColumnSlice slice = new ColumnSlice(startBounds.get(i), endBounds.get(i));
                if (!slice.isAlwaysEmpty(cfm.comparator, isReversed))
                    l.add(slice);
            }

            if (l.isEmpty())
                return staticSlice == null ? null : sliceFilter(staticSlice, limit, toGroup);
            if (staticSlice == null)
                return sliceFilter(l.toArray(new ColumnSlice[l.size()]), limit, toGroup);

            // The slices should not overlap. We know the slices built from startBounds/endBounds don't, but if there is
            // a static slice, it could overlap with the 2nd slice. Check for it and correct if that's the case
            ColumnSlice[] slices;
            if (isReversed)
            {
                if (l.get(l.size() - 1).includes(cfm.comparator.reverseComparator(), staticSlice.start))
                {
                    slices = l.toArray(new ColumnSlice[l.size()]);
                    slices[slices.length-1] = new ColumnSlice(slices[slices.length-1].start, Composites.EMPTY);
                }
                else
                {
                    slices = l.toArray(new ColumnSlice[l.size()+1]);
                    slices[slices.length-1] = staticSlice;
                }
            }
            else
            {
                if (l.get(0).includes(cfm.comparator, staticSlice.finish))
                {
                    slices = new ColumnSlice[l.size()];
                    slices[0] = new ColumnSlice(Composites.EMPTY, l.get(0).finish);
                    for (int i = 1; i < l.size(); i++)
                        slices[i] = l.get(i);
                }
                else
                {
                    slices = new ColumnSlice[l.size()+1];
                    slices[0] = staticSlice;
                    for (int i = 0; i < l.size(); i++)
                        slices[i+1] = l.get(i);
                }
            }
            return sliceFilter(slices, limit, toGroup);
        }
        else
        {
            SortedSet<CellName> cellNames = getRequestedColumns(options);
            if (cellNames == null) // in case of IN () for the last column of the key
                return null;
            QueryProcessor.validateCellNames(cellNames, cfm.comparator);
            return new NamesQueryFilter(cellNames, true);
        }
    }

    private SliceQueryFilter sliceFilter(ColumnSlice slice, int limit, int toGroup)
    {
        return sliceFilter(new ColumnSlice[]{ slice }, limit, toGroup);
    }

    private SliceQueryFilter sliceFilter(ColumnSlice[] slices, int limit, int toGroup)
    {
        assert ColumnSlice.validateSlices(slices, cfm.comparator, isReversed) : String.format("Invalid slices: " + Arrays.toString(slices) + (isReversed ? " (reversed)" : ""));
        return new SliceQueryFilter(slices, isReversed, limit, toGroup);
    }

    private int getLimit(QueryOptions options) throws InvalidRequestException
    {
        if (limit != null)
        {
            ByteBuffer b = checkNotNull(limit.bindAndGet(options), "Invalid null value of limit");

            try
            {
                Int32Type.instance.validate(b);
                int l = Int32Type.instance.compose(b);
                checkTrue(l > 0, "LIMIT must be strictly positive");
                return l;
            }
            catch (MarshalException e)
            {
                throw new InvalidRequestException("Invalid limit value");
            }
        }
        return Integer.MAX_VALUE;
    }

    private int updateLimitForQuery(int limit)
    {
        // Internally, we don't support exclusive bounds for slices. Instead, we query one more element if necessary
        // and exclude it later (in processColumnFamily)
        return restrictions.isNonCompositeSliceWithExclusiveBounds() && limit != Integer.MAX_VALUE
             ? limit + 1
             : limit;
    }

    private SortedSet<CellName> getRequestedColumns(QueryOptions options) throws InvalidRequestException
    {
        // Note: getRequestedColumns don't handle static columns, but due to CASSANDRA-5762
        // we always do a slice for CQL3 tables, so it's ok to ignore them here
        assert !restrictions.isColumnRange();
        SortedSet<CellName> columns = new TreeSet<CellName>(cfm.comparator);
        for (Composite composite : restrictions.getClusteringColumnsAsComposites(options))
            columns.addAll(addSelectedColumns(composite));
        return columns;
    }

    private SortedSet<CellName> addSelectedColumns(Composite prefix)
    {
        if (cfm.comparator.isDense())
        {
            return FBUtilities.singleton(cfm.comparator.create(prefix, null), cfm.comparator);
        }
        else
        {
            SortedSet<CellName> columns = new TreeSet<CellName>(cfm.comparator);

            // We need to query the selected column as well as the marker
            // column (for the case where the row exists but has no columns outside the PK)
            // Two exceptions are "static CF" (non-composite non-compact CF) and "super CF"
            // that don't have marker and for which we must query all columns instead
            if (cfm.comparator.isCompound() && !cfm.isSuper())
            {
                // marker
                columns.add(cfm.comparator.rowMarker(prefix));

                // selected columns
                for (ColumnDefinition def : selection.getColumns())
                    if (def.isRegular() || def.isStatic())
                        columns.add(cfm.comparator.create(prefix, def));
            }
            else
            {
                // We now that we're not composite so we can ignore static columns
                for (ColumnDefinition def : cfm.regularColumns())
                    columns.add(cfm.comparator.create(prefix, def));
            }
            return columns;
        }
    }

    public List<IndexExpression> getValidatedIndexExpressions(QueryOptions options) throws InvalidRequestException
    {
        if (!restrictions.usesSecondaryIndexing())
            return Collections.emptyList();

        List<IndexExpression> expressions = restrictions.getIndexExpressions(options);

        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(columnFamily());
        SecondaryIndexManager secondaryIndexManager = cfs.indexManager;
        secondaryIndexManager.validateIndexSearchersForQuery(expressions);

        return expressions;
    }

    private CellName makeExclusiveSliceBound(Bound bound, CellNameType type, QueryOptions options) throws InvalidRequestException
    {
        if (restrictions.areRequestedBoundsInclusive(bound))
            return null;

       return type.makeCellName(restrictions.getClusteringColumnsBounds(bound, options).get(0));
    }

    private Iterator<Cell> applySliceRestriction(final Iterator<Cell> cells, final QueryOptions options) throws InvalidRequestException
    {
        final CellNameType type = cfm.comparator;

        final CellName excludedStart = makeExclusiveSliceBound(Bound.START, type, options);
        final CellName excludedEnd = makeExclusiveSliceBound(Bound.END, type, options);

        return Iterators.filter(cells, new Predicate<Cell>()
        {
            public boolean apply(Cell c)
            {
                // For dynamic CF, the column could be out of the requested bounds (because we don't support strict bounds internally (unless
                // the comparator is composite that is)), filter here
                return !((excludedStart != null && type.compare(c.name(), excludedStart) == 0)
                            || (excludedEnd != null && type.compare(c.name(), excludedEnd) == 0));
            }
        });
    }

    private ResultSet process(List<Row> rows, QueryOptions options, int limit, long now) throws InvalidRequestException
    {
        Selection.ResultSetBuilder result = selection.resultSetBuilder(now);
        for (org.apache.cassandra.db.Row row : rows)
        {
            // Not columns match the query, skip
            if (row.cf == null)
                continue;

            processColumnFamily(row.key.getKey(), row.cf, options, now, result);
        }

        ResultSet cqlRows = result.build(options.getProtocolVersion());

        orderResults(cqlRows);

        // Internal calls always return columns in the comparator order, even when reverse was set
        if (isReversed)
            cqlRows.reverse();

        // Trim result if needed to respect the user limit
        cqlRows.trim(limit);
        return cqlRows;
    }

    // Used by ModificationStatement for CAS operations
    void processColumnFamily(ByteBuffer key, ColumnFamily cf, QueryOptions options, long now, Selection.ResultSetBuilder result)
    throws InvalidRequestException
    {
        CFMetaData cfm = cf.metadata();
        ByteBuffer[] keyComponents = null;
        if (cfm.getKeyValidator() instanceof CompositeType)
        {
            keyComponents = ((CompositeType)cfm.getKeyValidator()).split(key);
        }
        else
        {
            keyComponents = new ByteBuffer[]{ key };
        }

        Iterator<Cell> cells = cf.getSortedColumns().iterator();
        if (restrictions.isNonCompositeSliceWithExclusiveBounds())
            cells = applySliceRestriction(cells, options);

        CQL3Row.RowIterator iter = cfm.comparator.CQL3RowBuilder(cfm, now).group(cells);

        // If there is static columns but there is no non-static row, then provided the select was a full
        // partition selection (i.e. not a 2ndary index search and there was no condition on clustering columns)
        // then we want to include the static columns in the result set (and we're done).
        CQL3Row staticRow = iter.getStaticRow();
        if (staticRow != null && !iter.hasNext() && !restrictions.usesSecondaryIndexing() && restrictions.hasNoClusteringColumnsRestriction())
        {
            result.newRow(options.getProtocolVersion());
            for (ColumnDefinition def : selection.getColumns())
            {
                switch (def.kind)
                {
                    case PARTITION_KEY:
                        result.add(keyComponents[def.position()]);
                        break;
                    case STATIC:
                        addValue(result, def, staticRow, options);
                        break;
                    default:
                        result.add((ByteBuffer)null);
                }
            }
            return;
        }

        while (iter.hasNext())
        {
            CQL3Row cql3Row = iter.next();

            // Respect requested order
            result.newRow(options.getProtocolVersion());
            // Respect selection order
            for (ColumnDefinition def : selection.getColumns())
            {
                switch (def.kind)
                {
                    case PARTITION_KEY:
                        result.add(keyComponents[def.position()]);
                        break;
                    case CLUSTERING_COLUMN:
                        result.add(cql3Row.getClusteringColumn(def.position()));
                        break;
                    case COMPACT_VALUE:
                        result.add(cql3Row.getColumn(null));
                        break;
                    case REGULAR:
                        addValue(result, def, cql3Row, options);
                        break;
                    case STATIC:
                        addValue(result, def, staticRow, options);
                        break;
                }
            }
        }
    }

    private static void addValue(Selection.ResultSetBuilder result, ColumnDefinition def, CQL3Row row, QueryOptions options)
    {
        if (row == null)
        {
            result.add((ByteBuffer)null);
            return;
        }

        if (def.type.isMultiCell())
        {
            List<Cell> cells = row.getMultiCellColumn(def.name);
            ByteBuffer buffer = cells == null
                             ? null
                             : ((CollectionType)def.type).serializeForNativeProtocol(cells, options.getProtocolVersion());
            result.add(buffer);
            return;
        }

        result.add(row.getColumn(def.name));
    }

    private boolean needsPostQueryOrdering()
    {
        // We need post-query ordering only for queries with IN on the partition key and an ORDER BY.
        return restrictions.keyIsInRelation() && !parameters.orderings.isEmpty();
    }

    /**
     * Orders results when multiple keys are selected (using IN)
     */
    private void orderResults(ResultSet cqlRows)
    {
        if (cqlRows.size() == 0 || !needsPostQueryOrdering())
            return;

        Collections.sort(cqlRows.rows, orderingComparator);
    }

    public static class RawStatement extends CFStatement
    {
        private final Parameters parameters;
        private final List<RawSelector> selectClause;
        private final List<Relation> whereClause;
        private final Term.Raw limit;

        public RawStatement(CFName cfName, Parameters parameters, List<RawSelector> selectClause, List<Relation> whereClause, Term.Raw limit)
        {
            super(cfName);
            this.parameters = parameters;
            this.selectClause = selectClause;
            this.whereClause = whereClause == null ? Collections.<Relation>emptyList() : whereClause;
            this.limit = limit;
        }

        public ParsedStatement.Prepared prepare() throws InvalidRequestException
        {
            CFMetaData cfm = ThriftValidation.validateColumnFamily(keyspace(), columnFamily());
            VariableSpecifications boundNames = getBoundVariables();

            Selection selection = selectClause.isEmpty()
                                ? Selection.wildcard(cfm)
                                : Selection.fromSelectors(cfm, selectClause);

            StatementRestrictions restrictions = prepareRestrictions(cfm, boundNames, selection);

            if (parameters.isDistinct)
                validateDistinctSelection(cfm, selection, restrictions);

            Comparator<List<ByteBuffer>> orderingComparator = null;
            boolean isReversed = false;

            if (!parameters.orderings.isEmpty())
            {
                verifyOrderingIsAllowed(restrictions);
                orderingComparator = getOrderingComparator(cfm, selection, restrictions);
                isReversed = isReversed(cfm);
            }

            if (isReversed)
                restrictions.reverse();

            checkNeedsFiltering(restrictions);

            SelectStatement stmt = new SelectStatement(cfm,
                                                        boundNames.size(),
                                                        parameters,
                                                        selection,
                                                        restrictions,
                                                        isReversed,
                                                        orderingComparator,
                                                        prepareLimit(boundNames));

            return new ParsedStatement.Prepared(stmt, boundNames);
        }

        /**
         * Prepares the restrictions.
         *
         * @param cfm the column family meta data
         * @param boundNames the variable specifications
         * @param selection the selection
         * @return the restrictions
         * @throws InvalidRequestException if a problem occurs while building the restrictions
         */
        private StatementRestrictions prepareRestrictions(CFMetaData cfm,
                                                          VariableSpecifications boundNames,
                                                          Selection selection) throws InvalidRequestException
        {
            try
            {
                return new StatementRestrictions(cfm,
                                                 whereClause,
                                                 boundNames,
                                                 selection.containsOnlyStaticColumns(),
                                                 selection.containsACollection());
            }
            catch (UnrecognizedEntityException e)
            {
                if (containsAlias(e.entity))
                    throw invalidRequest("Aliases aren't allowed in the where clause ('%s')", e.relation);
                throw e;
            }
        }

        /** Returns a Term for the limit or null if no limit is set */
        private Term prepareLimit(VariableSpecifications boundNames) throws InvalidRequestException
        {
            if (limit == null)
                return null;

            Term prepLimit = limit.prepare(keyspace(), limitReceiver());
            prepLimit.collectMarkerSpecification(boundNames);
            return prepLimit;
        }

        private static void verifyOrderingIsAllowed(StatementRestrictions restrictions) throws InvalidRequestException
        {
            checkFalse(restrictions.usesSecondaryIndexing(), "ORDER BY with 2ndary indexes is not supported.");
            checkFalse(restrictions.isKeyRange(), "ORDER BY is only supported when the partition key is restricted by an EQ or an IN.");
        }

        private static void validateDistinctSelection(CFMetaData cfm,
                                                      Selection selection,
                                                      StatementRestrictions restrictions)
                                                      throws InvalidRequestException
        {
            Collection<ColumnDefinition> requestedColumns = selection.getColumns();
            for (ColumnDefinition def : requestedColumns)
                checkFalse(!def.isPartitionKey() && !def.isStatic(),
                           "SELECT DISTINCT queries must only request partition key columns and/or static columns (not %s)",
                           def.name);

            // If it's a key range, we require that all partition key columns are selected so we don't have to bother
            // with post-query grouping.
            if (!restrictions.isKeyRange())
                return;

            for (ColumnDefinition def : cfm.partitionKeyColumns())
                checkTrue(requestedColumns.contains(def),
                          "SELECT DISTINCT queries must request all the partition key columns (missing %s)", def.name);
        }

        private void handleUnrecognizedOrderingColumn(ColumnIdentifier column) throws InvalidRequestException
        {
            checkFalse(containsAlias(column), "Aliases are not allowed in order by clause ('%s')", column);
            checkFalse(true, "Order by on unknown column %s", column);
        }

        private Comparator<List<ByteBuffer>> getOrderingComparator(CFMetaData cfm,
                                                                   Selection selection,
                                                                   StatementRestrictions restrictions)
                                                                   throws InvalidRequestException
        {
            if (!restrictions.keyIsInRelation())
                return null;

            Map<ColumnIdentifier, Integer> orderingIndexes = getOrderingIndex(cfm, selection);

            List<Integer> idToSort = new ArrayList<Integer>();
            List<Comparator<ByteBuffer>> sorters = new ArrayList<Comparator<ByteBuffer>>();

            for (ColumnIdentifier.Raw raw : parameters.orderings.keySet())
            {
                ColumnIdentifier identifier = raw.prepare(cfm);
                ColumnDefinition orderingColumn = cfm.getColumnDefinition(identifier);
                idToSort.add(orderingIndexes.get(orderingColumn.name));
                sorters.add(orderingColumn.type);
            }
            return idToSort.size() == 1 ? new SingleColumnComparator(idToSort.get(0), sorters.get(0))
                    : new CompositeComparator(sorters, idToSort);
        }

        private Map<ColumnIdentifier, Integer> getOrderingIndex(CFMetaData cfm, Selection selection)
                throws InvalidRequestException
        {
            // If we order post-query (see orderResults), the sorted column needs to be in the ResultSet for sorting,
            // even if we don't
            // ultimately ship them to the client (CASSANDRA-4911).
            Map<ColumnIdentifier, Integer> orderingIndexes = new HashMap<>();
            for (ColumnIdentifier.Raw raw : parameters.orderings.keySet())
            {
                ColumnIdentifier column = raw.prepare(cfm);
                final ColumnDefinition def = cfm.getColumnDefinition(column);
                if (def == null)
                    handleUnrecognizedOrderingColumn(column);
                int index = selection.indexOf(def);
                if (index < 0)
                    index = selection.addColumnForOrdering(def);
                orderingIndexes.put(def.name, index);
            }
            return orderingIndexes;
        }

        private boolean isReversed(CFMetaData cfm) throws InvalidRequestException
        {
            Boolean[] reversedMap = new Boolean[cfm.clusteringColumns().size()];
            int i = 0;
            for (Map.Entry<ColumnIdentifier.Raw, Boolean> entry : parameters.orderings.entrySet())
            {
                ColumnIdentifier column = entry.getKey().prepare(cfm);
                boolean reversed = entry.getValue();

                ColumnDefinition def = cfm.getColumnDefinition(column);
                if (def == null)
                    handleUnrecognizedOrderingColumn(column);

                checkTrue(def.isClusteringColumn(),
                          "Order by is currently only supported on the clustered columns of the PRIMARY KEY, got %s", column);

                checkTrue(i++ == def.position(),
                          "Order by currently only support the ordering of columns following their declared order in the PRIMARY KEY");

                reversedMap[def.position()] = (reversed != def.isReversedType());
            }

            // Check that all boolean in reversedMap, if set, agrees
            Boolean isReversed = null;
            for (Boolean b : reversedMap)
            {
                // Column on which order is specified can be in any order
                if (b == null)
                    continue;

                if (isReversed == null)
                {
                    isReversed = b;
                    continue;
                }
                checkTrue(isReversed.equals(b), "Unsupported order by relation");
            }
            assert isReversed != null;
            return isReversed;
        }

        /** If ALLOW FILTERING was not specified, this verifies that it is not needed */
        private void checkNeedsFiltering(StatementRestrictions restrictions) throws InvalidRequestException
        {
            // non-key-range non-indexed queries cannot involve filtering underneath
            if (!parameters.allowFiltering && (restrictions.isKeyRange() || restrictions.usesSecondaryIndexing()))
            {
                // We will potentially filter data if either:
                //  - Have more than one IndexExpression
                //  - Have no index expression and the column filter is not the identity
                checkFalse(restrictions.needFiltering(),
                           "Cannot execute this query as it might involve data filtering and " +
                           "thus may have unpredictable performance. If you want to execute " +
                           "this query despite the performance unpredictability, use ALLOW FILTERING");
            }

            // We don't internally support exclusive slice bounds on non-composite tables. To deal with it we do an
            // inclusive slice and remove post-query the value that shouldn't be returned. One problem however is that
            // if there is a user limit, that limit may make the query return before the end of the slice is reached,
            // in which case, once we'll have removed bound post-query, we might end up with less results than
            // requested which would be incorrect. For single-partition query, this is not a problem, we just ask for
            // one more result (see updateLimitForQuery()) since that's enough to compensate for that problem. For key
            // range however, each returned row may include one result that will have to be trimmed, so we would have
            // to bump the query limit by N where N is the number of rows we will return, but we don't know that in
            // advance. So, since we currently don't have a good way to handle such query, we refuse it (#7059) rather
            // than answering with something that is wrong.
            if (restrictions.isNonCompositeSliceWithExclusiveBounds() && restrictions.isKeyRange() && limit != null)
            {
                SingleColumnRelation rel = findInclusiveClusteringRelationForCompact(restrictions.cfm);
                throw invalidRequest("The query requests a restriction of rows with a strict bound (%s) over a range of partitions. "
                                   + "This is not supported by the underlying storage engine for COMPACT tables if a LIMIT is provided. "
                                   + "Please either make the condition non strict (%s) or remove the user LIMIT", rel, rel.withNonStrictOperator());
            }
        }

        private SingleColumnRelation findInclusiveClusteringRelationForCompact(CFMetaData cfm)
        {
            for (Relation r : whereClause)
            {
                // We only call this when sliceRestriction != null, i.e. for compact table with non composite comparator,
                // so it can't be a MultiColumnRelation.
                SingleColumnRelation rel = (SingleColumnRelation)r;

                if (cfm.getColumnDefinition(rel.getEntity().prepare(cfm)).isClusteringColumn()
                        && (rel.operator() == Operator.GT || rel.operator() == Operator.LT))
                    return rel;
            }

            // We're not supposed to call this method unless we know this can't happen
            throw new AssertionError();
        }

        private boolean containsAlias(final ColumnIdentifier name)
        {
            return Iterables.any(selectClause, new Predicate<RawSelector>()
                                               {
                                                   public boolean apply(RawSelector raw)
                                                   {
                                                       return name.equals(raw.alias);
                                                   }
                                               });
        }

        private ColumnSpecification limitReceiver()
        {
            return new ColumnSpecification(keyspace(), columnFamily(), new ColumnIdentifier("[limit]", true), Int32Type.instance);
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                          .add("name", cfName)
                          .add("selectClause", selectClause)
                          .add("whereClause", whereClause)
                          .add("isDistinct", parameters.isDistinct)
                          .toString();
        }
    }

    public static class Parameters
    {
        private final Map<ColumnIdentifier.Raw, Boolean> orderings;
        private final boolean isDistinct;
        private final boolean allowFiltering;

        public Parameters(Map<ColumnIdentifier.Raw, Boolean> orderings,
                          boolean isDistinct,
                          boolean allowFiltering)
        {
            this.orderings = orderings;
            this.isDistinct = isDistinct;
            this.allowFiltering = allowFiltering;
        }
    }

    /**
     * Used in orderResults(...) method when single 'ORDER BY' condition where given
     */
    private static class SingleColumnComparator implements Comparator<List<ByteBuffer>>
    {
        private final int index;
        private final Comparator<ByteBuffer> comparator;

        public SingleColumnComparator(int columnIndex, Comparator<ByteBuffer> orderer)
        {
            index = columnIndex;
            comparator = orderer;
        }

        public int compare(List<ByteBuffer> a, List<ByteBuffer> b)
        {
            return comparator.compare(a.get(index), b.get(index));
        }
    }

    /**
     * Used in orderResults(...) method when multiple 'ORDER BY' conditions where given
     */
    private static class CompositeComparator implements Comparator<List<ByteBuffer>>
    {
        private final List<Comparator<ByteBuffer>> orderTypes;
        private final List<Integer> positions;

        private CompositeComparator(List<Comparator<ByteBuffer>> orderTypes, List<Integer> positions)
        {
            this.orderTypes = orderTypes;
            this.positions = positions;
        }

        public int compare(List<ByteBuffer> a, List<ByteBuffer> b)
        {
            for (int i = 0; i < positions.size(); i++)
            {
                Comparator<ByteBuffer> type = orderTypes.get(i);
                int columnPos = positions.get(i);

                ByteBuffer aValue = a.get(columnPos);
                ByteBuffer bValue = b.get(columnPos);

                int comparison = type.compare(aValue, bValue);

                if (comparison != 0)
                    return comparison;
            }

            return 0;
        }
    }
}
