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
 * Copyright (C) 2015 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include "cql3/statements/cf_statement.hh"
#include "cql3/cql_statement.hh"
#include "cql3/selection/selection.hh"
#include "cql3/selection/raw_selector.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "cql3/result_set.hh"
#include "exceptions/unrecognized_entity_exception.hh"
#include "service/client_state.hh"
#include "core/shared_ptr.hh"
#include "core/distributed.hh"
#include "validation.hh"

namespace cql3 {

namespace statements {

/**
 * Encapsulates a completely parsed SELECT query, including the target
 * column family, expression, result count, and ordering clause.
 *
 */
class select_statement : public cql_statement {
public:
    class parameters final {
    public:
        using orderings_type = std::vector<std::pair<shared_ptr<column_identifier::raw>, bool>>;
    private:
        const orderings_type _orderings;
        const bool _is_distinct;
        const bool _allow_filtering;
    public:
        parameters();
        parameters(orderings_type orderings,
            bool is_distinct,
            bool allow_filtering);
        bool is_distinct();
        bool allow_filtering();
        orderings_type const& orderings();
    };
private:
    static constexpr int DEFAULT_COUNT_PAGE_SIZE = 10000;
    static thread_local const ::shared_ptr<parameters> _default_parameters;
    schema_ptr _schema;
    uint32_t _bound_terms;
    ::shared_ptr<parameters> _parameters;
    ::shared_ptr<selection::selection> _selection;
    ::shared_ptr<restrictions::statement_restrictions> _restrictions;
    bool _is_reversed;
    ::shared_ptr<term> _limit;

    template<typename T>
    using compare_fn = std::function<bool(const T&, const T&)>;

    using result_row_type = std::vector<bytes_opt>;
    using ordering_comparator_type = compare_fn<result_row_type>;

    /**
     * The comparator used to orders results when multiple keys are selected (using IN).
     */
    ordering_comparator_type _ordering_comparator;

    query::partition_slice::option_set _opts;
public:
    select_statement(schema_ptr schema,
            uint32_t bound_terms,
            ::shared_ptr<parameters> parameters,
            ::shared_ptr<selection::selection> selection,
            ::shared_ptr<restrictions::statement_restrictions> restrictions,
            bool is_reversed,
            ordering_comparator_type ordering_comparator,
            ::shared_ptr<term> limit);

    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const override;

    // Creates a simple select based on the given selection
    // Note that the results select statement should not be used for actual queries, but only for processing already
    // queried data through processColumnFamily.
    static ::shared_ptr<select_statement> for_selection(
        schema_ptr schema, ::shared_ptr<selection::selection> selection);

    ::shared_ptr<cql3::metadata> get_result_metadata() const;
    virtual uint32_t get_bound_terms() override;
    virtual void check_access(const service::client_state& state) override;
    virtual void validate(distributed<service::storage_proxy>&, const service::client_state& state) override;
    virtual bool depends_on_keyspace(const sstring& ks_name) const;
    virtual bool depends_on_column_family(const sstring& cf_name) const;

    virtual future<::shared_ptr<transport::messages::result_message>> execute(distributed<service::storage_proxy>& proxy,
        service::query_state& state, const query_options& options) override;

    virtual future<::shared_ptr<transport::messages::result_message>> execute_internal(distributed<service::storage_proxy>& proxy,
            service::query_state& state, const query_options& options) override;

    future<::shared_ptr<transport::messages::result_message>> execute(distributed<service::storage_proxy>& proxy,
        lw_shared_ptr<query::read_command> cmd, std::vector<query::partition_range>&& partition_ranges, service::query_state& state,
         const query_options& options, db_clock::time_point now);

    shared_ptr<transport::messages::result_message> process_results(foreign_ptr<lw_shared_ptr<query::result>> results,
        lw_shared_ptr<query::read_command> cmd, const query_options& options, db_clock::time_point now);
#if 0
    private ResultMessage.Rows pageAggregateQuery(QueryPager pager, QueryOptions options, int pageSize, long now)
            throws RequestValidationException, RequestExecutionException
    {
        Selection.ResultSetBuilder result = _selection->resultSetBuilder(now);
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
#endif

    const sstring& keyspace() const;

    const sstring& column_family() const;

    query::partition_slice make_partition_slice(const query_options& options);

#if 0
    private SliceQueryFilter sliceFilter(ColumnSlice slice, int limit, int toGroup)
    {
        return sliceFilter(new ColumnSlice[]{ slice }, limit, toGroup);
    }

    private SliceQueryFilter sliceFilter(ColumnSlice[] slices, int limit, int toGroup)
    {
        assert ColumnSlice.validateSlices(slices, _schema.comparator, _is_reversed) : String.format("Invalid slices: " + Arrays.toString(slices) + (_is_reversed ? " (reversed)" : ""));
        return new SliceQueryFilter(slices, _is_reversed, limit, toGroup);
    }
#endif

private:
    int32_t get_limit(const query_options& options) const;
    bool needs_post_query_ordering() const;

#if 0
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
#endif

public:
    class raw_statement;
};

class select_statement::raw_statement : public cf_statement
{
private:
    ::shared_ptr<parameters> _parameters;
    std::vector<::shared_ptr<selection::raw_selector>> _select_clause;
    std::vector<::shared_ptr<relation>> _where_clause;
    ::shared_ptr<term::raw> _limit;
public:
    raw_statement(::shared_ptr<cf_name> cf_name,
            ::shared_ptr<parameters> parameters,
            std::vector<::shared_ptr<selection::raw_selector>> select_clause,
            std::vector<::shared_ptr<relation>> where_clause,
            ::shared_ptr<term::raw> limit);

    virtual ::shared_ptr<prepared> prepare(database& db) override;
private:
    ::shared_ptr<restrictions::statement_restrictions> prepare_restrictions(
        database& db,
        schema_ptr schema,
        ::shared_ptr<variable_specifications> bound_names,
        ::shared_ptr<selection::selection> selection);

    /** Returns a ::shared_ptr<term> for the limit or null if no limit is set */
    ::shared_ptr<term> prepare_limit(database& db, ::shared_ptr<variable_specifications> bound_names);

    static void verify_ordering_is_allowed(::shared_ptr<restrictions::statement_restrictions> restrictions);

    static void validate_distinct_selection(schema_ptr schema,
        ::shared_ptr<selection::selection> selection,
        ::shared_ptr<restrictions::statement_restrictions> restrictions);

    void handle_unrecognized_ordering_column(::shared_ptr<column_identifier> column);

    select_statement::ordering_comparator_type get_ordering_comparator(schema_ptr schema,
        ::shared_ptr<selection::selection> selection,
        ::shared_ptr<restrictions::statement_restrictions> restrictions);

    bool is_reversed(schema_ptr schema);

    /** If ALLOW FILTERING was not specified, this verifies that it is not needed */
    void check_needs_filtering(::shared_ptr<restrictions::statement_restrictions> restrictions);

    bool contains_alias(::shared_ptr<column_identifier> name);

    ::shared_ptr<column_specification> limit_receiver();

#if 0
    public:
        virtual sstring to_string() override {
            return sstring("raw_statement(")
                + "name=" + cf_name->to_string()
                + ", selectClause=" + to_string(_select_clause)
                + ", whereClause=" + to_string(_where_clause)
                + ", isDistinct=" + to_string(_parameters->is_distinct())
                + ")";
        }
    };
#endif
};

}

}
