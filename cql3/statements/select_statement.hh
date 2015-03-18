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
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#ifndef CQL3_STATEMENTS_SELECT_STATEMENT_HH
#define CQL3_STATEMENTS_SELECT_STATEMENT_HH

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
        using orderings_type = std::unordered_map<shared_ptr<column_identifier::raw>, bool,
            shared_ptr_value_hash<column_identifier::raw>, shared_ptr_equal_by_value<column_identifier::raw>>;
    private:
        const orderings_type _orderings;
        const bool _is_distinct;
        const bool _allow_filtering;
    public:
        parameters()
            : _is_distinct{false}
            , _allow_filtering{false}
        { }
        parameters(orderings_type orderings,
            bool is_distinct,
            bool allow_filtering)
            : _orderings{std::move(orderings)}
            , _is_distinct{is_distinct}
            , _allow_filtering{allow_filtering}
        { }
        bool is_distinct() { return _is_distinct; }
        bool allow_filtering() { return _allow_filtering; }
        orderings_type const& orderings() { return _orderings; }
    };
private:
    static constexpr int DEFAULT_COUNT_PAGE_SIZE = 10000;
    static const ::shared_ptr<parameters> _default_parameters;
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
public:
    select_statement(schema_ptr schema,
            uint32_t bound_terms,
            ::shared_ptr<parameters> parameters,
            ::shared_ptr<selection::selection> selection,
            ::shared_ptr<restrictions::statement_restrictions> restrictions,
            bool is_reversed,
            ordering_comparator_type ordering_comparator,
            ::shared_ptr<term> limit)
        : _schema(schema)
        , _bound_terms(bound_terms)
        , _parameters(std::move(parameters))
        , _selection(std::move(selection))
        , _restrictions(std::move(restrictions))
        , _is_reversed(is_reversed)
        , _limit(std::move(limit))
        , _ordering_comparator(std::move(ordering_comparator))
    { }

    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const override {
        return _selection->uses_function(ks_name, function_name)
                || _restrictions->uses_function(ks_name, function_name)
                || (_limit && _limit->uses_function(ks_name, function_name));
    }

    // Creates a simple select based on the given selection
    // Note that the results select statement should not be used for actual queries, but only for processing already
    // queried data through processColumnFamily.
    static ::shared_ptr<select_statement> for_selection(schema_ptr schema,
            ::shared_ptr<selection::selection> selection) {
        return ::make_shared<select_statement>(schema,
            0,
            _default_parameters,
            selection,
            ::make_shared<restrictions::statement_restrictions>(schema),
            false,
            ordering_comparator_type{},
            ::shared_ptr<term>{});
    }

    ::shared_ptr<metadata> get_result_metadata() {
        return _selection->get_result_metadata();
    }

    virtual uint32_t get_bound_terms() override {
        return _bound_terms;
    }

    virtual void check_access(const service::client_state& state) override {
        warn(unimplemented::cause::PERMISSIONS);
#if 0
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.SELECT);
#endif
    }

    virtual void validate(const service::client_state& state) override {
        // Nothing to do, all validation has been done by raw_statemet::prepare()
    }

    virtual future<::shared_ptr<transport::messages::result_message>> execute(service::storage_proxy& proxy,
        service::query_state& state, const query_options& options) override;

    virtual future<::shared_ptr<transport::messages::result_message>> execute_internal(database& db,
            service::query_state& state, const query_options& options) {
        throw std::runtime_error("not implemented");
    }

    future<::shared_ptr<transport::messages::result_message>> execute(service::storage_proxy& proxy,
        lw_shared_ptr<query::read_command> cmd, service::query_state& state,
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

    public String keyspace()
    {
        return _schema.ks_name;
    }

    public String columnFamily()
    {
        return _schema.cfName;
    }
#endif

    query::partition_slice make_partition_slice(const query_options& options) {
        std::vector<column_id> static_columns;
        std::vector<column_id> regular_columns;

        if (_selection->contains_static_columns()) {
            static_columns.reserve(_selection->get_column_count());
        }

        regular_columns.reserve(_selection->get_column_count());

        for (auto&& col : _selection->get_columns()) {
            if (col->is_static()) {
                static_columns.push_back(col->id);
            } else if (col->is_regular()) {
                regular_columns.push_back(col->id);
            }
        }

        if (_parameters->is_distinct()) {
            return query::partition_slice({}, std::move(static_columns), {});
        }

        return query::partition_slice(_restrictions->get_clustering_bounds(options),
            std::move(static_columns), std::move(regular_columns));
    }

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
    int32_t get_limit(const query_options& options) const {
        if (!_limit) {
            return std::numeric_limits<int32_t>::max();
        }

        auto val = _limit->bind_and_get(options);
        if (!val) {
            throw exceptions::invalid_request_exception("Invalid null value of limit");
        }

        try {
            int32_type->validate(*val);
            auto l = *boost::any_cast<std::experimental::optional<int32_t>>(int32_type->deserialize(*val));
            if (l <= 0) {
                throw exceptions::invalid_request_exception("LIMIT must be strictly positive");
            }
            return l;
        } catch (const marshal_exception& e) {
            throw exceptions::invalid_request_exception("Invalid limit value");
        }
    }

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

private:
    bool needs_post_query_ordering() const {
        // We need post-query ordering only for queries with IN on the partition key and an ORDER BY.
        return _restrictions->key_is_in_relation() && !_parameters->orderings().empty();
    }

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
            ::shared_ptr<term::raw> limit)
        : cf_statement(std::move(cf_name))
        , _parameters(std::move(parameters))
        , _select_clause(std::move(select_clause))
        , _where_clause(std::move(where_clause))
        , _limit(std::move(limit))
    { }

    virtual ::shared_ptr<prepared> prepare(database& db) override {
        schema_ptr schema = validation::validate_column_family(db, keyspace(), column_family());
        auto bound_names = get_bound_variables();

        auto selection = _select_clause.empty()
                            ? selection::selection::wildcard(schema)
                            : selection::selection::from_selectors(schema, _select_clause);

        auto restrictions = prepare_restrictions(schema, bound_names, selection);

        if (_parameters->is_distinct()) {
            validate_distinct_selection(schema, selection, restrictions);
        }

        select_statement::ordering_comparator_type ordering_comparator;
        bool is_reversed_ = false;

        if (!_parameters->orderings().empty()) {
            verify_ordering_is_allowed(restrictions);
            ordering_comparator = get_ordering_comparator(schema, selection, restrictions);
            is_reversed_ = is_reversed(schema);
        }

        if (is_reversed_) {
            restrictions->reverse();
        }

        check_needs_filtering(restrictions);

        auto stmt = ::make_shared<select_statement>(schema,
            bound_names->size(),
            _parameters,
            std::move(selection),
            std::move(restrictions),
            is_reversed_,
            std::move(ordering_comparator),
            prepare_limit(bound_names));

        return ::make_shared<parsed_statement::prepared>(std::move(stmt), std::move(*bound_names));
    }

private:
    ::shared_ptr<restrictions::statement_restrictions> prepare_restrictions(schema_ptr schema,
            ::shared_ptr<variable_specifications> bound_names, ::shared_ptr<selection::selection> selection) {
        try {
            return ::make_shared<restrictions::statement_restrictions>(schema, std::move(_where_clause), bound_names,
                selection->contains_only_static_columns(), selection->contains_a_collection());
        } catch (const exceptions::unrecognized_entity_exception& e) {
            if (contains_alias(e.entity)) {
                throw exceptions::invalid_request_exception(sprint("Aliases aren't allowed in the where clause ('%s')", e.relation->to_string()));
            }
            throw;
        }
    }

    /** Returns a ::shared_ptr<term> for the limit or null if no limit is set */
    ::shared_ptr<term> prepare_limit(::shared_ptr<variable_specifications> bound_names) {
        if (!_limit) {
            return {};
        }

        auto prep_limit = _limit->prepare(keyspace(), limit_receiver());
        prep_limit->collect_marker_specification(bound_names);
        return prep_limit;
    }

    static void verify_ordering_is_allowed(::shared_ptr<restrictions::statement_restrictions> restrictions) {
        if (restrictions->uses_secondary_indexing()) {
            throw exceptions::invalid_request_exception("ORDER BY with 2ndary indexes is not supported.");
        }
        if (restrictions->is_key_range()) {
            throw exceptions::invalid_request_exception("ORDER BY is only supported when the partition key is restricted by an EQ or an IN.");
        }
    }

    static void validate_distinct_selection(schema_ptr schema, ::shared_ptr<selection::selection> selection,
            ::shared_ptr<restrictions::statement_restrictions> restrictions) {
        for (auto&& def : selection->get_columns()) {
            if (!def->is_partition_key() && !def->is_static()) {
                throw exceptions::invalid_request_exception(sprint(
                       "SELECT DISTINCT queries must only request partition key columns and/or static columns (not %s)",
                       def->name_as_text()));
            }
        }

        // If it's a key range, we require that all partition key columns are selected so we don't have to bother
        // with post-query grouping.
        if (!restrictions->is_key_range()) {
            return;
        }

        for (auto&& def : schema->partition_key_columns()) {
            if (!selection->has_column(def)) {
                throw exceptions::invalid_request_exception(sprint(
                      "SELECT DISTINCT queries must request all the partition key columns (missing %s)", def.name_as_text()));
            }
        }
    }

    void handle_unrecognized_ordering_column(::shared_ptr<column_identifier> column) {
        if (contains_alias(column)) {
            throw exceptions::invalid_request_exception(sprint("Aliases are not allowed in order by clause ('%s')", *column));
        }
        throw exceptions::invalid_request_exception(sprint("Order by on unknown column %s", *column));
    }

    select_statement::ordering_comparator_type get_ordering_comparator(schema_ptr schema, ::shared_ptr<selection::selection> selection,
            ::shared_ptr<restrictions::statement_restrictions> restrictions) {
        if (!restrictions->key_is_in_relation()) {
            return {};
        }

        std::vector<std::pair<uint32_t, data_type>> sorters;
        sorters.reserve(_parameters->orderings().size());

        // If we order post-query (see orderResults), the sorted column needs to be in the ResultSet for sorting,
        // even if we don't
        // ultimately ship them to the client (CASSANDRA-4911).
        for (auto&& e : _parameters->orderings()) {
            auto&& raw = e.first;
            ::shared_ptr<column_identifier> column = raw->prepare_column_identifier(schema);
            const column_definition* def = schema->get_column_definition(column->name());
            if (!def) {
                handle_unrecognized_ordering_column(column);
            }
            auto index = selection->index_of(*def);
            if (index < 0) {
                index = selection->add_column_for_ordering(*def);
            }

            sorters.emplace_back(index, def->type);
        }

        return [sorters = std::move(sorters)] (const result_row_type& r1, const result_row_type& r2) mutable {
            for (auto&& e : sorters) {
                auto& c1 = r1[e.first];
                auto& c2 = r2[e.first];
                auto type = e.second;

                if (bool(c1) != bool(c2)) {
                    return bool(c2);
                }
                if (c1) {
                    int result = type->compare(*c1, *c2);
                    if (result != 0) {
                        return result < 0;
                    }
                }
            }
            return false;
        };
    }

    bool is_reversed(schema_ptr schema) {
        std::experimental::optional<bool> reversed_map[schema->clustering_key_size()];

        uint32_t i = 0;
        for (auto&& e : _parameters->orderings()) {
            ::shared_ptr<column_identifier> column = e.first->prepare_column_identifier(schema);
            bool reversed = e.second;

            auto def = schema->get_column_definition(column->name());
            if (!def) {
                handle_unrecognized_ordering_column(column);
            }

            if (!def->is_clustering_key()) {
                throw exceptions::invalid_request_exception(sprint(
                      "Order by is currently only supported on the clustered columns of the PRIMARY KEY, got %s", *column));
            }

            if (i != def->component_index()) {
                throw exceptions::invalid_request_exception(
                      "Order by currently only support the ordering of columns following their declared order in the PRIMARY KEY");
            }

            reversed_map[i] = std::experimental::make_optional(reversed != def->type->is_reversed());
            ++i;
        }

        // GCC incorrenctly complains about "*is_reversed_" below
        #pragma GCC diagnostic push
        #pragma GCC diagnostic ignored "-Wmaybe-uninitialized"

        // Check that all bool in reversedMap, if set, agrees
        std::experimental::optional<bool> is_reversed_{};
        for (auto&& b : reversed_map) {
            if (b) {
                if (!is_reversed_) {
                    is_reversed_ = b;
                } else {
                    if ((*is_reversed_) != *b) {
                        throw exceptions::invalid_request_exception("Unsupported order by relation");
                    }
                }
            }
        }

        assert(is_reversed_);
        return *is_reversed_;

        #pragma GCC diagnostic pop
    }

    /** If ALLOW FILTERING was not specified, this verifies that it is not needed */
    void check_needs_filtering(::shared_ptr<restrictions::statement_restrictions> restrictions) {
        // non-key-range non-indexed queries cannot involve filtering underneath
        if (!_parameters->allow_filtering() && (restrictions->is_key_range() || restrictions->uses_secondary_indexing())) {
            // We will potentially filter data if either:
            //  - Have more than one IndexExpression
            //  - Have no index expression and the column filter is not the identity
            if (restrictions->need_filtering()) {
                throw exceptions::invalid_request_exception(
                       "Cannot execute this query as it might involve data filtering and "
                       "thus may have unpredictable performance. If you want to execute "
                       "this query despite the performance unpredictability, use ALLOW FILTERING");
            }
        }
    }

    bool contains_alias(::shared_ptr<column_identifier> name) {
        return std::any_of(_select_clause.begin(), _select_clause.end(), [name] (auto raw) {
            return *name == *raw->alias;
        });
    }

    ::shared_ptr<column_specification> limit_receiver() {
        return ::make_shared<column_specification>(keyspace(), column_family(), ::make_shared<column_identifier>("[limit]", true),
            int32_type);
    }

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

#endif
