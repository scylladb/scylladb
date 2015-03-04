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

#pragma once

#include <vector>
#include "to_string.hh"
#include "schema.hh"
#include "cql3/restrictions/restrictions.hh"
#include "cql3/restrictions/primary_key_restrictions.hh"
#include "cql3/restrictions/single_column_restrictions.hh"
#include "cql3/restrictions/single_column_primary_key_restrictions.hh"
#include "cql3/restrictions/reverse_primary_key_restrictions.hh"
#include "cql3/relation.hh"
#include "cql3/variable_specifications.hh"

namespace cql3 {

namespace restrictions {


/**
 * The restrictions corresponding to the relations specified on the where-clause of CQL query.
 */
class statement_restrictions {
private:
    schema_ptr _schema;

    /**
     * Restrictions on partitioning columns
     */
    ::shared_ptr<primary_key_restrictions> _partition_key_restrictions;

    /**
     * Restrictions on clustering columns
     */
    ::shared_ptr<primary_key_restrictions> _clustering_columns_restrictions;

    /**
     * Restriction on non-primary key columns (i.e. secondary index restrictions)
     */
    ::shared_ptr<single_column_restrictions> _nonprimary_key_restrictions;

    /**
     * The restrictions used to build the index expressions
     */
    std::vector<::shared_ptr<restrictions>> _index_restrictions;

    /**
     * <code>true</code> if the secondary index need to be queried, <code>false</code> otherwise
     */
    bool _uses_secondary_indexing = false;

    /**
     * Specify if the query will return a range of partition keys.
     */
    bool _is_key_range = false;

public:
    /**
     * Creates a new empty <code>StatementRestrictions</code>.
     *
     * @param cfm the column family meta data
     * @return a new empty <code>StatementRestrictions</code>.
     */
    statement_restrictions(schema_ptr schema)
        : _schema(schema)
        , _nonprimary_key_restrictions(::make_shared<single_column_restrictions>(schema))
    { }

    statement_restrictions(schema_ptr schema,
            const std::vector<::shared_ptr<relation>>& where_clause,
            ::shared_ptr<variable_specifications> bound_names,
            bool selects_only_static_columns,
            bool select_a_collection)
        : statement_restrictions(schema)
    {
        /*
         * WHERE clause. For a given entity, rules are: - EQ relation conflicts with anything else (including a 2nd EQ)
         * - Can't have more than one LT(E) relation (resp. GT(E) relation) - IN relation are restricted to row keys
         * (for now) and conflicts with anything else (we could allow two IN for the same entity but that doesn't seem
         * very useful) - The value_alias cannot be restricted in any way (we don't support wide rows with indexed value
         * in CQL so far)
         */
        if (!where_clause.empty()) {
            for (auto&& relation : where_clause) {
                add_restriction(relation->to_restriction(schema, bound_names));
            }
        }

        warn(unimplemented::cause::INDEXES);
#if 0
        ColumnFamilyStore cfs = Keyspace.open(cfm.ks_name).getColumnFamilyStore(cfm.cfName);
        secondary_index_manager secondaryIndexManager = cfs.index_manager;
#endif
        bool has_queriable_clustering_column_index = false; /*_clustering_columns_restrictions->has_supporting_index(secondaryIndexManager);*/
        bool has_queriable_index = false; /*has_queriable_clustering_column_index
                || _partition_key_restrictions->has_supporting_index(secondaryIndexManager)
                || nonprimary_key_restrictions->has_supporting_index(secondaryIndexManager);*/

        // At this point, the select statement if fully constructed, but we still have a few things to validate
        process_partition_key_restrictions(has_queriable_index);

        // Some but not all of the partition key columns have been specified;
        // hence we need turn these restrictions into index expressions.
        if (_uses_secondary_indexing) {
            _index_restrictions.push_back(_partition_key_restrictions);
        }

        if (selects_only_static_columns && has_clustering_columns_restriction()) {
            throw exceptions::invalid_request_exception(
                "Cannot restrict clustering columns when selecting only static columns");
        }

        process_clustering_columns_restrictions(has_queriable_index, select_a_collection);

        // Covers indexes on the first clustering column (among others).
        if (_is_key_range && has_queriable_clustering_column_index)
            _uses_secondary_indexing = true;

        if (_uses_secondary_indexing) {
            _index_restrictions.push_back(_clustering_columns_restrictions);
        } else if (_clustering_columns_restrictions && _clustering_columns_restrictions->is_contains()) {
            fail(unimplemented::cause::INDEXES);
#if 0
            _index_restrictions.push_back(new Forwardingprimary_key_restrictions() {

                @Override
                protected primary_key_restrictions getDelegate()
                {
                    return _clustering_columns_restrictions;
                }

                @Override
                public void add_index_expression_to(List<::shared_ptr<index_expression>> expressions, const query_options& options) throws InvalidRequestException
                {
                    List<::shared_ptr<index_expression>> list = new ArrayList<>();
                    super.add_index_expression_to(list, options);

                    for (::shared_ptr<index_expression> expression : list)
                    {
                        if (expression.is_contains() || expression.is_containsKey())
                            expressions.add(expression);
                    }
                }
            });
            uses_secondary_indexing = true;
#endif
        }
        // Even if uses_secondary_indexing is false at this point, we'll still have to use one if
        // there is restrictions not covered by the PK.
        if (!_nonprimary_key_restrictions->empty()) {
            _uses_secondary_indexing = true;
            _index_restrictions.push_back(_nonprimary_key_restrictions);
        }

        if (_uses_secondary_indexing) {
            fail(unimplemented::cause::INDEXES);
#if 0
            validate_secondary_index_selections(selects_only_static_columns);
#endif
        }
    }

private:
    void add_restriction(::shared_ptr<restriction> restriction) {
        if (restriction->is_multi_column()) {
            throw std::runtime_error("not implemented");
#if 0
            if (!_clustering_columns_restrictions) {
                _clustering_columns_restrictions = static_pointer_cast<multi_column_restriction>(restriction);
            } else {
                _clustering_columns_restrictions->merge_with(restriction);
            }
#endif
        } else if (restriction->is_on_token()) {
            throw std::runtime_error("not implemented");
#if 0
            if (!_partition_key_restrictions) {
                _partition_key_restrictions = static_pointer_cast<token_restriction>(restriction);
            } else {
                _partition_key_restrictions->merge_with(restriction);
            }
#endif
        } else {
            add_single_column_restriction(::static_pointer_cast<single_column_restriction>(restriction));
        }
    }

    void add_single_column_restriction(::shared_ptr<single_column_restriction> restriction) {
        auto& def = restriction->get_column_def();
        if (def.is_partition_key()) {
            if (!_partition_key_restrictions) {
                _partition_key_restrictions = ::make_shared<single_column_primary_key_restrictions>(_schema,
                    _schema->partition_key_prefix_type);
            }
            _partition_key_restrictions->merge_with(restriction);
        } else if (def.is_clustering_key()) {
            if (!_clustering_columns_restrictions) {
                _clustering_columns_restrictions = ::make_shared<single_column_primary_key_restrictions>(_schema,
                    _schema->clustering_key_prefix_type);
            }
            _clustering_columns_restrictions->merge_with(restriction);
        } else {
            _nonprimary_key_restrictions->add_restriction(restriction);
        }
    }

public:
    bool uses_function(const sstring& ks_name, const sstring& function_name) const {
        return  (_partition_key_restrictions && _partition_key_restrictions->uses_function(ks_name, function_name))
                || (_clustering_columns_restrictions && _clustering_columns_restrictions->uses_function(ks_name, function_name))
                || _nonprimary_key_restrictions->uses_function(ks_name, function_name);
    }

    /**
     * Checks if the restrictions on the partition key is an IN restriction.
     *
     * @return <code>true</code> the restrictions on the partition key is an IN restriction, <code>false</code>
     * otherwise.
     */
    bool key_is_in_relation() const {
        return _partition_key_restrictions && _partition_key_restrictions->is_IN();
    }

    /**
     * Checks if the query request a range of partition keys.
     *
     * @return <code>true</code> if the query request a range of partition keys, <code>false</code> otherwise.
     */
    bool is_key_range() const {
        return _is_key_range;
    }

    /**
     * Checks if the secondary index need to be queried.
     *
     * @return <code>true</code> if the secondary index need to be queried, <code>false</code> otherwise.
     */
    bool uses_secondary_indexing() const {
        return _uses_secondary_indexing;
    }

private:
    void process_partition_key_restrictions(bool has_queriable_index) {
        // If there is a queriable index, no special condition are required on the other restrictions.
        // But we still need to know 2 things:
        // - If we don't have a queriable index, is the query ok
        // - Is it queriable without 2ndary index, which is always more efficient
        // If a component of the partition key is restricted by a relation, all preceding
        // components must have a EQ. Only the last partition key component can be in IN relation.
        if (_partition_key_restrictions && _partition_key_restrictions->is_on_token()) {
            _is_key_range = true;
        } else if (has_partition_key_unrestricted_components()) {
            if (_partition_key_restrictions) {
                if (!has_queriable_index) {
                    throw exceptions::invalid_request_exception(sprint("Partition key parts: %s must be restricted as other parts are",
                        join(", ", get_partition_key_unrestricted_components())));
                }
            }

            _is_key_range = true;
            _uses_secondary_indexing = has_queriable_index;
        }
    }

    /**
     * Checks if the partition key has some unrestricted components.
     * @return <code>true</code> if the partition key has some unrestricted components, <code>false</code> otherwise.
     */
    bool has_partition_key_unrestricted_components() const {
        if (!_partition_key_restrictions) {
            return _schema->partition_key_size();
        }
        return _partition_key_restrictions->size() < _schema->partition_key_size();
    }

    /**
     * Returns the partition key components that are not restricted.
     * @return the partition key components that are not restricted.
     */
    std::vector<::shared_ptr<column_identifier>> get_partition_key_unrestricted_components() const;

    /**
     * Processes the clustering column restrictions.
     *
     * @param has_queriable_index <code>true</code> if some of the queried data are indexed, <code>false</code> otherwise
     * @param select_a_collection <code>true</code> if the query should return a collection column
     * @throws InvalidRequestException if the request is invalid
     */
    void process_clustering_columns_restrictions(bool has_queriable_index, bool select_a_collection) {
        if (!has_clustering_columns_restriction()) {
            return;
        }

        if (_clustering_columns_restrictions->is_IN() && select_a_collection) {
            throw exceptions::invalid_request_exception(
                "Cannot restrict clustering columns by IN relations when a collection is selected by the query");
        }
        if (_clustering_columns_restrictions->is_contains() && !has_queriable_index) {
            throw exceptions::invalid_request_exception(
                "Cannot restrict clustering columns by a CONTAINS relation without a secondary index");
        }

        auto clustering_columns_iter = _schema->clustering_key_columns().begin();

        for (auto&& restricted_column : _clustering_columns_restrictions->get_column_defs()) {
            const column_definition* clustering_column = &(*clustering_columns_iter);
            ++clustering_columns_iter;

            if (clustering_column != restricted_column) {
                if (!has_queriable_index) {
                    throw exceptions::invalid_request_exception(sprint(
                          "PRIMARY KEY column \"%s\" cannot be restricted as preceding column \"%s\" is not restricted",
                          restricted_column->name_as_text(), clustering_column->name_as_text()));
                }

                _uses_secondary_indexing = true; // handle gaps and non-keyrange cases.
                break;
            }
        }

        if (_clustering_columns_restrictions->is_contains()) {
            _uses_secondary_indexing = true;
        }
    }

#if 0
    std::vector<::shared_ptr<index_expression>> get_index_expressions(const query_options& options) {
        if (!_uses_secondary_indexing || _index_restrictions.empty()) {
            return {};
        }

        std::vector<::shared_ptr<index_expression>> expressions;
        for (auto&& restrictions : _index_restrictions) {
            restrictions->add_index_expression_to(expressions, options);
        }

        return expressions;
    }
#endif

#if 0
    /**
     * Returns the partition keys for which the data is requested.
     *
     * @param options the query options
     * @return the partition keys for which the data is requested.
     * @throws InvalidRequestException if the partition keys cannot be retrieved
     */
    std::vector<bytes> get_partition_keys(const query_options& options) const {
        return _partition_key_restrictions->values(options);
    }
#endif

public:
    /**
     * Returns the specified range of the partition key.
     *
     * @param b the boundary type
     * @param options the query options
     * @return the specified bound of the partition key
     * @throws InvalidRequestException if the boundary cannot be retrieved
     */
    std::vector<query::range> get_partition_key_ranges(const query_options& options) const {
        if (!_partition_key_restrictions) {
            return {query::range::make_open_ended_both_sides()};
        }
        return _partition_key_restrictions->bounds(options);
    }

#if 0
    /**
     * Returns the partition key bounds.
     *
     * @param options the query options
     * @return the partition key bounds
     * @throws InvalidRequestException if the query is invalid
     */
    AbstractBounds<RowPosition> get_partition_key_bounds(const query_options& options) {
        auto p = global_partitioner();

        if (_partition_key_restrictions->is_on_token()) {
            return get_partition_key_bounds_for_token_restrictions(p, options);
        }

        return get_partition_key_bounds(p, options);
    }

private:
    private AbstractBounds<RowPosition> get_partition_key_bounds(IPartitioner p,
                                                              const query_options& options) throws InvalidRequestException
    {
        ByteBuffer startKeyBytes = get_partition_key_bound(Bound.START, options);
        ByteBuffer finishKeyBytes = get_partition_key_bound(Bound.END, options);

        RowPosition startKey = RowPosition.ForKey.get(startKeyBytes, p);
        RowPosition finishKey = RowPosition.ForKey.get(finishKeyBytes, p);

        if (startKey.compareTo(finishKey) > 0 && !finishKey.isMinimum())
            return null;

        if (_partition_key_restrictions->isInclusive(Bound.START))
        {
            return _partition_key_restrictions->isInclusive(Bound.END)
                    ? new Bounds<>(startKey, finishKey)
                    : new IncludingExcludingBounds<>(startKey, finishKey);
        }

        return _partition_key_restrictions->isInclusive(Bound.END)
                ? new Range<>(startKey, finishKey)
                : new ExcludingBounds<>(startKey, finishKey);
    }

    private AbstractBounds<RowPosition> get_partition_key_bounds_for_token_restriction(IPartitioner p,
                                                                                  const query_options& options)
                                                                                          throws InvalidRequestException
    {
        Token startToken = getTokenBound(Bound.START, options, p);
        Token endToken = getTokenBound(Bound.END, options, p);

        bool includeStart = _partition_key_restrictions->isInclusive(Bound.START);
        bool includeEnd = _partition_key_restrictions->isInclusive(Bound.END);

        /*
         * If we ask SP.getRangeSlice() for (token(200), token(200)], it will happily return the whole ring.
         * However, wrapping range doesn't really make sense for CQL, and we want to return an empty result in that
         * case (CASSANDRA-5573). So special case to create a range that is guaranteed to be empty.
         *
         * In practice, we want to return an empty result set if either startToken > endToken, or both are equal but
         * one of the bound is excluded (since [a, a] can contains something, but not (a, a], [a, a) or (a, a)).
         * Note though that in the case where startToken or endToken is the minimum token, then this special case
         * rule should not apply.
         */
        int cmp = startToken.compareTo(endToken);
        if (!startToken.isMinimum() && !endToken.isMinimum()
                && (cmp > 0 || (cmp == 0 && (!includeStart || !includeEnd))))
            return null;

        RowPosition start = includeStart ? startToken.minKeyBound() : startToken.maxKeyBound();
        RowPosition end = includeEnd ? endToken.maxKeyBound() : endToken.minKeyBound();

        return new Range<>(start, end);
    }

    private Token getTokenBound(Bound b, const query_options& options, IPartitioner p) throws InvalidRequestException
    {
        if (!_partition_key_restrictions->hasBound(b))
            return p.getMinimumToken();

        ByteBuffer value = _partition_key_restrictions->bounds(b, options).get(0);
        checkNotNull(value, "Invalid null token value");
        return p.getTokenFactory().fromByteArray(value);
    }
#endif

public:
    /**
     * Checks if the query does not contains any restriction on the clustering columns.
     *
     * @return <code>true</code> if the query does not contains any restriction on the clustering columns,
     * <code>false</code> otherwise.
     */
    bool has_no_clustering_columns_restriction() const {
        return bool(_clustering_columns_restrictions);
    }

#if 0
    // For non-composite slices, we don't support internally the difference between exclusive and
    // inclusive bounds, so we deal with it manually.
    bool is_non_composite_slice_with_exclusive_bounds()
    {
        return !cfm.comparator.isCompound()
                && _clustering_columns_restrictions->isSlice()
                && (!_clustering_columns_restrictions->isInclusive(Bound.START) || !_clustering_columns_restrictions->isInclusive(Bound.END));
    }

    /**
    * Returns the requested clustering columns as <code>Composite</code>s.
    *
    * @param options the query options
    * @return the requested clustering columns as <code>Composite</code>s
    * @throws InvalidRequestException if the query is not valid
    */
    public List<Composite> getClusteringColumnsAsComposites(QueryOptions options) throws InvalidRequestException
    {
        return clusteringColumnsRestrictions.valuesAsComposites(options);
    }
#endif

public:
    std::vector<query::range> get_clustering_bounds(const query_options& options) const {
        if (!_clustering_columns_restrictions) {
            return {query::range::make_open_ended_both_sides()};
        }
        return _clustering_columns_restrictions->bounds(options);
    }

    /**
     * Checks if the query need to use filtering.
     * @return <code>true</code> if the query need to use filtering, <code>false</code> otherwise.
     */
    bool need_filtering() {
        uint32_t number_of_restricted_columns = 0;
        for (auto&& restrictions : _index_restrictions) {
            number_of_restricted_columns += restrictions->size();
        }

        return number_of_restricted_columns > 1
                || (number_of_restricted_columns == 0 && has_clustering_columns_restriction())
                || (number_of_restricted_columns != 0 && _nonprimary_key_restrictions->has_multiple_contains());
    }

    void validate_secondary_index_selections(bool selects_only_static_columns) {
        if (key_is_in_relation()) {
            throw exceptions::invalid_request_exception(
                   "Select on indexed columns and with IN clause for the PRIMARY KEY are not supported");
        }
        // When the user only select static columns, the intent is that we don't query the whole partition but just
        // the static parts. But 1) we don't have an easy way to do that with 2i and 2) since we don't support index on
        // static columns
        // so far, 2i means that you've restricted a non static column, so the query is somewhat non-sensical.
        if (selects_only_static_columns) {
            throw exceptions::invalid_request_exception(
                "Queries using 2ndary indexes don't support selecting only static columns");
        }
    }

    /**
     * Checks if the query has some restrictions on the clustering columns.
     *
     * @return <code>true</code> if the query has some restrictions on the clustering columns,
     * <code>false</code> otherwise.
     */
    bool has_clustering_columns_restriction() {
        return bool(_clustering_columns_restrictions);
    }

    void reverse() {
        if (_clustering_columns_restrictions) {
            _clustering_columns_restrictions = ::make_shared<reversed_primary_key_restrictions>(_clustering_columns_restrictions);
        }
    }
};

}

}
