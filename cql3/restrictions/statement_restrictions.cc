#include <boost/range/algorithm/transform.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/range/adaptors.hpp>
#include "cql3/restrictions/statement_restrictions.hh"

namespace cql3 {
namespace restrictions {

using boost::adaptors::filtered;
using boost::adaptors::transformed;

std::vector<::shared_ptr<column_identifier>>
statement_restrictions::get_partition_key_unrestricted_components() const {
    std::vector<::shared_ptr<column_identifier>> r;

    auto restricted = _partition_key_restrictions->get_column_defs();
    auto is_not_restricted = [&restricted] (const column_definition& def) {
        return !boost::count(restricted, &def);
    };

    boost::copy(_schema->partition_key_columns() | filtered(is_not_restricted) | transformed(to_identifier),
        std::back_inserter(r));
    return r;
}

statement_restrictions::statement_restrictions(schema_ptr schema)
    : _schema(schema)
    , _nonprimary_key_restrictions(::make_shared<single_column_restrictions>(schema))
{ }

statement_restrictions::statement_restrictions(database& db,
        schema_ptr schema,
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
            add_restriction(relation->to_restriction(db, schema, bound_names));
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

void statement_restrictions::add_restriction(::shared_ptr<restriction> restriction) {
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

void statement_restrictions::add_single_column_restriction(::shared_ptr<single_column_restriction> restriction) {
    auto& def = restriction->get_column_def();
    if (def.is_partition_key()) {
        if (!_partition_key_restrictions) {
            _partition_key_restrictions = ::make_shared<single_column_primary_key_restrictions<partition_key>>(_schema);
        }
        _partition_key_restrictions->merge_with(restriction);
    } else if (def.is_clustering_key()) {
        if (!_clustering_columns_restrictions) {
            _clustering_columns_restrictions = ::make_shared<single_column_primary_key_restrictions<clustering_key_prefix>>(_schema);
        }
        _clustering_columns_restrictions->merge_with(restriction);
    } else {
        _nonprimary_key_restrictions->add_restriction(restriction);
    }
}

bool statement_restrictions::uses_function(const sstring& ks_name, const sstring& function_name) const {
    return  (_partition_key_restrictions && _partition_key_restrictions->uses_function(ks_name, function_name))
            || (_clustering_columns_restrictions && _clustering_columns_restrictions->uses_function(ks_name, function_name))
            || _nonprimary_key_restrictions->uses_function(ks_name, function_name);
}

void statement_restrictions::process_partition_key_restrictions(bool has_queriable_index) {
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

bool statement_restrictions::has_partition_key_unrestricted_components() const {
    if (!_partition_key_restrictions) {
        return _schema->partition_key_size();
    }
    return _partition_key_restrictions->size() < _schema->partition_key_size();
}

void statement_restrictions::process_clustering_columns_restrictions(bool has_queriable_index, bool select_a_collection) {
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

std::vector<query::partition_range> statement_restrictions::get_partition_key_ranges(const query_options& options) const {
    if (!_partition_key_restrictions) {
        return {query::partition_range::make_open_ended_both_sides()};
    }
    std::vector<query::partition_range> ranges;
    ranges.reserve(_partition_key_restrictions->size());
    for (query::range<partition_key>& r : _partition_key_restrictions->bounds(options)) {
        if (!r.is_singular()) {
            throw exceptions::invalid_request_exception("Range queries on partition key values not supported.");
        }
        ranges.emplace_back(std::move(r).transform<query::ring_position>(
            [this] (partition_key&& k) -> query::ring_position {
                auto token = dht::global_partitioner().get_token(*_schema, k);
                return { std::move(token), std::move(k) };
            }));
    }
    return ranges;
}

std::vector<query::clustering_range> statement_restrictions::get_clustering_bounds(const query_options& options) const {
    if (!_clustering_columns_restrictions) {
        return {query::clustering_range::make_open_ended_both_sides()};
    }
    return _clustering_columns_restrictions->bounds(options);
}

bool statement_restrictions::need_filtering() {
    uint32_t number_of_restricted_columns = 0;
    for (auto&& restrictions : _index_restrictions) {
        number_of_restricted_columns += restrictions->size();
    }

    return number_of_restricted_columns > 1
           || (number_of_restricted_columns == 0 && has_clustering_columns_restriction())
           || (number_of_restricted_columns != 0 && _nonprimary_key_restrictions->has_multiple_contains());
}

void statement_restrictions::validate_secondary_index_selections(bool selects_only_static_columns) {
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

void statement_restrictions::reverse() {
    if (_clustering_columns_restrictions) {
        _clustering_columns_restrictions = ::make_shared<reversed_primary_key_restrictions<clustering_key_prefix>>(
            _clustering_columns_restrictions);
    }
}

}
}
