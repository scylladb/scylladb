
/*
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

#include <boost/range/algorithm/transform.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/range/adaptors.hpp>

#include "statement_restrictions.hh"
#include "single_column_primary_key_restrictions.hh"
#include "token_restriction.hh"

#include "cql3/single_column_relation.hh"
#include "cql3/constants.hh"

#include "stdx.hh"

namespace cql3 {
namespace restrictions {

using boost::adaptors::filtered;
using boost::adaptors::transformed;

template<typename T>
class statement_restrictions::initial_key_restrictions : public primary_key_restrictions<T> {
public:
    using bounds_range_type = typename primary_key_restrictions<T>::bounds_range_type;

    ::shared_ptr<primary_key_restrictions<T>> do_merge_to(schema_ptr schema, ::shared_ptr<restriction> restriction) const {
        if (restriction->is_multi_column()) {
            throw std::runtime_error(sprint("%s not implemented", __PRETTY_FUNCTION__));
        }
        return ::make_shared<single_column_primary_key_restrictions<T>>(schema)->merge_to(schema, restriction);
    }
    ::shared_ptr<primary_key_restrictions<T>> merge_to(schema_ptr schema, ::shared_ptr<restriction> restriction) override {
        if (restriction->is_multi_column()) {
            throw std::runtime_error(sprint("%s not implemented", __PRETTY_FUNCTION__));
        }
        if (restriction->is_on_token()) {
            return static_pointer_cast<token_restriction>(restriction);
        }
        return ::make_shared<single_column_primary_key_restrictions<T>>(schema)->merge_to(restriction);
    }
    void merge_with(::shared_ptr<restriction> restriction) override {
        throw exceptions::unsupported_operation_exception();
    }
    std::vector<bytes_opt> values(const query_options& options) const override {
        // throw? should not reach?
        return {};
    }
    std::vector<T> values_as_keys(const query_options& options) const override {
        // throw? should not reach?
        return {};
    }
    std::vector<bounds_range_type> bounds_ranges(const query_options&) const override {
        // throw? should not reach?
        return {};
    }
    std::vector<const column_definition*> get_column_defs() const override {
        // throw? should not reach?
        return {};
    }
    bool uses_function(const sstring&, const sstring&) const override {
        return false;
    }
    bool empty() const override {
        return true;
    }
    uint32_t size() const override {
        return 0;
    }
    virtual bool has_supporting_index(const secondary_index::secondary_index_manager& index_manager) const override {
        return false;
    }
    sstring to_string() const override {
        return "Initial restrictions";
    }
    virtual bool is_satisfied_by(const schema& schema,
                                 const partition_key& key,
                                 const clustering_key_prefix& ckey,
                                 const row& cells,
                                 const query_options& options,
                                 gc_clock::time_point now) const override {
        return true;
    }
};

template<>
::shared_ptr<primary_key_restrictions<partition_key>>
statement_restrictions::initial_key_restrictions<partition_key>::merge_to(schema_ptr schema, ::shared_ptr<restriction> restriction) {
    if (restriction->is_on_token()) {
        return static_pointer_cast<token_restriction>(restriction);
    }
    return do_merge_to(std::move(schema), std::move(restriction));
}

template<>
::shared_ptr<primary_key_restrictions<clustering_key_prefix>>
statement_restrictions::initial_key_restrictions<clustering_key_prefix>::merge_to(schema_ptr schema, ::shared_ptr<restriction> restriction) {
    if (restriction->is_multi_column()) {
        return static_pointer_cast<primary_key_restrictions<clustering_key_prefix>>(restriction);
    }
    return do_merge_to(std::move(schema), std::move(restriction));
}

template<typename T>
::shared_ptr<primary_key_restrictions<T>> statement_restrictions::get_initial_key_restrictions() {
    static thread_local ::shared_ptr<primary_key_restrictions<T>> initial_kr = ::make_shared<initial_key_restrictions<T>>();
    return initial_kr;
}

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
    , _partition_key_restrictions(get_initial_key_restrictions<partition_key>())
    , _clustering_columns_restrictions(get_initial_key_restrictions<clustering_key_prefix>())
    , _nonprimary_key_restrictions(::make_shared<single_column_restrictions>(schema))
{ }
#if 0
static const column_definition*
to_column_definition(const schema_ptr& schema, const ::shared_ptr<column_identifier::raw>& entity) {
    return get_column_definition(schema,
            *entity->prepare_column_identifier(schema));
}
#endif

statement_restrictions::statement_restrictions(database& db,
        schema_ptr schema,
        statements::statement_type type,
        const std::vector<::shared_ptr<relation>>& where_clause,
        ::shared_ptr<variable_specifications> bound_names,
        bool selects_only_static_columns,
        bool select_a_collection,
        bool for_view)
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
            if (relation->get_operator() == cql3::operator_type::IS_NOT) {
                single_column_relation* r =
                        dynamic_cast<single_column_relation*>(relation.get());
                // The "IS NOT NULL" restriction is only supported (and
                // mandatory) for materialized view creation:
                if (!r) {
                    throw exceptions::invalid_request_exception("IS NOT only supports single column");
                }
                // currently, the grammar only allows the NULL argument to be
                // "IS NOT", so this assertion should not be able to fail
                assert(r->get_value() == cql3::constants::NULL_LITERAL);

                auto col_id = r->get_entity()->prepare_column_identifier(schema);
                const auto *cd = get_column_definition(schema, *col_id);
                if (!cd) {
                    throw exceptions::invalid_request_exception(sprint("restriction '%s' unknown column %s", relation->to_string(), r->get_entity()->to_string()));
                }
                _not_null_columns.insert(cd);

                if (!for_view) {
                    throw exceptions::invalid_request_exception(sprint("restriction '%s' is only supported in materialized view creation", relation->to_string()));
                }
            } else {
                add_restriction(relation->to_restriction(db, schema, bound_names));
            }
        }
    }
    auto& cf = db.find_column_family(schema);
    auto& sim = cf.get_index_manager();
    bool has_queriable_clustering_column_index = _clustering_columns_restrictions->has_supporting_index(sim);
    bool has_queriable_index = has_queriable_clustering_column_index
            || _partition_key_restrictions->has_supporting_index(sim)
            || _nonprimary_key_restrictions->has_supporting_index(sim);

    // At this point, the select statement if fully constructed, but we still have a few things to validate
    process_partition_key_restrictions(has_queriable_index, for_view);

    // Some but not all of the partition key columns have been specified;
    // hence we need turn these restrictions into index expressions.
    if (_uses_secondary_indexing) {
        _index_restrictions.push_back(_partition_key_restrictions);
    }

    if (selects_only_static_columns && has_clustering_columns_restriction()) {
        if (type.is_update() || type.is_delete()) {
            throw exceptions::invalid_request_exception(sprint(
                "Invalid restrictions on clustering columns since the %s statement modifies only static columns", type));
        }

        if (type.is_select()) {
            throw exceptions::invalid_request_exception(
                "Cannot restrict clustering columns when selecting only static columns");
        }
    }

    process_clustering_columns_restrictions(has_queriable_index, select_a_collection, for_view);

    // Covers indexes on the first clustering column (among others).
    if (_is_key_range && has_queriable_clustering_column_index)
    _uses_secondary_indexing = true;

    if (_uses_secondary_indexing) {
        _index_restrictions.push_back(_clustering_columns_restrictions);
    } else if (_clustering_columns_restrictions->is_contains()) {
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

    if (_uses_secondary_indexing && !for_view) {
        validate_secondary_index_selections(selects_only_static_columns);
    }
}

void statement_restrictions::add_restriction(::shared_ptr<restriction> restriction) {
    if (restriction->is_multi_column()) {
        _clustering_columns_restrictions = _clustering_columns_restrictions->merge_to(_schema, restriction);
    } else if (restriction->is_on_token()) {
        _partition_key_restrictions = _partition_key_restrictions->merge_to(_schema, restriction);
    } else {
        add_single_column_restriction(::static_pointer_cast<single_column_restriction>(restriction));
    }
}

void statement_restrictions::add_single_column_restriction(::shared_ptr<single_column_restriction> restriction) {
    auto& def = restriction->get_column_def();
    if (def.is_partition_key()) {
        _partition_key_restrictions = _partition_key_restrictions->merge_to(_schema, restriction);
    } else if (def.is_clustering_key()) {
        _clustering_columns_restrictions = _clustering_columns_restrictions->merge_to(_schema, restriction);
    } else {
        _nonprimary_key_restrictions->add_restriction(restriction);
    }
}

bool statement_restrictions::uses_function(const sstring& ks_name, const sstring& function_name) const {
    return  _partition_key_restrictions->uses_function(ks_name, function_name)
            || _clustering_columns_restrictions->uses_function(ks_name, function_name)
            || _nonprimary_key_restrictions->uses_function(ks_name, function_name);
}

const std::vector<::shared_ptr<restrictions>>& statement_restrictions::index_restrictions() const {
    return _index_restrictions;
}

void statement_restrictions::process_partition_key_restrictions(bool has_queriable_index, bool for_view) {
    // If there is a queriable index, no special condition are required on the other restrictions.
    // But we still need to know 2 things:
    // - If we don't have a queriable index, is the query ok
    // - Is it queriable without 2ndary index, which is always more efficient
    // If a component of the partition key is restricted by a relation, all preceding
    // components must have a EQ. Only the last partition key component can be in IN relation.
    if (_partition_key_restrictions->is_on_token()) {
        _is_key_range = true;
    } else if (has_partition_key_unrestricted_components()) {
        if (!_partition_key_restrictions->empty() && !for_view) {
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
    return _partition_key_restrictions->size() < _schema->partition_key_size();
}

bool statement_restrictions::has_unrestricted_clustering_columns() const {
    return _clustering_columns_restrictions->size() < _schema->clustering_key_size();
}

void statement_restrictions::process_clustering_columns_restrictions(bool has_queriable_index, bool select_a_collection, bool for_view) {
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

        if (clustering_column != restricted_column && !for_view) {
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

dht::partition_range_vector statement_restrictions::get_partition_key_ranges(const query_options& options) const {
    if (_partition_key_restrictions->empty()) {
        return {dht::partition_range::make_open_ended_both_sides()};
    }
    return _partition_key_restrictions->bounds_ranges(options);
}

std::vector<query::clustering_range> statement_restrictions::get_clustering_bounds(const query_options& options) const {
    if (_clustering_columns_restrictions->empty()) {
        return {query::clustering_range::make_open_ended_both_sides()};
    }
    return _clustering_columns_restrictions->bounds_ranges(options);
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

static bytes_view_opt do_get_value(const schema& schema,
        const column_definition& cdef,
        const partition_key& key,
        const clustering_key_prefix& ckey,
        const row& cells,
        gc_clock::time_point now) {
    switch(cdef.kind) {
        case column_kind::partition_key:
            return key.get_component(schema, cdef.component_index());
        case column_kind::clustering_key:
            return ckey.get_component(schema, cdef.component_index());
        default:
            auto cell = cells.find_cell(cdef.id);
            if (!cell) {
                return stdx::nullopt;
            }
            assert(cdef.is_atomic());
            auto c = cell->as_atomic_cell();
            return c.is_dead(now) ? stdx::nullopt : bytes_view_opt(c.value());
    }
}

bytes_view_opt single_column_restriction::get_value(const schema& schema,
        const partition_key& key,
        const clustering_key_prefix& ckey,
        const row& cells,
        gc_clock::time_point now) const {
    return do_get_value(schema, _column_def, key, ckey, cells, std::move(now));
}

bool single_column_restriction::EQ::is_satisfied_by(const schema& schema,
        const partition_key& key,
        const clustering_key_prefix& ckey,
        const row& cells,
        const query_options& options,
        gc_clock::time_point now) const {
    if (_column_def.type->is_counter()) {
        fail(unimplemented::cause::COUNTERS);
    }
    auto operand = value(options);
    if (operand) {
        auto cell_value = get_value(schema, key, ckey, cells, now);
        return cell_value && _column_def.type->compare(*operand, *cell_value) == 0;
    }
    return false;
}

bool single_column_restriction::IN::is_satisfied_by(const schema& schema,
        const partition_key& key,
        const clustering_key_prefix& ckey,
        const row& cells,
        const query_options& options,
        gc_clock::time_point now) const {
    if (_column_def.type->is_counter()) {
        fail(unimplemented::cause::COUNTERS);
    }
    auto cell_value = get_value(schema, key, ckey, cells, now);
    if (!cell_value) {
        return false;
    }
    auto operands = values(options);
    return std::any_of(operands.begin(), operands.end(), [&] (auto&& operand) {
        return operand && _column_def.type->compare(*operand, *cell_value) == 0;
    });
}

static query::range<bytes_view> to_range(const term_slice& slice, const query_options& options) {
    using range_type = query::range<bytes_view>;
    auto extract_bound = [&] (statements::bound bound) -> stdx::optional<range_type::bound> {
        if (!slice.has_bound(bound)) {
            return { };
        }
        auto value = slice.bound(bound)->bind_and_get(options);
        if (!value) {
            return { };
        }
        return { range_type::bound(*value, slice.is_inclusive(bound)) };
    };
    return range_type(
        extract_bound(statements::bound::START),
        extract_bound(statements::bound::END));
}

bool single_column_restriction::slice::is_satisfied_by(const schema& schema,
        const partition_key& key,
        const clustering_key_prefix& ckey,
        const row& cells,
        const query_options& options,
        gc_clock::time_point now) const {
    if (_column_def.type->is_counter()) {
        fail(unimplemented::cause::COUNTERS);
    }
    auto cell_value = get_value(schema, key, ckey, cells, now);
    if (!cell_value) {
        return false;
    }
    return to_range(_slice, options).contains(*cell_value, _column_def.type->as_tri_comparator());
}

bool single_column_restriction::contains::is_satisfied_by(const schema& schema,
        const partition_key& key,
        const clustering_key_prefix& ckey,
        const row& cells,
        const query_options& options,
        gc_clock::time_point now) const {
    if (_column_def.type->is_counter()) {
        fail(unimplemented::cause::COUNTERS);
    }
    if (!_column_def.type->is_collection()) {
        return false;
    }

    auto col_type = static_pointer_cast<const collection_type_impl>(_column_def.type);
    if ((!_keys.empty() || !_entry_keys.empty()) && !col_type->is_map()) {
        return false;
    }
    assert(_entry_keys.size() == _entry_values.size());

    auto&& map_key_type = col_type->name_comparator();
    auto&& element_type = col_type->is_set() ? col_type->name_comparator() : col_type->value_comparator();
    if (_column_def.type->is_multi_cell()) {
        auto cell = cells.find_cell(_column_def.id);
        auto&& elements = col_type->deserialize_mutation_form(cell->as_collection_mutation()).cells;
        auto end = std::remove_if(elements.begin(), elements.end(), [now] (auto&& element) {
            return element.second.is_dead(now);
        });
        for (auto&& value : _values) {
            auto val = value->bind_and_get(options);
            if (!val) {
                continue;
            }
            auto found = std::find_if(elements.begin(), end, [&] (auto&& element) {
                return element_type->compare(element.second.value(), *val) == 0;
            });
            if (found == end) {
                return false;
            }
        }
        for (auto&& key : _keys) {
            auto k = key->bind_and_get(options);
            if (!k) {
                continue;
            }
            auto found = std::find_if(elements.begin(), end, [&] (auto&& element) {
                return map_key_type->compare(element.first, *k) == 0;
            });
            if (found == end) {
                return false;
            }
        }
        for (uint32_t i = 0; i < _entry_keys.size(); ++i) {
            auto map_key = _entry_keys[i]->bind_and_get(options);
            auto map_value = _entry_values[i]->bind_and_get(options);
            if (!map_key || !map_value) {
                continue;
            }
            auto found = std::find_if(elements.begin(), end, [&] (auto&& element) {
                return map_key_type->compare(element.first, *map_key) == 0;
            });
            if (found == end || element_type->compare(found->second.value(), *map_value) != 0) {
                return false;
            }
        }
    } else {
        auto cell_value = get_value(schema, key, ckey, cells, now);
        if (!cell_value) {
            return false;
        }
        auto deserialized = _column_def.type->deserialize(*cell_value);
        for (auto&& value : _values) {
            auto val = value->bind_and_get(options);
            if (!val) {
                continue;
            }
            auto exists_in = [&](auto&& range) {
                auto found = std::find_if(range.begin(), range.end(), [&] (auto&& element) {
                    return element_type->compare(element.serialize(), *val) == 0;
                });
                return found != range.end();
            };
            if (col_type->is_list()) {
                if (!exists_in(value_cast<list_type_impl::native_type>(deserialized))) {
                    return false;
                }
            } else if (col_type->is_set()) {
                if (!exists_in(value_cast<set_type_impl::native_type>(deserialized))) {
                    return false;
                }
            } else {
                auto data_map = value_cast<map_type_impl::native_type>(deserialized);
                if (!exists_in(data_map | boost::adaptors::transformed([] (auto&& p) { return p.second; }))) {
                    return false;
                }
            }
        }
        if (col_type->is_map()) {
            auto& data_map = value_cast<map_type_impl::native_type>(deserialized);
            for (auto&& key : _keys) {
                auto k = key->bind_and_get(options);
                if (!k) {
                    continue;
                }
                auto found = std::find_if(data_map.begin(), data_map.end(), [&] (auto&& element) {
                    return map_key_type->compare(element.first.serialize(), *k) == 0;
                });
                if (found == data_map.end()) {
                    return false;
                }
            }
            for (uint32_t i = 0; i < _entry_keys.size(); ++i) {
                auto map_key = _entry_keys[i]->bind_and_get(options);
                auto map_value = _entry_values[i]->bind_and_get(options);
                if (!map_key || !map_value) {
                    continue;
                }
                auto found = std::find_if(data_map.begin(), data_map.end(), [&] (auto&& element) {
                    return map_key_type->compare(element.first.serialize(), *map_key) == 0;
                });
                if (found == data_map.end() || element_type->compare(found->second.serialize(), *map_value) != 0) {
                    return false;
                }
            }
        }
    }

    return true;
}

bool token_restriction::EQ::is_satisfied_by(const schema& schema,
        const partition_key& key,
        const clustering_key_prefix& ckey,
        const row& cells,
        const query_options& options,
        gc_clock::time_point now) const {
    bool satisfied = false;
    auto cdef = _column_definitions.begin();
    for (auto&& operand : values(options)) {
        if (operand) {
            auto cell_value = do_get_value(schema, **cdef, key, ckey, cells, now);
            satisfied = cell_value && (*cdef)->type->compare(*operand, *cell_value) == 0;
        }
        if (!satisfied) {
            break;
        }
    }
    return satisfied;
}

bool token_restriction::slice::is_satisfied_by(const schema& schema,
        const partition_key& key,
        const clustering_key_prefix& ckey,
        const row& cells,
        const query_options& options,
        gc_clock::time_point now) const {
    bool satisfied = false;
    auto range = to_range(_slice, options);
    for (auto* cdef : _column_definitions) {
        auto cell_value = do_get_value(schema, *cdef, key, ckey, cells, now);
        if (!cell_value) {
            return false;
        }
        satisfied = range.contains(*cell_value, cdef->type->as_tri_comparator());
        if (!satisfied) {
            break;
        }
    }
    return satisfied;
}

}
}
