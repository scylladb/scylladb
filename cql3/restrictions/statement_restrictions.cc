
/*
 * Copyright (C) 2015-present ScyllaDB
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

#include <algorithm>
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>
#include <functional>
#include <ranges>
#include <stdexcept>

#include "query-result-reader.hh"
#include "statement_restrictions.hh"
#include "multi_column_restriction.hh"
#include "token_restriction.hh"
#include "database.hh"
#include "cartesian_product.hh"

#include "cql3/constants.hh"
#include "cql3/lists.hh"
#include "cql3/selection/selection.hh"
#include "cql3/single_column_relation.hh"
#include "cql3/statements/request_validations.hh"
#include "cql3/tuples.hh"
#include "types/list.hh"
#include "types/map.hh"
#include "types/set.hh"

namespace cql3 {
namespace restrictions {

static logging::logger rlogger("restrictions");

using boost::adaptors::filtered;
using boost::adaptors::transformed;
using statements::request_validations::invalid_request;

template<typename T>
class statement_restrictions::initial_key_restrictions : public primary_key_restrictions<T> {
    bool _allow_filtering;
public:
    initial_key_restrictions(bool allow_filtering)
        : _allow_filtering(allow_filtering) {
        this->expression = true;
    }
    using bounds_range_type = typename primary_key_restrictions<T>::bounds_range_type;

    ::shared_ptr<primary_key_restrictions<T>> do_merge_to(schema_ptr schema, ::shared_ptr<restriction> restriction) const {
        return ::make_shared<single_column_primary_key_restrictions<T>>(schema, _allow_filtering)->merge_to(schema, restriction);
    }
    ::shared_ptr<primary_key_restrictions<T>> merge_to(schema_ptr schema, ::shared_ptr<restriction> restriction) override {
        if (has_token(restriction->expression)) {
            return static_pointer_cast<token_restriction>(restriction);
        }
        return ::make_shared<single_column_primary_key_restrictions<T>>(schema, _allow_filtering)->merge_to(restriction);
    }
    void merge_with(::shared_ptr<restriction> restriction) override {
        throw exceptions::unsupported_operation_exception();
    }
    bytes_opt value_for(const column_definition& cdef, const query_options& options) const override {
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
    bool empty() const override {
        return true;
    }
    uint32_t size() const override {
        return 0;
    }
    virtual bool has_supporting_index(const secondary_index::secondary_index_manager& index_manager,
                                      expr::allow_local_index allow_local) const override {
        return false;
    }
};

template<>
::shared_ptr<primary_key_restrictions<partition_key>>
statement_restrictions::initial_key_restrictions<partition_key>::merge_to(schema_ptr schema, ::shared_ptr<restriction> restriction) {
    if (has_token(restriction->expression)) {
        return static_pointer_cast<token_restriction>(restriction);
    }
    return do_merge_to(std::move(schema), std::move(restriction));
}

template<>
::shared_ptr<primary_key_restrictions<clustering_key_prefix>>
statement_restrictions::initial_key_restrictions<clustering_key_prefix>::merge_to(schema_ptr schema, ::shared_ptr<restriction> restriction) {
    if (auto p = dynamic_pointer_cast<multi_column_restriction>(restriction)) {
        return p;
    }
    return do_merge_to(std::move(schema), std::move(restriction));
}

::shared_ptr<partition_key_restrictions> statement_restrictions::get_initial_partition_key_restrictions(bool allow_filtering) {
    static thread_local ::shared_ptr<partition_key_restrictions> initial_kr_true = ::make_shared<initial_key_restrictions<partition_key>>(true);
    static thread_local ::shared_ptr<partition_key_restrictions> initial_kr_false = ::make_shared<initial_key_restrictions<partition_key>>(false);
    return allow_filtering ? initial_kr_true : initial_kr_false;
}

::shared_ptr<clustering_key_restrictions> statement_restrictions::get_initial_clustering_key_restrictions(bool allow_filtering) {
    static thread_local ::shared_ptr<clustering_key_restrictions> initial_kr_true = ::make_shared<initial_key_restrictions<clustering_key>>(true);
    static thread_local ::shared_ptr<clustering_key_restrictions> initial_kr_false = ::make_shared<initial_key_restrictions<clustering_key>>(false);
    return allow_filtering ? initial_kr_true : initial_kr_false;
}

statement_restrictions::statement_restrictions(schema_ptr schema, bool allow_filtering)
    : _schema(schema)
    , _partition_key_restrictions(get_initial_partition_key_restrictions(allow_filtering))
    , _clustering_columns_restrictions(get_initial_clustering_key_restrictions(allow_filtering))
    , _nonprimary_key_restrictions(::make_shared<single_column_restrictions>(schema))
    , _partition_range_is_simple(true)
{ }
#if 0
static const column_definition*
to_column_definition(const schema_ptr& schema, const ::shared_ptr<column_identifier::raw>& entity) {
    return get_column_definition(schema,
            *entity->prepare_column_identifier(schema));
}
#endif

/// Every token, or if no tokens, an EQ/IN of every single PK column.
static std::vector<expr::expression> extract_partition_range(
        const expr::expression& where_clause, schema_ptr schema) {
    using namespace expr;
    struct {
        std::optional<expression> tokens;
        std::unordered_map<const column_definition*, expression> single_column;

        void operator()(const conjunction& c) {
            std::ranges::for_each(c.children, [this] (const expression& child) { std::visit(*this, child); });
        }

        void operator()(const binary_operator& b) {
            if (std::holds_alternative<token>(b.lhs)) {
                if (tokens) {
                    tokens = make_conjunction(std::move(*tokens), b);
                } else {
                    tokens = b;
                }
            }
            if (auto s = std::get_if<column_value>(&b.lhs)) {
                if (s->col->is_partition_key() && (b.op == oper_t::EQ || b.op == oper_t::IN)) {
                    const auto found = single_column.find(s->col);
                    if (found == single_column.end()) {
                        single_column[s->col] = b;
                    } else {
                        found->second = make_conjunction(std::move(found->second), b);
                    }
                }
            }
        }

        void operator()(bool) {}
    } v;
    std::visit(v, where_clause);
    if (v.tokens) {
        return {std::move(*v.tokens)};
    }
    if (v.single_column.size() == schema->partition_key_size()) {
        return boost::copy_range<std::vector<expression>>(v.single_column | boost::adaptors::map_values);
    }
    return {};
}

/// Extracts where_clause atoms with clustering-column LHS and copies them to a vector such that:
/// 1. all elements must be simultaneously satisfied (as restrictions) for where_clause to be satisfied
/// 2. each element is an atom or a conjunction of atoms
/// 3. either all atoms (across all elements) are multi-column or they are all single-column
/// 4. if single-column, then:
///   4.1 all atoms from an element have the same LHS, which we call the element's LHS
///   4.2 each element's LHS is different from any other element's LHS
///   4.3 the list of each element's LHS, in order, forms a clustering-key prefix
///   4.4 elements other than the last have only EQ or IN atoms
///   4.5 the last element has only EQ, IN, or is_slice() atoms
///
/// These elements define the boundaries of any clustering slice that can possibly meet where_clause.
///
/// This vector can be calculated before binding expression markers, since LHS and operator are always known.
static std::vector<expr::expression> extract_clustering_prefix_restrictions(
        const expr::expression& where_clause, schema_ptr schema) {
    using namespace expr;

    /// Collects all clustering-column restrictions from an expression.  Presumes the expression only uses
    /// conjunction to combine subexpressions.
    struct visitor {
        std::vector<expression> multi; ///< All multi-column restrictions.
        /// All single-clustering-column restrictions, grouped by column.  Each value is either an atom or a
        /// conjunction of atoms.
        std::unordered_map<const column_definition*, expression> single;

        void operator()(const conjunction& c) {
            std::ranges::for_each(c.children, [this] (const expression& child) { std::visit(*this, child); });
        }

        void operator()(const binary_operator& b) {
            if (std::holds_alternative<column_value_tuple>(b.lhs)) {
                multi.push_back(b);
            }
            if (auto s = std::get_if<column_value>(&b.lhs)) {
                if (s->col->is_clustering_key()) {
                    const auto found = single.find(s->col);
                    if (found == single.end()) {
                        single[s->col] = b;
                    } else {
                        found->second = make_conjunction(std::move(found->second), b);
                    }
                }
            }
        }

        void operator()(bool) {}
    } v;
    std::visit(v, where_clause);

    if (!v.multi.empty()) {
        return move(v.multi);
    }

    std::vector<expression> prefix;
    for (const auto& col : schema->clustering_key_columns()) {
        const auto found = v.single.find(&col);
        if (found == v.single.end()) { // Any further restrictions are skipping the CK order.
            break;
        }
        if (find_needs_filtering(found->second)) { // This column's restriction doesn't define a clear bound.
            // TODO: if this is a conjunction of filtering and non-filtering atoms, we could split them and add the
            // latter to the prefix.
            break;
        }
        prefix.push_back(found->second);
        if (has_slice(found->second)) {
            break;
        }
    }
    return prefix;
}

statement_restrictions::statement_restrictions(database& db,
        schema_ptr schema,
        statements::statement_type type,
        const std::vector<::shared_ptr<relation>>& where_clause,
        variable_specifications& bound_names,
        bool selects_only_static_columns,
        bool for_view,
        bool allow_filtering)
    : statement_restrictions(schema, allow_filtering)
{
    if (!where_clause.empty()) {
        for (auto&& relation : where_clause) {
            if (relation->get_operator() == expr::oper_t::IS_NOT) {
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

                auto col_id = r->get_entity()->prepare_column_identifier(*schema);
                const auto *cd = get_column_definition(*schema, *col_id);
                if (!cd) {
                    throw exceptions::invalid_request_exception(format("restriction '{}' unknown column {}", relation->to_string(), r->get_entity()->to_string()));
                }
                _not_null_columns.insert(cd);

                if (!for_view) {
                    throw exceptions::invalid_request_exception(format("restriction '{}' is only supported in materialized view creation", relation->to_string()));
                }
            } else {
                const auto restriction = relation->to_restriction(db, schema, bound_names);
                if (dynamic_pointer_cast<multi_column_restriction>(restriction)) {
                    _clustering_columns_restrictions = _clustering_columns_restrictions->merge_to(_schema, restriction);
                } else if (has_token(restriction->expression)) {
                    _partition_key_restrictions = _partition_key_restrictions->merge_to(_schema, restriction);
                } else {
                    auto single = ::static_pointer_cast<single_column_restriction>(restriction);
                    auto& def = single->get_column_def();
                    if (def.is_partition_key()) {
                        // View definition allows PK slices, because it's not a performance problem.
                        if (has_slice(restriction->expression) && !allow_filtering && !for_view) {
                            throw exceptions::invalid_request_exception(
                                    "Only EQ and IN relation are supported on the partition key "
                                    "(unless you use the token() function or allow filtering)");
                        }
                        _partition_key_restrictions = _partition_key_restrictions->merge_to(_schema, restriction);
                    } else if (def.is_clustering_key()) {
                        _clustering_columns_restrictions = _clustering_columns_restrictions->merge_to(_schema, restriction);
                    } else {
                        _nonprimary_key_restrictions->add_restriction(single);
                    }
                }
                _where = _where.has_value() ? make_conjunction(std::move(*_where), restriction->expression) : restriction->expression;
                _partition_range_is_simple &= !find(restriction->expression, expr::oper_t::IN);
            }
        }
    }
    if (_where.has_value()) {
        _clustering_prefix_restrictions = extract_clustering_prefix_restrictions(*_where, _schema);
        _partition_range_restrictions = extract_partition_range(*_where, _schema);
    }
    auto& cf = db.find_column_family(schema);
    auto& sim = cf.get_index_manager();
    const expr::allow_local_index allow_local(
            !_partition_key_restrictions->has_unrestricted_components(*_schema)
            && _partition_key_restrictions->is_all_eq());
    _has_queriable_ck_index = _clustering_columns_restrictions->has_supporting_index(sim, allow_local);
    _has_queriable_pk_index = _partition_key_restrictions->has_supporting_index(sim, allow_local);
    _has_queriable_regular_index = _nonprimary_key_restrictions->has_supporting_index(sim, allow_local);

    // At this point, the select statement if fully constructed, but we still have a few things to validate
    process_partition_key_restrictions(for_view, allow_filtering);

    // Some but not all of the partition key columns have been specified;
    // hence we need turn these restrictions into index expressions.
    if (_uses_secondary_indexing || _partition_key_restrictions->needs_filtering(*_schema)) {
        _index_restrictions.push_back(_partition_key_restrictions);
    }

    // If the only updated/deleted columns are static, then we don't need clustering columns.
    // And in fact, unless it is an INSERT, we reject if clustering columns are provided as that
    // suggest something unintended. For instance, given:
    //   CREATE TABLE t (k int, v int, s int static, PRIMARY KEY (k, v))
    // it can make sense to do:
    //   INSERT INTO t(k, v, s) VALUES (0, 1, 2)
    // but both
    //   UPDATE t SET s = 3 WHERE k = 0 AND v = 1
    //   DELETE s FROM t WHERE k = 0 AND v = 1
    // sounds like you don't really understand what your are doing.
    if (selects_only_static_columns && has_clustering_columns_restriction()) {
        if (type.is_update() || type.is_delete()) {
            throw exceptions::invalid_request_exception(format("Invalid restrictions on clustering columns since the {} statement modifies only static columns", type));
        }

        if (type.is_select()) {
            throw exceptions::invalid_request_exception(
                "Cannot restrict clustering columns when selecting only static columns");
        }
    }

    process_clustering_columns_restrictions(for_view, allow_filtering);

    // Covers indexes on the first clustering column (among others).
    if (_is_key_range && _has_queriable_ck_index &&
        !dynamic_pointer_cast<multi_column_restriction>(_clustering_columns_restrictions)) {
        _uses_secondary_indexing = true;
    }

    if (_uses_secondary_indexing || _clustering_columns_restrictions->needs_filtering(*_schema)) {
        _index_restrictions.push_back(_clustering_columns_restrictions);
    } else if (find_atom(_clustering_columns_restrictions->expression, expr::is_on_collection)) {
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

    if (!_nonprimary_key_restrictions->empty()) {
        if (_has_queriable_regular_index) {
            _uses_secondary_indexing = true;
        } else if (!allow_filtering) {
            throw exceptions::invalid_request_exception("Cannot execute this query as it might involve data filtering and "
                "thus may have unpredictable performance. If you want to execute "
                "this query despite the performance unpredictability, use ALLOW FILTERING");
        }
        _index_restrictions.push_back(_nonprimary_key_restrictions);
    }

    if (_uses_secondary_indexing && !(for_view || allow_filtering)) {
        validate_secondary_index_selections(selects_only_static_columns);
    }
}

const std::vector<::shared_ptr<restrictions>>& statement_restrictions::index_restrictions() const {
    return _index_restrictions;
}

// Current score table:
// local and restrictions include full partition key: 2
// global: 1
// local and restrictions does not include full partition key: 0 (do not pick)
int statement_restrictions::score(const secondary_index::index& index) const {
    if (index.metadata().local()) {
        const bool allow_local = !_partition_key_restrictions->has_unrestricted_components(*_schema) && _partition_key_restrictions->is_all_eq();
        return  allow_local ? 2 : 0;
    }
    return 1;
}

namespace {

using namespace cql3::restrictions;

/// If rs contains a restrictions_map of individual columns to their restrictions, returns it.  Otherwise, returns null.
const single_column_restrictions::restrictions_map* get_individual_restrictions_map(const restrictions* rs) {
    if (auto regular = dynamic_cast<const single_column_restrictions*>(rs)) {
        return &regular->restrictions();
    } else if (auto partition = dynamic_cast<const single_column_partition_key_restrictions*>(rs)) {
        return &partition->restrictions();
    } else if (auto clustering = dynamic_cast<const single_column_clustering_key_restrictions*>(rs)) {
        return &clustering->restrictions();
    }
    return nullptr;
}

} // anonymous namespace

std::pair<std::optional<secondary_index::index>, ::shared_ptr<cql3::restrictions::restrictions>> statement_restrictions::find_idx(secondary_index::secondary_index_manager& sim) const {
    std::optional<secondary_index::index> chosen_index;
    int chosen_index_score = 0;
    ::shared_ptr<cql3::restrictions::restrictions> chosen_index_restrictions;

    for (const auto& index : sim.list_indexes()) {
        auto cdef = _schema->get_column_definition(to_bytes(index.target_column()));
        for (::shared_ptr<cql3::restrictions::restrictions> restriction : index_restrictions()) {
            if (auto rmap = get_individual_restrictions_map(restriction.get())) {
                const auto found = rmap->find(cdef);
                if (found != rmap->end() && is_supported_by(found->second->expression, index)
                    && score(index) > chosen_index_score) {
                    chosen_index = index;
                    chosen_index_score = score(index);
                    chosen_index_restrictions = restriction;
                }
            }
        }
    }
    return {chosen_index, chosen_index_restrictions};
}

std::vector<const column_definition*> statement_restrictions::get_column_defs_for_filtering(database& db) const {
    std::vector<const column_definition*> column_defs_for_filtering;
    if (need_filtering()) {
        auto& sim = db.find_column_family(_schema).get_index_manager();
        auto opt_idx = std::get<0>(find_idx(sim));
        auto column_uses_indexing = [&opt_idx] (const column_definition* cdef, ::shared_ptr<single_column_restriction> restr) {
            return opt_idx && restr && is_supported_by(restr->expression, *opt_idx);
        };
        auto single_pk_restrs = dynamic_pointer_cast<single_column_partition_key_restrictions>(_partition_key_restrictions);
        if (_partition_key_restrictions->needs_filtering(*_schema)) {
            for (auto&& cdef : _partition_key_restrictions->get_column_defs()) {
                ::shared_ptr<single_column_restriction> restr;
                if (single_pk_restrs) {
                    auto it = single_pk_restrs->restrictions().find(cdef);
                    if (it != single_pk_restrs->restrictions().end()) {
                        restr = dynamic_pointer_cast<single_column_restriction>(it->second);
                    }
                }
                if (!column_uses_indexing(cdef, restr)) {
                    column_defs_for_filtering.emplace_back(cdef);
                }
            }
        }
        auto single_ck_restrs = dynamic_pointer_cast<single_column_clustering_key_restrictions>(_clustering_columns_restrictions);
        const bool pk_has_unrestricted_components = _partition_key_restrictions->has_unrestricted_components(*_schema);
        if (pk_has_unrestricted_components || _clustering_columns_restrictions->needs_filtering(*_schema)) {
            column_id first_filtering_id = pk_has_unrestricted_components ? 0 : _schema->clustering_key_columns().begin()->id +
                    _clustering_columns_restrictions->num_prefix_columns_that_need_not_be_filtered();
            for (auto&& cdef : _clustering_columns_restrictions->get_column_defs()) {
                ::shared_ptr<single_column_restriction> restr;
                if (single_ck_restrs) {
                    auto it = single_ck_restrs->restrictions().find(cdef);
                    if (it != single_ck_restrs->restrictions().end()) {
                        restr = dynamic_pointer_cast<single_column_restriction>(it->second);
                    }
                }
                if (cdef->id >= first_filtering_id && !column_uses_indexing(cdef, restr)) {
                    column_defs_for_filtering.emplace_back(cdef);
                }
            }
        }
        for (auto&& cdef : _nonprimary_key_restrictions->get_column_defs()) {
            auto restr = dynamic_pointer_cast<single_column_restriction>(_nonprimary_key_restrictions->get_restriction(*cdef));
            if (!column_uses_indexing(cdef, restr)) {
                column_defs_for_filtering.emplace_back(cdef);
            }
        }
    }
    return column_defs_for_filtering;
}

void statement_restrictions::process_partition_key_restrictions(bool for_view, bool allow_filtering) {
    // If there is a queriable index, no special condition are required on the other restrictions.
    // But we still need to know 2 things:
    // - If we don't have a queriable index, is the query ok
    // - Is it queriable without 2ndary index, which is always more efficient
    // If a component of the partition key is restricted by a relation, all preceding
    // components must have a EQ. Only the last partition key component can be in IN relation.
    if (has_token(_partition_key_restrictions->expression)) {
        _is_key_range = true;
    } else if (_partition_key_restrictions->empty()) {
        _is_key_range = true;
        _uses_secondary_indexing = _has_queriable_pk_index;
    }

    if (_partition_key_restrictions->needs_filtering(*_schema)) {
        if (!allow_filtering && !for_view && !_has_queriable_pk_index) {
            throw exceptions::invalid_request_exception("Cannot execute this query as it might involve data filtering and "
                "thus may have unpredictable performance. If you want to execute "
                "this query despite the performance unpredictability, use ALLOW FILTERING");
        }
        _is_key_range = true;
        _uses_secondary_indexing = _has_queriable_pk_index;
    }

}

bool statement_restrictions::has_partition_key_unrestricted_components() const {
    return _partition_key_restrictions->has_unrestricted_components(*_schema);
}

bool statement_restrictions::has_unrestricted_clustering_columns() const {
    return _clustering_columns_restrictions->has_unrestricted_components(*_schema);
}

void statement_restrictions::process_clustering_columns_restrictions(bool for_view, bool allow_filtering) {
    if (!has_clustering_columns_restriction()) {
        return;
    }

    if (find_atom(_clustering_columns_restrictions->expression, expr::is_on_collection)
        && !_has_queriable_ck_index && !allow_filtering) {
        throw exceptions::invalid_request_exception(
            "Cannot restrict clustering columns by a CONTAINS relation without a secondary index or filtering");
    }

    if (has_clustering_columns_restriction() && _clustering_columns_restrictions->needs_filtering(*_schema)) {
        if (_has_queriable_ck_index) {
            _uses_secondary_indexing = true;
        } else if (!allow_filtering && !for_view) {
            auto clustering_columns_iter = _schema->clustering_key_columns().begin();
            for (auto&& restricted_column : _clustering_columns_restrictions->get_column_defs()) {
                const column_definition* clustering_column = &(*clustering_columns_iter);
                ++clustering_columns_iter;
                if (clustering_column != restricted_column) {
                        throw exceptions::invalid_request_exception(format("PRIMARY KEY column \"{}\" cannot be restricted as preceding column \"{}\" is not restricted",
                            restricted_column->name_as_text(), clustering_column->name_as_text()));
                }
            }
        }
    }
}

namespace {

using namespace expr;

/// Computes partition-key ranges from token atoms in ex.
dht::partition_range_vector partition_ranges_from_token(const expr::expression& ex, const query_options& options) {
    auto values = possible_lhs_values(nullptr, ex, options);
    if (values == expr::value_set(expr::value_list{})) {
        return {};
    }
    const auto bounds = expr::to_range(values);
    const auto start_token = bounds.start() ? bounds.start()->value().with_linearized([] (bytes_view bv) { return dht::token::from_bytes(bv); })
            : dht::minimum_token();
    auto end_token = bounds.end() ? bounds.end()->value().with_linearized([] (bytes_view bv) { return dht::token::from_bytes(bv); })
            : dht::maximum_token();
    const bool include_start = bounds.start() && bounds.start()->is_inclusive();
    const auto include_end = bounds.end() && bounds.end()->is_inclusive();

    auto start = dht::partition_range::bound(include_start
                                             ? dht::ring_position::starting_at(start_token)
                                             : dht::ring_position::ending_at(start_token));
    auto end = dht::partition_range::bound(include_end
                                           ? dht::ring_position::ending_at(end_token)
                                           : dht::ring_position::starting_at(end_token));

    return {{std::move(start), std::move(end)}};
}

/// Turns a partition-key value into a partition_range. \p pk must have elements for all partition columns.
dht::partition_range range_from_bytes(const schema& schema, const std::vector<managed_bytes>& pk) {
    const auto k = partition_key::from_exploded(pk);
    const auto tok = dht::get_token(schema, k);
    const query::ring_position pos(std::move(tok), std::move(k));
    return dht::partition_range::make_singular(std::move(pos));
}

void error_if_exceeds(size_t size, size_t limit) {
    if (size > limit) {
        throw std::runtime_error(
                fmt::format("clustering-key cartesian product size {} is greater than maximum {}", size, limit));
    }
}

/// Computes partition-key ranges from expressions, which contains EQ/IN for every partition column.
dht::partition_range_vector partition_ranges_from_singles(
        const std::vector<expr::expression>& expressions, const query_options& options, const schema& schema) {
    const size_t size_limit =
            options.get_cql_config().restrictions.partition_key_restrictions_max_cartesian_product_size;
    // Each element is a vector of that column's possible values:
    std::vector<std::vector<managed_bytes>> column_values(schema.partition_key_size());
    size_t product_size = 1;
    for (const auto& e : expressions) {
        if (const auto arbitrary_binop = find_atom(e, [] (const binary_operator&) { return true; })) {
            if (auto cv = std::get_if<expr::column_value>(&arbitrary_binop->lhs)) {
                const value_set vals = possible_lhs_values(cv->col, e, options);
                if (auto lst = std::get_if<value_list>(&vals)) {
                    if (lst->empty()) {
                        return {};
                    }
                    product_size *= lst->size();
                    error_if_exceeds(product_size, size_limit);
                    column_values[schema.position(*cv->col)] = move(*lst);
                } else {
                    throw exceptions::invalid_request_exception(
                            "Only EQ and IN relation are supported on the partition key "
                            "(unless you use the token() function or allow filtering)");
                }
            }
        }
    }
    cartesian_product cp(column_values);
    dht::partition_range_vector ranges(product_size);
    std::transform(cp.begin(), cp.end(), ranges.begin(), std::bind_front(range_from_bytes, std::ref(schema)));
    return ranges;
}

/// Computes partition-key ranges from EQ restrictions on each partition column.  Returns a single singleton range if
/// the EQ restrictions are not mutually conflicting.  Otherwise, returns an empty vector.
dht::partition_range_vector partition_ranges_from_EQs(
        const std::vector<expr::expression>& eq_expressions, const query_options& options, const schema& schema) {
    std::vector<managed_bytes> pk_value(schema.partition_key_size());
    for (const auto& e : eq_expressions) {
        const auto col = std::get<column_value>(find(e, oper_t::EQ)->lhs).col;
        const auto vals = std::get<value_list>(possible_lhs_values(col, e, options));
        if (vals.empty()) { // Case of C=1 AND C=2.
            return {};
        }
        pk_value[schema.position(*col)] = std::move(vals[0]);
    }
    return {range_from_bytes(schema, pk_value)};
}

} // anonymous namespace

dht::partition_range_vector statement_restrictions::get_partition_key_ranges(const query_options& options) const {
    if (_partition_range_restrictions.empty()) {
        return {dht::partition_range::make_open_ended_both_sides()};
    }
    if (has_token(_partition_range_restrictions[0])) {
        if (_partition_range_restrictions.size() != 1) {
            on_internal_error(
                    rlogger,
                    format("Unexpected size of token restrictions: {}", _partition_range_restrictions.size()));
        }
        return partition_ranges_from_token(_partition_range_restrictions[0], options);
    } else if (_partition_range_is_simple) {
        // Special case to avoid extra allocations required for a Cartesian product.
        return partition_ranges_from_EQs(_partition_range_restrictions, options, *_schema);
    }
    return partition_ranges_from_singles(_partition_range_restrictions, options, *_schema);
}

namespace {

using namespace expr;

clustering_key_prefix::prefix_equal_tri_compare get_unreversed_tri_compare(const schema& schema) {
    clustering_key_prefix::prefix_equal_tri_compare cmp(schema);
    std::vector<data_type> types = cmp.prefix_type->types();
    for (auto& t : types) {
        if (t->is_reversed()) {
            t = t->underlying_type();
        }
    }
    cmp.prefix_type = make_lw_shared<compound_type<allow_prefixes::yes>>(types);
    return cmp;
}

/// True iff r1 start is strictly before r2 start.
bool starts_before_start(
        const query::clustering_range& r1,
        const query::clustering_range& r2,
        const clustering_key_prefix::prefix_equal_tri_compare& cmp) {
    if (!r2.start()) {
        return false; // r2 start is -inf, nothing is before that.
    }
    if (!r1.start()) {
        return true; // r1 start is -inf, while r2 start is finite.
    }
    const auto diff = cmp(r1.start()->value(), r2.start()->value());
    if (diff < 0) { // r1 start is strictly before r2 start.
        return true;
    }
    if (diff > 0) { // r1 start is strictly after r2 start.
        return false;
    }
    const auto len1 = r1.start()->value().representation().size();
    const auto len2 = r2.start()->value().representation().size();
    if (len1 == len2) { // The values truly are equal.
        return r1.start()->is_inclusive() && !r2.start()->is_inclusive();
    } else if (len1 < len2) { // r1 start is a prefix of r2 start.
        // (a)>=(1) starts before (a,b)>=(1,1), but (a)>(1) doesn't.
        return r1.start()->is_inclusive();
    } else { // r2 start is a prefix of r1 start.
        // (a,b)>=(1,1) starts before (a)>(1) but after (a)>=(1).
        return r2.start()->is_inclusive();
    }
}

/// True iff r1 start is before (or identical as) r2 end.
bool starts_before_or_at_end(
        const query::clustering_range& r1,
        const query::clustering_range& r2,
        const clustering_key_prefix::prefix_equal_tri_compare& cmp) {
    if (!r1.start()) {
        return true; // r1 start is -inf, must be before r2 end.
    }
    if (!r2.end()) {
        return true; // r2 end is +inf, everything is before it.
    }
    const auto diff = cmp(r1.start()->value(), r2.end()->value());
    if (diff < 0) { // r1 start is strictly before r2 end.
        return true;
    }
    if (diff > 0) { // r1 start is strictly after r2 end.
        return false;
    }
    const auto len1 = r1.start()->value().representation().size();
    const auto len2 = r2.end()->value().representation().size();
    if (len1 == len2) { // The values truly are equal.
        return r1.start()->is_inclusive() && r2.end()->is_inclusive();
    } else if (len1 < len2) { // r1 start is a prefix of r2 end.
        // a>=(1) starts before (a,b)<=(1,1) ends, but (a)>(1) doesn't.
        return r1.start()->is_inclusive();
    } else { // r2 end is a prefix of r1 start.
        // (a,b)>=(1,1) starts before (a)<=(1) ends but after (a)<(1) ends.
        return r2.end()->is_inclusive();
    }
}

/// True if r1 end is strictly before r2 end.
bool ends_before_end(
        const query::clustering_range& r1,
        const query::clustering_range& r2,
        const clustering_key_prefix::prefix_equal_tri_compare& cmp) {
    if (!r1.end()) {
        return false; // r1 end is +inf, which is after everything.
    }
    if (!r2.end()) {
        return true; // r2 end is +inf, while r1 end is finite.
    }
    const auto diff = cmp(r1.end()->value(), r2.end()->value());
    if (diff < 0) { // r1 end is strictly before r2 end.
        return true;
    }
    if (diff > 0) { // r1 end is strictly after r2 end.
        return false;
    }
    const auto len1 = r1.end()->value().representation().size();
    const auto len2 = r2.end()->value().representation().size();
    if (len1 == len2) { // The values truly are equal.
        return !r1.end()->is_inclusive() && r2.end()->is_inclusive();
    } else if (len1 < len2) { // r1 end is a prefix of r2 end.
        // (a)<(1) ends before (a,b)<=(1,1), but (a)<=(1) doesn't.
        return !r1.end()->is_inclusive();
    } else { // r2 end is a prefix of r1 end.
        // (a,b)<=(1,1) ends before (a)<=(1) but after (a)<(1).
        return r2.end()->is_inclusive();
    }
}

/// Correct clustering_range intersection.  See #8157.
std::optional<query::clustering_range> intersection(
        const query::clustering_range& r1,
        const query::clustering_range& r2,
        const clustering_key_prefix::prefix_equal_tri_compare& cmp) {
    // Assume r1's start is to the left of r2's start.
    if (starts_before_start(r2, r1, cmp)) {
        return intersection(r2, r1, cmp);
    }
    if (!starts_before_or_at_end(r2, r1, cmp)) {
        return {};
    }
    const auto& intersection_start = r2.start();
    const auto& intersection_end = ends_before_end(r1, r2, cmp) ? r1.end() : r2.end();
    if (intersection_start == intersection_end && intersection_end.has_value()) {
        return query::clustering_range::make_singular(intersection_end->value());
    }
    return query::clustering_range(intersection_start, intersection_end);
}

struct range_less {
    const class schema& s;
    clustering_key_prefix::less_compare cmp = clustering_key_prefix::less_compare(s);
    bool operator()(const query::clustering_range& x, const query::clustering_range& y) const {
        if (!x.start() && !y.start()) {
            return false;
        }
        if (!x.start()) {
            return true;
        }
        if (!y.start()) {
            return false;
        }
        return cmp(x.start()->value(), y.start()->value());
    }
};

/// An expression visitor that translates multi-column atoms into clustering ranges.
struct multi_column_range_accumulator {
    const query_options& options;
    const schema_ptr schema;
    std::vector<query::clustering_range> ranges{query::clustering_range::make_open_ended_both_sides()};
    const clustering_key_prefix::prefix_equal_tri_compare prefix3cmp = get_unreversed_tri_compare(*schema);

    void operator()(const binary_operator& binop) {
        if (is_compare(binop.op)) {
            auto opt_values = dynamic_pointer_cast<tuples::value>(binop.rhs->bind(options))->get_elements();
            auto& lhs = std::get<column_value_tuple>(binop.lhs);
            std::vector<managed_bytes> values(lhs.elements.size());
            for (size_t i = 0; i < lhs.elements.size(); ++i) {
                values[i] = *statements::request_validations::check_not_null(
                        opt_values[i],
                        "Invalid null value in condition for column %s", lhs.elements.at(i).col->name_as_text());
            }
            intersect_all(to_range(binop.op, clustering_key_prefix(std::move(values))));
        } else if (binop.op == oper_t::IN) {
            if (auto dv = dynamic_pointer_cast<lists::delayed_value>(binop.rhs)) {
                process_in_values(
                        dv->get_elements() | transformed(
                                [&] (const ::shared_ptr<term>& t) {
                                    return static_pointer_cast<tuples::value>(t->bind(options))->get_elements();
                                }));
            } else if (auto mkr = dynamic_pointer_cast<tuples::in_marker>(binop.rhs)) {
                // This is `(a,b) IN ?`.  RHS elements are themselves tuples, represented as vector<bytes_opt>.
                const auto tup = static_pointer_cast<tuples::in_value>(mkr->bind(options));
                statements::request_validations::check_not_null(tup, "Invalid null value for IN restriction");
                process_in_values(tup->get_split_values());
            }
            else {
                on_internal_error(rlogger, format("multi_column_range_accumulator: unexpected atom {}", binop));
            }
        } else {
            on_internal_error(rlogger, format("multi_column_range_accumulator: unexpected atom {}", binop));
        }
    }

    void operator()(const conjunction& c) {
        std::ranges::for_each(c.children, [this] (const expression& child) { std::visit(*this, child); });
    }

    void operator()(bool b) {
        if (!b) {
            ranges.clear();
        }
    }

    /// Intersects each range with v.  If any intersection is empty, clears ranges.
    void intersect_all(const query::clustering_range& v) {
        for (auto& r : ranges) {
            auto intrs = intersection(r, v, prefix3cmp);
            if (!intrs) {
                ranges.clear();
                break;
            }
            r = *intrs;
        }
    }

    template<std::ranges::range Range>
    requires std::convertible_to<typename Range::value_type::value_type, managed_bytes_opt>
    void process_in_values(Range in_values) {
        if (ranges.empty()) {
            return; // Shortcircuit an easy case.
        }
        std::set<query::clustering_range, range_less> new_ranges(range_less{*schema});
        for (const auto& current_tuple : in_values) {
            // Each IN value is like a separate EQ restriction ANDed to the existing state.
            auto current_range = to_range(
                    oper_t::EQ, clustering_key_prefix::from_optional_exploded(*schema, current_tuple));
            for (const auto& r : ranges) {
                auto intrs = intersection(r, current_range, prefix3cmp);
                if (intrs) {
                    new_ranges.insert(*intrs);
                }
            }
        }
        ranges.assign(new_ranges.cbegin(), new_ranges.cend());
    }
};

/// Calculates clustering bounds for the multi-column case.
std::vector<query::clustering_range> get_multi_column_clustering_bounds(
        const query_options& options,
        schema_ptr schema,
        const std::vector<expression>& multi_column_restrictions) {
    multi_column_range_accumulator acc{options, schema};
    for (const auto& restr : multi_column_restrictions) {
        std::visit(acc, restr);
    }
    return acc.ranges;
}

/// Reverses the range if the type is reversed.  Why don't we have nonwrapping_interval::reverse()??
query::clustering_range reverse_if_reqd(query::clustering_range r, const abstract_type& t) {
    return t.is_reversed() ? query::clustering_range(r.end(), r.start()) : std::move(r);
}

constexpr bool inclusive = true;

/// Calculates clustering bounds for the single-column case.
std::vector<query::clustering_range> get_single_column_clustering_bounds(
        const query_options& options,
        schema_ptr schema,
        const std::vector<expression>& single_column_restrictions) {
    const size_t size_limit =
            options.get_cql_config().restrictions.clustering_key_restrictions_max_cartesian_product_size;
    size_t product_size = 1;
    std::vector<std::vector<managed_bytes>> prior_column_values; // Equality values of columns seen so far.
    for (size_t i = 0; i < single_column_restrictions.size(); ++i) {
        auto values = possible_lhs_values(
                &schema->clustering_column_at(i), // This should be the LHS of restrictions[i].
                single_column_restrictions[i],
                options);
        if (auto list = std::get_if<value_list>(&values)) {
            if (list->empty()) { // Impossible condition -- no rows can possibly match.
                return {};
            }
            prior_column_values.push_back(*list);
            product_size *= list->size();
            error_if_exceeds(product_size, size_limit);
        } else if (auto last_range = std::get_if<nonwrapping_interval<managed_bytes>>(&values)) {
            // Must be the last column in the prefix, since it's neither EQ nor IN.
            std::vector<query::clustering_range> ck_ranges;
            if (prior_column_values.empty()) {
                // This is the first and last range; just turn it into a clustering_key_prefix.
                ck_ranges.push_back(
                        reverse_if_reqd(
                                last_range->transform([] (const managed_bytes& val) { return clustering_key_prefix::from_range(std::array<managed_bytes, 1>{val}); }),
                                *schema->clustering_column_at(i).type));
            } else {
                // Prior clustering columns are equality-restricted (either via = or IN), producing one or more
                // prior_column_values elements.  Now we will turn each such element into a CK range dictated by those
                // equalities and this inequality represented by last_range.  Each CK range's upper/lower bound is
                // formed by extending the Cartesian-product element with the corresponding last_range bound, if it
                // exists; if it doesn't, the CK range bound is just the Cartesian-product element, inclusive.
                //
                // For example, the expression `c1=1 AND c2=2 AND c3>3` makes lower CK bound (1,2,3) exclusive and
                // upper CK bound (1,2) inclusive.
                ck_ranges.reserve(product_size);
                const auto extra_lb = last_range->start(), extra_ub = last_range->end();
                for (auto& b : cartesian_product(prior_column_values)) {
                    auto new_lb = b, new_ub = b;
                    if (extra_lb) {
                        new_lb.push_back(extra_lb->value());
                    }
                    if (extra_ub) {
                        new_ub.push_back(extra_ub->value());
                    }
                    query::clustering_range::bound new_start(new_lb, extra_lb ? extra_lb->is_inclusive() : inclusive);
                    query::clustering_range::bound new_end  (new_ub, extra_ub ? extra_ub->is_inclusive() : inclusive);
                    ck_ranges.push_back(reverse_if_reqd({new_start, new_end}, *schema->clustering_column_at(i).type));
                }
            }
            sort(ck_ranges.begin(), ck_ranges.end(), range_less{*schema});
            return ck_ranges;
        }
    }
    // All prefix columns are restricted by EQ or IN.  The resulting CK ranges are just singular ranges of corresponding
    // prior_column_values.
    std::vector<query::clustering_range> ck_ranges(product_size);
    cartesian_product cp(prior_column_values);
    std::transform(cp.begin(), cp.end(), ck_ranges.begin(), std::bind_front(query::clustering_range::make_singular));
    sort(ck_ranges.begin(), ck_ranges.end(), range_less{*schema});
    return ck_ranges;
}

using opt_bound = std::optional<query::clustering_range::bound>;

/// Makes a partial bound out of whole_bound's prefix.  If the partial bound is strictly shorter than the whole, it is
/// exclusive.  Otherwise, it matches the whole_bound's inclusivity.
opt_bound make_prefix_bound(
        size_t prefix_len, const std::vector<bytes>& whole_bound, bool whole_bound_is_inclusive) {
    if (whole_bound.empty()) {
        return {};
    }
    // Couldn't get std::ranges::subrange(whole_bound, prefix_len) to compile :(
    std::vector<bytes> partial_bound(
            whole_bound.cbegin(), whole_bound.cbegin() + std::min(prefix_len, whole_bound.size()));
    return query::clustering_range::bound(
            clustering_key_prefix(move(partial_bound)),
            prefix_len >= whole_bound.size() && whole_bound_is_inclusive);
}

/// Given a multi-column range in CQL order, breaks it into an equivalent union of clustering-order ranges.  Returns
/// those ranges as vector elements.
///
/// A difference between CQL order and clustering order means that the right-hand side of a clustering-key comparison is
/// not necessarily a single (lower or upper) bound on the clustering key in storage.  Eg, `WITH CLUSTERING ORDER BY (a
/// ASC, b DESC)` indicates that "a less than 5" means "a comes before 5 in storage", but "b less than 5" means "b comes
/// AFTER 5 in storage".  Therefore the CQL expression (a,b)<(5,5) cannot be executed by fetching a single range from
/// the storage layer -- the right-hand side is not a single upper bound from the storage layer's perspective.
///
/// When translating the WHERE clause into clustering ranges to fetch, it's natural to first calculate the CQL-order
/// ranges: comparisons define ranges, the AND operator intersects them, the IN operator makes a Cartesian product.  The
/// result of this is a union of ranges in CQL order that define the clustering slice to fetch.  And if the clustering
/// order is the same as the CQL order, these ranges can be sent to the storage proxy directly to fetch the correct
/// result.  But if the two orders differ, there is some work to be done first.  This is simple enough for ranges that
/// only vary a single column -- see reverse_if_reqd().  Multi-column ranges are more complicated; they are translated
/// into an equivalent union of clustering-order ranges by get_equivalent_ranges().
///
/// Continuing the above example, we can translate the CQL expression (a,b)<(5,5) into a union of several ranges that
/// are continuous in storage.  We begin by observing that (a,b)<(5,5) is the same as a<5 OR (a=5 AND b<5).  This is a
/// union of two ranges: the range corresponding to a<5, plus the range corresponding to (a=5 AND b<5).  Note that both
/// of these ranges are continuous in storage because they only vary a single column:
///
///  * a<5 is a range from -inf to clustering_key_prefix(5) exclusive
///
///  * (a=5 AND b<5) is a range from clustering_key_prefix(5,5) exclusive to clustering_key_prefix(5) inclusive; note
///    the clustering order between those start/end bounds
///
/// Here is an illustration of those two ranges in storage, with rows represented vertically and clustering-ordered left
/// to right:
///
///        a: 4 4 4 4 4 4 5 5 5 5 5 5 5 5 6 6 6 6 6
///        b: 5 4 3 2 1 0 7 6 5 4 3 2 1 0 5 4 3 2 1
/// 1st range ^^^^^^^^^^^       ^^^^^^^^^ 2nd range
///
/// For more examples of this range translation, please see the statement_restrictions unit tests.
std::vector<query::clustering_range> get_equivalent_ranges(
        const query::clustering_range& cql_order_range, const schema& schema) {
    const auto& cql_lb = cql_order_range.start();
    const auto& cql_ub = cql_order_range.end();
    if (cql_lb == cql_ub && (!cql_lb || cql_lb->is_inclusive())) {
        return {cql_order_range};
    }
    const auto cql_lb_bytes = cql_lb ? cql_lb->value().explode(schema) : std::vector<bytes>{};
    const auto cql_ub_bytes = cql_ub ? cql_ub->value().explode(schema) : std::vector<bytes>{};
    const bool cql_lb_is_inclusive = cql_lb ? cql_lb->is_inclusive() : false;
    const bool cql_ub_is_inclusive = cql_ub ? cql_ub->is_inclusive() : false;

    size_t common_prefix_len = 0;
    // Skip equal values; they don't contribute to equivalent-range generation.
    while (common_prefix_len < cql_lb_bytes.size() && common_prefix_len < cql_ub_bytes.size() &&
           cql_lb_bytes[common_prefix_len] == cql_ub_bytes[common_prefix_len]) {
        ++common_prefix_len;
    }

    std::vector<query::clustering_range> ranges;
    // First range is special: it has both bounds.
    opt_bound lb1 = make_prefix_bound(
            common_prefix_len + 1, cql_lb_bytes, cql_lb_is_inclusive);
    opt_bound ub1 = make_prefix_bound(
            common_prefix_len + 1, cql_ub_bytes, cql_ub_is_inclusive);
    auto range1 = schema.clustering_column_at(common_prefix_len).type->is_reversed() ?
            query::clustering_range(ub1, lb1) : query::clustering_range(lb1, ub1);
    ranges.push_back(std::move(range1));

    for (size_t p = common_prefix_len + 2; p <= cql_lb_bytes.size(); ++p) {
        opt_bound lb = make_prefix_bound(p, cql_lb_bytes, cql_lb_is_inclusive);
        opt_bound ub = make_prefix_bound(p - 1, cql_lb_bytes, /*irrelevant:*/true);
        if (ub) {
            ub = query::clustering_range::bound(ub->value(), inclusive);
        }
        auto range = schema.clustering_column_at(p - 1).type->is_reversed() ?
                query::clustering_range(ub, lb) : query::clustering_range(lb, ub);
        ranges.push_back(std::move(range));
    }

    for (size_t p = common_prefix_len + 2; p <= cql_ub_bytes.size(); ++p) {
        // Note the difference from the cql_lb_bytes case above!
        opt_bound ub = make_prefix_bound(p, cql_ub_bytes, cql_ub_is_inclusive);
        opt_bound lb = make_prefix_bound(p - 1, cql_ub_bytes, /*irrelevant:*/true);
        if (lb) {
            lb = query::clustering_range::bound(lb->value(), inclusive);
        }
        auto range = schema.clustering_column_at(p - 1).type->is_reversed() ?
                query::clustering_range(ub, lb) : query::clustering_range(lb, ub);
        ranges.push_back(std::move(range));
    }

    return ranges;
}

/// Extracts raw multi-column bounds from exprs; last one wins.
query::clustering_range range_from_raw_bounds(
        const std::vector<expression>& exprs, const query_options& options, const schema& schema) {
    opt_bound lb, ub;
    for (const auto& e : exprs) {
        if (auto b = find_clustering_order(e)) {
            const auto tup = dynamic_pointer_cast<tuples::value>(b->rhs->bind(options));
            if (!tup) {
                on_internal_error(rlogger, format("range_from_raw_bounds: unexpected atom {}", *b));
            }
            const auto r = to_range(
                    b->op, clustering_key_prefix::from_optional_exploded(schema, tup->get_elements()));
            if (r.start()) {
                lb = r.start();
            }
            if (r.end()) {
                ub = r.end();
            }
        }
    }
    return {lb, ub};
}

} // anonymous namespace

std::vector<query::clustering_range> statement_restrictions::get_clustering_bounds(const query_options& options) const {
    if (_clustering_prefix_restrictions.empty()) {
        return {query::clustering_range::make_open_ended_both_sides()};
    }
    if (count_if(_clustering_prefix_restrictions[0], expr::is_multi_column)) {
        bool all_natural = true, all_reverse = true; ///< Whether column types are reversed or natural.
        for (auto& r : _clustering_prefix_restrictions) { // TODO: move to constructor, do only once.
            using namespace expr;
            const auto& binop = std::get<binary_operator>(r);
            if (is_clustering_order(binop)) {
                return {range_from_raw_bounds(_clustering_prefix_restrictions, options, *_schema)};
            }
            for (auto& cv : std::get<column_value_tuple>(binop.lhs).elements) {
                if (cv.col->type->is_reversed()) {
                    all_natural = false;
                } else {
                    all_reverse = false;
                }
            }
        }
        auto bounds = get_multi_column_clustering_bounds(options, _schema, _clustering_prefix_restrictions);
        if (!all_natural && !all_reverse) {
            std::vector<query::clustering_range> bounds_in_clustering_order;
            for (const auto& b : bounds) {
                const auto eqv = get_equivalent_ranges(b, *_schema);
                bounds_in_clustering_order.insert(bounds_in_clustering_order.end(), eqv.cbegin(), eqv.cend());
            }
            return bounds_in_clustering_order;
        }
        if (all_reverse) {
            for (auto& crange : bounds) {
                crange = query::clustering_range(crange.end(), crange.start());
            }
        }
        return bounds;
    } else {
        return get_single_column_clustering_bounds(options, _schema, _clustering_prefix_restrictions);
    }
}

namespace {

/// True iff get_partition_slice_for_global_index_posting_list() will be able to calculate the token value from the
/// given restrictions.  Keep in sync with the get_partition_slice_for_global_index_posting_list() source.
bool token_known(const statement_restrictions& r) {
    return !r.has_partition_key_unrestricted_components() && r.get_partition_key_restrictions()->is_all_eq();
}

} // anonymous namespace

bool statement_restrictions::need_filtering() const {
    using namespace expr;

    const auto npart = _partition_key_restrictions->size();
    if (npart > 0 && npart < _schema->partition_key_size()) {
        // Can't calculate the token value, so a naive base-table query must be filtered.  Same for any index tables,
        // except if there's only one restriction supported by an index.
        return !(npart == 1 && _has_queriable_pk_index &&
                 _clustering_columns_restrictions->empty() && _nonprimary_key_restrictions->empty());
    }
    if (_partition_key_restrictions->needs_filtering(*_schema)) {
        // We most likely cannot calculate token(s).  Neither base-table nor index-table queries can avoid filtering.
        return true;
    }
    // Now we know the partition key is either unrestricted or fully restricted.

    const auto nreg = _nonprimary_key_restrictions->size();
    if (nreg > 1 || (nreg == 1 && !_has_queriable_regular_index)) {
        return true; // Regular columns are unsorted in storage and no single index suffices.
    }
    if (nreg == 1) { // Single non-key restriction supported by an index.
        // Will the index-table query require filtering?  That depends on whether its clustering key is restricted to a
        // continuous range.  Recall that this clustering key is (token, pk, ck) of the base table.
        if (npart == 0 && _clustering_columns_restrictions->empty()) {
            return false; // No clustering key restrictions => whole partitions.
        }
        return !token_known(*this) || _clustering_columns_restrictions->needs_filtering(*_schema);
    }
    // Now we know there are no nonkey restrictions.

    if (dynamic_pointer_cast<multi_column_restriction>(_clustering_columns_restrictions)) {
        // Multicolumn bounds mean lexicographic order, implying a continuous clustering range.  Multicolumn IN means a
        // finite set of continuous ranges.  Multicolumn restrictions cannot currently be combined with single-column
        // clustering restrictions.  Therefore, a continuous clustering range is guaranteed.
        return false;
    }
    if (!_clustering_columns_restrictions->needs_filtering(*_schema)) { // Guaranteed continuous clustering range.
        return false;
    }
    // Now we know there are some clustering-column restrictions that are out-of-order or not EQ.  A naive base-table
    // query must be filtered.  What about an index-table query?  That can only avoid filtering if there is exactly one
    // EQ supported by an index.
    return !(_clustering_columns_restrictions->size() == 1 && _has_queriable_ck_index);

    // TODO: it is also possible to avoid filtering here if a non-empty CK prefix is specified and token_known, plus
    // there's exactly one out-of-order-but-index-supported clustering-column restriction.
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

const single_column_restrictions::restrictions_map& statement_restrictions::get_single_column_partition_key_restrictions() const {
    static single_column_restrictions::restrictions_map empty;
    auto single_restrictions = dynamic_pointer_cast<single_column_partition_key_restrictions>(_partition_key_restrictions);
    if (!single_restrictions) {
        if (dynamic_pointer_cast<initial_key_restrictions<partition_key>>(_partition_key_restrictions)) {
            return empty;
        }
        throw std::runtime_error("statement restrictions for multi-column partition key restrictions are not implemented yet");
    }
    return single_restrictions->restrictions();
}

/**
 * @return clustering key restrictions split into single column restrictions (e.g. for filtering support).
 */
const single_column_restrictions::restrictions_map& statement_restrictions::get_single_column_clustering_key_restrictions() const {
    static single_column_restrictions::restrictions_map empty;
    auto single_restrictions = dynamic_pointer_cast<single_column_clustering_key_restrictions>(_clustering_columns_restrictions);
    if (!single_restrictions) {
        if (dynamic_pointer_cast<initial_key_restrictions<clustering_key>>(_clustering_columns_restrictions)) {
            return empty;
        }
        throw std::runtime_error("statement restrictions for multi-column partition key restrictions are not implemented yet");
    }
    return single_restrictions->restrictions();
}

} // namespace restrictions
} // namespace cql3
