
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <algorithm>
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>
#include <functional>
#include <ranges>
#include <stdexcept>

#include "cql3/expr/expression.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expr-utils.hh"
#include "query-result-reader.hh"
#include "statement_restrictions.hh"
#include "data_dictionary/data_dictionary.hh"
#include "cartesian_product.hh"
#include "cql3/cql_config.hh"

#include "cql3/constants.hh"
#include "cql3/lists.hh"
#include "cql3/selection/selection.hh"
#include "cql3/statements/request_validations.hh"
#include "types/list.hh"
#include "types/map.hh"
#include "types/set.hh"

namespace {
struct maybe_column_definition {
    const column_definition* value;
};
}

template<>
struct fmt::formatter<maybe_column_definition> : fmt::formatter<std::string_view> {
    template <typename FormatContext>
    auto format(const maybe_column_definition& cd, FormatContext& ctx) const {
        if (cd.value != nullptr) {
            return fmt::format_to(ctx.out(), "{}", *cd.value);
        } else {
            return fmt::format_to(ctx.out(), "(null)");
        }
    }
};

namespace cql3 {
namespace restrictions {

static logging::logger rlogger("restrictions");

using boost::adaptors::filtered;
using boost::adaptors::transformed;
using statements::request_validations::invalid_request;

statement_restrictions::statement_restrictions(schema_ptr schema, bool allow_filtering)
    : _schema(schema)
    , _partition_range_is_simple(true)
{ }

template <typename Visitor>
concept visitor_with_binary_operator_context = requires (Visitor v) {
    { v.current_binary_operator } -> std::convertible_to<const expr::binary_operator*>;
};

void with_current_binary_operator(
        visitor_with_binary_operator_context auto& visitor,
        std::invocable<const expr::binary_operator&> auto func) {
    if (!visitor.current_binary_operator) {
        throw std::logic_error("Evaluation expected within binary operator");
    }
    func(*visitor.current_binary_operator);
}

/// Every token, or if no tokens, an EQ/IN of every single PK column.
static std::vector<expr::expression> extract_partition_range(
        const expr::expression& where_clause, schema_ptr schema) {
    using namespace expr;
    struct extract_partition_range_visitor {
        schema_ptr table_schema;
        std::optional<expression> tokens;
        std::unordered_map<const column_definition*, expression> single_column;
        const binary_operator* current_binary_operator = nullptr;

        void operator()(const conjunction& c) {
            std::ranges::for_each(c.children, [this] (const expression& child) { expr::visit(*this, child); });
        }

        void operator()(const binary_operator& b) {
            if (current_binary_operator) {
                throw std::logic_error("Nested binary operators are not supported");
            }
            current_binary_operator = &b;
            expr::visit(*this, b.lhs);
            current_binary_operator = nullptr;
        }

        void operator()(const function_call& token_fun_call) {
            if (!is_partition_token_for_schema(token_fun_call, *table_schema)) {
                on_internal_error(rlogger, "extract_partition_range(function_call)");
            }

            with_current_binary_operator(*this, [&] (const binary_operator& b) {
                if (tokens) {
                    tokens = make_conjunction(std::move(*tokens), b);
                } else {
                    tokens = b;
                }
            });
        }

        void operator()(const column_value& cv) {
            auto s = &cv;
            with_current_binary_operator(*this, [&] (const binary_operator& b) {
                if (s->col->is_partition_key() && (b.op == oper_t::EQ || b.op == oper_t::IN)) {
                    const auto found = single_column.find(s->col);
                    if (found == single_column.end()) {
                        single_column[s->col] = b;
                    } else {
                        found->second = make_conjunction(std::move(found->second), b);
                    }
                }
            });
        }

        void operator()(const tuple_constructor& s) {
            // Partition key columns are not legal in tuples, so ignore tuples.
        }

        void operator()(const subscript& sub) {
            const column_value& cval = get_subscripted_column(sub.val);

            with_current_binary_operator(*this, [&] (const binary_operator& b) {
                if (cval.col->is_partition_key() && (b.op == oper_t::EQ || b.op == oper_t::IN)) {
                    const auto found = single_column.find(cval.col);
                    if (found == single_column.end()) {
                        single_column[cval.col] = b;
                    } else {
                        found->second = make_conjunction(std::move(found->second), b);
                    }
                }
            });
        }

        void operator()(const constant&) {}

        void operator()(const unresolved_identifier&) {
            on_internal_error(rlogger, "extract_partition_range(unresolved_identifier)");
        }

        void operator()(const column_mutation_attribute&) {
            on_internal_error(rlogger, "extract_partition_range(column_mutation_attribute)");
        }

        void operator()(const cast&) {
            on_internal_error(rlogger, "extract_partition_range(cast)");
        }

        void operator()(const field_selection&) {
            on_internal_error(rlogger, "extract_partition_range(field_selection)");
        }

        void operator()(const bind_variable&) {
            on_internal_error(rlogger, "extract_partition_range(bind_variable)");
        }

        void operator()(const untyped_constant&) {
            on_internal_error(rlogger, "extract_partition_range(untyped_constant)");
        }

        void operator()(const collection_constructor&) {
            on_internal_error(rlogger, "extract_partition_range(collection_constructor)");
        }

        void operator()(const usertype_constructor&) {
            on_internal_error(rlogger, "extract_partition_range(usertype_constructor)");
        }

        void operator()(const temporary&) {
            on_internal_error(rlogger, "extract_partition_range(temporary)");
        }
    };

    extract_partition_range_visitor v {
        .table_schema = schema
    };

    expr::visit(v, where_clause);
    if (v.tokens) {
        return {std::move(*v.tokens)};
    }
    if (v.single_column.size() == schema->partition_key_size()) {
        return boost::copy_range<std::vector<expression>>(v.single_column | boost::adaptors::map_values);
    }
    return {};
}

/// Extracts where_clause atoms with clustering-column LHS and copies them to a vector.  These elements define the
/// boundaries of any clustering slice that can possibly meet where_clause.  This vector can be calculated before
/// binding expression markers, since LHS and operator are always known.
static std::vector<expr::expression> extract_clustering_prefix_restrictions(
        const expr::expression& where_clause, schema_ptr schema) {
    using namespace expr;

    /// Collects all clustering-column restrictions from an expression.  Presumes the expression only uses
    /// conjunction to combine subexpressions.
    struct visitor {
        schema_ptr table_schema;
        std::vector<expression> multi; ///< All multi-column restrictions.
        /// All single-clustering-column restrictions, grouped by column.  Each value is either an atom or a
        /// conjunction of atoms.
        std::unordered_map<const column_definition*, expression> single;
        const binary_operator* current_binary_operator = nullptr;

        void operator()(const conjunction& c) {
            std::ranges::for_each(c.children, [this] (const expression& child) { expr::visit(*this, child); });
        }

        void operator()(const binary_operator& b) {
            if (current_binary_operator) {
                throw std::logic_error("Nested binary operators are not supported");
            }
            current_binary_operator = &b;
            expr::visit(*this, b.lhs);
            current_binary_operator = nullptr;
        }

        void operator()(const tuple_constructor& tc) {
            for (auto& e : tc.elements) {
                if (!expr::is<column_value>(e)) {
                    on_internal_error(rlogger, fmt::format("extract_clustering_prefix_restrictions: tuple of non-column_value: {}", tc));
                }
            }
            with_current_binary_operator(*this, [&] (const binary_operator& b) {
                multi.push_back(b);
            });
        }

        void operator()(const column_value& cv) {
            auto s = &cv;
            with_current_binary_operator(*this, [&] (const binary_operator& b) {
                if (s->col->is_clustering_key()) {
                    const auto found = single.find(s->col);
                    if (found == single.end()) {
                        single[s->col] = b;
                    } else {
                        found->second = make_conjunction(std::move(found->second), b);
                    }
                }
            });
        }

        void operator()(const subscript& sub) {
            const column_value& cval = get_subscripted_column(sub.val);

            with_current_binary_operator(*this, [&] (const binary_operator& b) {
                if (cval.col->is_clustering_key()) {
                    const auto found = single.find(cval.col);
                    if (found == single.end()) {
                        single[cval.col] = b;
                    } else {
                        found->second = make_conjunction(std::move(found->second), b);
                    }
                }
            });
        }

        void operator()(const function_call& fun_call) {
            if (is_partition_token_for_schema(fun_call, *table_schema)) {
                // A token cannot be a clustering prefix restriction
                return;
            }

            on_internal_error(rlogger, "extract_clustering_prefix_restrictions(function_call)");
        }

        void operator()(const constant&) {}

        void operator()(const unresolved_identifier&) {
            on_internal_error(rlogger, "extract_clustering_prefix_restrictions(unresolved_identifier)");
        }

        void operator()(const column_mutation_attribute&) {
            on_internal_error(rlogger, "extract_clustering_prefix_restrictions(column_mutation_attribute)");
        }

        void operator()(const cast&) {
            on_internal_error(rlogger, "extract_clustering_prefix_restrictions(cast)");
        }

        void operator()(const field_selection&) {
            on_internal_error(rlogger, "extract_clustering_prefix_restrictions(field_selection)");
        }

        void operator()(const bind_variable&) {
            on_internal_error(rlogger, "extract_clustering_prefix_restrictions(bind_variable)");
        }

        void operator()(const untyped_constant&) {
            on_internal_error(rlogger, "extract_clustering_prefix_restrictions(untyped_constant)");
        }

        void operator()(const collection_constructor&) {
            on_internal_error(rlogger, "extract_clustering_prefix_restrictions(collection_constructor)");
        }

        void operator()(const usertype_constructor&) {
            on_internal_error(rlogger, "extract_clustering_prefix_restrictions(usertype_constructor)");
        }

        void operator()(const temporary&) {
            on_internal_error(rlogger, "extract_clustering_prefix_restrictions(temporary)");
        }
    };
    visitor v {
        .table_schema = schema
    };

    expr::visit(v, where_clause);

    if (!v.multi.empty()) {
        return std::move(v.multi);
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

statement_restrictions::statement_restrictions(data_dictionary::database db,
        schema_ptr schema,
        statements::statement_type type,
        const expr::expression& where_clause,
        prepare_context& ctx,
        bool selects_only_static_columns,
        bool for_view,
        bool allow_filtering,
        check_indexes do_check_indexes)
    : statement_restrictions(schema, allow_filtering)
{
    _check_indexes = do_check_indexes;
    for (auto&& relation_expr : boolean_factors(where_clause)) {
        const expr::binary_operator* relation_binop = expr::as_if<expr::binary_operator>(&relation_expr);

        if (relation_binop == nullptr) {
            on_internal_error(rlogger, format("statement_restrictions: where clause has non-binop element: {}", relation_expr));
        }

        expr::binary_operator prepared_restriction = expr::validate_and_prepare_new_restriction(*relation_binop, db, schema, ctx);
        add_restriction(prepared_restriction, schema, allow_filtering, for_view);

        if (prepared_restriction.op != expr::oper_t::IS_NOT) {
            _where = _where.has_value() ? make_conjunction(std::move(*_where), prepared_restriction) : prepared_restriction;
        }
    }
    if (_where.has_value()) {
        if (!has_token_restrictions()) {
            _single_column_partition_key_restrictions = expr::get_single_column_restrictions_map(_partition_key_restrictions);
        }
        if (!expr::contains_multi_column_restriction(_clustering_columns_restrictions)) {
            _single_column_clustering_key_restrictions = expr::get_single_column_restrictions_map(_clustering_columns_restrictions);
        }
        _single_column_nonprimary_key_restrictions = expr::get_single_column_restrictions_map(_nonprimary_key_restrictions);
        _clustering_prefix_restrictions = extract_clustering_prefix_restrictions(*_where, _schema);
        _partition_range_restrictions = extract_partition_range(*_where, _schema);
    }
    _has_multi_column = find_binop(_clustering_columns_restrictions, expr::is_multi_column);
  if (_check_indexes) {
    auto cf = db.find_column_family(schema);
    auto& sim = cf.get_index_manager();
    const expr::allow_local_index allow_local(
            !has_partition_key_unrestricted_components()
            && partition_key_restrictions_is_all_eq());
    _has_multi_column = find_binop(_clustering_columns_restrictions, expr::is_multi_column);
    _has_queriable_ck_index = clustering_columns_restrictions_have_supporting_index(sim, allow_local)
            && !type.is_delete();
    _has_queriable_pk_index = parition_key_restrictions_have_supporting_index(sim, allow_local)
            && !type.is_delete();
    _has_queriable_regular_index = expr::index_supports_some_column(_nonprimary_key_restrictions, sim, allow_local)
            && !type.is_delete();
  } else {
    _has_queriable_ck_index = false;
    _has_queriable_pk_index = false;
    _has_queriable_regular_index = false;
  }

    // At this point, the select statement if fully constructed, but we still have a few things to validate
    process_partition_key_restrictions(for_view, allow_filtering);

    // Some but not all of the partition key columns have been specified;
    // hence we need turn these restrictions into index expressions.
    if (_uses_secondary_indexing || pk_restrictions_need_filtering()) {
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
    if (_is_key_range && _has_queriable_ck_index && !_has_multi_column) {
        _uses_secondary_indexing = true;
    }

    if (_uses_secondary_indexing || clustering_key_restrictions_need_filtering()) {
        _index_restrictions.push_back(_clustering_columns_restrictions);
    } else if (find_binop(_clustering_columns_restrictions, expr::is_on_collection)) {
        fail(unimplemented::cause::INDEXES);
    }

    if (!expr::is_empty_restriction(_nonprimary_key_restrictions)) {
        if (_has_queriable_regular_index && _partition_range_is_simple) {
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

bool
statement_restrictions::clustering_key_restrictions_has_IN() const {
    return find(_clustering_columns_restrictions, expr::oper_t::IN);
}

bool
statement_restrictions::clustering_key_restrictions_has_only_eq() const {
    return expr::has_only_eq_binops(_clustering_columns_restrictions);
}

bool
statement_restrictions::has_token_restrictions() const {
    return has_partition_token(_partition_key_restrictions, *_schema);
}

bool
statement_restrictions::key_is_in_relation() const {
    return find(_partition_key_restrictions, expr::oper_t::IN);
}

const expr::expression&
statement_restrictions::get_restrictions(column_kind kind) const {
    switch (kind) {
    case column_kind::partition_key: return _partition_key_restrictions;
    case column_kind::clustering_key: return _clustering_columns_restrictions;
    default: return _nonprimary_key_restrictions;
    }
}

bool
statement_restrictions::has_clustering_columns_restriction() const {
    return !expr::is_empty_restriction(_clustering_columns_restrictions);
}

bool
statement_restrictions::has_non_primary_key_restriction() const {
    return !expr::is_empty_restriction(_nonprimary_key_restrictions);
}

bool
statement_restrictions::ck_restrictions_need_filtering() const {
    if (expr::is_empty_restriction(_clustering_columns_restrictions)) {
        return false;
    }

    return has_partition_key_unrestricted_components()
    || clustering_key_restrictions_need_filtering()
    // If token restrictions are present in an indexed query, then all other restrictions need to be filtered.
    // A single token restriction can have multiple matching partition key values.
    // Because of this we can't create a clustering prefix with more than token restriction.
    || (_uses_secondary_indexing && has_token_restrictions());
}

bool
statement_restrictions::is_restricted(const column_definition* cdef) const {
    if (_not_null_columns.contains(cdef)) {
        return true;
    }

    auto restricted = expr::get_sorted_column_defs(get_restrictions(cdef->kind));
    return std::find(restricted.begin(), restricted.end(), cdef) != restricted.end();
}


const std::vector<expr::expression>& statement_restrictions::index_restrictions() const {
    return _index_restrictions;
}

// Current score table:
// local and restrictions include full partition key: 2
// global: 1
// local and restrictions does not include full partition key: 0 (do not pick)
int statement_restrictions::score(const secondary_index::index& index) const {
    if (index.metadata().local()) {
        const bool allow_local = !has_partition_key_unrestricted_components() && partition_key_restrictions_is_all_eq();
        return  allow_local ? 2 : 0;
    }
    return 1;
}

std::pair<std::optional<secondary_index::index>, expr::expression> statement_restrictions::find_idx(const secondary_index::secondary_index_manager& sim) const {
    std::optional<secondary_index::index> chosen_index;
    int chosen_index_score = 0;
    expr::expression chosen_index_restrictions;

    for (const auto& index : sim.list_indexes()) {
        auto cdef = _schema->get_column_definition(to_bytes(index.target_column()));
        for (const expr::expression& restriction : index_restrictions()) {
            if (has_partition_token(restriction, *_schema) || contains_multi_column_restriction(restriction)) {
                continue;
            }

            expr::single_column_restrictions_map rmap = expr::get_single_column_restrictions_map(restriction);
            const auto found = rmap.find(cdef);
            if (found != rmap.end() && is_supported_by(found->second, index)
                && score(index) > chosen_index_score) {
                chosen_index = index;
                chosen_index_score = score(index);
                chosen_index_restrictions = restriction;
            }
        }
    }
    return {chosen_index, chosen_index_restrictions};
}

bool statement_restrictions::has_eq_restriction_on_column(const column_definition& column) const {
    if (!_where.has_value()) {
        return false;
    }

    return expr::has_eq_restriction_on_column(column, *_where);
}

std::vector<const column_definition*> statement_restrictions::get_column_defs_for_filtering(data_dictionary::database db) const {
    std::vector<const column_definition*> column_defs_for_filtering;
    if (need_filtering()) {
        std::optional<secondary_index::index> opt_idx;
      if (_check_indexes) {
        auto cf = db.find_column_family(_schema);
        auto& sim = cf.get_index_manager();
        opt_idx = std::get<0>(find_idx(sim));
      }
        auto column_uses_indexing = [&opt_idx] (const column_definition* cdef, const expr::expression* single_col_restr) {
            return opt_idx && single_col_restr && is_supported_by(*single_col_restr, *opt_idx);
        };
        if (pk_restrictions_need_filtering()) {
            for (auto&& cdef : expr::get_sorted_column_defs(_partition_key_restrictions)) {
                const expr::expression* single_col_restr = nullptr;
                auto it = _single_column_partition_key_restrictions.find(cdef);
                if (it != _single_column_partition_key_restrictions.end()) {
                    if (is_single_column_restriction(it->second)) {
                        single_col_restr = &it->second;
                    }
                }
                if (!column_uses_indexing(cdef, single_col_restr)) {
                    column_defs_for_filtering.emplace_back(cdef);
                }
            }
        }
        const bool pk_has_unrestricted_components = has_partition_key_unrestricted_components();
        if (pk_has_unrestricted_components || clustering_key_restrictions_need_filtering()) {
            column_id first_filtering_id = pk_has_unrestricted_components ? 0 : _schema->clustering_key_columns().begin()->id +
                    num_clustering_prefix_columns_that_need_not_be_filtered();
            for (auto&& cdef : expr::get_sorted_column_defs(_clustering_columns_restrictions)) {
                const expr::expression* single_col_restr = nullptr;
                auto it = _single_column_partition_key_restrictions.find(cdef);
                if (it != _single_column_partition_key_restrictions.end()) {
                    single_col_restr = &it->second;
                }
                if (cdef->id >= first_filtering_id && !column_uses_indexing(cdef, single_col_restr)) {
                    column_defs_for_filtering.emplace_back(cdef);
                }
            }
        }
        for (auto&& [cdef, cur_restr] : _single_column_nonprimary_key_restrictions) {
            if (!column_uses_indexing(cdef, &cur_restr)) {
                column_defs_for_filtering.emplace_back(cdef);
            }
        }
    }
    return column_defs_for_filtering;
}

void statement_restrictions::add_restriction(const expr::binary_operator& restr, schema_ptr schema, bool allow_filtering, bool for_view) {
    if (restr.op == expr::oper_t::IS_NOT) {
        // Handle IS NOT NULL restrictions seperately
        add_is_not_restriction(restr, schema, for_view);
    } else if (expr::is_multi_column(restr)) {
        // Multi column restrictions are only allowed on clustering columns
        add_multi_column_clustering_key_restriction(restr);
    } else if (has_partition_token(restr, *_schema)) {
        // Token always restricts the partition key
        add_token_partition_key_restriction(restr);
    } else if (expr::is_single_column_restriction(restr)) {
        const column_definition* def = get_the_only_column(restr).col;
        if (def->is_partition_key()) {
            add_single_column_parition_key_restriction(restr, schema, allow_filtering, for_view);
        } else if (def->is_clustering_key()) {
            add_single_column_clustering_key_restriction(restr, schema, allow_filtering);
        } else {
            add_single_column_nonprimary_key_restriction(restr);
        }
    } else {
        throw exceptions::invalid_request_exception(format("Unhandled restriction: {}", restr));
    }
}

void statement_restrictions::add_is_not_restriction(const expr::binary_operator& restr, schema_ptr schema, bool for_view) {
    const expr::column_value* lhs_col_def = expr::as_if<expr::column_value>(&restr.lhs);
    // The "IS NOT NULL" restriction is only supported (and
    // mandatory) for materialized view creation:
    if (lhs_col_def == nullptr) {
        throw exceptions::invalid_request_exception("IS NOT only supports single column");
    }
    // currently, the grammar only allows the NULL argument to be
    // "IS NOT", so this assertion should not be able to fail
    if (!expr::is<expr::constant>(restr.rhs) || !expr::as<expr::constant>(restr.rhs).is_null()) {
        throw exceptions::invalid_request_exception("Only IS NOT NULL is supported");
    }

    _not_null_columns.insert(lhs_col_def->col);

    if (!for_view) {
        throw exceptions::invalid_request_exception(format("restriction '{}' is only supported in materialized view creation", restr));
    }
}

void statement_restrictions::add_single_column_parition_key_restriction(const expr::binary_operator& restr, schema_ptr schema, bool allow_filtering, bool for_view) {
    // View definition allows PK slices, because it's not a performance problem.
    if (restr.op != expr::oper_t::EQ && restr.op != expr::oper_t::IN && !allow_filtering && !for_view) {
        throw exceptions::invalid_request_exception(
                "Only EQ and IN relation are supported on the partition key "
                "(unless you use the token() function or allow filtering)");
    }
    if (has_token_restrictions()) {
        throw exceptions::invalid_request_exception(
                format("Columns \"{}\" cannot be restricted by both a normal relation and a token relation",
                       fmt::join(expr::get_sorted_column_defs(_partition_key_restrictions) |
                                 boost::adaptors::transformed([](auto* p) {
                                   return maybe_column_definition{p};
                                 }),
                                 ", ")));
    }

    _partition_key_restrictions = expr::make_conjunction(_partition_key_restrictions, restr);
    _partition_range_is_simple &= !find(restr, expr::oper_t::IN);
}

void statement_restrictions::add_token_partition_key_restriction(const expr::binary_operator& restr) {
    if (!partition_key_restrictions_is_empty() && !has_token_restrictions()) {
        throw exceptions::invalid_request_exception(
                format("Columns \"{}\" cannot be restricted by both a normal relation and a token relation",
                        fmt::join(expr::get_sorted_column_defs(_partition_key_restrictions) |
                                  boost::adaptors::transformed([](auto* p) {
                                    return maybe_column_definition{p};
                                  }),
                                  ", ")));
    }

    _partition_key_restrictions = expr::make_conjunction(_partition_key_restrictions, restr);
}

void statement_restrictions::add_single_column_clustering_key_restriction(const expr::binary_operator& restr, schema_ptr schema, bool allow_filtering) {
    if (find_binop(_clustering_columns_restrictions, [] (const expr::binary_operator& b) {
                return expr::is<expr::tuple_constructor>(b.lhs);
            })) {
        throw exceptions::invalid_request_exception(
            "Mixing single column relations and multi column relations on clustering columns is not allowed");
    }

    const column_definition* new_column = expr::get_the_only_column(restr).col;
    const column_definition* last_column = expr::get_last_column_def(_clustering_columns_restrictions);

    if (last_column != nullptr && !allow_filtering) {
        if (has_slice(_clustering_columns_restrictions) && schema->position(*new_column) > schema->position(*last_column)) {
            throw exceptions::invalid_request_exception(format("Clustering column \"{}\" cannot be restricted (preceding column \"{}\" is restricted by a non-EQ relation)",
                new_column->name_as_text(), last_column->name_as_text()));
        }

        if (schema->position(*new_column) < schema->position(*last_column)) {
            if (has_slice(restr)) {
                throw exceptions::invalid_request_exception(format("PRIMARY KEY column \"{}\" cannot be restricted (preceding column \"{}\" is restricted by a non-EQ relation)",
                    last_column->name_as_text(), new_column->name_as_text()));
            }
        }
    }

    _clustering_columns_restrictions = expr::make_conjunction(_clustering_columns_restrictions, restr);
}

void statement_restrictions::add_multi_column_clustering_key_restriction(const expr::binary_operator& restr) {
    if (expr::is_empty_restriction(_clustering_columns_restrictions)) {
        _clustering_columns_restrictions = restr;
        return;
    }

    if (!find_binop(_clustering_columns_restrictions, [] (const expr::binary_operator& b) {
                return expr::is<expr::tuple_constructor>(b.lhs);
    })) {
        throw exceptions::invalid_request_exception("Mixing single column relations and multi column relations on clustering columns is not allowed");
    }

    if (restr.op == expr::oper_t::EQ) {
        throw exceptions::invalid_request_exception(format("{} cannot be restricted by more than one relation if it includes an Equal",
            expr::get_columns_in_commons(_clustering_columns_restrictions, restr)));
    } else if (restr.op == expr::oper_t::IN) {
        throw exceptions::invalid_request_exception(format("{} cannot be restricted by more than one relation if it includes a IN",
                                                           expr::get_columns_in_commons(_clustering_columns_restrictions, restr)));
    } else if (is_slice(restr.op)) {
        if (!expr::has_slice(_clustering_columns_restrictions)) {
            throw exceptions::invalid_request_exception(format("Column \"{}\" cannot be restricted by both an equality and an inequality relation",
                                                           expr::get_columns_in_commons(_clustering_columns_restrictions, restr)));
        }

        const expr::binary_operator* other_slice = expr::find_in_expression<expr::binary_operator>(_clustering_columns_restrictions, [](const expr::binary_operator){return true;});
        if (other_slice == nullptr) {
            on_internal_error(rlogger, "add_multi_column_clustering_key_restriction: _clustering_columns_restrictions is empty!");
        }

        // Don't allow to mix plain and SCYLLA_CLUSTERING_BOUND bounds
        if (other_slice->order != restr.order) {
            static auto order2str = [](auto o) { return o == expr::comparison_order::cql ? "plain" : "SCYLLA_CLUSTERING_BOUND"; };
            throw exceptions::invalid_request_exception(
                    format("Invalid combination of restrictions ({} / {})",
                    order2str(other_slice->order), order2str(restr.order)));
        }

        // Here check that there aren't two < <= or two > and >=
        auto is_greater = [](expr::oper_t op) {return op == expr::oper_t::GT || op == expr::oper_t::GTE; };
        auto is_less = [](expr::oper_t op) {return op == expr::oper_t::LT || op == expr::oper_t::LTE; };

        if (is_greater(restr.op) && is_greater(other_slice->op)) {
            throw exceptions::invalid_request_exception(format(
            "More than one restriction was found for the start bound on {}",
                expr::get_columns_in_commons(restr, *other_slice)));
        }

        if (is_less(restr.op) && is_less(other_slice->op)) {
            throw exceptions::invalid_request_exception(format(
                "More than one restriction was found for the end bound on {}",
                expr::get_columns_in_commons(restr, *other_slice)));
        }

        _clustering_columns_restrictions = expr::make_conjunction(_clustering_columns_restrictions, restr);
    } else {
        throw exceptions::invalid_request_exception(format("Unsupported multi-column relation: ", restr));
    }
}

void statement_restrictions::add_single_column_nonprimary_key_restriction(const expr::binary_operator& restr) {
    _nonprimary_key_restrictions = expr::make_conjunction(_nonprimary_key_restrictions, restr);
}

void statement_restrictions::process_partition_key_restrictions(bool for_view, bool allow_filtering) {
    // If there is a queriable index, no special condition are required on the other restrictions.
    // But we still need to know 2 things:
    // - If we don't have a queriable index, is the query ok
    // - Is it queriable without 2ndary index, which is always more efficient
    // If a component of the partition key is restricted by a relation, all preceding
    // components must have a EQ. Only the last partition key component can be in IN relation.
    if (has_token_restrictions()) {
        _is_key_range = true;
    } else if (expr::is_empty_restriction(_partition_key_restrictions)) {
        _is_key_range = true;
        _uses_secondary_indexing = _has_queriable_pk_index;
    }

    if (pk_restrictions_need_filtering()) {
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
    std::vector<const column_definition*> pk_columns = expr::get_sorted_column_defs(_partition_key_restrictions);
    bool all_restricted = pk_columns.size() == _schema->partition_key_size();
    return !all_restricted;
}

bool statement_restrictions::partition_key_restrictions_is_empty() const {
    return expr::is_empty_restriction(_partition_key_restrictions);
}

bool statement_restrictions::partition_key_restrictions_is_all_eq() const {
    return expr::has_only_eq_binops(_partition_key_restrictions);
}

size_t statement_restrictions::partition_key_restrictions_size() const {
    return expr::get_sorted_column_defs(_partition_key_restrictions).size();
}

bool statement_restrictions::pk_restrictions_need_filtering() const {
     return !expr::is_empty_restriction(_partition_key_restrictions)
         && !has_token_restrictions()
         && (has_partition_key_unrestricted_components() || expr::has_slice_or_needs_filtering(_partition_key_restrictions));
}

size_t statement_restrictions::clustering_columns_restrictions_size() const {
    return expr::get_sorted_column_defs(_clustering_columns_restrictions).size();
}

bool statement_restrictions::clustering_key_restrictions_need_filtering() const {
    if (expr::contains_multi_column_restriction(_clustering_columns_restrictions)) {
        return false;
    }

    return num_clustering_prefix_columns_that_need_not_be_filtered() < clustering_columns_restrictions_size();
}

bool statement_restrictions::has_unrestricted_clustering_columns() const {
    return clustering_columns_restrictions_size() < _schema->clustering_key_size();
}

const column_definition& statement_restrictions::unrestricted_column(column_kind kind) const {
    const auto& restrictions = get_restrictions(kind);
    const auto sorted_cols = expr::get_sorted_column_defs(restrictions);
    for (size_t i = 0, count = _schema->columns_count(kind); i < count; ++i) {
        if (i >= sorted_cols.size() || sorted_cols[i]->component_index() != i) {
            return _schema->column_at(kind, i);
        }
    }
    on_internal_error(rlogger, format(
            "no missing columns with kind {} found in expression {}",
            to_sstring(kind), restrictions));
};

bool statement_restrictions::clustering_columns_restrictions_have_supporting_index(
        const secondary_index::secondary_index_manager& index_manager,
        expr::allow_local_index allow_local) const {
    // Single column restrictions can be handled by the existing code
    if (!expr::contains_multi_column_restriction(_clustering_columns_restrictions)) {
        return expr::index_supports_some_column(_clustering_columns_restrictions, index_manager, allow_local);
    }

    // Multi column restrictions have to be handled separately
    for (const auto& index : index_manager.list_indexes()) {
        if (!allow_local && index.metadata().local()) {
            continue;
        }
        if (multi_column_clustering_restrictions_are_supported_by(index)) {
            return true;
        }
    }
    return false;
}

bool statement_restrictions::multi_column_clustering_restrictions_are_supported_by(
        const secondary_index::index& index) const {
    // Slice restrictions have to be checked depending on the clustering slice
    if (has_slice(_clustering_columns_restrictions)) {
        bounds_slice clustering_slice = get_clustering_slice();

        const expr::column_value* supported_column =
            find_in_expression<expr::column_value>(_clustering_columns_restrictions,
                [&](const expr::column_value& cval) -> bool {
                    return clustering_slice.is_supported_by(*cval.col, index);
                }
        );
        return supported_column != nullptr;
    }

    // Otherwise it has to be a singe binary operator with EQ or IN.
    // This is checked earlier during add_restriction.
    const expr::binary_operator* single_binop =
        expr::as_if<expr::binary_operator>(&_clustering_columns_restrictions);
    if (single_binop == nullptr) {
        on_internal_error(rlogger, format(
            "multi_column_clustering_restrictions_are_supported_by more than one non-slice restriction: {}",
            _clustering_columns_restrictions));
    }

    if (single_binop->op != expr::oper_t::IN && single_binop->op != expr::oper_t::EQ) {
        on_internal_error(rlogger, format("Disallowed multi column restriction: {}", *single_binop));
    }

    const expr::column_value* supported_column =
        find_in_expression<expr::column_value>(_clustering_columns_restrictions,
            [&](const expr::column_value& cval) -> bool {
                return index.supports_expression(*cval.col, single_binop->op);
            }
    );
    return supported_column != nullptr;
}

bounds_slice statement_restrictions::get_clustering_slice() const {
    std::optional<bounds_slice> result;

    expr::for_each_expression<expr::binary_operator>(_clustering_columns_restrictions,
        [&](const expr::binary_operator& binop) {
            bounds_slice cur_slice = bounds_slice::from_binary_operator(binop);
            if (!result.has_value()) {
                result = cur_slice;
            } else {
                result->merge(cur_slice);
            }
        }
    );

    return *result;
}

bool statement_restrictions::parition_key_restrictions_have_supporting_index(const secondary_index::secondary_index_manager& index_manager,
                                      expr::allow_local_index allow_local) const {
    // Token restrictions can't be supported by an index
    if (has_token_restrictions()) {
        return false;
    }

    return expr::index_supports_some_column(_partition_key_restrictions, index_manager, allow_local);
}

void statement_restrictions::process_clustering_columns_restrictions(bool for_view, bool allow_filtering) {
    if (!has_clustering_columns_restriction()) {
        return;
    }

    if (find_binop(_clustering_columns_restrictions, expr::is_on_collection)
        && !_has_queriable_ck_index && !allow_filtering) {
        throw exceptions::invalid_request_exception(
            "Cannot restrict clustering columns by a CONTAINS relation without a secondary index or filtering");
    }

    if (has_clustering_columns_restriction() && clustering_key_restrictions_need_filtering()) {
        if (_has_queriable_ck_index) {
            _uses_secondary_indexing = true;
        } else if (!allow_filtering && !for_view) {
            auto clustering_columns_iter = _schema->clustering_key_columns().begin();
            for (auto&& restricted_column : expr::get_sorted_column_defs(_clustering_columns_restrictions)) {
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
dht::partition_range_vector partition_ranges_from_token(const expr::expression& ex,
                                                        const query_options& options,
                                                        const schema& table_schema) {
    auto values = possible_partition_token_values(ex, options, table_schema);
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
        if (const auto arbitrary_binop = find_binop(e, [] (const binary_operator&) { return true; })) {
            if (auto cv = expr::as_if<expr::column_value>(&arbitrary_binop->lhs)) {
                const value_set vals = possible_column_values(cv->col, e, options);
                if (auto lst = std::get_if<value_list>(&vals)) {
                    if (lst->empty()) {
                        return {};
                    }
                    product_size *= lst->size();
                    error_if_exceeds(product_size, size_limit);
                    column_values[schema.position(*cv->col)] = std::move(*lst);
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
        const auto col = expr::get_subscripted_column(find(e, oper_t::EQ)->lhs).col;
        const auto vals = std::get<value_list>(possible_column_values(col, e, options));
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
    if (has_partition_token(_partition_range_restrictions[0], *_schema)) {
        if (_partition_range_restrictions.size() != 1) {
            on_internal_error(
                    rlogger,
                    format("Unexpected size of token restrictions: {}", _partition_range_restrictions.size()));
        }
        return partition_ranges_from_token(_partition_range_restrictions[0], options, *_schema);
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
            auto opt_values = expr::get_tuple_elements(expr::evaluate(binop.rhs, options), *type_of(binop.rhs));
            auto& lhs = expr::as<tuple_constructor>(binop.lhs);
            std::vector<managed_bytes> values(lhs.elements.size());
            for (size_t i = 0; i < lhs.elements.size(); ++i) {
                auto& col = expr::as<column_value>(lhs.elements.at(i));
                values[i] = *statements::request_validations::check_not_null(
                        opt_values[i],
                        "Invalid null value in condition for column {}", col.col->name_as_text());
            }
            intersect_all(to_range(binop.op, clustering_key_prefix(std::move(values))));
        } else if (binop.op == oper_t::IN) {
            const cql3::raw_value tup = expr::evaluate(binop.rhs, options);
            utils::chunked_vector<std::vector<managed_bytes_opt>> tuple_elems;
            if (tup.is_value()) {
                tuple_elems = expr::get_list_of_tuples_elements(tup, *type_of(binop.rhs));
            }
            process_in_values(std::move(tuple_elems));
        } else {
            on_internal_error(rlogger, format("multi_column_range_accumulator: unexpected atom {}", binop));
        }
    }

    void operator()(const conjunction& c) {
        std::ranges::for_each(c.children, [this] (const expression& child) { expr::visit(*this, child); });
    }

    void operator()(const constant& v) {
        std::optional<bool> bool_val = get_bool_value(v);
        if (!bool_val.has_value()) {
            on_internal_error(rlogger, "non-bool constant encountered outside binary operator");
        }

        if (*bool_val == false) {
            ranges.clear();
        }
    }

    void operator()(const column_value&) {
        on_internal_error(rlogger, "Column encountered outside binary operator");
    }

    void operator()(const subscript&) {
        on_internal_error(rlogger, "Subscript encountered outside binary operator");
    }

    void operator()(const unresolved_identifier&) {
        on_internal_error(rlogger, "Unresolved identifier encountered outside binary operator");
    }

    void operator()(const column_mutation_attribute&) {
        on_internal_error(rlogger, "writetime/ttl encountered outside binary operator");
    }

    void operator()(const function_call&) {
        on_internal_error(rlogger, "function call encountered outside binary operator");
    }

    void operator()(const cast&) {
        on_internal_error(rlogger, "typecast encountered outside binary operator");
    }

    void operator()(const field_selection&) {
        on_internal_error(rlogger, "field selection encountered outside binary operator");
    }

    void operator()(const bind_variable&) {
        on_internal_error(rlogger, "bind variable encountered outside binary operator");
    }

    void operator()(const untyped_constant&) {
        on_internal_error(rlogger, "untyped constant encountered outside binary operator");
    }

    void operator()(const tuple_constructor&) {
        on_internal_error(rlogger, "tuple constructor encountered outside binary operator");
    }

    void operator()(const collection_constructor&) {
        on_internal_error(rlogger, "collection constructor encountered outside binary operator");
    }

    void operator()(const usertype_constructor&) {
        on_internal_error(rlogger, "collection constructor encountered outside binary operator");
    }

    void operator()(const temporary&) {
        on_internal_error(rlogger, "temporary encountered outside binary operator");
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
        expr::visit(acc, restr);
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
        const schema& schema,
        const std::vector<expression>& single_column_restrictions) {
    const size_t size_limit =
            options.get_cql_config().restrictions.clustering_key_restrictions_max_cartesian_product_size;
    size_t product_size = 1;
    std::vector<std::vector<managed_bytes>> prior_column_values; // Equality values of columns seen so far.
    for (size_t i = 0; i < single_column_restrictions.size(); ++i) {
        auto values = possible_column_values(
                &schema.clustering_column_at(i), // This should be the LHS of restrictions[i].
                single_column_restrictions[i],
                options);
        if (auto list = std::get_if<value_list>(&values)) {
            if (list->empty()) { // Impossible condition -- no rows can possibly match.
                return {};
            }
            product_size *= list->size();
            prior_column_values.push_back(std::move(*list));
            error_if_exceeds(product_size, size_limit);
        } else if (auto last_range = std::get_if<nonwrapping_interval<managed_bytes>>(&values)) {
            // Must be the last column in the prefix, since it's neither EQ nor IN.
            std::vector<query::clustering_range> ck_ranges;
            if (prior_column_values.empty()) {
                // This is the first and last range; just turn it into a clustering_key_prefix.
                ck_ranges.push_back(
                        reverse_if_reqd(
                                last_range->transform([] (const managed_bytes& val) { return clustering_key_prefix::from_range(std::array<managed_bytes, 1>{val}); }),
                                *schema.clustering_column_at(i).type));
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
                    ck_ranges.push_back(reverse_if_reqd({new_start, new_end}, *schema.clustering_column_at(i).type));
                }
            }
            sort(ck_ranges.begin(), ck_ranges.end(), range_less{schema});
            return ck_ranges;
        }
    }
    // All prefix columns are restricted by EQ or IN.  The resulting CK ranges are just singular ranges of corresponding
    // prior_column_values.
    std::vector<query::clustering_range> ck_ranges(product_size);
    cartesian_product cp(prior_column_values);
    std::transform(cp.begin(), cp.end(), ck_ranges.begin(), std::bind_front(query::clustering_range::make_singular));
    sort(ck_ranges.begin(), ck_ranges.end(), range_less{schema});
    return ck_ranges;
}

// In old v1 indexes the token column was of type blob.
// This causes problems because blobs are sorted differently than the bigint token values that they represent.
// Tokens are encoded as 8 byte big endian two's complement signed integers,
// which with blob sorting makes them ordered like this:
// 0, 1, 2, 3, 4, 5, ..., bigint_max, bigint_min, ...., -5, -4, -3, -2, -1
// Because of this clustering restrictions like token(p) > -4 and token(p) < 4 need to be translated
// to two clustering ranges on the old index.
// All binary_operators in token_restriction must have column_value{token_column} as their LHS.
static std::vector<query::clustering_range> get_index_v1_token_range_clustering_bounds(
        const query_options& options,
        const column_definition& token_column,
        const expression& token_restriction) {

    // A workaround in order to make possible_column_values work properly.
    // possible_column_values looks at the column type and uses this type's comparator.
    // This is a problem because when using blob's comparator, -4 is greater than 4.
    // This makes possible_column_values think that an expression like token(p) > -4 and token(p) < 4
    // is impossible to fulfill.
    // Create a fake token column with the type set to bigint, translate the restriction to use this column
    // and use this restriction to calculate possible lhs values.
    column_definition token_column_bigint = token_column;
    token_column_bigint.type = long_type;
    expression new_token_restrictions = replace_column_def(token_restriction, &token_column_bigint);

    std::variant<value_list, nonwrapping_range<managed_bytes>> values =
        possible_column_values(&token_column_bigint, new_token_restrictions, options);

    return std::visit(overloaded_functor {
        [](const value_list& list) {
            std::vector<query::clustering_range> ck_ranges;
            ck_ranges.reserve(list.size());

            for (auto&& value : list) {
                ck_ranges.emplace_back(query::clustering_range::make_singular(std::vector<managed_bytes>{value}));
            }

            return ck_ranges;
        },
        [](const nonwrapping_interval<managed_bytes>& range) {
            auto int64_from_be_bytes = [](const managed_bytes& int_bytes) -> int64_t {
                if (int_bytes.size() != 8) {
                    throw std::runtime_error("token restriction value should be 8 bytes");
                }

                return read_be<int64_t>((const char*)&int_bytes[0]);
            };

            auto int64_to_be_bytes = [](int64_t int_val) -> managed_bytes {
                managed_bytes int_bytes(managed_bytes::initialized_later{}, 8);
                write_be((char*)&int_bytes[0], int_val);
                return int_bytes;
            };

            int64_t token_low = std::numeric_limits<int64_t>::min();
            int64_t token_high = std::numeric_limits<int64_t>::max();
            bool low_inclusive = true, high_inclusive = true;

            const std::optional<interval_bound<managed_bytes>>& start = range.start();
            const std::optional<interval_bound<managed_bytes>>& end = range.end();

            if (start.has_value()) {
                token_low = int64_from_be_bytes(start->value());
                low_inclusive = start->is_inclusive();
            }

            if (end.has_value()) {
                token_high = int64_from_be_bytes(end->value());
                high_inclusive = end->is_inclusive();
            }

            query::clustering_range::bound lower_bound(std::vector({int64_to_be_bytes(token_low)}), low_inclusive);
            query::clustering_range::bound upper_bound(std::vector({int64_to_be_bytes(token_high)}), high_inclusive);

            std::vector<query::clustering_range> ck_ranges;

            if (token_high < token_low) {
                // Impossible range, return empty clustering ranges
                return ck_ranges;
            }

            // Blob encoded tokens are sorted like this:
            // 0, 1, 2, 3, 4, 5, ..., bigint_max, bigint_min, ...., -5, -4, -3, -2, -1
            // This means that in cases where low >= 0 or high < 0 we can simply use the whole range.
            // In other cases we have to take two ranges: (low, -1] and [0, high).
            if (token_low >= 0 || token_high < 0) {
                ck_ranges.emplace_back(std::move(lower_bound), std::move(upper_bound));
            } else {
                query::clustering_range::bound zero_bound(std::vector({int64_to_be_bytes(0)}));
                query::clustering_range::bound min1_bound(std::vector({int64_to_be_bytes(-1)}));

                ck_ranges.reserve(2);

                if (!(token_high == 0 && !high_inclusive)) {
                    ck_ranges.emplace_back(std::move(zero_bound), std::move(upper_bound));
                }

                if (!(token_low == -1 && !low_inclusive)) {
                    ck_ranges.emplace_back(std::move(lower_bound), std::move(min1_bound));
                }
            }

            return ck_ranges;
        }
    }, values);
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
            clustering_key_prefix(std::move(partial_bound)),
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
            cql3::raw_value tup_val = expr::evaluate(b->rhs, options);
            if (tup_val.is_null()) {
                on_internal_error(rlogger, format("range_from_raw_bounds: unexpected atom {}", *b));
            }

            const auto r = to_range(
                    b->op, clustering_key_prefix::from_optional_exploded(schema, expr::get_tuple_elements(tup_val, *type_of(b->rhs))));
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
    if (find_binop(_clustering_prefix_restrictions[0], expr::is_multi_column)) {
        bool all_natural = true, all_reverse = true; ///< Whether column types are reversed or natural.
        for (auto& r : _clustering_prefix_restrictions) { // TODO: move to constructor, do only once.
            using namespace expr;
            const auto& binop = expr::as<binary_operator>(r);
            if (is_clustering_order(binop)) {
                return {range_from_raw_bounds(_clustering_prefix_restrictions, options, *_schema)};
            }
            for (auto& element : expr::as<tuple_constructor>(binop.lhs).elements) {
                auto& cv = expr::as<column_value>(element);
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
        return get_single_column_clustering_bounds(options, *_schema, _clustering_prefix_restrictions);
    }
}

namespace {

/// True iff get_partition_slice_for_global_index_posting_list() will be able to calculate the token value from the
/// given restrictions.  Keep in sync with the get_partition_slice_for_global_index_posting_list() source.
bool token_known(const statement_restrictions& r) {
    return !r.has_partition_key_unrestricted_components() && r.partition_key_restrictions_is_all_eq();
}

} // anonymous namespace

bool statement_restrictions::need_filtering() const {
    using namespace expr;

    if (_uses_secondary_indexing && has_token_restrictions()) {
        // If there is a token(p1, p2) restriction, no p1, p2 restrictions are allowed in the query.
        // All other restrictions must be on clustering or regular columns.
        int64_t non_pk_restrictions_count = clustering_columns_restrictions_size();
        non_pk_restrictions_count += expr::get_sorted_column_defs(_nonprimary_key_restrictions).size();

        // We are querying using an index, one restriction goes to the index restriction.
        // If there are some restrictions other than token() and index column then we need to do filtering.
        // p1, p2 can have many different values, so clustering prefix breaks.
        return non_pk_restrictions_count > 1;
    }

    const auto npart = partition_key_restrictions_size();
    if (npart > 0 && npart < _schema->partition_key_size()) {
        // Can't calculate the token value, so a naive base-table query must be filtered.  Same for any index tables,
        // except if there's only one restriction supported by an index.
        return !(npart == 1 && _has_queriable_pk_index &&
                 expr::is_empty_restriction(_clustering_columns_restrictions) &&
                 expr::is_empty_restriction(_nonprimary_key_restrictions));
    }
    if (pk_restrictions_need_filtering()) {
        // We most likely cannot calculate token(s).  Neither base-table nor index-table queries can avoid filtering.
        return true;
    }
    // Now we know the partition key is either unrestricted or fully restricted.

    const auto nreg = expr::get_sorted_column_defs(_nonprimary_key_restrictions).size();
    if (nreg > 1 || (nreg == 1 && !_has_queriable_regular_index)) {
        return true; // Regular columns are unsorted in storage and no single index suffices.
    }
    if (nreg == 1) { // Single non-key restriction supported by an index.
        // Will the index-table query require filtering?  That depends on whether its clustering key is restricted to a
        // continuous range.  Recall that this clustering key is (token, pk, ck) of the base table.
        if (npart == 0 && expr::is_empty_restriction(_clustering_columns_restrictions)) {
            return false; // No clustering key restrictions => whole partitions.
        }
        return !token_known(*this) || clustering_key_restrictions_need_filtering()
                // Multi-column restrictions don't require filtering when querying the base table, but the index
                // table has a different clustering key and may require filtering.
                || _has_multi_column;
    }
    // Now we know there are no nonkey restrictions.

    if (_has_multi_column) {
        // Multicolumn bounds mean lexicographic order, implying a continuous clustering range.  Multicolumn IN means a
        // finite set of continuous ranges.  Multicolumn restrictions cannot currently be combined with single-column
        // clustering restrictions.  Therefore, a continuous clustering range is guaranteed.
        return false;
    }

    if (_has_queriable_ck_index && _uses_secondary_indexing) {
        // In cases where we use an index, clustering column restrictions might cause the need for filtering.
        // TODO: This is overly conservative, there are some cases when this returns true but filtering
        // is not needed. Because of that the data_dictionary::database will sometimes perform filtering when it's not actually needed.
        // Query performance shouldn't be affected much, at most we will filter rows that are all correct.
        // Here are some cases to consider:
        // On a table with primary key (p, c1, c2, c3) with an index on c3
        // WHERE c3 = ? - doesn't require filtering
        // WHERE c1 = ? AND c2 = ? AND c3 = ? - requires filtering
        // WHERE p = ? AND c1 = ? AND c3 = ? - doesn't require filtering, but we conservatively report it does
        // WHERE p = ? AND c1 LIKE ? AND c3 = ? - requires filtering
        // WHERE p = ? AND c1 = ? AND c2 LIKE ? AND c3 = ? - requires filtering
        // WHERE p = ? AND c1 = ? AND c2 = ? AND c3 = ? - doesn't use an index
        // WHERE p = ? AND c1 = ? AND c2 < ? AND c3 = ? - doesn't require filtering, but we report it does
        return clustering_columns_restrictions_size() > 1;
    }
    // Now we know that the query doesn't use an index.

    // The only thing that can cause filtering now are the clustering columns.
    return clustering_key_restrictions_need_filtering();
}

void statement_restrictions::validate_secondary_index_selections(bool selects_only_static_columns) {
    if (key_is_in_relation()) {
        throw exceptions::invalid_request_exception(
            "Index cannot be used if the partition key is restricted with IN clause. This query would require filtering instead.");
    }
}

const expr::single_column_restrictions_map& statement_restrictions::get_single_column_partition_key_restrictions() const {
    return _single_column_partition_key_restrictions;
}

/**
 * @return clustering key restrictions split into single column restrictions (e.g. for filtering support).
 */
const expr::single_column_restrictions_map& statement_restrictions::get_single_column_clustering_key_restrictions() const {
    return _single_column_clustering_key_restrictions;
}

void statement_restrictions::prepare_indexed_global(const schema& idx_tbl_schema) {
    if (!_partition_range_is_simple) {
        return;
    }

    const column_definition* token_column = &idx_tbl_schema.clustering_column_at(0);

    if (has_token_restrictions()) {
        // When there is a token(p1, p2) >/</= ? restriction, it is not allowed to have restrictions on p1 or p2.
        // This means that p1 and p2 can have many different values (token is a hash, can have collisions).
        // Clustering prefix ends after token_restriction, all further restrictions have to be filtered.
        expr::expression token_restriction = replace_partition_token(_partition_key_restrictions, token_column, *_schema);
        _idx_tbl_ck_prefix = std::vector{std::move(token_restriction)};

        return;
    }

    // If we're here, it means the index cannot be on a partition column: process_partition_key_restrictions()
    // avoids indexing when _partition_range_is_simple.  See _idx_tbl_ck_prefix blurb for its composition.
    _idx_tbl_ck_prefix = std::vector<expr::expression>(1 + _schema->partition_key_size());
    _idx_tbl_ck_prefix->reserve(_idx_tbl_ck_prefix->size() + idx_tbl_schema.clustering_key_size());
    for (const auto& e : _partition_range_restrictions) {
        const auto col = expr::as<column_value>(find(e, oper_t::EQ)->lhs).col;
        const auto pos = _schema->position(*col) + 1;
        (*_idx_tbl_ck_prefix)[pos] = replace_column_def(e, &idx_tbl_schema.clustering_column_at(pos));
    }

    add_clustering_restrictions_to_idx_ck_prefix(idx_tbl_schema);

    (*_idx_tbl_ck_prefix)[0] = binary_operator(
            column_value(token_column),
            oper_t::EQ,
            // TODO: This should be a unique marker whose value we set at execution time.  There is currently no
            // handy mechanism for doing that in query_options.
            expr::constant::make_null(token_column->type));
}

void statement_restrictions::prepare_indexed_local(const schema& idx_tbl_schema) {
    if (!_partition_range_is_simple) {
        return;
    }

    // Local index clustering key is (indexed column, base clustering key)
    _idx_tbl_ck_prefix = std::vector<expr::expression>();
    _idx_tbl_ck_prefix->reserve(1 + _clustering_prefix_restrictions.size());

    const column_definition& indexed_column = idx_tbl_schema.column_at(column_kind::clustering_key, 0);
    const column_definition& indexed_column_base_schema = *_schema->get_column_definition(indexed_column.name());

    // Find index column restrictions in the WHERE clause
    std::vector<expr::expression> idx_col_restrictions =
        extract_single_column_restrictions_for_column(*_where, indexed_column_base_schema);
    expr::expression idx_col_restriction_expr = expr::expression(expr::conjunction{std::move(idx_col_restrictions)});

    // Translate the restriction to use column from the index schema and add it
    expr::expression replaced_idx_restriction = replace_column_def(idx_col_restriction_expr, &indexed_column);
    _idx_tbl_ck_prefix->push_back(replaced_idx_restriction);

    // Add restrictions for the clustering key
    add_clustering_restrictions_to_idx_ck_prefix(idx_tbl_schema);
}

void statement_restrictions::add_clustering_restrictions_to_idx_ck_prefix(const schema& idx_tbl_schema) {
    for (const auto& e : _clustering_prefix_restrictions) {
        if (find_binop(_clustering_prefix_restrictions[0], expr::is_multi_column)) {
            // TODO: We could handle single-element tuples, eg. `(c)>=(123)`.
            break;
        }
        const auto any_binop = find_binop(e, [] (auto&&) { return true; });
        if (!any_binop) {
            break;
        }
        const auto col = expr::as<column_value>(any_binop->lhs).col;
        _idx_tbl_ck_prefix->push_back(replace_column_def(e, idx_tbl_schema.get_column_definition(col->name())));
    }
}

// How many of the restrictions (in column order) do not need filtering
// because they are implemented as a slice (potentially, a contiguous disk
// read). For example, if we have the filter "c1 < 3 and c2 > 3", c1 does not
// need filtering but c2 does so num_prefix_columns_that_need_not_be_filtered
// will be 1.
unsigned int statement_restrictions::num_clustering_prefix_columns_that_need_not_be_filtered() const {
    if (expr::contains_multi_column_restriction(_clustering_columns_restrictions)) {
        return 0;
    }

    expr::single_column_restrictions_map column_restrictions =
        expr::get_single_column_restrictions_map(_clustering_columns_restrictions);

    // Restrictions currently need filtering in three cases:
    // 1. any of them is a CONTAINS restriction
    // 2. restrictions do not form a contiguous prefix (i.e. there are gaps in it)
    // 3. a SLICE restriction isn't on a last place
    column_id position = 0;
    unsigned int count = 0;
    for (const auto& restriction : column_restrictions | boost::adaptors::map_values) {
        if (find_needs_filtering(restriction)
            || position != get_the_only_column(restriction).col->id) {
            return count;
        }
        if (!has_slice(restriction)) {
            position = get_the_only_column(restriction).col->id + 1;
        }
        count++;
    }
    return count;
}

std::vector<query::clustering_range> statement_restrictions::get_global_index_clustering_ranges(
        const query_options& options,
        const schema& idx_tbl_schema) const {
    if (!_idx_tbl_ck_prefix) {
        on_internal_error(
                rlogger, "statement_restrictions::get_global_index_clustering_ranges called with unprepared index");
    }
    std::vector<managed_bytes> pk_value(_schema->partition_key_size());
    for (const auto& e : _partition_range_restrictions) {
        const auto col = expr::as<column_value>(find(e, oper_t::EQ)->lhs).col;
        const auto vals = std::get<value_list>(possible_column_values(col, e, options));
        if (vals.empty()) { // Case of C=1 AND C=2.
            return {};
        }
        pk_value[_schema->position(*col)] = std::move(vals[0]);
    }
    std::vector<bytes> pkv_linearized(pk_value.size());
    std::transform(pk_value.cbegin(), pk_value.cend(), pkv_linearized.begin(),
                   [] (const managed_bytes& mb) { return to_bytes(mb); });
    auto& token_column = idx_tbl_schema.clustering_column_at(0);
    bytes token_bytes = token_column.get_computation().compute_value(*_schema, pkv_linearized);

    // WARNING: We must not yield to another fiber from here until the function's end, lest this RHS be
    // overwritten.
    const_cast<expr::expression&>(expr::as<binary_operator>((*_idx_tbl_ck_prefix)[0]).rhs) =
            expr::constant(raw_value::make_value(token_bytes), token_column.type);

    // Multi column restrictions are not added to _idx_tbl_ck_prefix, they are handled later by filtering.
    return get_single_column_clustering_bounds(options, idx_tbl_schema, *_idx_tbl_ck_prefix);
}

std::vector<query::clustering_range> statement_restrictions::get_global_index_token_clustering_ranges(
    const query_options& options,
    const schema& idx_tbl_schema
) const {
    if (!_idx_tbl_ck_prefix.has_value()) {
        on_internal_error(
                rlogger, "statement_restrictions::get_global_index_token_clustering_ranges called with unprepared index");
    }

    const column_definition& token_column = idx_tbl_schema.clustering_column_at(0);

    // In old indexes the token column was of type blob.
    // This causes problems with sorting and must be handled separately.
    if (token_column.type != long_type) {
        return get_index_v1_token_range_clustering_bounds(options, token_column, _idx_tbl_ck_prefix->at(0));
    }

    return get_single_column_clustering_bounds(options, idx_tbl_schema, *_idx_tbl_ck_prefix);
}

std::vector<query::clustering_range> statement_restrictions::get_local_index_clustering_ranges(
        const query_options& options,
        const schema& idx_tbl_schema) const {
    if (!_idx_tbl_ck_prefix.has_value()) {
        on_internal_error(
            rlogger, "statement_restrictions::get_local_index_clustering_ranges called with unprepared index");
    }

    // Multi column restrictions are not added to _idx_tbl_ck_prefix, they are handled later by filtering.
    return get_single_column_clustering_bounds(options, idx_tbl_schema, *_idx_tbl_ck_prefix);
}

sstring statement_restrictions::to_string() const {
    return _where ? expr::to_string(*_where) : "";
}

static void validate_primary_key_restrictions(const query_options& options, const std::vector<expr::expression>& restrictions) {
    for (const auto& r: restrictions) {
        for_each_expression<binary_operator>(r, [&](const binary_operator& binop) {
            if (binop.op != oper_t::EQ && binop.op != oper_t::IN) {
                return;
            }
            const auto* c = as_if<column_value>(&binop.lhs);
            if (!c) {
                return;
            }
            if (evaluate(binop.rhs, options).is_null()) {
                throw exceptions::invalid_request_exception(format("Invalid null value in condition for column {}",
                    c->col->name_as_text()));
            }
        });
    }
}

void statement_restrictions::validate_primary_key(const query_options& options) const {
    validate_primary_key_restrictions(options, _partition_range_restrictions);
    validate_primary_key_restrictions(options, _clustering_prefix_restrictions);
}


const std::unordered_set<const column_definition*> statement_restrictions::get_not_null_columns() const {
    return _not_null_columns;
}

} // namespace restrictions
} // namespace cql3
