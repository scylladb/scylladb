
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <algorithm>
#include <boost/range/algorithm/set_algorithm.hpp>
#include <functional>
#include <ranges>
#include <stdexcept>

#include "cql3/expr/expression.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expr-utils.hh"
#include "statement_restrictions.hh"
#include "data_dictionary/data_dictionary.hh"
#include "cartesian_product.hh"

#include "cql3/cql_config.hh"
#include "cql3/query_options.hh"
#include "cql3/selection/selection.hh"
#include "cql3/statements/request_validations.hh"
#include "cql3/functions/token_fct.hh"
#include "dht/i_partitioner.hh"
#include "db/schema_tables.hh"
#include "types/tuple.hh"
#include "utils/overloaded_functor.hh"

namespace {
struct maybe_column_definition {
    const column_definition* value;
};
}

template<>
struct fmt::formatter<maybe_column_definition> : fmt::formatter<string_view> {
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

using namespace expr;

static logging::logger rlogger("restrictions");
static auto& expr_logger = rlogger; // compatibility with code moved from expression.cc

/// Turns value_set into a range, unless it's a multi-valued list (in which case this throws).
extern interval<managed_bytes> to_range(const value_set&);

/// A range of all X such that X op val.
interval<clustering_key_prefix> to_range(oper_t op, const clustering_key_prefix& val);

inline bool needs_filtering(oper_t op) {
    return (op == oper_t::CONTAINS) || (op == oper_t::CONTAINS_KEY) || (op == oper_t::LIKE) ||
           (op == oper_t::IS_NOT) || (op == oper_t::NEQ) || (op == oper_t::NOT_IN);
}

inline auto find_needs_filtering(const expression& e) {
    return find_binop(e, [] (const binary_operator& bo) { return needs_filtering(bo.op); });
}

inline bool has_slice_or_needs_filtering(const expression& e) {
    return find_binop(e, [] (const binary_operator& o) { return is_slice(o.op) || needs_filtering(o.op); });
}

/// True iff binary_operator involves a collection.
extern bool is_on_collection(const binary_operator&);

bool contains_multi_column_restriction(const expression&);

bool has_only_eq_binops(const expression&);

static
value_set
solve(const predicate& ac, const query_options& options) {
    if (ac.solve_for) {
        return ac.solve_for(options);
    }

    on_internal_error(rlogger, "solve: no solve_for function");
}

namespace {

const value_set empty_value_set = value_list{};
const value_set unbounded_value_set = interval<managed_bytes>::make_open_ended_both_sides();

struct intersection_visitor {
    const abstract_type* type;
    value_set operator()(const value_list& a, const value_list& b) const {
        value_list common;
        common.reserve(std::max(a.size(), b.size()));
        boost::set_intersection(a, b, back_inserter(common), type->as_less_comparator());
        return common;
    }

    value_set operator()(const interval<managed_bytes>& a, const value_list& b) const {
        auto common = b | std::views::filter([&] (const managed_bytes& el) { return a.contains(el, type->as_tri_comparator()); });
        return common | std::ranges::to<value_list>();
    }

    value_set operator()(const value_list& a, const interval<managed_bytes>& b) const {
        return (*this)(b, a);
    }

    value_set operator()(const interval<managed_bytes>& a, const interval<managed_bytes>& b) const {
        const auto common_range = a.intersection(b, type->as_tri_comparator());
        return common_range ? *common_range : empty_value_set;
    }
};

value_set intersection(value_set a, value_set b, const abstract_type* type) {
    return std::visit(intersection_visitor{type}, std::move(a), std::move(b));
}

static
managed_bytes
value_set_to_singleton(const value_set& vs) {
    if (std::holds_alternative<value_list>(vs)) {
        const auto& vl = std::get<value_list>(vs);
        if (vl.size() == 1) {
            return vl.front();
        }
    }
    throw std::logic_error("value_set_to_singleton: value_set is not a singleton");
}

template<std::ranges::forward_range Range>
value_list to_sorted_vector(Range r, const serialized_compare& comparator) {
    value_list tmp(r.begin(), r.end()); // Need random-access range to sort (r is not necessarily random-access).
    std::ranges::sort(tmp, comparator);
    auto last = std::unique(tmp.begin(), tmp.end());
    tmp.resize(last - tmp.begin());
    return tmp;
}

const auto non_null = std::views::filter([] (const managed_bytes_opt& b) { return b.has_value(); });

const auto deref = std::views::transform([] (const managed_bytes_opt& b) { return b.value(); });

/// Returns possible values from t, which must be RHS of IN.
value_list get_IN_values(
        const expression& e, const query_options& options, const serialized_compare& comparator,
        std::string_view column_name) {
    const cql3::raw_value in_list = evaluate(e, options);
    if (in_list.is_null()) {
        return value_list();
    }
    utils::chunked_vector<managed_bytes_opt> list_elems = get_list_elements(in_list);
    return to_sorted_vector(std::move(list_elems) | non_null | deref, comparator);
}

static constexpr bool inclusive = true, exclusive = false;

} // anonymous namespace

template<typename T>
interval<std::remove_cvref_t<T>> to_range(oper_t op, T&& val) {
    using U = std::remove_cvref_t<T>;
    static constexpr bool inclusive = true, exclusive = false;
    switch (op) {
    case oper_t::EQ:
        return interval<U>::make_singular(std::forward<T>(val));
    case oper_t::GT:
        return interval<U>::make_starting_with(interval_bound(std::forward<T>(val), exclusive));
    case oper_t::GTE:
        return interval<U>::make_starting_with(interval_bound(std::forward<T>(val), inclusive));
    case oper_t::LT:
        return interval<U>::make_ending_with(interval_bound(std::forward<T>(val), exclusive));
    case oper_t::LTE:
        return interval<U>::make_ending_with(interval_bound(std::forward<T>(val), inclusive));
    default:
        throw std::logic_error(format("to_range: unknown comparison operator {}", op));
    }
}

interval<clustering_key_prefix> to_range(oper_t op, const clustering_key_prefix& val) {
    return to_range<const clustering_key_prefix&>(op, val);
}

static
data_type
type(const predicate& p) {
    return std::visit(
        overloaded_functor{
            [] (const on_row&) { return boolean_type; }, // Not true, but the type won't be used.
            [] (const on_column& oc) { return oc.column->type->without_reversed().shared_from_this(); },
            [] (const on_partition_key_token&) { return long_type; },
            [] (const on_clustering_key_prefix&) -> data_type { on_internal_error(rlogger, "type: asked for clustering key prefix type"); },
        },
        p.on);
}

static
predicate
make_conjunction(predicate a, predicate b) {
    if (a.on != b.on) {
        on_internal_error(rlogger, "make_conjunction: merging predicate targets");
    }

    if (!a.comparable && !b.comparable) {
        on_internal_error(rlogger, "make_conjunction: merging non-comparable columns");
    }

    if (a.order != b.order) {
        on_internal_error(rlogger, "make_conjunction: merging predicates with different comparison orders");
    }

    auto& sa = a.solve_for;
    auto& sb = b.solve_for;

    auto sa_and_sb = std::invoke([&] -> solve_for_t {
        if (sa && sb) {
            return [sa = std::move(sa), sb = std::move(sb), type = type(a)] (const query_options& options) {
                return intersection(sa(options), sb(options), type.get());
            };
        } else {
            return {};
        }
    });

    return predicate{
        .solve_for = std::move(sa_and_sb),
        .filter = make_conjunction(std::move(a.filter), std::move(b.filter)),
        .on = a.on,
        .is_singleton = false,  // Even if both columns are singletons, the conjunction of them can return zero values.
        .comparable = a.comparable && b.comparable,  // Result is only comparable if both inputs follow CQL comparison semantics.
        .is_multi_column = a.is_multi_column,  // Both predicates are on the same target, so they agree on multi-column-ness.
        .is_not_null_single_column = false,  // A conjunction is not a pure IS NOT NULL check.
        .equality = false,        // A conjunction is not a single EQ.
        .is_in = false,           // A conjunction is not a single IN.
        .is_slice = false,        // A conjunction is not a single slice.
        .is_upper_bound = false,  // A conjunction has no single direction.
        .is_lower_bound = false,  // A conjunction has no single direction.
        .order = a.order,         // Both predicates are on the same column, so comparison order must agree.
        .op = std::nullopt,       // A conjunction has no single operator.
        .is_subscript = a.is_subscript,  // Both predicates are on the same target, so they agree on subscript-ness.
    };
}

static
const column_definition*
require_on_single_column(const predicate& p) {
    if (auto* pcol = std::get_if<on_column>(&p.on)) {
        return pcol->column;
    }
    on_internal_error(rlogger, "require_on_single_column: predicate is not on a single column");
}

static
bool
is_null_constant(const expression& e) {
    if (auto* c = as_if<constant>(&e)) {
        return c->value.is_null();
    }
    return false;
}

/// Given an expression, decompose it into a set of predicates, on individual columns,
/// the table's tokens, or multiple columns. A predicate may know how to solve for
/// the set of all column values that would satisfy the expression, treated a a boolean
/// predicate on the column. If it does, the .solve_for member is set.
///
/// An expression restricts possible values of a column or token:
/// - `A>5` restricts A from below
/// - `A>5 AND A>6 AND B<10 AND A=12 AND B>0` restricts A to 12 and B to between 0 and 10
/// - `A IN (1, 3, 5)` restricts A to 1, 3, or 5
/// - `A IN (1, 3, 5) AND A>3` restricts A to just 5
/// - `A=1 AND A<=0` restricts A to an empty list; no value is able to satisfy the expression
/// - `A>=NULL` also restricts A to an empty list; all comparisons to NULL are false
/// - an expression without A "restricts" A to unbounded range
//
// When cdef == nullptr it finds possible token values instead of column values.
// When finding token values the table_schema_opt argument has to point to a valid schema,
// but it isn't used when finding values for column.
// The schema is needed to find out whether a call to token() function represents
// the partition token.
static
std::vector<predicate>
to_predicates(
        const expression& expr,
        const schema* table_schema_opt) {
    static auto to_vector = [] (predicate p) -> std::vector<predicate> {
        return {std::move(p)};
    };
    static auto cannot_solve = [] (const expression& e) -> std::vector<predicate> {
        return to_vector(predicate{
                .solve_for = nullptr,
                .filter = e,
                .on = on_row{},
        });
    };
    static auto cannot_solve_on_column = [] (const expression& e, const column_definition* cdef) -> std::vector<predicate> {
        return to_vector(predicate{
                .solve_for = nullptr,
                .filter = e,
                .on = on_column{cdef},
        });
    };
    return expr::visit(overloaded_functor{
            [] (const constant& constant_val) -> std::vector<predicate> {
                std::optional<bool> bool_val = get_bool_value(constant_val);
                if (bool_val.has_value()) {
                    auto solve = *bool_val
                            ? solve_for_t([] (const query_options&) { return unbounded_value_set; })
                            : solve_for_t([] (const query_options&) { return empty_value_set; });
                    return to_vector(predicate{
                            .solve_for = std::move(solve),
                            .filter = constant_val,
                            .on = on_row{},
                    });
                }

                return to_vector(predicate{
                        .solve_for = [] (const query_options&) { return unbounded_value_set; },
                        .filter = constant_val,
                        .on = on_row{},
                    });
            },
            [&] (const conjunction& conj) -> std::vector<predicate> {
                std::vector<predicate> ret;
                for (auto& pa : conj.children) {
                    auto p = to_predicates(pa, table_schema_opt);
                    ret.insert(ret.end(), p.begin(), p.end());
                }
                return ret;
            },
            [&] (const binary_operator& oper) -> std::vector<predicate> {
                return expr::visit(overloaded_functor{
                        [&] (const column_value& col) -> std::vector<predicate> {
                            auto cdef = col.col;
                            auto type = &cdef->type->without_reversed();
                            if (oper.op == oper_t::IS_NOT) {
                                return to_vector(predicate{
                                    .solve_for = nullptr,
                                    .filter = oper,
                                    .on = on_column{col.col},
                                    .is_not_null_single_column = is_null_constant(oper.rhs),
                                    .op = oper.op,
                                });
                            }
                            if (is_compare(oper.op)) {
                                auto solve = [oper] (const query_options& options) {
                                    managed_bytes_opt val = evaluate(oper.rhs, options).to_managed_bytes_opt();
                                    if (!val) {
                                        return empty_value_set; // All NULL comparisons fail; no column values match.
                                    }
                                    return oper.op == oper_t::EQ ? value_set(value_list{*val})
                                    : to_range(oper.op, std::move(*val));
                                };
                                return to_vector(predicate{
                                    .solve_for = std::move(solve),
                                    .filter = oper,
                                    .on = on_column{col.col},
                                    .is_singleton = (oper.op == oper_t::EQ),
                                    .equality = (oper.op == oper_t::EQ),
                                    .is_slice = expr::is_slice(oper.op),
                                    .is_upper_bound = (oper.op == oper_t::LT || oper.op == oper_t::LTE),
                                    .is_lower_bound = (oper.op == oper_t::GT || oper.op == oper_t::GTE),
                                    .order = oper.order,
                                    .op = oper.op,
                                });
                            } else if (oper.op == oper_t::IN) {
                                auto solve = [oper, type, cdef] (const query_options& options) {
                                    return get_IN_values(oper.rhs, options, type->as_less_comparator(), cdef->name_as_text());
                                };
                                return to_vector(predicate{
                                    .solve_for = std::move(solve),
                                    .filter = oper,
                                    .on = on_column{col.col},
                                    .is_singleton = false,
                                    .is_in = true,
                                    .order = oper.order,
                                    .op = oper.op,
                                });
                            } else if (oper.op == oper_t::CONTAINS || oper.op == oper_t::CONTAINS_KEY) {
                                auto solve = [oper] (const query_options& options) {
                                    managed_bytes_opt val = evaluate(oper.rhs, options).to_managed_bytes_opt();
                                    if (!val) {
                                        return empty_value_set; // All NULL comparisons fail; no column values match.
                                    }
                                    return value_set(value_list{*val});
                                };
                                return to_vector(predicate{
                                    .solve_for = std::move(solve),
                                    .filter = oper,
                                    .on = on_column{col.col},
                                    .is_singleton = false,
                                    .order = oper.order,
                                    .op = oper.op,
                                });
                            }
                            return cannot_solve_on_column(oper, col.col);
                        },
                        [&] (const subscript& s) -> std::vector<predicate> {
                            const column_value& col = get_subscripted_column(s);

                            if (oper.op == oper_t::EQ) {
                                auto solve = [s, oper] (const query_options& options) {
                                    managed_bytes_opt sval = evaluate(s.sub, options).to_managed_bytes_opt();
                                    if (!sval) {
                                        return empty_value_set; // NULL can't be a map key
                                    }

                                    managed_bytes_opt rval = evaluate(oper.rhs, options).to_managed_bytes_opt();
                                    if (!rval) {
                                        return empty_value_set; // All NULL comparisons fail; no column values match.
                                    }
                                    managed_bytes_opt elements[] = {sval, rval};
                                    managed_bytes val = tuple_type_impl::build_value_fragmented(elements);
                                    return value_set(value_list{val});
                                };
                                return to_vector(predicate{
                                    .solve_for = std::move(solve),
                                    .filter = oper,
                                    .on = on_column{col.col},
                                    .is_singleton = true,
                                    .equality = true,
                                    .order = oper.order,
                                    .op = oper.op,
                                    .is_subscript = true,
                                });
                            }
                            return cannot_solve_on_column(oper, col.col);
                        },
                        [&] (const tuple_constructor& tuple) -> std::vector<predicate> {
                            auto columns = tuple.elements
                            | std::views::transform([] (const expression& e) { return as<column_value>(e).col; })
                            | std::ranges::to<std::vector>();
                            for (unsigned i = 0; i < columns.size(); ++i) {
                                if (!columns[i]->is_clustering_key() || columns[i]->position() != i) {
                                    on_internal_error(rlogger, "to_predicates: multi-column relation not on a clustering key prefix");
                                }
                            }
                            // The solve_for lambda is only correct for EQ; other operators
                            // (IN, slices) are handled directly by
                            // build_get_multi_column_clustering_bounds_fn() which bypasses
                            // solve_for and evaluates the binary_operator's RHS itself.
                            solve_for_t solve = nullptr;
                            if (oper.op == oper_t::EQ) {
                                solve = [oper] (const query_options& options) {
                                    managed_bytes_opt val = evaluate(oper.rhs, options).to_managed_bytes_opt();
                                    if (!val) {
                                        return empty_value_set; // All NULL comparisons fail; no column values match.
                                    }
                                    return value_set(value_list{*val});
                                };
                            }
                            return to_vector(predicate{
                                .solve_for = std::move(solve),
                                .filter = oper,
                                .on = on_clustering_key_prefix{std::move(columns)},
                                .is_singleton = oper.op == oper_t::EQ,
                                .is_multi_column = true,
                                .equality = (oper.op == oper_t::EQ),
                                .is_in = (oper.op == oper_t::IN),
                                .is_slice = expr::is_slice(oper.op),
                                .is_upper_bound = (oper.op == oper_t::LT || oper.op == oper_t::LTE),
                                .is_lower_bound = (oper.op == oper_t::GT || oper.op == oper_t::GTE),
                                .order = oper.order,
                                .op = oper.op,
                            });
                        },
                        [&] (const function_call& token_fun_call) -> std::vector<predicate> {
                            if (!is_partition_token_for_schema(token_fun_call, *table_schema_opt)) {
                                return cannot_solve(oper);
                            }

                            if (!(oper.op == oper_t::EQ || is_slice(oper.op))) {
                                return cannot_solve(oper);
                            }
                            auto solve = [oper] (const query_options& options) -> value_set {
                                auto val = evaluate(oper.rhs, options).to_managed_bytes_opt();
                                if (!val) {
                                    return empty_value_set; // All NULL comparisons fail; no token values match.
                                }
                                if (oper.op == oper_t::EQ) {
                                    return value_list{*val};
                                } else if (oper.op == oper_t::GT) {
                                    return interval<managed_bytes>::make_starting_with(interval_bound(std::move(*val), exclusive));
                                } else if (oper.op == oper_t::GTE) {
                                    return interval<managed_bytes>::make_starting_with(interval_bound(std::move(*val), inclusive));
                                }
                                static const managed_bytes MININT = managed_bytes(serialized(std::numeric_limits<int64_t>::min())),
                                MAXINT = managed_bytes(serialized(std::numeric_limits<int64_t>::max()));
                                // Undocumented feature: when the user types `token(...) < MININT`, we interpret
                                // that as MAXINT for some reason.
                                const auto adjusted_val = (*val == MININT) ? MAXINT : *val;
                                if (oper.op == oper_t::LT) {
                                    return interval<managed_bytes>::make_ending_with(interval_bound(std::move(adjusted_val), exclusive));
                                } else if (oper.op == oper_t::LTE) {
                                    return interval<managed_bytes>::make_ending_with(interval_bound(std::move(adjusted_val), inclusive));
                                }
                                throw std::logic_error(format("get_token_interval unexpected operator {}", oper.op));
                            };
                            return to_vector(predicate{
                                .solve_for = std::move(solve),
                                .filter = oper,
                                .on = on_partition_key_token{table_schema_opt},
                                .is_singleton = (oper.op == oper_t::EQ),
                                .equality = (oper.op == oper_t::EQ),
                                .is_slice = expr::is_slice(oper.op),
                                .is_upper_bound = (oper.op == oper_t::LT || oper.op == oper_t::LTE),
                                .is_lower_bound = (oper.op == oper_t::GT || oper.op == oper_t::GTE),
                                .order = oper.order,
                                .op = oper.op,
                            });
                        },
                        [&] (const binary_operator&) -> std::vector<predicate> {
                            return cannot_solve(oper);
                        },
                        [&] (const conjunction&) -> std::vector<predicate> {
                            return cannot_solve(oper);
                        },
                        [&] (const constant&) -> std::vector<predicate> {
                            return cannot_solve(oper);
                        },
                        [&] (const unresolved_identifier&) -> std::vector<predicate> {
                            return cannot_solve(oper);
                        },
                        [&] (const column_mutation_attribute&) -> std::vector<predicate> {
                            return cannot_solve(oper);
                        },
                        [&] (const cast&) -> std::vector<predicate> {
                            return cannot_solve(oper);
                        },
                        [&] (const field_selection&) -> std::vector<predicate> {
                            return cannot_solve(oper);
                        },
                        [&] (const bind_variable&) -> std::vector<predicate> {
                            return cannot_solve(oper);
                        },
                        [&] (const untyped_constant&) -> std::vector<predicate> {
                            return cannot_solve(oper);
                        },
                        [&] (const collection_constructor&) -> std::vector<predicate> {
                            return cannot_solve(oper);
                        },
                        [&] (const usertype_constructor&) -> std::vector<predicate> {
                            return cannot_solve(oper);
                        },
                        [&] (const temporary&) -> std::vector<predicate> {
                            return cannot_solve(oper);
                        },
                    }, oper.lhs);
                },
            [] (const column_value& cv) -> std::vector<predicate> {
                return cannot_solve(cv);
            },
            [] (const subscript& s) -> std::vector<predicate> {
                return cannot_solve(s);
            },
            [] (const unresolved_identifier& ui) -> std::vector<predicate> {
                return cannot_solve(ui);
            },
            [] (const column_mutation_attribute& cma) -> std::vector<predicate> {
                return cannot_solve(cma);
            },
            [] (const function_call& fc) -> std::vector<predicate> {
                return cannot_solve(fc);
            },
            [] (const cast& c) -> std::vector<predicate> {
                return cannot_solve(c);
            },
            [] (const field_selection& fs) -> std::vector<predicate> {
                return cannot_solve(fs);
            },
            [] (const bind_variable& bv) -> std::vector<predicate> {
                return cannot_solve(bv);
            },
            [] (const untyped_constant& uc) -> std::vector<predicate> {
                return cannot_solve(uc);
            },
            [] (const tuple_constructor& tc) -> std::vector<predicate> {
                return cannot_solve(tc);
            },
            [] (const collection_constructor& cc) -> std::vector<predicate> {
                return cannot_solve(cc);
            },
            [] (const usertype_constructor& uc) -> std::vector<predicate> {
                return cannot_solve(uc);
            },
            [] (const temporary& t) -> std::vector<predicate> {
                return cannot_solve(t);
            },
        }, expr);
}

// Convert an expression to a predicate on a column. If cdef is nullptr, the predicate
// is on the partition key token.
static
predicate
to_predicate_on_column(
        const expression& expr,
        const column_definition* cdef,
        const schema* table_schema_opt) {
    auto predicates = to_predicates(expr, table_schema_opt);
    using on_t = std::variant<
        on_row,                        // cannot determine, so predicate is on entire row
        on_column,                     // solving for a single column: e.g. c1 = 3
        on_partition_key_token,        // solving for the token, e.g. token(pk1, pk2) >= :var
        on_clustering_key_prefix       // solving for a clustering key prefix: e.g. (ck1, ck2) >= (3, 4)
    >;
    auto target = cdef ? on_t(on_column{cdef}) : on_t(on_partition_key_token{table_schema_opt});
    auto collected = std::vector<predicate>{};
    for (auto& predicate : predicates) {
        if (predicate.on == target) {
            collected.push_back(std::move(predicate));
            continue;
        }
    }
    if (collected.empty()) {
        on_internal_error(rlogger, "to_predicate_on_column: no predicates found");
    }
    auto ret = std::ranges::fold_left_first(
        collected | std::views::as_rvalue,
        make_conjunction
    );
    if (!ret) {
        on_internal_error(rlogger, "to_predicate_on_column: no predicates found");
    }
    return std::move(*ret);
}

interval<managed_bytes> to_range(const value_set& s) {
    return std::visit(overloaded_functor{
            [] (const interval<managed_bytes>& r) { return r; },
            [] (const value_list& lst) {
                if (lst.size() != 1) {
                    throw std::logic_error(format("to_range called on list of size {}", lst.size()));
                }
                return interval<managed_bytes>::make_singular(lst[0]);
            },
        }, s);
}

/// Replaces every column_definition in an expression with this one.  Throws if any LHS is not a single
/// column_value.
static
predicate
replace_column_def(predicate p, const column_definition* col) {
    // Note: does not replace and `col` embedded in the p.solve_for
    p.filter = expr::replace_column_def(p.filter, col);
    p.on = on_column{col};
    return p;
}

namespace {
constexpr inline secondary_index::index::supports_expression_v operator&&(secondary_index::index::supports_expression_v v1, secondary_index::index::supports_expression_v v2) {
    using namespace secondary_index;
    auto True = index::supports_expression_v::from_bool(true);
    return v1 == True && v2 == True ? True : index::supports_expression_v::from_bool(false);
}

}

// Like is_supported_by_helper, but operates on a single predicate instead of walking
// an expression tree.  Returns how an index supports this predicate: UsualYes, CollectionYes, or No.
static secondary_index::index::supports_expression_v
is_predicate_supported_by(const predicate& pred, const secondary_index::index& idx) {
    using ret_t = secondary_index::index::supports_expression_v;
    if (!pred.op) {
        return ret_t::from_bool(false);
    }
    return std::visit(overloaded_functor{
        [&] (const on_column& oc) -> ret_t {
            if (pred.is_subscript) {
                return idx.supports_subscript_expression(*oc.column, *pred.op);
            }
            return idx.supports_expression(*oc.column, *pred.op);
        },
        [&] (const on_clustering_key_prefix& ocp) -> ret_t {
            // Single-element tuple_constructor: treat like a single column
            if (ocp.columns.size() == 1) {
                return idx.supports_expression(*ocp.columns[0], *pred.op);
            }
            // Multi-element tuple: index cannot avoid filtering
            return ret_t::from_bool(false);
        },
        [&] (const on_partition_key_token&) -> ret_t {
            return ret_t::from_bool(false);
        },
        [&] (const on_row&) -> ret_t {
            return ret_t::from_bool(false);
        },
    }, pred.on);
}

struct index_search_group {
    const single_column_predicate_vectors& pred_vectors;
    const expr::expression& restriction_expr;
};

// Like index_supports_some_column, but operates on per-column predicate vectors
// instead of walking per-column expression trees.
static bool index_supports_some_column(
        const single_column_predicate_vectors& per_column_predicates,
        const secondary_index::secondary_index_manager& index_manager,
        allow_local_index allow_local) {
    using namespace secondary_index;
    for (auto& [col, preds] : per_column_predicates) {
        for (const auto& idx : index_manager.list_indexes()) {
            if (!allow_local && idx.metadata().local()) {
                continue;
            }
            if (preds.empty()) {
                continue;
            }
            // AND all predicate results for this column-index pair, mirroring the
            // conjunction logic in is_supported_by_helper.  Seed with the first
            // predicate's result instead of from_bool(true) (which is UsualYes)
            // so that CollectionYes values are preserved through the chain.
            auto result = is_predicate_supported_by(preds[0], idx);
            for (size_t i = 1; i < preds.size(); ++i) {
                result = result && is_predicate_supported_by(preds[i], idx);
            }
            if (result) {
                return true;
            }
        }
    }
    return false;
}

// Check if any index supports any multi-column clustering predicate.
// For each predicate in mc_preds, checks if any column in the predicate's
// columns list is supported by the index for that predicate's operator.
static bool multi_column_predicates_have_supporting_index(
        const std::vector<predicate>& mc_preds,
        const secondary_index::secondary_index_manager& index_manager,
        allow_local_index allow_local) {
    for (const auto& idx : index_manager.list_indexes()) {
        if (!allow_local && idx.metadata().local()) {
            continue;
        }
        for (const auto& pred : mc_preds) {
            if (!pred.op) {
                continue;
            }
            auto* ocp = std::get_if<on_clustering_key_prefix>(&pred.on);
            if (!ocp) {
                continue;
            }
            for (const auto* col : ocp->columns) {
                if (idx.supports_expression(*col, *pred.op)) {
                    return true;
                }
            }
        }
    }
    return false;
}

// Check if all predicates for a column are supported by an index.
// Mirrors the conjunction logic of is_supported_by_helper: initializes with
// the first predicate's result, then ANDs the rest.
static bool are_predicates_supported_by(const std::vector<predicate>& preds,
                                        const secondary_index::index& idx) {
    if (preds.empty()) {
        return true;
    }
    auto result = is_predicate_supported_by(preds[0], idx);
    for (size_t i = 1; i < preds.size(); ++i) {
        result = result && is_predicate_supported_by(preds[i], idx);
    }
    return bool(result);
}

static std::pair<std::optional<secondary_index::index>, expr::expression> do_find_idx(
        bool uses_secondary_indexing,
        const secondary_index::secondary_index_manager& sim,
        std::span<const index_search_group> search_groups,
        allow_local_index allow_local);


bool is_on_collection(const binary_operator& b) {
    if (b.op == oper_t::CONTAINS || b.op == oper_t::CONTAINS_KEY) {
        return true;
    }
    if (auto tuple = expr::as_if<tuple_constructor>(&b.lhs)) {
        return std::ranges::any_of(tuple->elements, [] (const expression& v) { return expr::is<subscript>(v); });
    }
    return false;
}

bool is_empty_restriction(const expression& e) {
    bool contains_non_conjunction = recurse_until(e, [&](const expression& e) -> bool {
        return !is<conjunction>(e);
    });

    return !contains_non_conjunction;
}

static
std::function<bytes_opt (const query_options&)>
build_value_for_fn(const column_definition& cdef, const expression& e, const schema& s) {
    auto ac = to_predicate_on_column(e, &cdef, &s);
    return [ac] (const query_options& options) -> bytes_opt {
        value_set possible_vals = solve(ac, options);
        return std::visit(overloaded_functor {
            [&](const value_list& val_list) -> bytes_opt {
                if (val_list.empty()) {
                    return std::nullopt;
                }

                if (val_list.size() != 1) {
                    on_internal_error(expr_logger, format("expr::value_for - multiple possible values for column: {}", ac.filter));
                }

                return to_bytes(val_list.front());
            },
            [&](const interval<managed_bytes>&) -> bytes_opt {
                on_internal_error(expr_logger, format("expr::value_for - possible values are a range: {}", ac.filter));
            }
        }, possible_vals);
    };
}

bool contains_multi_column_restriction(const expression& e) {
    const binary_operator* find_res = find_binop(e, [](const binary_operator& binop) {
        return is<tuple_constructor>(binop.lhs);
    });
    return find_res != nullptr;
}

bool has_only_eq_binops(const expression& e) {
    const expr::binary_operator* non_eq_binop = find_in_expression<expr::binary_operator>(e,
        [](const expr::binary_operator& binop) {
            return binop.op != expr::oper_t::EQ;
        }
    );

    return non_eq_binop == nullptr;
}

statement_restrictions::statement_restrictions(private_tag, schema_ptr schema, bool allow_filtering)
    : _schema(schema)
    , _partition_range_is_simple(true)
{ }

statement_restrictions::statement_restrictions(private_tag,
        data_dictionary::database db,
        schema_ptr schema,
        statements::statement_type type,
        const expr::expression& where_clause,
        prepare_context& ctx,
        bool selects_only_static_columns,
        bool for_view,
        bool allow_filtering,
        check_indexes do_check_indexes)
    : statement_restrictions(private_tag{}, schema, allow_filtering)
{
    _check_indexes = do_check_indexes;
    std::vector<binary_operator> prepared_where_clause;
    for (auto&& relation_expr : boolean_factors(where_clause)) {
        const expr::binary_operator* relation_binop = expr::as_if<expr::binary_operator>(&relation_expr);

        if (relation_binop == nullptr) {
            on_internal_error(rlogger, format("statement_restrictions: where clause has non-binop element: {}", relation_expr));
        }

        expr::binary_operator prepared_restriction = expr::validate_and_prepare_new_restriction(*relation_binop, db, schema, ctx);
        prepared_where_clause.push_back(std::move(prepared_restriction));
    }

    std::vector<predicate> predicates;
    for (auto& prepared_restriction : prepared_where_clause) {
        auto preds = to_predicates(prepared_restriction, _schema.get());
        predicates.insert(predicates.end(), std::make_move_iterator(preds.begin()), std::make_move_iterator(preds.end()));
    }

    bool ck_is_empty = true;
    bool has_mc_clustering = false;
    bool ck_has_slice = false;
    const column_definition* ck_last_column = nullptr;
    const predicate* first_mc_pred = nullptr;
    bool pk_is_empty = true;
    bool has_token = false;
    std::optional<predicate> token_pred;
    std::unordered_map<const column_definition*, predicate> pk_range_preds;
    std::vector<predicate> mc_ck_preds;
    std::unordered_map<const column_definition*, predicate> sc_ck_preds;
    single_column_predicate_vectors sc_pk_pred_vectors;
    single_column_predicate_vectors sc_ck_pred_vectors;
    single_column_predicate_vectors sc_nonpk_pred_vectors;
    for (auto& pred : predicates) {
        if (pred.is_not_null_single_column) {
            auto* col = require_on_single_column(pred);
            _not_null_columns.insert(col);

            if (!for_view) {
                throw exceptions::invalid_request_exception(format("restriction '{}' is only supported in materialized view creation", pred.filter));
            }
        } else if (pred.is_multi_column) {
            // Multi column restrictions are only allowed on clustering columns
            if (ck_is_empty) {
                _clustering_columns_restrictions = pred.filter;
                ck_is_empty = false;
                has_mc_clustering = true;
                first_mc_pred = &pred;
                mc_ck_preds.push_back(pred);
                if (pred.is_slice) {
                    ck_has_slice = true;
                }
            } else {

                if (!has_mc_clustering) {
                    throw exceptions::invalid_request_exception("Mixing single column relations and multi column relations on clustering columns is not allowed");
                }

                if (pred.equality) {
                    throw exceptions::invalid_request_exception(format("{} cannot be restricted by more than one relation if it includes an Equal",
                        expr::get_columns_in_commons(_clustering_columns_restrictions, pred.filter)));
                } else if (pred.is_in) {
                    throw exceptions::invalid_request_exception(format("{} cannot be restricted by more than one relation if it includes a IN",
                                                                    expr::get_columns_in_commons(_clustering_columns_restrictions, pred.filter)));
                } else if (pred.is_slice) {
                    if (!ck_has_slice) {
                        throw exceptions::invalid_request_exception(format("Column \"{}\" cannot be restricted by both an equality and an inequality relation",
                                                                    expr::get_columns_in_commons(_clustering_columns_restrictions, pred.filter)));
                    }

                    // Don't allow to mix plain and SCYLLA_CLUSTERING_BOUND bounds
                    if (first_mc_pred->order != pred.order) {
                        static auto order2str = [](auto o) { return o == expr::comparison_order::cql ? "plain" : "SCYLLA_CLUSTERING_BOUND"; };
                        throw exceptions::invalid_request_exception(
                                format("Invalid combination of restrictions ({} / {})",
                                order2str(first_mc_pred->order), order2str(pred.order)));
                    }

                    // Here check that there aren't two < <= or two > and >=
                    if (pred.is_lower_bound && first_mc_pred->is_lower_bound) {
                        throw exceptions::invalid_request_exception(format(
                        "More than one restriction was found for the start bound on {}",
                            expr::get_columns_in_commons(pred.filter, first_mc_pred->filter)));
                    }

                    if (pred.is_upper_bound && first_mc_pred->is_upper_bound) {
                        throw exceptions::invalid_request_exception(format(
                            "More than one restriction was found for the end bound on {}",
                            expr::get_columns_in_commons(pred.filter, first_mc_pred->filter)));
                    }

                    _clustering_columns_restrictions = expr::make_conjunction(_clustering_columns_restrictions, pred.filter);
                    mc_ck_preds.push_back(pred);
                    ck_has_slice = true;
                } else {
                    throw exceptions::invalid_request_exception(format("Unsupported multi-column relation: ", pred.filter));
                }
            }
        } else if (std::holds_alternative<on_partition_key_token>(pred.on)) {
            // Token always restricts the partition key
            if (!pk_is_empty && !has_token) {
                throw exceptions::invalid_request_exception(
                        seastar::format("Columns \"{}\" cannot be restricted by both a normal relation and a token relation",
                                fmt::join(expr::get_sorted_column_defs(_partition_key_restrictions) |
                                        std::views::transform([](auto* p) {
                                            return maybe_column_definition{p};
                                        }),
                                        ", ")));
            }

            _partition_key_restrictions = expr::make_conjunction(_partition_key_restrictions, pred.filter);
            pk_is_empty = false;
            has_token = true;
            if (token_pred) {
                token_pred = make_conjunction(std::move(*token_pred), pred);
            } else {
                token_pred = pred;
            }
        } else if (std::holds_alternative<on_column>(pred.on)) {
            const column_definition* def = std::get<on_column>(pred.on).column;
            if (def->is_partition_key()) {
                // View definition allows PK slices, because it's not a performance problem.
                if (!pred.equality && !pred.is_in && !allow_filtering && !for_view) {
                    throw exceptions::invalid_request_exception(
                            "Only EQ and IN relation are supported on the partition key "
                            "(unless you use the token() function or ALLOW FILTERING)");
                }
                if (has_token) {
                    throw exceptions::invalid_request_exception(
                            seastar::format("Columns \"{}\" cannot be restricted by both a normal relation and a token relation",
                                fmt::join(expr::get_sorted_column_defs(_partition_key_restrictions) |
                                            std::views::transform([](auto* p) {
                                            return maybe_column_definition{p};
                                            }),
                                            ", ")));
                }

                _partition_key_restrictions = expr::make_conjunction(_partition_key_restrictions, pred.filter);
                pk_is_empty = false;
                {
                    auto [it, inserted] = _single_column_partition_key_restrictions.try_emplace(def, expr::conjunction{});
                    it->second = expr::make_conjunction(std::move(it->second), pred.filter);
                }
                sc_pk_pred_vectors[def].push_back(pred);
                if (pred.equality || pred.is_in) {
                    auto [it, inserted] = pk_range_preds.try_emplace(def, pred);
                    if (!inserted) {
                        it->second = make_conjunction(std::move(it->second), pred);
                    }
                }
                _partition_range_is_simple &= !pred.is_in;
            } else if (def->is_clustering_key()) {
                if (has_mc_clustering) {
                    throw exceptions::invalid_request_exception(
                        "Mixing single column relations and multi column relations on clustering columns is not allowed");
                }

                const column_definition* new_column = std::get<on_column>(pred.on).column;
                const column_definition* last_column = ck_last_column;

                if (last_column != nullptr && !allow_filtering) {
                    if (ck_has_slice && schema->position(*new_column) > schema->position(*last_column)) {
                        throw exceptions::invalid_request_exception(format("Clustering column \"{}\" cannot be restricted (preceding column \"{}\" is restricted by a non-EQ relation)",
                            new_column->name_as_text(), last_column->name_as_text()));
                    }

                    if (schema->position(*new_column) < schema->position(*last_column)) {
                        if (pred.is_slice) {
                            throw exceptions::invalid_request_exception(format("PRIMARY KEY column \"{}\" cannot be restricted (preceding column \"{}\" is restricted by a non-EQ relation)",
                                last_column->name_as_text(), new_column->name_as_text()));
                        }
                    }
                }

                _clustering_columns_restrictions = expr::make_conjunction(_clustering_columns_restrictions, pred.filter);
                ck_is_empty = false;
                {
                    auto [it, inserted] = _single_column_clustering_key_restrictions.try_emplace(def, expr::conjunction{});
                    it->second = expr::make_conjunction(std::move(it->second), pred.filter);
                }
                sc_ck_pred_vectors[def].push_back(pred);
                {
                    auto [it, inserted] = sc_ck_preds.try_emplace(def, pred);
                    if (!inserted) {
                        it->second = make_conjunction(std::move(it->second), pred);
                    }
                }
                if (pred.is_slice) {
                    ck_has_slice = true;
                }
                if (ck_last_column == nullptr || schema->position(*new_column) > schema->position(*ck_last_column)) {
                    ck_last_column = new_column;
                }
            } else {
                _nonprimary_key_restrictions = expr::make_conjunction(_nonprimary_key_restrictions, pred.filter);
                {
                    auto [it, inserted] = _single_column_nonprimary_key_restrictions.try_emplace(def, expr::conjunction{});
                    it->second = expr::make_conjunction(std::move(it->second), pred.filter);
                }
                sc_nonpk_pred_vectors[def].push_back(pred);
            }
        } else {
            throw exceptions::invalid_request_exception(format("Unhandled restriction: {}", pred.filter));
        }

        if (!pred.is_not_null_single_column) {
            _where.push_back(pred.filter);
        }
        // Subscript EQ (e.g. m[1] = 'a') is not considered an EQ on the column
        // itself, matching the behavior of the old expression-walking code which
        // only recognized column_value and tuple_constructor in the LHS.
        if (pred.equality && !pred.is_subscript) {
            if (auto* sc = std::get_if<on_column>(&pred.on)) {
                _columns_with_eq.insert(sc->column);
            } else if (auto* mc = std::get_if<on_clustering_key_prefix>(&pred.on)) {
                _columns_with_eq.insert(mc->columns.begin(), mc->columns.end());
            }
        }
    }
    if (!_where.empty()) {
        if (!mc_ck_preds.empty()) {
            _clustering_prefix_restrictions = std::move(mc_ck_preds);
        } else {
            std::vector<predicate> prefix;
            for (const auto& col : _schema->clustering_key_columns()) {
                const auto found = sc_ck_preds.find(&col);
                if (found == sc_ck_preds.end()) {
                    break;
                }
                if (find_needs_filtering(found->second.filter)) {
                    break;
                }
                prefix.push_back(found->second);
                if (has_slice(found->second.filter)) {
                    break;
                }
            }
            _clustering_prefix_restrictions = std::move(prefix);
        }
        if (token_pred) {
            _partition_range_restrictions = token_range_restrictions{
                .token_restrictions = std::move(*token_pred),
            };
        } else if (pk_range_preds.size() == _schema->partition_key_size()) {
            _partition_range_restrictions = single_column_partition_range_restrictions{
                .per_column_restrictions = std::move(pk_range_preds) | std::views::values | std::ranges::to<std::vector>(),
            };
        }
    }
    _has_multi_column = has_mc_clustering;
    if (_check_indexes) {
        auto cf = db.find_column_family(schema);
        auto& sim = cf.get_index_manager();
        const expr::allow_local_index allow_local(
                !has_partition_key_unrestricted_components()
                && partition_key_restrictions_is_all_eq());
        if (!_has_multi_column) {
            _has_queriable_ck_index = index_supports_some_column(sc_ck_pred_vectors, sim, allow_local)
                    && !type.is_delete();
        } else {
            _has_queriable_ck_index = multi_column_predicates_have_supporting_index(mc_ck_preds, sim, allow_local)
                    && !type.is_delete();
        }
        _has_queriable_pk_index = !has_token
                && index_supports_some_column(sc_pk_pred_vectors, sim, allow_local)
                && !type.is_delete();
        _has_queriable_regular_index = index_supports_some_column(sc_nonpk_pred_vectors, sim, allow_local)
                && !type.is_delete();
    } else {
        _has_queriable_ck_index = false;
        _has_queriable_pk_index = false;
        _has_queriable_regular_index = false;
    }

    // At this point, the select statement if fully constructed, but we still have a few things to validate
    process_partition_key_restrictions(for_view, allow_filtering, type);

    // Some but not all of the partition key columns have been specified;
    // hence we need turn these restrictions into index expressions.
    std::vector<index_search_group> search_groups;
    if (_uses_secondary_indexing || pk_restrictions_need_filtering()) {
        _index_restrictions.push_back(_partition_key_restrictions);
        search_groups.push_back({sc_pk_pred_vectors, _partition_key_restrictions});
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
        search_groups.push_back({sc_ck_pred_vectors, _clustering_columns_restrictions});
    } else if (find_binop(_clustering_columns_restrictions, is_on_collection)) {
        fail(unimplemented::cause::INDEXES);
    }

    if (!is_empty_restriction(_nonprimary_key_restrictions)) {
        if (_has_queriable_regular_index && _partition_range_is_simple) {
            _uses_secondary_indexing = true;
        } else if (!allow_filtering && !type.is_delete() && !type.is_update()) {
            throw exceptions::invalid_request_exception("Cannot execute this query as it might involve data filtering and "
                "thus may have unpredictable performance. If you want to execute "
                "this query despite the performance unpredictability, use ALLOW FILTERING");
        }
        _index_restrictions.push_back(_nonprimary_key_restrictions);
        search_groups.push_back({sc_nonpk_pred_vectors, _nonprimary_key_restrictions});
    }

    if (_uses_secondary_indexing && !(for_view || allow_filtering)) {
        validate_secondary_index_selections(selects_only_static_columns);
    }

    if (_check_indexes) {
        auto cf = db.find_column_family(_schema);
        auto& sim = cf.get_index_manager();
        const expr::allow_local_index allow_local_for_idx(
                !has_partition_key_unrestricted_components()
                && partition_key_restrictions_is_all_eq());
        std::tie(_idx_opt, _idx_restrictions) = do_find_idx(
                _uses_secondary_indexing, sim, search_groups, allow_local_for_idx);
    }

    calculate_column_defs_for_filtering_and_erase_restrictions_used_for_index(db, sc_pk_pred_vectors, sc_ck_pred_vectors, sc_nonpk_pred_vectors);

    if (pk_restrictions_need_filtering()) {
        auto partition_key_filter = expr::conjunction{
            .children = _single_column_partition_key_restrictions
                    | std::ranges::views::values
                    | std::ranges::to<std::vector>(),
        };
        _partition_level_filter = expr::make_conjunction(std::move(_partition_level_filter), std::move(partition_key_filter));
    }

    if (ck_restrictions_need_filtering()) {
        auto clustering_key_filter = expr::conjunction{
            .children = _single_column_clustering_key_restrictions
                    | std::ranges::views::values
                    | std::ranges::to<std::vector>(),
        };
        _clustering_row_level_filter = expr::make_conjunction(std::move(_clustering_row_level_filter), std::move(clustering_key_filter));
    }

    auto check_column_kind = [] (column_kind kind, const expr::single_column_restrictions_map::value_type& v) -> bool {
        return v.first->kind == kind;
    };

    auto make_column_kind_checker = [&] (column_kind kind) {
        return std::bind_front(check_column_kind, kind);
    };

    auto static_columns_filter = expr::conjunction{
        .children = _single_column_nonprimary_key_restrictions
                | std::ranges::views::filter(make_column_kind_checker(column_kind::static_column))
                | std::ranges::views::values
                | std::ranges::to<std::vector>(),
    };

    _partition_level_filter = expr::make_conjunction(std::move(_partition_level_filter), std::move(static_columns_filter));

    auto regular_columns_filter = expr::conjunction{
        .children = _single_column_nonprimary_key_restrictions
                | std::ranges::views::filter(make_column_kind_checker(column_kind::regular_column))
                | std::ranges::views::values
                | std::ranges::to<std::vector>(),
    };

    _clustering_row_level_filter = expr::make_conjunction(std::move(_clustering_row_level_filter), std::move(regular_columns_filter));

    auto multi_column_restrictions = expr::conjunction{
        .children = expr::boolean_factors(_clustering_columns_restrictions)
                | std::ranges::views::filter(contains_multi_column_restriction)
                | std::ranges::to<std::vector>()
    };

    _clustering_row_level_filter = expr::make_conjunction(std::move(_clustering_row_level_filter), std::move(multi_column_restrictions));

    if (uses_secondary_indexing()) {
        auto& index_opt = _idx_opt;
        if (!index_opt) {
            throw std::runtime_error("No index found.");
        }

        const auto& im = index_opt->metadata();
        if (db::schema_tables::view_should_exist(im)) {
            sstring index_table_name = im.name() + "_index";
            schema_ptr view_schema = db.find_schema(schema->ks_name(), index_table_name);
            _view_schema = view_schema;

            if (im.local()) {
                prepare_indexed_local(*view_schema, sc_pk_pred_vectors, sc_ck_pred_vectors, sc_nonpk_pred_vectors);
            } else {
                prepare_indexed_global(*view_schema);
            }
        }
    }

    _get_partition_key_ranges_fn = build_partition_key_ranges_fn();

    _get_clustering_bounds_fn = build_get_clustering_bounds_fn();
    _get_global_index_clustering_ranges_fn = build_get_global_index_clustering_ranges_fn();
    _get_global_index_token_clustering_ranges_fn = build_get_global_index_token_clustering_ranges_fn();
    _get_local_index_clustering_ranges_fn = build_get_local_index_clustering_ranges_fn();
    _value_for_index_partition_key_fn = build_value_for_index_partition_key_fn();
}

bool
statement_restrictions::clustering_key_restrictions_has_IN() const {
    return find(_clustering_columns_restrictions, expr::oper_t::IN);
}

bool
statement_restrictions::clustering_key_restrictions_has_only_eq() const {
    return has_only_eq_binops(_clustering_columns_restrictions);
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
    return !is_empty_restriction(_clustering_columns_restrictions);
}

bool
statement_restrictions::has_non_primary_key_restriction() const {
    return !is_empty_restriction(_nonprimary_key_restrictions);
}

bool
statement_restrictions::ck_restrictions_need_filtering() const {
    if (is_empty_restriction(_clustering_columns_restrictions)) {
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

bool statement_restrictions::is_empty() const {
    return _where.empty();
}


static std::pair<std::optional<secondary_index::index>, expr::expression> do_find_idx(
        bool uses_secondary_indexing,
        const secondary_index::secondary_index_manager& sim,
        std::span<const index_search_group> search_groups,
        allow_local_index allow_local) {
    if (!uses_secondary_indexing) {
        return {std::nullopt, expr::conjunction({})};
    }

    // Current score table:
    // local and restrictions include full partition key: 2
    // global: 1
    // local and restrictions does not include full partition key: 0 (do not pick)
    auto index_score = [&] (const secondary_index::index& index) -> int {
        if (index.metadata().local()) {
            return allow_local ? 2 : 0;
        }
        return 1;
    };

    std::optional<secondary_index::index> chosen_index;
    int chosen_index_score = 0;
    expr::expression chosen_index_restrictions = expr::conjunction({});

    // Several indexes may be usable for this query. When their score is tied,
    // let's pick one by order of the columns mentioned in the restriction
    // expression. This specific order isn't important (and maybe in the
    // future we could plan a better order based on the specificity of each
    // index), but it is critical that two coordinators - or the same
    // coordinator over time - must choose the same index for the same query.
    // Otherwise, paging can break (see issue #7969).
    for (const auto& group : search_groups) {
        // Iterate columns in WHERE-clause order (from the restriction expression)
        // rather than schema-position order (from the pred_vectors map).  When
        // scores are tied the first column visited wins (strict >), so the
        // iteration order determines which index is chosen for equal-score
        // candidates -- matching the old expression-based do_find_idx behaviour.
        expr::for_each_expression<expr::column_value>(group.restriction_expr, [&](const expr::column_value& cval) {
            auto it = group.pred_vectors.find(cval.col);
            if (it == group.pred_vectors.end()) {
                return;
            }
            const auto& [col, preds] = *it;
            for (const auto& index : sim.list_indexes()) {
                if (col->name_as_text() == index.target_column() &&
                        are_predicates_supported_by(preds, index) &&
                        index_score(index) > chosen_index_score) {
                    chosen_index = index;
                    chosen_index_score = index_score(index);
                    chosen_index_restrictions = group.restriction_expr;
                }
            }
        });
    }
    return {chosen_index, chosen_index_restrictions};
}

std::pair<std::optional<secondary_index::index>, expr::expression>
statement_restrictions::find_idx(const secondary_index::secondary_index_manager& sim) const {
    return {_idx_opt, _idx_restrictions};
}

bool statement_restrictions::has_eq_restriction_on_column(const column_definition& column) const {
    return _columns_with_eq.contains(&column);
}

std::vector<const column_definition*> statement_restrictions::get_column_defs_for_filtering(data_dictionary::database db) const {
    return _column_defs_for_filtering;
}

void statement_restrictions::calculate_column_defs_for_filtering_and_erase_restrictions_used_for_index(
        data_dictionary::database db,
        const single_column_predicate_vectors& sc_pk_pred_vectors,
        const single_column_predicate_vectors& sc_ck_pred_vectors,
        const single_column_predicate_vectors& sc_nonpk_pred_vectors) {
    std::vector<const column_definition*> column_defs_for_filtering;
    if (need_filtering()) {
        std::optional<secondary_index::index> opt_idx;
        if (_check_indexes) {
            opt_idx = _idx_opt;
        }
        auto column_uses_indexing = [&opt_idx] (const single_column_predicate_vectors& pred_vectors,
                                                const column_definition* cdef) {
            if (!opt_idx) {
                return false;
            }
            auto it = pred_vectors.find(cdef);
            if (it == pred_vectors.end()) {
                return false;
            }
            return are_predicates_supported_by(it->second, *opt_idx);
        };
        if (pk_restrictions_need_filtering()) {
            for (auto&& cdef : expr::get_sorted_column_defs(_partition_key_restrictions)) {
                auto it = _single_column_partition_key_restrictions.find(cdef);
                if (!column_uses_indexing(sc_pk_pred_vectors, cdef)) {
                    column_defs_for_filtering.emplace_back(cdef);
                } else {
                    _single_column_partition_key_restrictions.erase(it);
                }
            }
        }
        const bool pk_has_unrestricted_components = has_partition_key_unrestricted_components();
        if (pk_has_unrestricted_components || clustering_key_restrictions_need_filtering()) {
            column_id first_filtering_id = pk_has_unrestricted_components ? 0 : _schema->clustering_key_columns().begin()->id +
                    num_clustering_prefix_columns_that_need_not_be_filtered();
            for (auto&& cdef : expr::get_sorted_column_defs(_clustering_columns_restrictions)) {
                auto it = _single_column_clustering_key_restrictions.find(cdef);
                if (cdef->id >= first_filtering_id && !column_uses_indexing(sc_ck_pred_vectors, cdef)) {
                    column_defs_for_filtering.emplace_back(cdef);
                } else {
                    _single_column_clustering_key_restrictions.erase(it);
                }
            }
        }
        for (auto it = _single_column_nonprimary_key_restrictions.begin(); it != _single_column_nonprimary_key_restrictions.end();) {
            auto&& [cdef, cur_restr] = *it;
            if (!column_uses_indexing(sc_nonpk_pred_vectors, cdef)) {
                column_defs_for_filtering.emplace_back(cdef);
                ++it;
            } else {
                it = _single_column_nonprimary_key_restrictions.erase(it);
            }
        }
    }
    _column_defs_for_filtering = std::move(column_defs_for_filtering);
}

void statement_restrictions::process_partition_key_restrictions(bool for_view, bool allow_filtering, statements::statement_type type) {
    // If there is a queryable index, no special condition are required on the other restrictions.
    // But we still need to know 2 things:
    // - If we don't have a queryable index, is the query ok
    // - Is it queryable without 2ndary index, which is always more efficient
    // If a component of the partition key is restricted by a relation, all preceding
    // components must have a EQ. Only the last partition key component can be in IN relation.
    if (has_token_restrictions()) {
        _is_key_range = true;
    } else if (is_empty_restriction(_partition_key_restrictions)) {
        _is_key_range = true;
        _uses_secondary_indexing = _has_queriable_pk_index;
    }

    if (pk_restrictions_need_filtering()) {
        if (!allow_filtering && !for_view && !_has_queriable_pk_index && !type.is_delete() && !type.is_update()) {
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
    return is_empty_restriction(_partition_key_restrictions);
}

bool statement_restrictions::partition_key_restrictions_is_all_eq() const {
    return has_only_eq_binops(_partition_key_restrictions);
}

size_t statement_restrictions::partition_key_restrictions_size() const {
    return expr::get_sorted_column_defs(_partition_key_restrictions).size();
}

bool statement_restrictions::pk_restrictions_need_filtering() const {
     return !is_empty_restriction(_partition_key_restrictions)
         && !has_token_restrictions()
         && (has_partition_key_unrestricted_components() || has_slice_or_needs_filtering(_partition_key_restrictions));
}

size_t statement_restrictions::clustering_columns_restrictions_size() const {
    return expr::get_sorted_column_defs(_clustering_columns_restrictions).size();
}

bool statement_restrictions::clustering_key_restrictions_need_filtering() const {
    if (contains_multi_column_restriction(_clustering_columns_restrictions)) {
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

void statement_restrictions::process_clustering_columns_restrictions(bool for_view, bool allow_filtering) {
    if (!has_clustering_columns_restriction()) {
        return;
    }

    if (find_binop(_clustering_columns_restrictions, is_on_collection)
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
dht::partition_range_vector partition_ranges_from_token(const predicate& ex,
                                                        const query_options& options,
                                                        const schema& table_schema) {
    auto values = solve(ex, options);
    if (values == value_set(value_list{})) {
        return {};
    }
    const auto bounds = to_range(values);
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

void error_if_exceeds_partition_key_limit(size_t size, size_t partition_limit) {
    if (size > partition_limit) {
        throw std::runtime_error(
                fmt::format("size of partition-key IN list or partition-key cartesian product of IN list {} is greater than maximum {}. Use --max-partition-key-restrictions-per-query configuration option to change default setting.", size, partition_limit)); 
    }
}

void error_if_exceeds_clustering_key_limit(size_t size, size_t clustering_limit) {
    if (size > clustering_limit) {
        throw std::runtime_error(
                fmt::format("size of clustering-key IN list or clustering-key cartesian product of IN list {} is greater than maximum {}. Use --max-clustering-key-restrictions-per-query configuration option to change default setting.", size, clustering_limit)); 
    }
}

/// Computes partition-key ranges from expressions, which contains EQ/IN for every partition column.
dht::partition_range_vector partition_ranges_from_singles(
        const std::vector<predicate>& expressions, const query_options& options, const schema& schema) {
    const size_t size_limit =
            options.get_cql_config().restrictions.partition_key_restrictions_max_cartesian_product_size;
    // Each element is a vector of that column's possible values:
    std::vector<std::vector<managed_bytes>> column_values(schema.partition_key_size());
    size_t product_size = 1;
    for (const auto& e : expressions) {
                const value_set vals = solve(e, options);
                if (auto lst = std::get_if<value_list>(&vals)) {
                    if (lst->empty()) {
                        return {};
                    }
                    product_size *= lst->size();
                    error_if_exceeds_partition_key_limit(product_size, size_limit);
                    column_values[schema.position(*require_on_single_column(e))] = std::move(*lst);
                } else {
                    throw exceptions::invalid_request_exception(
                            "Only EQ and IN relation are supported on the partition key "
                            "(unless you use the token() function or ALLOW FILTERING)");
                }
    }
    cartesian_product cp(column_values);
    dht::partition_range_vector ranges;
    ranges.reserve(product_size);
    std::transform(cp.begin(), cp.end(), std::back_inserter(ranges), std::bind_front(range_from_bytes, std::ref(schema)));
    return ranges;
}

/// Computes partition-key ranges from EQ restrictions on each partition column.  Returns a single singleton range if
/// the EQ restrictions are not mutually conflicting.  Otherwise, returns an empty vector.
dht::partition_range_vector partition_ranges_from_EQs(
        const std::vector<predicate>& eq_expressions, const query_options& options, const schema& schema) {
    std::vector<managed_bytes> pk_value(schema.partition_key_size());
    for (const auto& e : eq_expressions) {
        const auto vals = std::get<value_list>(solve(e, options));
        if (vals.empty()) { // Case of C=1 AND C=2.
            return {};
        }
        pk_value[schema.position(*require_on_single_column(e))] = std::move(vals[0]);
    }
    return {range_from_bytes(schema, pk_value)};
}

} // anonymous namespace

dht::partition_range_vector statement_restrictions::get_partition_key_ranges(const query_options& options) const {
    return _get_partition_key_ranges_fn(options);
}

get_partition_key_ranges_fn_t
statement_restrictions::build_partition_key_ranges_fn() const {
    return std::visit(overloaded_functor{
        [&] (const no_partition_range_restrictions&) -> get_partition_key_ranges_fn_t {
            return [] (const query_options& options) -> dht::partition_range_vector{
                return {dht::partition_range::make_open_ended_both_sides()};
            };
        },
        [&] (const token_range_restrictions& r) -> get_partition_key_ranges_fn_t {
            return [&] (const query_options& options) -> dht::partition_range_vector {
                return partition_ranges_from_token(r.token_restrictions, options, *_schema);
            };
        },
        [&] (const single_column_partition_range_restrictions& r) -> get_partition_key_ranges_fn_t {
            if (_partition_range_is_simple) {
                return [&] (const query_options& options) {
                    // Special case to avoid extra allocations required for a Cartesian product.
                    return partition_ranges_from_EQs(r.per_column_restrictions, options, *_schema);
                };
            } else {
                return [&] (const query_options& options) {
                    return partition_ranges_from_singles(r.per_column_restrictions, options, *_schema);
                };
            }
        }}, _partition_range_restrictions);
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
        // (a)>=(1) starts before (a)>(1)
        return r1.start()->is_inclusive() && !r2.start()->is_inclusive();
    } else if (len1 < len2) { // r1 start is a prefix of r2 start.
        // (a)>=(1) starts before (a,b)>=(1,1), but (a)>(1) doesn't.
        return r1.start()->is_inclusive();
    } else { // r2 start is a prefix of r1 start.
        // (a,b)>=(1,1) starts before (a)>(1) but after (a)>=(1).
        return !r2.start()->is_inclusive();
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
        // (a)>=(1) starts at end of (a)<=(1)
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
        // (a)<(1) ends before (a)<=(1) ends
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
    // If needed, swap r1 and r2 so that r1's start is to the left of r2's
    // start. Note that to avoid infinite recursion (#18688) the function
    // starts_before_start() must never return true for both (r1,r2) and
    // (r2,r1) - in other words, it must be a *strict* partial order.
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


struct multi_column_range_accumulator {
    std::vector<query::clustering_range> ranges{query::clustering_range::make_open_ended_both_sides()};
};

/// Intersects each range with v.  If any intersection is empty, clears ranges.
void intersect_all(multi_column_range_accumulator& acc, const clustering_key_prefix::prefix_equal_tri_compare& prefix3cmp, const query::clustering_range& v) {
    auto& ranges = acc.ranges;
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
void process_in_values(multi_column_range_accumulator& acc, const clustering_key_prefix::prefix_equal_tri_compare& prefix3cmp, const schema_ptr& schema, Range in_values) {
    auto& ranges = acc.ranges;
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

std::vector<query::clustering_range> get_equivalent_ranges(
        const query::clustering_range& cql_order_range, const schema& schema);

/// Calculates clustering bounds for the multi-column case.
std::function<std::vector<query::clustering_range> (const query_options&)>
build_get_multi_column_clustering_bounds_fn(
        schema_ptr schema,
        const std::vector<predicate>& multi_column_restrictions,
        bool all_natural, bool all_reverse) {
    const auto prefix3cmp = get_unreversed_tri_compare(*schema);
    std::vector<std::function<void (multi_column_range_accumulator&, const query_options&)>> range_builders;
    for (const auto& pred : multi_column_restrictions) {
        const auto& binop = expr::as<binary_operator>(pred.filter);
        range_builders.emplace_back([binop, schema, prefix3cmp] (multi_column_range_accumulator& acc, const query_options& options) {
            auto& lhs = expr::as<tuple_constructor>(binop.lhs);
            if (is_compare(binop.op)) {
                auto opt_values = expr::get_tuple_elements(expr::evaluate(binop.rhs, options), *type_of(binop.rhs));
                std::vector<managed_bytes> values(lhs.elements.size());
                for (size_t i = 0; i < lhs.elements.size(); ++i) {
                    auto& col = expr::as<column_value>(lhs.elements.at(i));
                    values[i] = *statements::request_validations::check_not_null(
                            opt_values[i],
                            "Invalid null value in condition for column {}", col.col->name_as_text());
                }
                intersect_all(acc, prefix3cmp, to_range(binop.op, clustering_key_prefix(std::move(values))));
            } else if (binop.op == oper_t::IN) {
                const cql3::raw_value tup = expr::evaluate(binop.rhs, options);
                utils::chunked_vector<std::vector<managed_bytes_opt>> tuple_elems;
                if (tup.is_value()) {
                    tuple_elems = expr::get_list_of_tuples_elements(tup, *type_of(binop.rhs));
                }
                for (size_t i = 0; i < tuple_elems.size(); ++i) {
                    if (tuple_elems[i].size() != lhs.elements.size()) {
                        throw exceptions::invalid_request_exception(format("Expected {} elements in value tuple, but got {}",
                        lhs.elements.size(), tuple_elems[i].size()));
                    }
                    for (size_t j = 0; j < lhs.elements.size(); ++j) {
                        auto& col = expr::as<column_value>(lhs.elements.at(j));
                        statements::request_validations::check_not_null(
                                tuple_elems[i][j],
                                "Invalid null value in condition for column {}", col.col->name_as_text());
                    }
                }
                process_in_values(acc, prefix3cmp, schema, std::move(tuple_elems));
            } else {
                on_internal_error(rlogger, format("multi_column_range_accumulator: unexpected atom {}", binop));
            }
        });
    }
    return [schema, range_builders, all_natural, all_reverse] (const query_options& options) -> std::vector<query::clustering_range> {
        multi_column_range_accumulator acc;
        for (auto& builder : range_builders) {
            builder(acc, options);
        }
        auto bounds = std::move(acc.ranges);

        if (!all_natural && !all_reverse) {
            std::vector<query::clustering_range> bounds_in_clustering_order;
            for (const auto& b : bounds) {
                const auto eqv = get_equivalent_ranges(b, *schema);
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
    };
}

/// Reverses the range if the type is reversed.  Why don't we have interval::reverse()??
query::clustering_range reverse_if_reqd(query::clustering_range r, const abstract_type& t) {
    return t.is_reversed() ? query::clustering_range(r.end(), r.start()) : std::move(r);
}

/// Calculates clustering bounds for the single-column case.
std::vector<query::clustering_range> get_single_column_clustering_bounds(
        const query_options& options,
        const schema& schema,
        const std::vector<predicate>& single_column_restrictions) {
    const size_t size_limit =
            options.get_cql_config().restrictions.clustering_key_restrictions_max_cartesian_product_size;
    size_t product_size = 1;
    std::vector<std::vector<managed_bytes>> prior_column_values; // Equality values of columns seen so far.
    for (size_t i = 0; i < single_column_restrictions.size(); ++i) {
        if (&schema.clustering_column_at(i) != require_on_single_column(single_column_restrictions[i])) {
            break;
        }
        auto values = solve(
                single_column_restrictions[i],
                options);
        if (auto list = std::get_if<value_list>(&values)) {
            if (list->empty()) { // Impossible condition -- no rows can possibly match.
                return {};
            }
            product_size *= list->size();
            prior_column_values.push_back(std::move(*list));
            error_if_exceeds_clustering_key_limit(product_size, size_limit);
        } else if (auto last_range = std::get_if<interval<managed_bytes>>(&values)) {
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
                const auto extra_lb = last_range->start_copy(), extra_ub = last_range->end_copy();
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
        const predicate& token_restriction) {

    // A workaround in order to make to_predicate work properly.
    // to_predicate looks at the column type and uses this type's comparator.
    // This is a problem because when using blob's comparator, -4 is greater than 4.
    // This makes to_predicate think that an expression like token(p) > -4 and token(p) < 4
    // is impossible to fulfill.
    // Create a fake token column with the type set to bigint, translate the restriction to use this column
    // and use this restriction to calculate possible lhs values.
    column_definition token_column_bigint = token_column;
    token_column_bigint.type = long_type;
    predicate new_token_restrictions = replace_column_def(token_restriction, &token_column_bigint);

    std::variant<value_list, interval<managed_bytes>> values =
        new_token_restrictions.solve_for(options);

    return std::visit(overloaded_functor {
        [](const value_list& list) {
            std::vector<query::clustering_range> ck_ranges;
            ck_ranges.reserve(list.size());

            for (auto&& value : list) {
                ck_ranges.emplace_back(query::clustering_range::make_singular(std::vector<managed_bytes>{value}));
            }

            return ck_ranges;
        },
        [](const interval<managed_bytes>& range) {
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
get_clustering_bounds_fn_t
build_range_from_raw_bounds_fn(
        const std::vector<predicate>& exprs, const schema& schema) {
    std::vector<std::function<query::clustering_range (const query_options&)>> range_builders;
    for (const auto& e : exprs | std::views::transform(&predicate::filter)) {
        if (auto b = find_clustering_order(e)) {
            range_builders.emplace_back([bb = *b, &schema] (const query_options& options) {
                auto* b = &bb;
                cql3::raw_value tup_val = expr::evaluate(b->rhs, options);
                if (tup_val.is_null()) {
                    on_internal_error(rlogger, format("range_from_raw_bounds: unexpected atom {}", *b));
                }

                const auto r = to_range(
                    b->op, clustering_key_prefix::from_optional_exploded(schema, expr::get_tuple_elements(tup_val, *type_of(b->rhs))));
                return r;
            });
        }
    }
    return [range_builders] (const query_options& options) -> std::vector<query::clustering_range> {
        opt_bound lb, ub;
        for (auto& builder : range_builders) {
            auto r = builder(options);

            if (r.start()) {
                lb = r.start();
            }
            if (r.end()) {
                ub = r.end();
            }
        }
        return {{lb, ub}};
    };
}

} // anonymous namespace

get_clustering_bounds_fn_t
statement_restrictions::build_get_clustering_bounds_fn() const {
    if (_clustering_prefix_restrictions.empty()) {
        return [&] (const query_options& options) -> std::vector<query::clustering_range> {
            return {query::clustering_range::make_open_ended_both_sides()};
        };
    }
    if (_clustering_prefix_restrictions[0].is_multi_column) {
        bool all_natural = true, all_reverse = true; ///< Whether column types are reversed or natural.
        for (auto& pred : _clustering_prefix_restrictions) {
            if (pred.order == expr::comparison_order::clustering) {
                return build_range_from_raw_bounds_fn(_clustering_prefix_restrictions, *_schema);
            }
            auto& lhs = expr::as<expr::tuple_constructor>(expr::as<expr::binary_operator>(pred.filter).lhs);
            for (auto& element : lhs.elements) {
                auto& cv = expr::as<expr::column_value>(element);
                if (cv.col->type->is_reversed()) {
                    all_natural = false;
                } else {
                    all_reverse = false;
                }
            }
        }
        return build_get_multi_column_clustering_bounds_fn(_schema, _clustering_prefix_restrictions,
            all_natural, all_reverse);
        } else {
            return [&] (const query_options& options) -> std::vector<query::clustering_range> {
                return get_single_column_clustering_bounds(options, *_schema, _clustering_prefix_restrictions);
            };
        }
    }

std::vector<query::clustering_range> statement_restrictions::get_clustering_bounds(const query_options& options) const {
    return _get_clustering_bounds_fn(options);
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
                 is_empty_restriction(_clustering_columns_restrictions) &&
                 is_empty_restriction(_nonprimary_key_restrictions));
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
        if (npart == 0 && is_empty_restriction(_clustering_columns_restrictions)) {
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

void statement_restrictions::validate_secondary_index_selections(bool selects_only_static_columns) const {
    if (key_is_in_relation()) {
        throw exceptions::invalid_request_exception(
            "Index cannot be used if the partition key is restricted with IN clause. This query would require filtering instead.");
    }
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
        _idx_tbl_ck_prefix = std::vector{to_predicate_on_column(token_restriction, token_column, _schema.get())};

        return;
    }

    // If we're here, it means the index cannot be on a partition column: process_partition_key_restrictions()
    // avoids indexing when _partition_range_is_simple.  See _idx_tbl_ck_prefix blurb for its composition.
    _idx_tbl_ck_prefix = std::vector<predicate>(1 + _schema->partition_key_size(), predicate{
        .solve_for = nullptr,  // FIXME: this is all overwritten later. Should be refactored.
        .filter = expr::expression(expr::conjunction{}),
        .on = on_column{nullptr}, // Illegal but will be overwritten
        .is_singleton = false,
    });
    _idx_tbl_ck_prefix->reserve(_idx_tbl_ck_prefix->size() + idx_tbl_schema.clustering_key_size());
    auto *single_column_partition_key_restrictions = std::get_if<single_column_partition_range_restrictions>(&_partition_range_restrictions);
    if (single_column_partition_key_restrictions) {
        for (const auto& e : single_column_partition_key_restrictions->per_column_restrictions) {
            const auto col = require_on_single_column(e);
            const auto pos = _schema->position(*col) + 1;
            (*_idx_tbl_ck_prefix)[pos] = replace_column_def(e, &idx_tbl_schema.clustering_column_at(pos));
        }
    }

    if (std::ranges::any_of(*_idx_tbl_ck_prefix | std::views::drop(1) | std::views::transform(&predicate::filter), is_empty_restriction)) {
        // If the partition key is not fully restricted, the index clustering key is of no use.
        (*_idx_tbl_ck_prefix) = std::vector<predicate>();
        return;
    }

    add_clustering_restrictions_to_idx_ck_prefix(idx_tbl_schema);

    auto pk_expressions = (*_idx_tbl_ck_prefix)
            | std::views::transform(&predicate::filter)
            | std::views::drop(1)   // skip the token restriction
            | std::views::take(_schema->partition_key_size()) // take only the partition key restrictions
            | std::views::transform(expr::as<expr::binary_operator>) // we know it's an EQ
            | std::views::transform(std::mem_fn(&expr::binary_operator::rhs)) // "solve" for the column value
            | std::ranges::to<std::vector>();

    auto pk_solvers = (*_idx_tbl_ck_prefix)
            | std::views::drop(1) // skip the token restriction
            | std::views::take(_schema->partition_key_size()) // take only the partition key restrictions
            | std::views::transform(&predicate::solve_for)
            | std::ranges::to<std::vector>();

    auto is_singleton = std::ranges::all_of(
            (*_idx_tbl_ck_prefix)
            | std::views::drop(1)
            | std::views::take(_schema->partition_key_size()),
            &predicate::is_singleton);

    if (!is_singleton) {
        on_internal_error(rlogger, "Inconsistency in singleton calculation in indexed query");
    }

    auto token_func = make_shared<cql3::functions::token_fct>(_schema);

    auto token_expr = binary_operator(
            column_value(token_column),
            oper_t::EQ,
            expr::function_call{.func = std::move(token_func), .args = std::move(pk_expressions)});

    auto token_solver = [this, pk_solvers = std::move(pk_solvers)] (const query_options& options) -> value_set {
        auto pk_values = pk_solvers
            | std::views::transform([&] (auto&& solver) { return solver(options); })
            | std::views::transform(value_set_to_singleton)
            | std::ranges::to<utils::small_vector<managed_bytes, 4>>();
        auto pk = partition_key::from_exploded(pk_values);
        auto tok = dht::get_token(*_schema, pk);
        return value_list{managed_bytes(serialized(dht::token::to_int64(tok)))};
    };

    (*_idx_tbl_ck_prefix)[0] = predicate{
        .solve_for = std::move(token_solver),
        .filter = std::move(token_expr),
        .on = on_column{token_column},
        .is_singleton = is_singleton,
    };
}

void statement_restrictions::prepare_indexed_local(const schema& idx_tbl_schema,
        const single_column_predicate_vectors& sc_pk_pred_vectors,
        const single_column_predicate_vectors& sc_ck_pred_vectors,
        const single_column_predicate_vectors& sc_nonpk_pred_vectors) {
    if (!_partition_range_is_simple) {
        return;
    }

    // Local index clustering key is (indexed column, base clustering key)
    _idx_tbl_ck_prefix = std::vector<predicate>();
    _idx_tbl_ck_prefix->reserve(1 + _clustering_prefix_restrictions.size());

    const column_definition& indexed_column = idx_tbl_schema.column_at(column_kind::clustering_key, 0);
    const column_definition& indexed_column_base_schema = *_schema->get_column_definition(indexed_column.name());

    // Find index column restrictions in the pre-built predicate vectors
    const single_column_predicate_vectors* pvecs;
    switch (indexed_column_base_schema.kind) {
    case column_kind::partition_key:  pvecs = &sc_pk_pred_vectors; break;
    case column_kind::clustering_key: pvecs = &sc_ck_pred_vectors; break;
    default:                          pvecs = &sc_nonpk_pred_vectors; break;
    }
    auto it = pvecs->find(&indexed_column_base_schema);
    if (it == pvecs->end()) {
        on_internal_error(rlogger, format("prepare_indexed_local: no predicates found for column {}", indexed_column_base_schema.name_as_text()));
    }
    const auto& preds = it->second;

    // Translate each predicate to use column from the index schema, then merge
    auto folded = std::ranges::fold_left_first(
        preds | std::views::transform([&indexed_column](const predicate& p) {
            return replace_column_def(p, &indexed_column);
        }),
        make_conjunction
    );
    _idx_tbl_ck_prefix->push_back(std::move(*folded));

    // Add restrictions for the clustering key
    add_clustering_restrictions_to_idx_ck_prefix(idx_tbl_schema);
}

void statement_restrictions::add_clustering_restrictions_to_idx_ck_prefix(const schema& idx_tbl_schema) {
    for (const auto& e : _clustering_prefix_restrictions) {
        if (_clustering_prefix_restrictions[0].is_multi_column) {
            // TODO: We could handle single-element tuples, eg. `(c)>=(123)`.
            break;
        }
        const auto any_binop = find_binop(e.filter, [] (auto&&) { return true; });
        if (!any_binop) {
            break;
        }
        const auto col = expr::as<column_value>(any_binop->lhs).col;
        auto col_in_index = idx_tbl_schema.get_column_definition(col->name());
        auto replaced = replace_column_def(e.filter, col_in_index);
        auto a = to_predicate_on_column(replaced, col_in_index, &idx_tbl_schema);
        _idx_tbl_ck_prefix->push_back(std::move(a));
    }
}

// How many of the restrictions (in column order) do not need filtering
// because they are implemented as a slice (potentially, a contiguous disk
// read). For example, if we have the filter "c1 < 3 and c2 > 3", c1 does not
// need filtering but c2 does so num_prefix_columns_that_need_not_be_filtered
// will be 1.
//
// _clustering_prefix_restrictions is already built with exactly this logic
// (iterating CK columns in schema order, stopping at gaps, needs-filtering
// predicates, and after a slice), so its size is the answer.  Multi-column
// restrictions are treated as needing filtering.
unsigned int statement_restrictions::num_clustering_prefix_columns_that_need_not_be_filtered() const {
    if (_has_multi_column) {
        return 0;
    }
    return _clustering_prefix_restrictions.size();
}

get_clustering_bounds_fn_t
statement_restrictions::build_get_global_index_clustering_ranges_fn() const {
    if (!_idx_tbl_ck_prefix) {
        return {};
    }

    return [&] (const query_options& options) {
        // Multi column restrictions are not added to _idx_tbl_ck_prefix, they are handled later by filtering.
        return get_single_column_clustering_bounds(options, *_view_schema, *_idx_tbl_ck_prefix);
    };
}

std::vector<query::clustering_range> statement_restrictions::get_global_index_clustering_ranges(
        const query_options& options) const {
    return _get_global_index_clustering_ranges_fn(options);
}

get_clustering_bounds_fn_t
statement_restrictions::build_get_global_index_token_clustering_ranges_fn() const {
    if (!_idx_tbl_ck_prefix.has_value()) {
        return {};
    }

    const column_definition& token_column = _view_schema->clustering_column_at(0);

    // In old indexes the token column was of type blob.
    // This causes problems with sorting and must be handled separately.
    if (token_column.type != long_type) {
        return [&] (const query_options& options) {
            return get_index_v1_token_range_clustering_bounds(options, token_column, _idx_tbl_ck_prefix->at(0));
        };
    }

    return [&] (const query_options& options) {
        return get_single_column_clustering_bounds(options, *_view_schema, *_idx_tbl_ck_prefix);
    };
}

std::vector<query::clustering_range> statement_restrictions::get_global_index_token_clustering_ranges(
    const query_options& options) const {
    return _get_global_index_token_clustering_ranges_fn(options);
}

get_clustering_bounds_fn_t
statement_restrictions::build_get_local_index_clustering_ranges_fn() const {
    if (!_idx_tbl_ck_prefix.has_value()) {
        return {};
    }

    return [&] (const query_options& options) {
        // Multi column restrictions are not added to _idx_tbl_ck_prefix, they are handled later by filtering.
        return get_single_column_clustering_bounds(options, *_view_schema, *_idx_tbl_ck_prefix);
    };
}

std::vector<query::clustering_range> statement_restrictions::get_local_index_clustering_ranges(
        const query_options& options) const {
    return _get_local_index_clustering_ranges_fn(options);
}

get_singleton_value_fn_t
statement_restrictions::build_value_for_index_partition_key_fn() const {
    if (!_idx_opt) {
        return {};
    }
    const column_definition* cdef = _schema->get_column_definition(to_bytes(_idx_opt->target_column()));
    if (!cdef) {
        throw exceptions::invalid_request_exception("Indexed column not found in schema");
    }

    return build_value_for_fn(*cdef, _idx_restrictions, *_schema);
}

bytes_opt
statement_restrictions::value_for_index_partition_key(const query_options& options) const {
    return _value_for_index_partition_key_fn(options);
}

sstring statement_restrictions::to_string() const {
    return !_where.empty() ? expr::to_string(expr::conjunction{.children = _where}) : "";
}

static void validate_primary_key_restrictions(const query_options& options, std::ranges::range auto&& restrictions) {
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
    std::visit(overloaded_functor{
        [&] (const no_partition_range_restrictions&) {
        },
        [&] (const token_range_restrictions& r) {
            validate_primary_key_restrictions(options, std::span(&r.token_restrictions.filter, 1));
        },
        [&] (const single_column_partition_range_restrictions& r) {
            validate_primary_key_restrictions(options, r.per_column_restrictions | std::views::transform(&predicate::filter));
        }
    }, _partition_range_restrictions);
    validate_primary_key_restrictions(options, _clustering_prefix_restrictions | std::views::transform(&predicate::filter));
}


const std::unordered_set<const column_definition*> statement_restrictions::get_not_null_columns() const {
    return _not_null_columns;
}

shared_ptr<const statement_restrictions>
analyze_statement_restrictions(
        data_dictionary::database db,
        schema_ptr schema,
        statements::statement_type type,
        const expr::expression& where_clause,
        prepare_context& ctx,
        bool selects_only_static_columns,
        bool for_view,
        bool allow_filtering,
        check_indexes do_check_indexes) {
    return make_shared<statement_restrictions>(statement_restrictions::private_tag{}, db, std::move(schema), type, where_clause, ctx, selects_only_static_columns, for_view, allow_filtering, do_check_indexes);
}

shared_ptr<const statement_restrictions>
make_trivial_statement_restrictions(
        schema_ptr schema,
        bool allow_filtering) {
    return make_shared<statement_restrictions>(statement_restrictions::private_tag{}, std::move(schema), allow_filtering);
}

} // namespace restrictions
} // namespace cql3
