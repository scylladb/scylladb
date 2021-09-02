/*
 * Copyright (C) 2020-present ScyllaDB
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

#include "expression.hh"

#include <seastar/core/on_internal_error.hh>

#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm/set_algorithm.hpp>
#include <boost/range/algorithm/unique.hpp>
#include <boost/range/algorithm/sort.hpp>
#include <fmt/ostream.h>
#include <unordered_map>

#include "cql3/constants.hh"
#include "cql3/lists.hh"
#include "cql3/statements/request_validations.hh"
#include "cql3/tuples.hh"
#include "cql3/selection/selection.hh"
#include "index/secondary_index_manager.hh"
#include "types/list.hh"
#include "types/map.hh"
#include "types/set.hh"
#include "utils/like_matcher.hh"
#include "query-result-reader.hh"
#include "types/user.hh"

namespace cql3 {
namespace expr {

logging::logger expr_logger("cql_expression");

using boost::adaptors::filtered;
using boost::adaptors::transformed;


nested_expression::nested_expression(expression e)
        : _e(std::make_unique<expression>(std::move(e))) {
}

nested_expression::nested_expression(const nested_expression& o)
        : nested_expression(*o._e) {
}

nested_expression&
nested_expression::operator=(const nested_expression& o) {
    if (this != &o) {
        _e = std::make_unique<expression>(*o._e);
    }
    return *this;
}

binary_operator::binary_operator(expression lhs, oper_t op, ::shared_ptr<term> rhs, comparison_order order)
            : lhs(std::move(lhs))
            , op(op)
            , rhs(std::move(rhs))
            , order(order) {
}

// Since column_identifier_raw is forward-declared in expression.hh, delay destructor instantiation here
unresolved_identifier::~unresolved_identifier() = default;

namespace {

using children_t = std::vector<expression>; // conjunction's children.

children_t explode_conjunction(expression e) {
    return std::visit(overloaded_functor{
            [] (const conjunction& c) { return std::move(c.children); },
            [&] (const auto&) { return children_t{std::move(e)}; },
        }, e);
}

using cql3::selection::selection;

/// Serialized values for all types of cells, plus selection (to find a column's index) and options (for
/// subscript term's value).
struct row_data_from_partition_slice {
    const std::vector<bytes>& partition_key;
    const std::vector<bytes>& clustering_key;
    const std::vector<managed_bytes_opt>& other_columns;
    const selection& sel;
};

/// Everything needed to compute column values during restriction evaluation.
struct column_value_eval_bag {
    const query_options& options; // For evaluating subscript terms.
    row_data_from_partition_slice row_data;
};

/// Returns col's value from queried data.
managed_bytes_opt get_value(const column_value& col, const column_value_eval_bag& bag) {
    auto cdef = col.col;
    const row_data_from_partition_slice& data = bag.row_data;
    const query_options& options = bag.options;
    if (col.sub) {
        auto col_type = static_pointer_cast<const collection_type_impl>(cdef->type);
        if (!col_type->is_map()) {
            throw exceptions::invalid_request_exception(format("subscripting non-map column {}", cdef->name_as_text()));
        }
        const auto deserialized = cdef->type->deserialize(managed_bytes_view(*data.other_columns[data.sel.index_of(*cdef)]));
        const auto& data_map = value_cast<map_type_impl::native_type>(deserialized);
        const auto key = evaluate_to_raw_view(col.sub, options);
        auto&& key_type = col_type->name_comparator();
        const auto found = key.with_linearized([&] (bytes_view key_bv) {
            using entry = std::pair<data_value, data_value>;
            return std::find_if(data_map.cbegin(), data_map.cend(), [&] (const entry& element) {
                return key_type->compare(element.first.serialize_nonnull(), key_bv) == 0;
            });
        });
        return found == data_map.cend() ? std::nullopt : managed_bytes_opt(found->second.serialize_nonnull());
    } else {
        switch (cdef->kind) {
        case column_kind::partition_key:
            return managed_bytes(data.partition_key[cdef->id]);
        case column_kind::clustering_key:
            return managed_bytes(data.clustering_key[cdef->id]);
        case column_kind::static_column:
        case column_kind::regular_column:
            return managed_bytes_opt(data.other_columns[data.sel.index_of(*cdef)]);
        default:
            throw exceptions::unsupported_operation_exception("Unknown column kind");
        }
    }
}

/// Type for comparing results of get_value().
const abstract_type* get_value_comparator(const column_definition* cdef) {
    return &cdef->type->without_reversed();
}

/// Type for comparing results of get_value().
const abstract_type* get_value_comparator(const column_value& cv) {
    return cv.sub ? static_pointer_cast<const collection_type_impl>(cv.col->type)->value_comparator().get()
            : get_value_comparator(cv.col);
}

/// True iff lhs's value equals rhs.
bool equal(const managed_bytes_opt& rhs, const column_value& lhs, const column_value_eval_bag& bag) {
    if (!rhs) {
        return false;
    }
    const auto value = get_value(lhs, bag);
    if (!value) {
        return false;
    }
    return get_value_comparator(lhs)->equal(managed_bytes_view(*value), managed_bytes_view(*rhs));
}

/// Convenience overload for term.
bool equal(term& rhs, const column_value& lhs, const column_value_eval_bag& bag) {
    return equal(to_managed_bytes_opt(evaluate_to_raw_view(rhs, bag.options)), lhs, bag);
}

/// True iff columns' values equal t.
bool equal(term& t, const tuple_constructor& columns_tuple, const column_value_eval_bag& bag) {
    const constant tup = evaluate(t, bag.options);
    if (!tup.type->is_tuple()) {
        throw exceptions::invalid_request_exception("multi-column equality has right-hand side that isn't a tuple");
    }
    const auto& rhs = get_tuple_elements(tup);
    if (rhs.size() != columns_tuple.elements.size()) {
        throw exceptions::invalid_request_exception(
                format("tuple equality size mismatch: {} elements on left-hand side, {} on right",
                       columns_tuple.elements.size(), rhs.size()));
    }
    auto as_column_value = [] (const expression& e) { return std::get<column_value>(e); };
    return boost::equal(rhs, columns_tuple.elements | boost::adaptors::transformed(as_column_value), [&] (const managed_bytes_opt& b, const column_value& lhs) {
        return equal(b, lhs, bag);
    });
}

/// True iff lhs is limited by rhs in the manner prescribed by op.
bool limits(managed_bytes_view lhs, oper_t op, managed_bytes_view rhs, const abstract_type& type) {
    const auto cmp = type.compare(lhs, rhs);
    switch (op) {
    case oper_t::LT:
        return cmp < 0;
    case oper_t::LTE:
        return cmp <= 0;
    case oper_t::GT:
        return cmp > 0;
    case oper_t::GTE:
        return cmp >= 0;
    case oper_t::EQ:
        return cmp == 0;
    case oper_t::NEQ:
        return cmp != 0;
    default:
        throw std::logic_error(format("limits() called on non-compare op {}", op));
    }
}

/// True iff the column value is limited by rhs in the manner prescribed by op.
bool limits(const column_value& col, oper_t op, term& rhs, const column_value_eval_bag& bag) {
    if (!is_slice(op)) { // For EQ or NEQ, use equal().
        throw std::logic_error("limits() called on non-slice op");
    }
    auto lhs = get_value(col, bag);
    if (!lhs) {
        return false;
    }
    const auto b = to_managed_bytes_opt(evaluate_to_raw_view(rhs, bag.options));
    return b ? limits(*lhs, op, *b, *get_value_comparator(col)) : false;
}

/// True iff the column values are limited by t in the manner prescribed by op.
bool limits(const tuple_constructor& columns_tuple, const oper_t op, term& t,
            const column_value_eval_bag& bag) {
    if (!is_slice(op)) { // For EQ or NEQ, use equal().
        throw std::logic_error("limits() called on non-slice op");
    }
    const constant tup = evaluate(t, bag.options);
    if (!tup.type->is_tuple()) {
        throw exceptions::invalid_request_exception(
                "multi-column comparison has right-hand side that isn't a tuple");
    }
    const auto& rhs = get_tuple_elements(tup);
    if (rhs.size() != columns_tuple.elements.size()) {
        throw exceptions::invalid_request_exception(
                format("tuple comparison size mismatch: {} elements on left-hand side, {} on right",
                       columns_tuple.elements.size(), rhs.size()));
    }
    for (size_t i = 0; i < rhs.size(); ++i) {
        auto& cv = std::get<column_value>(columns_tuple.elements[i]);
        const auto cmp = get_value_comparator(cv)->compare(
                // CQL dictates that columns_tuple.elements[i] is a clustering column and non-null.
                *get_value(cv, bag),
                *rhs[i]);
        // If the components aren't equal, then we just learned the LHS/RHS order.
        if (cmp < 0) {
            if (op == oper_t::LT || op == oper_t::LTE) {
                return true;
            } else if (op == oper_t::GT || op == oper_t::GTE) {
                return false;
            } else {
                throw std::logic_error("Unknown slice operator");
            }
        } else if (cmp > 0) {
            if (op == oper_t::LT || op == oper_t::LTE) {
                return false;
            } else if (op == oper_t::GT || op == oper_t::GTE) {
                return true;
            } else {
                throw std::logic_error("Unknown slice operator");
            }
        }
        // Otherwise, we don't know the LHS/RHS order, so check the next component.
    }
    // Getting here means LHS == RHS.
    return op == oper_t::LTE || op == oper_t::GTE;
}

/// True iff collection (list, set, or map) contains value.
bool contains(const data_value& collection, const raw_value_view& value) {
    if (!value) {
        return true; // Compatible with old code, which skips null terms in value comparisons.
    }
    auto col_type = static_pointer_cast<const collection_type_impl>(collection.type());
    auto&& element_type = col_type->is_set() ? col_type->name_comparator() : col_type->value_comparator();
    return value.with_linearized([&] (bytes_view val) {
        auto exists_in = [&](auto&& range) {
            auto found = std::find_if(range.begin(), range.end(), [&] (auto&& element) {
                return element_type->compare(element.serialize_nonnull(), val) == 0;
            });
            return found != range.end();
        };
        if (col_type->is_list()) {
            return exists_in(value_cast<list_type_impl::native_type>(collection));
        } else if (col_type->is_set()) {
            return exists_in(value_cast<set_type_impl::native_type>(collection));
        } else if (col_type->is_map()) {
            auto data_map = value_cast<map_type_impl::native_type>(collection);
            using entry = std::pair<data_value, data_value>;
            return exists_in(data_map | transformed([] (const entry& e) { return e.second; }));
        } else {
            throw std::logic_error("unsupported collection type in a CONTAINS expression");
        }
    });
}

/// True iff a column is a collection containing value.
bool contains(const column_value& col, const raw_value_view& value, const column_value_eval_bag& bag) {
    if (col.sub) {
        throw exceptions::unsupported_operation_exception("CONTAINS lhs is subscripted");
    }
    const auto collection = get_value(col, bag);
    if (collection) {
        return contains(col.col->type->deserialize(managed_bytes_view(*collection)), value);
    } else {
        return false;
    }
}

/// True iff a column is a map containing \p key.
bool contains_key(const column_value& col, cql3::raw_value_view key, const column_value_eval_bag& bag) {
    if (col.sub) {
        throw exceptions::unsupported_operation_exception("CONTAINS KEY lhs is subscripted");
    }
    if (!key) {
        return true; // Compatible with old code, which skips null terms in key comparisons.
    }
    auto type = col.col->type;
    const auto collection = get_value(col, bag);
    if (!collection) {
        return false;
    }
    const auto data_map = value_cast<map_type_impl::native_type>(type->deserialize(managed_bytes_view(*collection)));
    auto key_type = static_pointer_cast<const collection_type_impl>(type)->name_comparator();
    auto found = key.with_linearized([&] (bytes_view k_bv) {
        using entry = std::pair<data_value, data_value>;
        return std::find_if(data_map.begin(), data_map.end(), [&] (const entry& element) {
            return key_type->compare(element.first.serialize_nonnull(), k_bv) == 0;
        });
    });
    return found != data_map.end();
}

/// Fetches the next cell value from iter and returns its (possibly null) value.
managed_bytes_opt next_value(query::result_row_view::iterator_type& iter, const column_definition* cdef) {
    if (cdef->type->is_multi_cell()) {
        auto cell = iter.next_collection_cell();
        if (cell) {
            return managed_bytes(*cell);
        }
    } else {
        auto cell = iter.next_atomic_cell();
        if (cell) {
            return managed_bytes(cell->value());
        }
    }
    return std::nullopt;
}

/// Returns values of non-primary-key columns from selection.  The kth element of the result
/// corresponds to the kth column in selection.
std::vector<managed_bytes_opt> get_non_pk_values(const selection& selection, const query::result_row_view& static_row,
                                         const query::result_row_view* row) {
    const auto& cols = selection.get_columns();
    std::vector<managed_bytes_opt> vals(cols.size());
    auto static_row_iterator = static_row.iterator();
    auto row_iterator = row ? std::optional<query::result_row_view::iterator_type>(row->iterator()) : std::nullopt;
    for (size_t i = 0; i < cols.size(); ++i) {
        switch (cols[i]->kind) {
        case column_kind::static_column:
            vals[i] = next_value(static_row_iterator, cols[i]);
            break;
        case column_kind::regular_column:
            if (row) {
                vals[i] = next_value(*row_iterator, cols[i]);
            }
            break;
        default: // Skip.
            break;
        }
    }
    return vals;
}

/// True iff cv matches the CQL LIKE pattern.
bool like(const column_value& cv, const raw_value_view& pattern, const column_value_eval_bag& bag) {
    if (!cv.col->type->is_string()) {
        throw exceptions::invalid_request_exception(
                format("LIKE is allowed only on string types, which {} is not", cv.col->name_as_text()));
    }
    auto value = get_value(cv, bag);
    // TODO: reuse matchers.
    if (pattern && value) {
        return value->with_linearized([&pattern] (bytes_view linearized_value) {
            return pattern.with_linearized([linearized_value] (bytes_view linearized_pattern) {
                return like_matcher(linearized_pattern)(linearized_value);
            });
        });
    } else {
        return false;
    }
}

/// True iff the column value is in the set defined by rhs.
bool is_one_of(const column_value& col, term& rhs, const column_value_eval_bag& bag) {
    const constant in_list = evaluate_IN_list(rhs, bag.options);
    statements::request_validations::check_false(
            in_list.is_null(), "Invalid null value for column %s", col.col->name_as_text());

    if (!in_list.type->without_reversed().is_list()) {
        throw std::logic_error("unexpected term type in is_one_of(single column)");
    }
    return boost::algorithm::any_of(get_list_elements(in_list), [&] (const managed_bytes_opt& b) {
        return equal(b, col, bag);
    });
}

/// True iff the tuple of column values is in the set defined by rhs.
bool is_one_of(const tuple_constructor& tuple, term& rhs, const column_value_eval_bag& bag) {
    constant in_list = evaluate_IN_list(rhs, bag.options);
    if (!in_list.type->without_reversed().is_list()) {
        throw std::logic_error("unexpected term type in is_one_of(multi-column)");
    }
    return boost::algorithm::any_of(get_list_of_tuples_elements(in_list), [&] (const std::vector<managed_bytes_opt>& el) {
        return boost::equal(tuple.elements, el, [&] (const expression& c, const managed_bytes_opt& b) {
            return equal(b, std::get<column_value>(c), bag);
        });
    });
}

const value_set empty_value_set = value_list{};
const value_set unbounded_value_set = nonwrapping_range<managed_bytes>::make_open_ended_both_sides();

struct intersection_visitor {
    const abstract_type* type;
    value_set operator()(const value_list& a, const value_list& b) const {
        value_list common;
        common.reserve(std::max(a.size(), b.size()));
        boost::set_intersection(a, b, back_inserter(common), type->as_less_comparator());
        return std::move(common);
    }

    value_set operator()(const nonwrapping_range<managed_bytes>& a, const value_list& b) const {
        const auto common = b | filtered([&] (const managed_bytes& el) { return a.contains(el, type->as_tri_comparator()); });
        return value_list(common.begin(), common.end());
    }

    value_set operator()(const value_list& a, const nonwrapping_range<managed_bytes>& b) const {
        return (*this)(b, a);
    }

    value_set operator()(const nonwrapping_range<managed_bytes>& a, const nonwrapping_range<managed_bytes>& b) const {
        const auto common_range = a.intersection(b, type->as_tri_comparator());
        return common_range ? *common_range : empty_value_set;
    }
};

value_set intersection(value_set a, value_set b, const abstract_type* type) {
    return std::visit(intersection_visitor{type}, std::move(a), std::move(b));
}

bool is_satisfied_by(const binary_operator& opr, const column_value_eval_bag& bag) {
    return std::visit(overloaded_functor{
            [&] (const column_value& col) {
                if (opr.op == oper_t::EQ) {
                    return equal(*opr.rhs, col, bag);
                } else if (opr.op == oper_t::NEQ) {
                    return !equal(*opr.rhs, col, bag);
                } else if (is_slice(opr.op)) {
                    return limits(col, opr.op, *opr.rhs, bag);
                } else if (opr.op == oper_t::CONTAINS) {
                    return contains(col, evaluate_to_raw_view(opr.rhs, bag.options), bag);
                } else if (opr.op == oper_t::CONTAINS_KEY) {
                    return contains_key(col, evaluate_to_raw_view(opr.rhs, bag.options), bag);
                } else if (opr.op == oper_t::LIKE) {
                    return like(col, evaluate_to_raw_view(opr.rhs, bag.options), bag);
                } else if (opr.op == oper_t::IN) {
                    return is_one_of(col, *opr.rhs, bag);
                } else {
                    throw exceptions::unsupported_operation_exception(format("Unhandled binary_operator: {}", opr));
                }
            },
            [&] (const tuple_constructor& cvs) {
                if (opr.op == oper_t::EQ) {
                    return equal(*opr.rhs, cvs, bag);
                } else if (is_slice(opr.op)) {
                    return limits(cvs, opr.op, *opr.rhs, bag);
                } else if (opr.op == oper_t::IN) {
                    return is_one_of(cvs, *opr.rhs, bag);
                } else {
                    throw exceptions::unsupported_operation_exception(
                            format("Unhandled multi-column binary_operator: {}", opr));
                }
            },
            [] (const token& tok) -> bool {
                // The RHS value was already used to ensure we fetch only rows in the specified
                // token range.  It is impossible for any fetched row not to match now.
                return true;
            },
            [] (const constant&) -> bool {
                on_internal_error(expr_logger, "is_satisfied_by: A constant cannot serve as the LHS of a binary expression");
            },
            [] (const conjunction&) -> bool {
                on_internal_error(expr_logger, "is_satisfied_by: a conjunction cannot serve as the LHS of a binary expression");
            },
            [] (const binary_operator&) -> bool {
                on_internal_error(expr_logger, "is_satisfied_by: binary operators cannot be nested");
            },
            [] (const unresolved_identifier&) -> bool {
                on_internal_error(expr_logger, "is_satisfied_by: an unresolved identifier cannot serve as the LHS of a binary expression");
            },
            [] (const column_mutation_attribute&) -> bool {
                on_internal_error(expr_logger, "is_satisified_by: column_mutation_attribute cannot serve as the LHS of a binary expression");
            },
            [] (const function_call&) -> bool {
                on_internal_error(expr_logger, "is_satisified_by: function_call cannot serve as the LHS of a binary expression");
            },
            [] (const cast&) -> bool {
                on_internal_error(expr_logger, "is_satisified_by: cast cannot serve as the LHS of a binary expression");
            },
            [] (const field_selection&) -> bool {
                on_internal_error(expr_logger, "is_satisified_by: field_selection cannot serve as the LHS of a binary expression");
            },
            [] (const null&) -> bool {
                on_internal_error(expr_logger, "is_satisified_by: null cannot serve as the LHS of a binary expression");
            },
            [] (const bind_variable&) -> bool {
                on_internal_error(expr_logger, "is_satisified_by: bind_variable cannot serve as the LHS of a binary expression");
            },
            [] (const untyped_constant&) -> bool {
                on_internal_error(expr_logger, "is_satisified_by: untyped_constant cannot serve as the LHS of a binary expression");
            },
            [] (const collection_constructor&) -> bool {
                on_internal_error(expr_logger, "is_satisified_by: collection_constructor cannot serve as the LHS of a binary expression");
            },
            [] (const usertype_constructor&) -> bool {
                on_internal_error(expr_logger, "is_satisified_by: usertype_constructor cannot serve as the LHS of a binary expression");
            },
        }, *opr.lhs);
}

bool is_satisfied_by(const expression& restr, const column_value_eval_bag& bag) {
    return std::visit(overloaded_functor{
            [] (const constant& constant_val) {
                std::optional<bool> bool_val = get_bool_value(constant_val);
                if (bool_val.has_value()) {
                    return *bool_val;
                }

                on_internal_error(expr_logger,
                    "is_satisfied_by: a constant that is not a bool value cannot serve as a restriction by itself");
            },
            [&] (const conjunction& conj) {
                return boost::algorithm::all_of(conj.children, [&] (const expression& c) {
                    return is_satisfied_by(c, bag);
                });
            },
            [&] (const binary_operator& opr) { return is_satisfied_by(opr, bag); },
            [] (const column_value&) -> bool {
                on_internal_error(expr_logger, "is_satisfied_by: a column cannot serve as a restriction by itself");
            },
            [] (const token&) -> bool {
                on_internal_error(expr_logger, "is_satisfied_by: the token function cannot serve as a restriction by itself");
            },
            [] (const unresolved_identifier&) -> bool {
                on_internal_error(expr_logger, "is_satisfied_by: an unresolved identifier cannot serve as a restriction");
            },
            [] (const column_mutation_attribute&) -> bool {
                on_internal_error(expr_logger, "is_satisfied_by: the writetime/ttl cannot serve as a restriction by itself");
            },
            [] (const function_call&) -> bool {
                on_internal_error(expr_logger, "is_satisfied_by: a function call cannot serve as a restriction by itself");
            },
            [] (const cast&) -> bool {
                on_internal_error(expr_logger, "is_satisfied_by: a a type cast cannot serve as a restriction by itself");
            },
            [] (const field_selection&) -> bool {
                on_internal_error(expr_logger, "is_satisfied_by: a field selection cannot serve as a restriction by itself");
            },
            [] (const null&) -> bool {
                on_internal_error(expr_logger, "is_satisfied_by: NULL cannot serve as a restriction by itself");
            },
            [] (const bind_variable&) -> bool {
                on_internal_error(expr_logger, "is_satisfied_by: a bind variable cannot serve as a restriction by itself");
            },
            [] (const untyped_constant&) -> bool {
                on_internal_error(expr_logger, "is_satisfied_by: an untyped constant cannot serve as a restriction by itself");
            },
            [] (const tuple_constructor&) -> bool {
                on_internal_error(expr_logger, "is_satisfied_by: a tuple constructor cannot serve as a restriction by itself");
            },
            [] (const collection_constructor&) -> bool {
                on_internal_error(expr_logger, "is_satisfied_by: a collection constructor cannot serve as a restriction by itself");
            },
            [] (const usertype_constructor&) -> bool {
                on_internal_error(expr_logger, "is_satisfied_by: a user type constructor cannot serve as a restriction by itself");
            },
        }, restr);
}

template<typename Range>
value_list to_sorted_vector(Range r, const serialized_compare& comparator) {
    BOOST_CONCEPT_ASSERT((boost::ForwardRangeConcept<Range>));
    value_list tmp(r.begin(), r.end()); // Need random-access range to sort (r is not necessarily random-access).
    const auto unique = boost::unique(boost::sort(tmp, comparator));
    return value_list(unique.begin(), unique.end());
}

const auto non_null = boost::adaptors::filtered([] (const managed_bytes_opt& b) { return b.has_value(); });

const auto deref = boost::adaptors::transformed([] (const managed_bytes_opt& b) { return b.value(); });

/// Returns possible values from t, which must be RHS of IN.
value_list get_IN_values(
        const ::shared_ptr<term>& t, const query_options& options, const serialized_compare& comparator,
        sstring_view column_name) {
    const constant in_list = evaluate_IN_list(t, options);
    if (in_list.is_unset_value()) {
        throw exceptions::invalid_request_exception(format("Invalid unset value for column {}", column_name));
    }
    statements::request_validations::check_false(in_list.is_null(), "Invalid null value for column %s", column_name);
    if (!in_list.type->without_reversed().is_list()) {
        throw std::logic_error(format("get_IN_values(single-column) on invalid term {}", *t));
    }
    utils::chunked_vector<managed_bytes> list_elems = get_list_elements(in_list);
    return to_sorted_vector(std::move(list_elems) | non_null | deref, comparator);
}

/// Returns possible values for k-th column from t, which must be RHS of IN.
value_list get_IN_values(const ::shared_ptr<term>& t, size_t k, const query_options& options,
                         const serialized_compare& comparator) {
        const constant in_list = evaluate_IN_list(t, options);
    if (!in_list.type->without_reversed().is_list()) {
        throw std::logic_error(format("get_IN_values(multi-column) on invalid term {}", *t));
    }
    const auto split_values = get_list_of_tuples_elements(in_list); // Need lvalue from which to make std::view.
    const auto result_range = split_values
            | boost::adaptors::transformed([k] (const std::vector<managed_bytes_opt>& v) { return v[k]; }) | non_null | deref;
    return to_sorted_vector(std::move(result_range), comparator);
}

static constexpr bool inclusive = true, exclusive = false;

} // anonymous namespace

expression make_conjunction(expression a, expression b) {
    auto children = explode_conjunction(std::move(a));
    boost::copy(explode_conjunction(std::move(b)), back_inserter(children));
    return conjunction{std::move(children)};
}

bool is_satisfied_by(
        const expression& restr,
        const std::vector<bytes>& partition_key, const std::vector<bytes>& clustering_key,
        const query::result_row_view& static_row, const query::result_row_view* row,
        const selection& selection, const query_options& options) {
    const auto regulars = get_non_pk_values(selection, static_row, row);
    return is_satisfied_by(
            restr, {options, row_data_from_partition_slice{partition_key, clustering_key, regulars, selection}});
}

template<typename T>
nonwrapping_range<std::remove_cvref_t<T>> to_range(oper_t op, T&& val) {
    using U = std::remove_cvref_t<T>;
    static constexpr bool inclusive = true, exclusive = false;
    switch (op) {
    case oper_t::EQ:
        return nonwrapping_range<U>::make_singular(std::forward<T>(val));
    case oper_t::GT:
        return nonwrapping_range<U>::make_starting_with(interval_bound(std::forward<T>(val), exclusive));
    case oper_t::GTE:
        return nonwrapping_range<U>::make_starting_with(interval_bound(std::forward<T>(val), inclusive));
    case oper_t::LT:
        return nonwrapping_range<U>::make_ending_with(interval_bound(std::forward<T>(val), exclusive));
    case oper_t::LTE:
        return nonwrapping_range<U>::make_ending_with(interval_bound(std::forward<T>(val), inclusive));
    default:
        throw std::logic_error(format("to_range: unknown comparison operator {}", op));
    }
}

nonwrapping_range<clustering_key_prefix> to_range(oper_t op, const clustering_key_prefix& val) {
    return to_range<const clustering_key_prefix&>(op, val);
}

value_set possible_lhs_values(const column_definition* cdef, const expression& expr, const query_options& options) {
    const auto type = cdef ? get_value_comparator(cdef) : long_type.get();
    return std::visit(overloaded_functor{
            [] (const constant& constant_val) {
                std::optional<bool> bool_val = get_bool_value(constant_val);
                if (bool_val.has_value()) {
                    return *bool_val ? unbounded_value_set : empty_value_set;
                }

                on_internal_error(expr_logger,
                    "possible_lhs_values: a constant that is not a bool value cannot serve as a restriction by itself");
            },
            [&] (const conjunction& conj) {
                return boost::accumulate(conj.children, unbounded_value_set,
                        [&] (const value_set& acc, const expression& child) {
                            return intersection(
                                    std::move(acc), possible_lhs_values(cdef, child, options), type);
                        });
            },
            [&] (const binary_operator& oper) -> value_set {
                return std::visit(overloaded_functor{
                        [&] (const column_value& col) -> value_set {
                            if (!cdef || cdef != col.col) {
                                return unbounded_value_set;
                            }
                            if (is_compare(oper.op)) {
                                managed_bytes_opt val = to_managed_bytes_opt(evaluate_to_raw_view(oper.rhs, options));
                                if (!val) {
                                    return empty_value_set; // All NULL comparisons fail; no column values match.
                                }
                                return oper.op == oper_t::EQ ? value_set(value_list{*val})
                                        : to_range(oper.op, std::move(*val));
                            } else if (oper.op == oper_t::IN) {
                                return get_IN_values(oper.rhs, options, type->as_less_comparator(), cdef->name_as_text());
                            }
                            throw std::logic_error(format("possible_lhs_values: unhandled operator {}", oper));
                        },
                        [&] (const tuple_constructor& tuple) -> value_set {
                            if (!cdef) {
                                return unbounded_value_set;
                            }
                            const auto found = boost::find_if(
                                    tuple.elements, [&] (const expression& c) { return std::get<column_value>(c).col == cdef; });
                            if (found == tuple.elements.end()) {
                                return unbounded_value_set;
                            }
                            const auto column_index_on_lhs = std::distance(tuple.elements.begin(), found);
                            if (is_compare(oper.op)) {
                                // RHS must be a tuple due to upstream checks.
                                managed_bytes_opt val = get_tuple_elements(evaluate(*oper.rhs, options)).at(column_index_on_lhs);
                                if (!val) {
                                    return empty_value_set; // All NULL comparisons fail; no column values match.
                                }
                                if (oper.op == oper_t::EQ) {
                                    return value_list{std::move(*val)};
                                }
                                if (column_index_on_lhs > 0) {
                                    // A multi-column comparison restricts only the first column, because
                                    // comparison is lexicographical.
                                    return unbounded_value_set;
                                }
                                return to_range(oper.op, std::move(*val));
                            } else if (oper.op == oper_t::IN) {
                                return get_IN_values(oper.rhs, column_index_on_lhs, options, type->as_less_comparator());
                            }
                            return unbounded_value_set;
                        },
                        [&] (token) -> value_set {
                            if (cdef) {
                                return unbounded_value_set;
                            }
                            const auto val = to_managed_bytes_opt(evaluate_to_raw_view(oper.rhs, options));
                            if (!val) {
                                return empty_value_set; // All NULL comparisons fail; no token values match.
                            }
                            if (oper.op == oper_t::EQ) {
                                return value_list{*val};
                            } else if (oper.op == oper_t::GT) {
                                return nonwrapping_range<managed_bytes>::make_starting_with(interval_bound(std::move(*val), exclusive));
                            } else if (oper.op == oper_t::GTE) {
                                return nonwrapping_range<managed_bytes>::make_starting_with(interval_bound(std::move(*val), inclusive));
                            }
                            static const managed_bytes MININT = managed_bytes(serialized(std::numeric_limits<int64_t>::min())),
                                    MAXINT = managed_bytes(serialized(std::numeric_limits<int64_t>::max()));
                            // Undocumented feature: when the user types `token(...) < MININT`, we interpret
                            // that as MAXINT for some reason.
                            const auto adjusted_val = (*val == MININT) ? MAXINT : *val;
                            if (oper.op == oper_t::LT) {
                                return nonwrapping_range<managed_bytes>::make_ending_with(interval_bound(std::move(adjusted_val), exclusive));
                            } else if (oper.op == oper_t::LTE) {
                                return nonwrapping_range<managed_bytes>::make_ending_with(interval_bound(std::move(adjusted_val), inclusive));
                            }
                            throw std::logic_error(format("get_token_interval invalid operator {}", oper.op));
                        },
                        [&] (const binary_operator&) -> value_set {
                            on_internal_error(expr_logger, "possible_lhs_values: nested binary operators are not supported");
                        },
                        [&] (const conjunction&) -> value_set {
                            on_internal_error(expr_logger, "possible_lhs_values: conjunctions are not supported as the LHS of a binary expression");
                        },
                        [] (const constant&) -> value_set {
                            on_internal_error(expr_logger, "possible_lhs_values: constants are not supported as the LHS of a binary expression");
                        },
                        [] (const unresolved_identifier&) -> value_set {
                            on_internal_error(expr_logger, "possible_lhs_values: unresolved identifiers are not supported as the LHS of a binary expression");
                        },
                        [] (const column_mutation_attribute&) -> value_set {
                            on_internal_error(expr_logger, "possible_lhs_values: writetime/ttl are not supported as the LHS of a binary expression");
                        },
                        [] (const function_call&) -> value_set {
                            on_internal_error(expr_logger, "possible_lhs_values: function calls are not supported as the LHS of a binary expression");
                        },
                        [] (const cast&) -> value_set {
                            on_internal_error(expr_logger, "possible_lhs_values: typecasts are not supported as the LHS of a binary expression");
                        },
                        [] (const field_selection&) -> value_set {
                            on_internal_error(expr_logger, "possible_lhs_values: field selections are not supported as the LHS of a binary expression");
                        },
                        [] (const null&) -> value_set {
                            on_internal_error(expr_logger, "possible_lhs_values: nulls are not supported as the LHS of a binary expression");
                        },
                        [] (const bind_variable&) -> value_set {
                            on_internal_error(expr_logger, "possible_lhs_values: bind variables are not supported as the LHS of a binary expression");
                        },
                        [] (const untyped_constant&) -> value_set {
                            on_internal_error(expr_logger, "possible_lhs_values: untyped constants are not supported as the LHS of a binary expression");
                        },
                        [] (const collection_constructor&) -> value_set {
                            on_internal_error(expr_logger, "possible_lhs_values: collection constructors are not supported as the LHS of a binary expression");
                        },
                        [] (const usertype_constructor&) -> value_set {
                            on_internal_error(expr_logger, "possible_lhs_values: user type constructors are not supported as the LHS of a binary expression");
                        },
                    }, *oper.lhs);
            },
            [] (const column_value&) -> value_set {
                on_internal_error(expr_logger, "possible_lhs_values: a column cannot serve as a restriction by itself");
            },
            [] (const token&) -> value_set {
                on_internal_error(expr_logger, "possible_lhs_values: the token function cannot serve as a restriction by itself");
            },
            [] (const unresolved_identifier&) -> value_set {
                on_internal_error(expr_logger, "is_satisfied_by: an unresolved identifier cannot serve as a restriction");
            },
            [] (const column_mutation_attribute&) -> value_set {
                on_internal_error(expr_logger, "possible_lhs_values: the writetime/ttl functions cannot serve as a restriction by itself");
            },
            [] (const function_call&) -> value_set {
                on_internal_error(expr_logger, "possible_lhs_values: a function call cannot serve as a restriction by itself");
            },
            [] (const cast&) -> value_set {
                on_internal_error(expr_logger, "possible_lhs_values: a typecast cannot serve as a restriction by itself");
            },
            [] (const field_selection&) -> value_set {
                on_internal_error(expr_logger, "possible_lhs_values: a field selection cannot serve as a restriction by itself");
            },
            [] (const null&) -> value_set {
                on_internal_error(expr_logger, "possible_lhs_values: a NULL cannot serve as a restriction by itself");
            },
            [] (const bind_variable&) -> value_set {
                on_internal_error(expr_logger, "possible_lhs_values: a bind variable cannot serve as a restriction by itself");
            },
            [] (const untyped_constant&) -> value_set {
                on_internal_error(expr_logger, "possible_lhs_values: an untyped constant cannot serve as a restriction by itself");
            },
            [] (const tuple_constructor&) -> value_set {
                on_internal_error(expr_logger, "possible_lhs_values: an tuple constructor cannot serve as a restriction by itself");
            },
            [] (const collection_constructor&) -> value_set {
                on_internal_error(expr_logger, "possible_lhs_values: a collection constructor cannot serve as a restriction by itself");
            },
            [] (const usertype_constructor&) -> value_set {
                on_internal_error(expr_logger, "possible_lhs_values: a user type constructor cannot serve as a restriction by itself");
            },
        }, expr);
}

nonwrapping_range<managed_bytes> to_range(const value_set& s) {
    return std::visit(overloaded_functor{
            [] (const nonwrapping_range<managed_bytes>& r) { return r; },
            [] (const value_list& lst) {
                if (lst.size() != 1) {
                    throw std::logic_error(format("to_range called on list of size {}", lst.size()));
                }
                return nonwrapping_range<managed_bytes>::make_singular(lst[0]);
            },
        }, s);
}

bool is_supported_by(const expression& expr, const secondary_index::index& idx) {
    using std::placeholders::_1;
    return std::visit(overloaded_functor{
            [&] (const conjunction& conj) {
                return boost::algorithm::all_of(conj.children, std::bind(is_supported_by, _1, idx));
            },
            [&] (const binary_operator& oper) {
                return std::visit(overloaded_functor{
                        [&] (const column_value& col) {
                            return idx.supports_expression(*col.col, oper.op);
                        },
                        [&] (const tuple_constructor& tuple) {
                            if (tuple.elements.size() == 1) {
                                if (auto column = std::get_if<column_value>(&tuple.elements[0])) {
                                    return idx.supports_expression(*column->col, oper.op);
                                }
                            }
                            // We don't use index table for multi-column restrictions, as it cannot avoid filtering.
                            return false;
                        },
                        [&] (const token&) { return false; },
                        [&] (const binary_operator&) -> bool {
                            on_internal_error(expr_logger, "is_supported_by: nested binary operators are not supported");
                        },
                        [&] (const conjunction&) -> bool {
                            on_internal_error(expr_logger, "is_supported_by: conjunctions are not supported as the LHS of a binary expression");
                        },
                        [] (const constant&) -> bool {
                            on_internal_error(expr_logger, "is_supported_by: constants are not supported as the LHS of a binary expression");
                        },
                        [] (const unresolved_identifier&) -> bool {
                            on_internal_error(expr_logger, "is_supported_by: an unresolved identifier is not supported as the LHS of a binary expression");
                        },
                        [&] (const column_mutation_attribute&) -> bool {
                            on_internal_error(expr_logger, "is_supported_by: writetime/ttl are not supported as the LHS of a binary expression");
                        },
                        [&] (const function_call&) -> bool {
                            on_internal_error(expr_logger, "is_supported_by: function calls are not supported as the LHS of a binary expression");
                        },
                        [&] (const cast&) -> bool {
                            on_internal_error(expr_logger, "is_supported_by: typecasts are not supported as the LHS of a binary expression");
                        },
                        [&] (const field_selection&) -> bool {
                            on_internal_error(expr_logger, "is_supported_by: field selections are not supported as the LHS of a binary expression");
                        },
                        [&] (const null&) -> bool {
                            on_internal_error(expr_logger, "is_supported_by: nulls are not supported as the LHS of a binary expression");
                        },
                        [&] (const bind_variable&) -> bool {
                            on_internal_error(expr_logger, "is_supported_by: bind variables are not supported as the LHS of a binary expression");
                        },
                        [&] (const untyped_constant&) -> bool {
                            on_internal_error(expr_logger, "is_supported_by: untyped constants are not supported as the LHS of a binary expression");
                        },
                        [&] (const collection_constructor&) -> bool {
                            on_internal_error(expr_logger, "is_supported_by: collection constructors are not supported as the LHS of a binary expression");
                        },
                        [&] (const usertype_constructor&) -> bool {
                            on_internal_error(expr_logger, "is_supported_by: user type constructors are not supported as the LHS of a binary expression");
                        },
                    }, *oper.lhs);
            },
            [] (const auto& default_case) { return false; }
        }, expr);
}

bool has_supporting_index(
        const expression& expr,
        const secondary_index::secondary_index_manager& index_manager,
        allow_local_index allow_local) {
    const auto indexes = index_manager.list_indexes();
    const auto support = std::bind(is_supported_by, std::ref(expr), std::placeholders::_1);
    return allow_local ? boost::algorithm::any_of(indexes, support)
            : boost::algorithm::any_of(
                    indexes | filtered([] (const secondary_index::index& i) { return !i.metadata().local(); }),
                    support);
}

std::ostream& operator<<(std::ostream& os, const column_value& cv) {
    os << cv.col->name_as_text();
    if (cv.sub) {
        os << '[' << *cv.sub << ']';
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, const expression& expr) {
    std::visit(overloaded_functor{
            [&] (const constant& v) { os << v.value.to_view(); },
            [&] (const conjunction& conj) { fmt::print(os, "({})", fmt::join(conj.children, ") AND (")); },
            [&] (const binary_operator& opr) {
                os << "(" << *opr.lhs << ") " << opr.op << ' ' << *opr.rhs;
            },
            [&] (const token& t) { os << "TOKEN"; },
            [&] (const column_value& col) {
                fmt::print(os, "{}", col);
            },
            [&] (const unresolved_identifier& ui) {
                fmt::print(os, "unresolved({})", *ui.ident);
            },
            [&] (const column_mutation_attribute& cma)  {
                fmt::print(os, "{}({})",
                        cma.kind == column_mutation_attribute::attribute_kind::ttl ? "TTL" : "WRITETIME",
                        *cma.column);
            },
            [&] (const function_call& fc)  {
                std::visit(overloaded_functor{
                    [&] (const functions::function_name& named) {
                        fmt::print(os, "{}({})", named, fmt::join(fc.args, ", "));
                    },
                    [&] (const shared_ptr<functions::function>& anon) {
                        fmt::print(os, "<anonymous function>({})", fmt::join(fc.args, ", "));
                    },
                }, fc.func);
            },
            [&] (const cast& c)  {
                std::visit(overloaded_functor{
                    [&] (const cql3_type& t) {
                        fmt::print(os, "({} AS {})", *c.arg, t);
                    },
                    [&] (const shared_ptr<cql3_type::raw>& t) {
                        fmt::print(os, "({}) {}", t, *c.arg);
                    },
                }, c.type);
            },
            [&] (const field_selection& fs)  {
                fmt::print(os, "({}.{})", *fs.structure, fs.field);
            },
            [&] (const null&) {
                // FIXME: adjust tests and change to NULL
                fmt::print(os, "null");
            },
            [&] (const bind_variable&) {
                // FIXME: store and present bind variable name
                fmt::print(os, "?");
            },
            [&] (const untyped_constant& uc) {
                if (uc.partial_type == untyped_constant::type_class::string) {
                    fmt::print(os, "'{}'", uc.raw_text);
                } else {
                    fmt::print(os, "{}", uc.raw_text);
                }
            },
            [&] (const tuple_constructor& tc) {
                fmt::print(os, "({})", join(", ", tc.elements));
            },
            [&] (const collection_constructor& cc) {
                switch (cc.style) {
                case collection_constructor::style_type::list: fmt::print(os, "{}", std::to_string(cc.elements)); return;
                case collection_constructor::style_type::set: {
                    fmt::print(os, "{{{}}}", fmt::join(cc.elements, ", "));
                    return;
                }
                case collection_constructor::style_type::map: {
                    fmt::print(os, "{{");
                    bool first = true;
                    for (auto& e : cc.elements) {
                        if (!first) {
                            fmt::print(os, ", ");
                        }
                        first = false;
                        auto& tuple = std::get<tuple_constructor>(e);
                        if (tuple.elements.size() != 2) {
                            on_internal_error(expr_logger, "map constructor element is not a tuple of arity 2");
                        }
                        fmt::print(os, "{}:{}", tuple.elements[0], tuple.elements[1]);
                    }
                    fmt::print(os, "}}");
                    return;
                }
                }
                on_internal_error(expr_logger, fmt::format("unexpected collection_constructor style {}", static_cast<unsigned>(cc.style)));
            },
            [&] (const usertype_constructor& uc) {
                fmt::print(os, "{{");
                bool first = true;
                for (auto& [k, v] : uc.elements) {
                    if (!first) {
                        fmt::print(os, ", ");
                    }
                    first = false;
                    fmt::print(os, "{}:{}", k, *v);
                }
                fmt::print(os, "}}");
            },
        }, expr);
    return os;
}

sstring to_string(const expression& expr) {
    return fmt::format("{}", expr);
}

bool is_on_collection(const binary_operator& b) {
    if (b.op == oper_t::CONTAINS || b.op == oper_t::CONTAINS_KEY) {
        return true;
    }
    if (auto tuple = std::get_if<tuple_constructor>(&*b.lhs)) {
        return boost::algorithm::any_of(tuple->elements, [] (const expression& v) { return std::get<column_value>(v).sub; });
    }
    return false;
}

expression replace_column_def(const expression& expr, const column_definition* new_cdef) {
    return search_and_replace(expr, [&] (const expression& expr) -> std::optional<expression> {
        if (std::holds_alternative<column_value>(expr)) {
            return column_value{new_cdef};
        } else if (std::holds_alternative<tuple_constructor>(expr)) {
            throw std::logic_error(format("replace_column_def invalid with column tuple: {}", to_string(expr)));
        } else {
            return std::nullopt;
        }
    });
}

expression replace_token(const expression& expr, const column_definition* new_cdef) {
    return search_and_replace(expr, [&] (const expression& expr) -> std::optional<expression> {
        if (std::holds_alternative<token>(expr)) {
            return column_value{new_cdef};
        } else {
            return std::nullopt;
        }
    });
}

expression search_and_replace(const expression& e,
        const noncopyable_function<std::optional<expression> (const expression& candidate)>& replace_candidate) {
    auto recurse = [&] (const expression& e) -> expression {
        return search_and_replace(e, replace_candidate);
    };
    auto replace_result = replace_candidate(e);
    if (replace_result) {
        return std::move(*replace_result);
    } else {
        return std::visit(
            overloaded_functor{
                [&] (const conjunction& conj) -> expression {
                    return conjunction{
                        boost::copy_range<std::vector<expression>>(
                            conj.children | boost::adaptors::transformed(recurse)
                        )
                    };
                },
                [&] (const binary_operator& oper) -> expression {
                    return binary_operator(recurse(*oper.lhs), oper.op, oper.rhs);
                },
                [&] (const column_mutation_attribute& cma) -> expression {
                    return column_mutation_attribute{cma.kind, recurse(*cma.column)};
                },
                [&] (const tuple_constructor& tc) -> expression {
                    return tuple_constructor{
                        boost::copy_range<std::vector<expression>>(
                            tc.elements | boost::adaptors::transformed(recurse)
                        )
                    };
                },
                [&] (const collection_constructor& c) -> expression {
                    return collection_constructor{
                        c.style,
                        boost::copy_range<std::vector<expression>>(
                            c.elements | boost::adaptors::transformed(recurse)
                        )
                    };
                },
                [&] (const usertype_constructor& uc) -> expression {
                    usertype_constructor::elements_map_type m;
                    for (auto& [k, v] : uc.elements) {
                        m.emplace(k, recurse(*v));
                    }
                    return usertype_constructor{std::move(m)};
                },
                [&] (const function_call& fc) -> expression {
                    return function_call{
                        fc.func,
                        boost::copy_range<std::vector<expression>>(
                            fc.args | boost::adaptors::transformed(recurse)
                        )
                    };
                },
                [&] (const cast& c) -> expression {
                    return cast{recurse(*c.arg), c.type};
                },
                [&] (const field_selection& fs) -> expression {
                    return field_selection{recurse(*fs.structure), fs.field};
                },
                [&] (LeafExpression auto const& e) -> expression {
                    return e;
                },
            }, e);
    }
}

std::ostream& operator<<(std::ostream& s, oper_t op) {
    switch (op) {
    case oper_t::EQ:
        return s << "=";
    case oper_t::NEQ:
        return s << "!=";
    case oper_t::LT:
        return s << "<";
    case oper_t::LTE:
        return s << "<=";
    case oper_t::GT:
        return s << ">";
    case oper_t::GTE:
        return s << ">=";
    case oper_t::IN:
        return s << "IN";
    case oper_t::CONTAINS:
        return s << "CONTAINS";
    case oper_t::CONTAINS_KEY:
        return s << "CONTAINS KEY";
    case oper_t::IS_NOT:
        return s << "IS NOT";
    case oper_t::LIKE:
        return s << "LIKE";
    }
    __builtin_unreachable();
}

std::vector<expression> extract_single_column_restrictions_for_column(const expression& expr,
                                                                      const column_definition& column) {
    struct visitor {
        std::vector<expression> restrictions;
        const column_definition& column;
        const binary_operator* current_binary_operator;

        void operator()(const constant&) {}

        void operator()(const conjunction& conj) {
            for (const expression& child : conj.children) {
                std::visit(*this, child);
            }
        }

        void operator()(const binary_operator& oper) {
            if (current_binary_operator != nullptr) {
                on_internal_error(expr_logger,
                    "extract_single_column_restrictions_for_column: nested binary operators are not supported");
            }

            current_binary_operator = &oper;
            std::visit(*this, *oper.lhs);
            current_binary_operator = nullptr;
        }

        void operator()(const column_value& cv) {
            if (*cv.col == column && current_binary_operator != nullptr) {
                restrictions.emplace_back(*current_binary_operator);
            }
        }

        void operator()(const token&) {}
        void operator()(const unresolved_identifier&) {}
        void operator()(const column_mutation_attribute&) {}
        void operator()(const function_call&) {}
        void operator()(const cast&) {}
        void operator()(const field_selection&) {}
        void operator()(const null&) {}
        void operator()(const bind_variable&) {}
        void operator()(const untyped_constant&) {}
        void operator()(const tuple_constructor&) {}
        void operator()(const collection_constructor&) {}
        void operator()(const usertype_constructor&) {}
    };

    visitor v {
        .restrictions = std::vector<expression>(),
        .column = column,
        .current_binary_operator = nullptr,
    };

    std::visit(v, expr);

    return std::move(v.restrictions);
}


constant::constant(cql3::raw_value val, data_type typ)
    : value(std::move(val)), type(std::move(typ)) {
}

constant constant::make_null(data_type val_type) {
    return constant(cql3::raw_value::make_null(), std::move(val_type));
}

constant constant::make_unset_value(data_type val_type) {
    return constant(cql3::raw_value::make_unset_value(), std::move(val_type));
}

constant constant::make_bool(bool bool_val) {
    return constant(raw_value::make_value(boolean_type->decompose(bool_val)), boolean_type);
}

bool constant::is_null() const {
    return value.is_null();
}

bool constant::is_unset_value() const {
    return value.is_unset_value();
}

bool constant::has_empty_value_bytes() const {
    if (is_null_or_unset()) {
        return false;
    }

    return value.to_view().size_bytes() == 0;
}

bool constant::is_null_or_unset() const {
    return is_null() || is_unset_value();
}

std::optional<bool> get_bool_value(const constant& constant_val) {
    if (constant_val.type->get_kind() != abstract_type::kind::boolean) {
        return std::nullopt;
    }

    if (constant_val.is_null_or_unset()) {
        return std::nullopt;
    }

    if (constant_val.has_empty_value_bytes()) {
        return std::nullopt;
    }

    return constant_val.value.to_view().deserialize<bool>(*boolean_type);
}

constant evaluate(term* term_ptr, const query_options& options) {
    if (term_ptr == nullptr) {
        return constant::make_null();
    }

    ::shared_ptr<terminal> bound = term_ptr->bind(options);
    if (bound.get() == nullptr) {
        return constant::make_null();
    }

    raw_value raw_val = bound->get(options);
    data_type val_type = bound->get_value_type();
    return constant(std::move(raw_val), std::move(val_type));
}

constant evaluate(const ::shared_ptr<term>& term_ptr, const query_options& options) {
    return evaluate(term_ptr.get(), options);
}

constant evaluate(term& term_ref, const query_options& options) {
    return evaluate(&term_ref, options);
}

constant evaluate_IN_list(term* term_ptr, const query_options& options) {
    if (term_ptr == nullptr) {
        return constant::make_null();
    }

    ::shared_ptr<terminal> bound;
    lists::delayed_value* delayed_list = dynamic_cast<lists::delayed_value*>(term_ptr);
    if (delayed_list != nullptr) {
        bound = delayed_list->bind_ignore_null(options);
    } else {
        bound = term_ptr->bind(options);
    }

    if (bound.get() == nullptr) {
        return constant::make_null();
    }

    lists::value* list_value = dynamic_cast<lists::value*>(bound.get());
    if (list_value != nullptr) {
        // Remove NULL elements from the list
        std::remove_if(list_value->_elements.begin(), list_value->_elements.end(),
                       [](const managed_bytes_opt& element) { return !element.has_value(); });
    }

    raw_value raw_val = bound->get(options);
    data_type val_type = bound->get_value_type();
    return constant(std::move(raw_val), std::move(val_type));
}

constant evaluate_IN_list(const ::shared_ptr<term>& term_ptr, const query_options& options) {
    return evaluate_IN_list(term_ptr.get(), options);
}

constant evaluate_IN_list(term& term_ref, const query_options& options) {
    return evaluate_IN_list(&term_ref, options);
}

cql3::raw_value_view evaluate_to_raw_view(const ::shared_ptr<term>& term_ptr, const query_options& options) {
    constant value = evaluate(term_ptr, options);
    return cql3::raw_value_view::make_temporary(std::move(value.value));
}

cql3::raw_value_view evaluate_to_raw_view(term& term_ref, const query_options& options) {
    constant value = evaluate(term_ref, options);
    return cql3::raw_value_view::make_temporary(std::move(value.value));
}

static void ensure_can_get_value_elements(const constant& val,
                                          abstract_type::kind expected_type_kind,
                                          const char* caller_name) {
    const abstract_type& val_type = val.type->without_reversed();

    if (val_type.get_kind() != expected_type_kind) {
        on_internal_error(expr_logger, fmt::format("{} called with wrong type: {}", caller_name, val_type.name()));
    }

    if (val.is_null()) {
        on_internal_error(expr_logger, fmt::format("{} called with null value", caller_name));
    }

    if (val.is_unset_value()) {
        on_internal_error(expr_logger, fmt::format("{} called with unset value", caller_name));
    }
}

utils::chunked_vector<managed_bytes> get_list_elements(const constant& val) {
    ensure_can_get_value_elements(val, abstract_type::kind::list, "expr::get_list_elements");

    return val.value.to_view().with_value([](const FragmentedView auto& value_bytes) {
        return partially_deserialize_listlike(value_bytes, cql_serialization_format::internal());
    });
}

utils::chunked_vector<managed_bytes> get_set_elements(const constant& val) {
    ensure_can_get_value_elements(val, abstract_type::kind::set, "expr::get_set_elements");

    return val.value.to_view().with_value([](const FragmentedView auto& value_bytes) {
        return partially_deserialize_listlike(value_bytes, cql_serialization_format::internal());
    });
}

std::vector<std::pair<managed_bytes, managed_bytes>> get_map_elements(const constant& val) {
    ensure_can_get_value_elements(val, abstract_type::kind::map, "expr::get_map_elements");

    return val.value.to_view().with_value([](const FragmentedView auto& value_bytes) {
        return partially_deserialize_map(value_bytes, cql_serialization_format::internal());
    });
}

std::vector<managed_bytes_opt> get_tuple_elements(const constant& val) {
    ensure_can_get_value_elements(val, abstract_type::kind::tuple, "expr::get_tuple_elements");

    return val.value.to_view().with_value([&](const FragmentedView auto& value_bytes) {
        const tuple_type_impl& ttype = static_cast<const tuple_type_impl&>(val.type->without_reversed());
        return ttype.split_fragmented(value_bytes);
    });
}

std::vector<managed_bytes_opt> get_user_type_elements(const constant& val) {
    ensure_can_get_value_elements(val, abstract_type::kind::user, "expr::get_user_type_elements");

    return val.value.to_view().with_value([&](const FragmentedView auto& value_bytes) {
        const user_type_impl& utype = static_cast<const user_type_impl&>(val.type->without_reversed());
        return utype.split_fragmented(value_bytes);
    });
}

static std::vector<managed_bytes_opt> convert_listlike(utils::chunked_vector<managed_bytes>&& elements) {
    return std::vector<managed_bytes_opt>(std::make_move_iterator(elements.begin()),
                                          std::make_move_iterator(elements.end()));
}

std::vector<managed_bytes_opt> get_elements(const constant& val) {
    const abstract_type& val_type = val.type->without_reversed();

    switch (val_type.get_kind()) {
        case abstract_type::kind::list:
            return convert_listlike(get_list_elements(val));

        case abstract_type::kind::set:
            return convert_listlike(get_set_elements(val));

        case abstract_type::kind::tuple:
            return get_tuple_elements(val);

        case abstract_type::kind::user:
            return get_user_type_elements(val);

        default:
            on_internal_error(expr_logger, fmt::format("expr::get_elements called on bad type: {}", val_type.name()));
    }
}

utils::chunked_vector<std::vector<managed_bytes_opt>> get_list_of_tuples_elements(const constant& val) {
    utils::chunked_vector<managed_bytes> elements = get_list_elements(val);
    const list_type_impl& list_typ = dynamic_cast<const list_type_impl&>(val.type->without_reversed());
    const tuple_type_impl& tuple_typ = dynamic_cast<const tuple_type_impl&>(*list_typ.get_elements_type());

    utils::chunked_vector<std::vector<managed_bytes_opt>> tuples_list;
    tuples_list.reserve(elements.size());

    for (managed_bytes& element : elements) {
        std::vector<managed_bytes_opt> cur_tuple = tuple_typ.split_fragmented(managed_bytes_view(element));
        tuples_list.emplace_back(std::move(cur_tuple));
    }

    return tuples_list;
}
} // namespace expr
} // namespace cql3
