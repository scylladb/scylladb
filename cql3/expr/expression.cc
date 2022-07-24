/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
#include "cql3/selection/selection.hh"
#include "cql3/util.hh"
#include "index/secondary_index_manager.hh"
#include "types/list.hh"
#include "types/map.hh"
#include "types/set.hh"
#include "utils/like_matcher.hh"
#include "query-result-reader.hh"
#include "types/user.hh"
#include "cql3/lists.hh"
#include "cql3/sets.hh"
#include "cql3/maps.hh"
#include "cql3/user_types.hh"
#include "cql3/functions/scalar_function.hh"
#include "cql3/prepare_context.hh"

namespace cql3 {
namespace expr {

logging::logger expr_logger("cql_expression");

using boost::adaptors::filtered;
using boost::adaptors::transformed;

bool operator==(const expression& e1, const expression& e2) {
    if (e1._v->v.index() != e2._v->v.index()) {
        return false;
    }
    return expr::visit([&] <typename T> (const T& e1_element) {
        const T& e2_element = expr::as<T>(e2);
        return e1_element == e2_element;
    }, e1);
}

bool operator!=(const expression& e1, const expression& e2) {
    return !(e1 == e2);
}

expression::expression(const expression& o)
        : _v(std::make_unique<impl>(*o._v)) {
}

expression&
expression::operator=(const expression& o) {
    *this = expression(o);
    return *this;
}

token::token(std::vector<expression> args_in)
    : args(std::move(args_in)) {
}

token::token(const std::vector<const column_definition*>& col_defs) {
    args.reserve(col_defs.size());
    for (const column_definition* col_def : col_defs) {
        args.push_back(column_value(col_def));
    }
}

token::token(const std::vector<::shared_ptr<column_identifier_raw>>& cols) {
    args.reserve(cols.size());
    for(const ::shared_ptr<column_identifier_raw>& col : cols) {
        args.push_back(unresolved_identifier{col});
    }
}

binary_operator::binary_operator(expression lhs, oper_t op, expression rhs, comparison_order order)
            : lhs(std::move(lhs))
            , op(op)
            , rhs(std::move(rhs))
            , order(order) {
}

// Since column_identifier_raw is forward-declared in expression.hh, delay destructor instantiation here
unresolved_identifier::~unresolved_identifier() = default;

static cql3::raw_value evaluate(const bind_variable&, const evaluation_inputs&);
static cql3::raw_value evaluate(const tuple_constructor&, const evaluation_inputs&);
static cql3::raw_value evaluate(const collection_constructor&, const evaluation_inputs&);
static cql3::raw_value evaluate(const usertype_constructor&, const evaluation_inputs&);
static cql3::raw_value evaluate(const function_call&, const evaluation_inputs&);

namespace {

using children_t = std::vector<expression>; // conjunction's children.

children_t explode_conjunction(expression e) {
    return expr::visit(overloaded_functor{
            [] (const conjunction& c) { return std::move(c.children); },
            [&] (const auto&) { return children_t{std::move(e)}; },
        }, e);
}

using cql3::selection::selection;

/// Serialized values for all types of cells, plus selection (to find a column's index) and options (for
/// subscript expression's value).
struct row_data_from_partition_slice {
    const std::vector<bytes>& partition_key;
    const std::vector<bytes>& clustering_key;
    const std::vector<managed_bytes_opt>& other_columns;
    const selection& sel;
};

/// Returns col's value from queried data.
managed_bytes_opt get_value(const column_value& col, const evaluation_inputs& inputs) {
    auto cdef = col.col;
    switch (cdef->kind) {
        case column_kind::partition_key:
            return managed_bytes((*inputs.partition_key)[cdef->id]);
        case column_kind::clustering_key:
            return managed_bytes((*inputs.clustering_key)[cdef->id]);
        case column_kind::static_column:
            [[fallthrough]];
        case column_kind::regular_column: {
            int32_t index = inputs.selection->index_of(*cdef);
            if (index == -1) {
                throw std::runtime_error(
                        format("Column definition {} does not match any column in the query selection",
                        cdef->name_as_text()));
            }
            return managed_bytes_opt((*inputs.static_and_regular_columns)[index]);
        }
        default:
            throw exceptions::unsupported_operation_exception("Unknown column kind");
    }
}

managed_bytes_opt
get_value(const subscript& s, const evaluation_inputs& inputs) {
    const column_definition* cdef = get_subscripted_column(s).col;

    auto col_type = static_pointer_cast<const collection_type_impl>(cdef->type);
    int32_t index = inputs.selection->index_of(*cdef);
    if (index == -1) {
        throw std::runtime_error(
                format("Column definition {} does not match any column in the query selection",
                cdef->name_as_text()));
    }
    const managed_bytes_opt& serialized = (*inputs.static_and_regular_columns)[index];
    if (!serialized) {
        // For null[i] we return null.
        return std::nullopt;
    }
    const auto deserialized = cdef->type->deserialize(managed_bytes_view(*serialized));
    const auto key = evaluate(s.sub, inputs);
    auto&& key_type = col_type->is_map() ? col_type->name_comparator() : int32_type;
    if (key.is_null()) {
        // For m[null] return null.
        // This is different from Cassandra - which treats m[null]
        // as an invalid request error. But m[null] -> null is more
        // consistent with our usual null treatement (e.g., both
        // null[2] and null < 2 return null). It will also allow us
        // to support non-constant subscripts (e.g., m[a]) where "a"
        // may be null in some rows and non-null in others, and it's
        // not an error.
        return std::nullopt;
    }
    if (key.is_unset_value()) {
        // An m[?] with ? bound to UNSET_VALUE is a invalid query.
        // We could have detected it earlier while binding, but since
        // we currently don't, we must protect the following code
        // which can't work with an UNSET_VALUE. Note that the
        // placement of this check here means that in an empty table,
        // where we never need to evaluate the filter expression, this
        // error will not be detected.
        throw exceptions::invalid_request_exception(
            format("Unsupported unset map key for column {}",
                cdef->name_as_text()));
    }
    if (col_type->is_map()) {
        const auto& data_map = value_cast<map_type_impl::native_type>(deserialized);
        const auto found = key.view().with_linearized([&] (bytes_view key_bv) {
            using entry = std::pair<data_value, data_value>;
            return std::find_if(data_map.cbegin(), data_map.cend(), [&] (const entry& element) {
                return key_type->compare(element.first.serialize_nonnull(), key_bv) == 0;
            });
        });
        return found == data_map.cend() ? std::nullopt : managed_bytes_opt(found->second.serialize_nonnull());
    } else if (col_type->is_list()) {
        const auto& data_list = value_cast<list_type_impl::native_type>(deserialized);
        auto key_deserialized = key.view().with_linearized([&] (bytes_view key_bv) {
            return key_type->deserialize(key_bv);
        });
        auto key_int = value_cast<int32_t>(key_deserialized);
        if (key_int < 0 || size_t(key_int) >= data_list.size()) {
            return std::nullopt;
        }
        return managed_bytes_opt(data_list[key_int].serialize_nonnull());
    } else {
        throw exceptions::invalid_request_exception(format("subscripting non-map, non-list column {}", cdef->name_as_text()));
    }
}

/// True iff lhs's value equals rhs.
bool equal(const expression& lhs, const managed_bytes_opt& rhs, const evaluation_inputs& inputs) {
    if (!rhs) {
        return false;
    }
    const auto value = evaluate(lhs, inputs).to_managed_bytes_opt();
    if (!value) {
        return false;
    }
    return type_of(lhs)->equal(managed_bytes_view(*value), managed_bytes_view(*rhs));
}

/// Convenience overload for expression.
bool equal(const expression& lhs, const expression& rhs, const evaluation_inputs& inputs) {
    return equal(lhs, evaluate(rhs, inputs).to_managed_bytes_opt(), inputs);
}

/// True iff columns' values equal t.
bool equal(const tuple_constructor& columns_tuple_lhs, const expression& t_rhs, const evaluation_inputs& inputs) {
    const cql3::raw_value tup = evaluate(t_rhs, inputs);
    const auto& rhs = get_tuple_elements(tup, *type_of(t_rhs));
    if (rhs.size() != columns_tuple_lhs.elements.size()) {
        throw exceptions::invalid_request_exception(
                format("tuple equality size mismatch: {} elements on left-hand side, {} on right",
                       columns_tuple_lhs.elements.size(), rhs.size()));
    }
    return boost::equal(columns_tuple_lhs.elements, rhs,
    [&] (const expression& lhs, const managed_bytes_opt& b) {
        return equal(lhs, b, inputs);
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
bool limits(const expression& col, oper_t op, const expression& rhs, const evaluation_inputs& inputs) {
    if (!is_slice(op)) { // For EQ or NEQ, use equal().
        throw std::logic_error("limits() called on non-slice op");
    }
    auto lhs = evaluate(col, inputs).to_managed_bytes_opt();
    if (!lhs) {
        return false;
    }
    const auto b = evaluate(rhs, inputs).to_managed_bytes_opt();
    return b ? limits(*lhs, op, *b, type_of(col)->without_reversed()) : false;
}

/// True iff the column values are limited by t in the manner prescribed by op.
bool limits(const tuple_constructor& columns_tuple, const oper_t op, const expression& e,
            const evaluation_inputs& inputs) {
    if (!is_slice(op)) { // For EQ or NEQ, use equal().
        throw std::logic_error("limits() called on non-slice op");
    }
    const cql3::raw_value tup = evaluate(e, inputs);
    const auto& rhs = get_tuple_elements(tup, *type_of(e));
    if (rhs.size() != columns_tuple.elements.size()) {
        throw exceptions::invalid_request_exception(
                format("tuple comparison size mismatch: {} elements on left-hand side, {} on right",
                       columns_tuple.elements.size(), rhs.size()));
    }
    for (size_t i = 0; i < rhs.size(); ++i) {
        auto& cv = columns_tuple.elements[i];
        auto lhs = evaluate(cv, inputs).to_managed_bytes_opt();
        if (!lhs || !rhs[i]) {
            // CQL dictates that columns_tuple.elements[i] is a clustering column and non-null, but
            // let's not rely on grammar constraints that can be later relaxed.
            //
            // NULL = always fails comparison
            return false;
        }
        const auto cmp = type_of(cv)->without_reversed().compare(
                *lhs,
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
bool contains(const column_value& col, const raw_value_view& value, const evaluation_inputs& inputs) {
    const auto collection = get_value(col, inputs);
    if (collection) {
        return contains(col.col->type->deserialize(managed_bytes_view(*collection)), value);
    } else {
        return false;
    }
}

/// True iff a column is a map containing \p key.
bool contains_key(const column_value& col, cql3::raw_value_view key, const evaluation_inputs& inputs) {
    if (!key) {
        return true; // Compatible with old code, which skips null terms in key comparisons.
    }
    auto type = col.col->type;
    const auto collection = get_value(col, inputs);
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

} // anonymous namespace

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

namespace {

/// True iff cv matches the CQL LIKE pattern.
bool like(const column_value& cv, const raw_value_view& pattern, const evaluation_inputs& inputs) {
    if (!cv.col->type->is_string()) {
        throw exceptions::invalid_request_exception(
                format("LIKE is allowed only on string types, which {} is not", cv.col->name_as_text()));
    }
    auto value = get_value(cv, inputs);
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
bool is_one_of(const expression& col, const expression& rhs, const evaluation_inputs& inputs) {
    const cql3::raw_value in_list = evaluate(rhs, inputs);
    statements::request_validations::check_false(
            in_list.is_null(), "Invalid null value for column {}", col);

    return boost::algorithm::any_of(get_list_elements(in_list), [&] (const managed_bytes_opt& b) {
        return equal(col, b, inputs);
    });
}

/// True iff the tuple of column values is in the set defined by rhs.
bool is_one_of(const tuple_constructor& tuple, const expression& rhs, const evaluation_inputs& inputs) {
    cql3::raw_value in_list = evaluate(rhs, inputs);
    return boost::algorithm::any_of(get_list_of_tuples_elements(in_list, *type_of(rhs)), [&] (const std::vector<managed_bytes_opt>& el) {
        return boost::equal(tuple.elements, el, [&] (const expression& c, const managed_bytes_opt& b) {
            return equal(c, b, inputs);
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

bool is_satisfied_by(const binary_operator& opr, const evaluation_inputs& inputs) {
    return expr::visit(overloaded_functor{
            [&] (const column_value& col) {
                if (opr.op == oper_t::EQ) {
                    return equal(col, opr.rhs, inputs);
                } else if (opr.op == oper_t::NEQ) {
                    return !equal(col, opr.rhs, inputs);
                } else if (is_slice(opr.op)) {
                    return limits(col, opr.op, opr.rhs, inputs);
                } else if (opr.op == oper_t::CONTAINS) {
                    cql3::raw_value val = evaluate(opr.rhs, inputs);
                    return contains(col, val.view(), inputs);
                } else if (opr.op == oper_t::CONTAINS_KEY) {
                    cql3::raw_value val = evaluate(opr.rhs, inputs);
                    return contains_key(col, val.view(), inputs);
                } else if (opr.op == oper_t::LIKE) {
                    cql3::raw_value val = evaluate(opr.rhs, inputs);
                    return like(col, val.view(), inputs);
                } else if (opr.op == oper_t::IN) {
                    return is_one_of(col, opr.rhs, inputs);
                } else {
                    throw exceptions::unsupported_operation_exception(format("Unhandled binary_operator: {}", opr));
                }
            },
            [&] (const subscript& sub) {
                if (opr.op == oper_t::EQ) {
                    return equal(sub, opr.rhs, inputs);
                } else if (opr.op == oper_t::NEQ) {
                    return !equal(sub, opr.rhs, inputs);
                } else if (is_slice(opr.op)) {
                    return limits(sub, opr.op, opr.rhs, inputs);
                } else if (opr.op == oper_t::CONTAINS) {
                    throw exceptions::unsupported_operation_exception("CONTAINS lhs is subscripted");
                } else if (opr.op == oper_t::CONTAINS_KEY) {
                    throw exceptions::unsupported_operation_exception("CONTAINS KEY lhs is subscripted");
                } else if (opr.op == oper_t::LIKE) {
                    throw exceptions::unsupported_operation_exception("LIKE lhs is subscripted");
                } else if (opr.op == oper_t::IN) {
                    return is_one_of(sub, opr.rhs, inputs);
                } else {
                    throw exceptions::unsupported_operation_exception(format("Unhandled binary_operator: {}", opr));
                }
            },
            [&] (const tuple_constructor& cvs) {
                if (opr.op == oper_t::EQ) {
                    return equal(cvs, opr.rhs, inputs);
                } else if (is_slice(opr.op)) {
                    return limits(cvs, opr.op, opr.rhs, inputs);
                } else if (opr.op == oper_t::IN) {
                    return is_one_of(cvs, opr.rhs, inputs);
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
        }, opr.lhs);
}

} // anonymous namespace

bool is_satisfied_by(const expression& restr, const evaluation_inputs& inputs) {
    return expr::visit(overloaded_functor{
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
                    return is_satisfied_by(c, inputs);
                });
            },
            [&] (const binary_operator& opr) { return is_satisfied_by(opr, inputs); },
            [] (const column_value&) -> bool {
                on_internal_error(expr_logger, "is_satisfied_by: a column cannot serve as a restriction by itself");
            },
            [] (const subscript&) -> bool {
                on_internal_error(expr_logger, "is_satisfied_by: a subscript cannot serve as a restriction by itself");
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

namespace {

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
        const expression& e, const query_options& options, const serialized_compare& comparator,
        sstring_view column_name) {
    const cql3::raw_value in_list = evaluate(e, options);
    if (in_list.is_unset_value()) {
        throw exceptions::invalid_request_exception(format("Invalid unset value for column {}", column_name));
    }
    statements::request_validations::check_false(in_list.is_null(), "Invalid null value for column {}", column_name);
    utils::chunked_vector<managed_bytes> list_elems = get_list_elements(in_list);
    return to_sorted_vector(std::move(list_elems) | non_null | deref, comparator);
}

/// Returns possible values for k-th column from t, which must be RHS of IN.
value_list get_IN_values(const expression& e, size_t k, const query_options& options,
                         const serialized_compare& comparator) {
    const cql3::raw_value in_list = evaluate(e, options);
    const auto split_values = get_list_of_tuples_elements(in_list, *type_of(e)); // Need lvalue from which to make std::view.
    const auto result_range = split_values
            | boost::adaptors::transformed([k] (const std::vector<managed_bytes_opt>& v) { return v[k]; }) | non_null | deref;
    return to_sorted_vector(std::move(result_range), comparator);
}

static constexpr bool inclusive = true, exclusive = false;

} // anonymous namespace

const column_value& get_subscripted_column(const subscript& sub) {
    if (!is<column_value>(sub.val)) {
        on_internal_error(expr_logger,
            fmt::format("Only columns can be subscripted using the [] operator, got {}", sub.val));
    }
    return as<column_value>(sub.val);
}

const column_value& get_subscripted_column(const expression& e) {
    return visit(overloaded_functor {
        [](const column_value& cval) -> const column_value& { return cval; },
        [](const subscript& sub) -> const column_value& {
            return get_subscripted_column(sub);
        },
        [&](const auto&) -> const column_value& {
            on_internal_error(expr_logger,
                fmt::format("get_subscripted_column called on bad expression variant: {}", e));
        }
    }, e);
}

expression make_conjunction(expression a, expression b) {
    auto children = explode_conjunction(std::move(a));
    boost::copy(explode_conjunction(std::move(b)), back_inserter(children));
    return conjunction{std::move(children)};
}

static
void
do_factorize(std::vector<expression>& factors, expression e) {
    if (auto c = expr::as_if<conjunction>(&e)) {
        for (auto&& element : c->children) {
            do_factorize(factors, std::move(element));
        }
    } else {
        factors.push_back(std::move(e));
    }
}

std::vector<expression>
boolean_factors(expression e) {
    std::vector<expression> ret;
    do_factorize(ret, std::move(e));
    return ret;
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
    const auto type = cdef ? &cdef->type->without_reversed() : long_type.get();
    return expr::visit(overloaded_functor{
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
                return expr::visit(overloaded_functor{
                        [&] (const column_value& col) -> value_set {
                            if (!cdef || cdef != col.col) {
                                return unbounded_value_set;
                            }
                            if (is_compare(oper.op)) {
                                managed_bytes_opt val = evaluate(oper.rhs, options).to_managed_bytes_opt();
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
                        [&] (const subscript& s) -> value_set {
                            on_internal_error(expr_logger, "possible_lhs_values: subscripts are not supported as the LHS of a binary expression");
                        },
                        [&] (const tuple_constructor& tuple) -> value_set {
                            if (!cdef) {
                                return unbounded_value_set;
                            }
                            const auto found = boost::find_if(
                                    tuple.elements, [&] (const expression& c) { return expr::as<column_value>(c).col == cdef; });
                            if (found == tuple.elements.end()) {
                                return unbounded_value_set;
                            }
                            const auto column_index_on_lhs = std::distance(tuple.elements.begin(), found);
                            if (is_compare(oper.op)) {
                                // RHS must be a tuple due to upstream checks.
                                managed_bytes_opt val = get_tuple_elements(evaluate(oper.rhs, options), *type_of(oper.rhs)).at(column_index_on_lhs);
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
                            const auto val = evaluate(oper.rhs, options).to_managed_bytes_opt();
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
                    }, oper.lhs);
            },
            [] (const column_value&) -> value_set {
                on_internal_error(expr_logger, "possible_lhs_values: a column cannot serve as a restriction by itself");
            },
            [] (const subscript&) -> value_set {
                on_internal_error(expr_logger, "possible_lhs_values: a subscript cannot serve as a restriction by itself");
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
    return expr::visit(overloaded_functor{
            [&] (const conjunction& conj) {
                return boost::algorithm::all_of(conj.children, std::bind(is_supported_by, _1, idx));
            },
            [&] (const binary_operator& oper) {
                return expr::visit(overloaded_functor{
                        [&] (const column_value& col) {
                            return idx.supports_expression(*col.col, oper.op);
                        },
                        [&] (const tuple_constructor& tuple) {
                            if (tuple.elements.size() == 1) {
                                if (auto column = expr::as_if<column_value>(&tuple.elements[0])) {
                                    return idx.supports_expression(*column->col, oper.op);
                                }
                            }
                            // We don't use index table for multi-column restrictions, as it cannot avoid filtering.
                            return false;
                        },
                        [&] (const token&) { return false; },
                        [&] (const subscript& s) -> bool {
                            // We don't support indexes on map entries yet.
                            return false;
                        },
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
                    }, oper.lhs);
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

bool index_supports_some_column(
        const expression& e,
        const secondary_index::secondary_index_manager& index_manager,
        allow_local_index allow_local) {
    single_column_restrictions_map single_col_restrictions = get_single_column_restrictions_map(e);

    for (auto&& [col, col_restrictions] : single_col_restrictions) {
        if (has_supporting_index(col_restrictions, index_manager, allow_local)) {
            return true;
        }
    }

    return false;
}

std::ostream& operator<<(std::ostream& os, const column_value& cv) {
    os << cv.col->name_as_text();
    return os;
}

std::ostream& operator<<(std::ostream& os, const expression& expr) {
    expression::printer pr {
        .expr_to_print = expr,
        .debug_mode = true
    };

    return os << pr;
}

std::ostream& operator<<(std::ostream& os, const expression::printer& pr) {
    // Wraps expression in expression::printer to forward formatting options
    auto to_printer = [&pr] (const expression& expr) -> expression::printer {
        return expression::printer {
            .expr_to_print = expr,
            .debug_mode = pr.debug_mode
        };
    };

    expr::visit(overloaded_functor{
            [&] (const constant& v) {
                if (pr.debug_mode) {
                    os << v.view();
                } else {
                    if (v.value.is_null()) {
                        os << "null";
                    } else if (v.value.is_unset_value()) {
                        os << "unset";
                    } else {
                        v.value.view().with_value([&](const FragmentedView auto& bytes_view) {
                            data_value deser_val = v.type->deserialize(bytes_view);
                            os << deser_val.to_parsable_string();
                        });
                    }
                }
            },
            [&] (const conjunction& conj) {
                fmt::print(os, "({})", fmt::join(conj.children | transformed(to_printer), ") AND ("));
            },
            [&] (const binary_operator& opr) {
                if (pr.debug_mode) {
                    os << "(" << to_printer(opr.lhs) << ") " << opr.op << ' ' << to_printer(opr.rhs);
                } else {
                    if (opr.op == oper_t::IN && is<collection_constructor>(opr.rhs)) {
                        tuple_constructor rhs_tuple {
                            .elements = as<collection_constructor>(opr.rhs).elements
                        };
                        os << to_printer(opr.lhs) << ' ' << opr.op << ' ' << to_printer(rhs_tuple);
                    } else if (opr.op == oper_t::IN && is<constant>(opr.rhs) && as<constant>(opr.rhs).type->without_reversed().is_list()) {
                        tuple_constructor rhs_tuple;
                        const list_type_impl* list_typ = dynamic_cast<const list_type_impl*>(&as<constant>(opr.rhs).type->without_reversed());
                        for (const managed_bytes& elem : get_list_elements(as<constant>(opr.rhs).value)) {
                            rhs_tuple.elements.push_back(constant(raw_value::make_value(elem), list_typ->get_elements_type()));
                        }
                        os << to_printer(opr.lhs) << ' ' << opr.op << ' ' << to_printer(rhs_tuple);
                    } else {
                        os << to_printer(opr.lhs) << ' ' << opr.op << ' ' << to_printer(opr.rhs);
                    }
                }
            },
            [&] (const token& t) {
                fmt::print(os, "token({})", fmt::join(t.args | transformed(to_printer), ", "));
            },
            [&] (const column_value& col) {
                fmt::print(os, "{}", cql3::util::maybe_quote(col.col->name_as_text()));
            },
            [&] (const subscript& sub) {
                fmt::print(os, "{}[{}]", to_printer(sub.val), to_printer(sub.sub));
            },
            [&] (const unresolved_identifier& ui) {
                if (pr.debug_mode) {
                    fmt::print(os, "unresolved({})", *ui.ident);
                } else {
                    fmt::print(os, "{}", cql3::util::maybe_quote(ui.ident->to_string()));
                }
            },
            [&] (const column_mutation_attribute& cma)  {
                fmt::print(os, "{}({})",
                        cma.kind == column_mutation_attribute::attribute_kind::ttl ? "TTL" : "WRITETIME",
                        to_printer(cma.column));
            },
            [&] (const function_call& fc)  {
                std::visit(overloaded_functor{
                    [&] (const functions::function_name& named) {
                        fmt::print(os, "{}({})", named, fmt::join(fc.args | transformed(to_printer), ", "));
                    },
                    [&] (const shared_ptr<functions::function>& anon) {
                        fmt::print(os, "<anonymous function>({})", fmt::join(fc.args | transformed(to_printer), ", "));
                    },
                }, fc.func);
            },
            [&] (const cast& c)  {
                std::visit(overloaded_functor{
                    [&] (const cql3_type& t) {
                        fmt::print(os, "({} AS {})", to_printer(c.arg), t);
                    },
                    [&] (const shared_ptr<cql3_type::raw>& t) {
                        fmt::print(os, "({}) {}", t, to_printer(c.arg));
                    },
                }, c.type);
            },
            [&] (const field_selection& fs)  {
                fmt::print(os, "({}.{})", to_printer(fs.structure), fs.field);
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
                fmt::print(os, "({})", fmt::join(tc.elements | transformed(to_printer), ", "));
            },
            [&] (const collection_constructor& cc) {
                switch (cc.style) {
                case collection_constructor::style_type::list: {
                    fmt::print(os, "[{}]", fmt::join(cc.elements | transformed(to_printer), ", "));
                    return;
                }
                case collection_constructor::style_type::set: {
                    fmt::print(os, "{{{}}}", fmt::join(cc.elements | transformed(to_printer), ", "));
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
                        auto& tuple = expr::as<tuple_constructor>(e);
                        if (tuple.elements.size() != 2) {
                            on_internal_error(expr_logger, "map constructor element is not a tuple of arity 2");
                        }
                        fmt::print(os, "{}:{}", to_printer(tuple.elements[0]), to_printer(tuple.elements[1]));
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
                    fmt::print(os, "{}:{}", k, to_printer(v));
                }
                fmt::print(os, "}}");
            },
        }, pr.expr_to_print);
    return os;
}

sstring to_string(const expression& expr) {
    return fmt::format("{}", expr);
}

bool is_on_collection(const binary_operator& b) {
    if (b.op == oper_t::CONTAINS || b.op == oper_t::CONTAINS_KEY) {
        return true;
    }
    if (auto tuple = expr::as_if<tuple_constructor>(&b.lhs)) {
        return boost::algorithm::any_of(tuple->elements, [] (const expression& v) { return expr::is<subscript>(v); });
    }
    return false;
}

bool contains_column(const column_definition& column, const expression& e) {
    const column_value* find_res = find_in_expression<column_value>(e,
        [&](const column_value& column_val) -> bool {
            return (*column_val.col == column);
        }
    );
    return find_res != nullptr;
}

bool contains_nonpure_function(const expression& e) {
    const function_call* find_res = find_in_expression<function_call>(e,
        [&](const function_call& fc) {
            return std::visit(overloaded_functor {
                [](const functions::function_name&) -> bool {
                    on_internal_error(expr_logger,
                    "contains_nonpure_function called on unprepared expression - expected function pointer, got name");
                },
                [](const ::shared_ptr<functions::function>& fun) -> bool {
                    return !fun->is_pure();
                }
            }, fc.func);
        }
    );
    return find_res != nullptr;
}

bool has_eq_restriction_on_column(const column_definition& column, const expression& e) {
    std::function<bool(const expression&)> column_in_lhs = [&](const expression& e) -> bool {
        return visit(overloaded_functor {
            [&](const column_value& cv) {
                // Use column_defintion::operator== for comparison,
                // columns with the same name but different schema will not be equal.
                return *cv.col == column;
            },
            [&](const tuple_constructor& tc) {
                for (const expression& elem : tc.elements) {
                    if (column_in_lhs(elem)) {
                        return true;
                    }
                }

                return false;
            },
            [&](const auto&) {return false;}
        }, e);
    };

    // Look for binary operator describing eq relation with this column on lhs
    const binary_operator* eq_restriction_search_res = find_binop(e, [&](const binary_operator& b) {
        if (b.op != oper_t::EQ) {
            return false;
        }

        if (!column_in_lhs(b.lhs)) {
            return false;
        }

        // These conditions are not allowed to occur in the current code,
        // but they might be allowed in the future.
        // They are added now to avoid surprises later.
        //
        // These conditions detect cases like:
        // WHERE column1 = column2
        // WHERE column1 = row_number()
        if (contains_column(column, b.rhs) || contains_nonpure_function(b.rhs)) {
            return false;
        }

        return true;
    });

    return eq_restriction_search_res != nullptr;
}

expression replace_column_def(const expression& expr, const column_definition* new_cdef) {
    return search_and_replace(expr, [&] (const expression& expr) -> std::optional<expression> {
        if (expr::is<column_value>(expr)) {
            return column_value{new_cdef};
        } else if (expr::is<tuple_constructor>(expr)) {
            throw std::logic_error(format("replace_column_def invalid with column tuple: {}", to_string(expr)));
        } else {
            return std::nullopt;
        }
    });
}

expression replace_token(const expression& expr, const column_definition* new_cdef) {
    return search_and_replace(expr, [&] (const expression& expr) -> std::optional<expression> {
        if (expr::is<token>(expr)) {
            return column_value{new_cdef};
        } else {
            return std::nullopt;
        }
    });
}

bool recurse_until(const expression& e, const noncopyable_function<bool (const expression&)>& predicate_fun) {
    if (auto res = predicate_fun(e)) {
        return res;
    }
    return expr::visit(overloaded_functor{
            [&] (const binary_operator& op) {
                if (auto found = recurse_until(op.lhs, predicate_fun)) {
                    return found;
                }
                return recurse_until(op.rhs, predicate_fun);
            },
            [&] (const conjunction& conj) {
                for (auto& child : conj.children) {
                    if (auto found = recurse_until(child, predicate_fun)) {
                        return found;
                    }
                }
                return false;
            },
            [&] (const subscript& sub) {
                if (recurse_until(sub.val, predicate_fun)) {
                    return true;
                }
                return recurse_until(sub.sub, predicate_fun);
            },
            [&] (const column_mutation_attribute& a) {
                return recurse_until(a.column, predicate_fun);
            },
            [&] (const function_call& fc) {
                for (auto& arg : fc.args) {
                    if (auto found = recurse_until(arg, predicate_fun)) {
                        return found;
                    }
                }
                return false;
            },
            [&] (const cast& c) {
                return recurse_until(c.arg, predicate_fun);
            },
            [&] (const field_selection& fs) {
                return recurse_until(fs.structure, predicate_fun);
            },
            [&] (const tuple_constructor& t) {
                for (auto& e : t.elements) {
                    if (auto found = recurse_until(e, predicate_fun)) {
                        return found;
                    }
                }
                return false;
            },
            [&] (const collection_constructor& c) {
                for (auto& e : c.elements) {
                    if (auto found = recurse_until(e, predicate_fun)) {
                        return found;
                    }
                }
                return false;
            },
            [&] (const usertype_constructor& c) {
                for (auto& [k, v] : c.elements) {
                    if (auto found = recurse_until(v, predicate_fun)) {
                        return found;
                    }
                }
                return false;
            },
            [&] (const token& tok) {
                for (auto& a : tok.args) {
                    if (auto found = recurse_until(a, predicate_fun)) {
                        return found;
                    }
                }
                return false;
            },
            [](LeafExpression auto const&) {
                return false;
            }
        }, e);
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
        return expr::visit(
            overloaded_functor{
                [&] (const conjunction& conj) -> expression {
                    return conjunction{
                        boost::copy_range<std::vector<expression>>(
                            conj.children | boost::adaptors::transformed(recurse)
                        )
                    };
                },
                [&] (const binary_operator& oper) -> expression {
                    return binary_operator(recurse(oper.lhs), oper.op, recurse(oper.rhs));
                },
                [&] (const column_mutation_attribute& cma) -> expression {
                    return column_mutation_attribute{cma.kind, recurse(cma.column)};
                },
                [&] (const tuple_constructor& tc) -> expression {
                    return tuple_constructor{
                        boost::copy_range<std::vector<expression>>(
                            tc.elements | boost::adaptors::transformed(recurse)
                        ),
                        tc.type
                    };
                },
                [&] (const collection_constructor& c) -> expression {
                    return collection_constructor{
                        c.style,
                        boost::copy_range<std::vector<expression>>(
                            c.elements | boost::adaptors::transformed(recurse)
                        ),
                        c.type
                    };
                },
                [&] (const usertype_constructor& uc) -> expression {
                    usertype_constructor::elements_map_type m;
                    for (auto& [k, v] : uc.elements) {
                        m.emplace(k, recurse(v));
                    }
                    return usertype_constructor{std::move(m), uc.type};
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
                    return cast{recurse(c.arg), c.type};
                },
                [&] (const field_selection& fs) -> expression {
                    return field_selection{recurse(fs.structure), fs.field};
                },
                [&] (const subscript& s) -> expression {
                    return subscript {
                        .val = recurse(s.val),
                        .sub = recurse(s.sub)
                    };
                    throw std::runtime_error("expr: search_and_replace - subscript not added to expression yet");
                },
                [&](const token& tok) -> expression {
                    return token {
                        boost::copy_range<std::vector<expression>>(
                            tok.args | boost::adaptors::transformed(recurse)
                        )
                    };
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
                expr::visit(*this, child);
            }
        }

        void operator()(const binary_operator& oper) {
            if (current_binary_operator != nullptr) {
                on_internal_error(expr_logger,
                    "extract_single_column_restrictions_for_column: nested binary operators are not supported");
            }

            current_binary_operator = &oper;
            expr::visit(*this, oper.lhs);
            current_binary_operator = nullptr;
        }

        void operator()(const column_value& cv) {
            if (*cv.col == column && current_binary_operator != nullptr) {
                restrictions.emplace_back(*current_binary_operator);
            }
        }

        void operator()(const subscript& s) {
            const column_value& cv = get_subscripted_column(s);
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

    expr::visit(v, expr);

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

    return value.view().size_bytes() == 0;
}

bool constant::is_null_or_unset() const {
    return is_null() || is_unset_value();
}

cql3::raw_value_view constant::view() const {
    return value.view();
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

    return constant_val.view().deserialize<bool>(*boolean_type);
}

cql3::raw_value evaluate(const expression& e, const evaluation_inputs& inputs) {
    return expr::visit(overloaded_functor {
        [](const binary_operator&) -> cql3::raw_value {
            on_internal_error(expr_logger, "Can't evaluate a binary_operator");
        },
        [](const conjunction&) -> cql3::raw_value {
            on_internal_error(expr_logger, "Can't evaluate a conjunction");
        },
        [](const token&) -> cql3::raw_value {
            on_internal_error(expr_logger, "Can't evaluate token");
        },
        [](const unresolved_identifier&) -> cql3::raw_value {
            on_internal_error(expr_logger, "Can't evaluate unresolved_identifier");
        },
        [](const column_mutation_attribute&) -> cql3::raw_value {
            on_internal_error(expr_logger, "Can't evaluate a column_mutation_attribute");
        },
        [&](const cast& c) -> cql3::raw_value {
            auto ret = evaluate(c.arg, inputs);
            auto type = std::get_if<data_type>(&c.type);
            if (!type) {
                on_internal_error(expr_logger, "attempting to evaluate an unprepared cast");
            }
            return ret;
        },
        [](const field_selection&) -> cql3::raw_value {
            on_internal_error(expr_logger, "Can't evaluate a field_selection");
        },

        [&](const column_value& cv) -> cql3::raw_value {
            return raw_value::make_value(get_value(cv, inputs));
        },
        [&](const subscript& s) -> cql3::raw_value {
            return raw_value::make_value(get_value(s, inputs));
        },
        [](const untyped_constant&) -> cql3::raw_value {
            on_internal_error(expr_logger, "Can't evaluate a untyped_constant ");
        },

        [](const null&) { return cql3::raw_value::make_null(); },
        [](const constant& c) { return c.value; },
        [&](const bind_variable& bind_var) { return evaluate(bind_var, inputs); },
        [&](const tuple_constructor& tup) { return evaluate(tup, inputs); },
        [&](const collection_constructor& col) { return evaluate(col, inputs); },
        [&](const usertype_constructor& user_val) { return evaluate(user_val, inputs); },
        [&](const function_call& fun_call) { return evaluate(fun_call, inputs); }
    }, e);
}

cql3::raw_value evaluate(const expression& e, const query_options& options) {
    return evaluate(e, evaluation_inputs{.options = &options});
}

// Takes a value and reserializes it where needs_to_be_reserialized() says it's needed
template <FragmentedView View>
static managed_bytes reserialize_value(View value_bytes,
                                       const abstract_type& type,
                                       const cql_serialization_format& sf) {
    if (type.is_list()) {
        utils::chunked_vector<managed_bytes> elements = partially_deserialize_listlike(value_bytes, sf);

        const abstract_type& element_type = dynamic_cast<const list_type_impl&>(type).get_elements_type()->without_reversed();
        if (element_type.bound_value_needs_to_be_reserialized(sf)) {
            for (managed_bytes& element : elements) {
                element = reserialize_value(managed_bytes_view(element), element_type, sf);
            }
        }

        return collection_type_impl::pack_fragmented(
            elements.begin(),
            elements.end(),
            elements.size(), cql_serialization_format::internal()
        );
    }

    if (type.is_set()) {
        utils::chunked_vector<managed_bytes> elements = partially_deserialize_listlike(value_bytes, sf);

        const abstract_type& element_type = dynamic_cast<const set_type_impl&>(type).get_elements_type()->without_reversed();
        if (element_type.bound_value_needs_to_be_reserialized(sf)) {
            for (managed_bytes& element : elements) {
                element = reserialize_value(managed_bytes_view(element), element_type, sf);
            }
        }

        std::set<managed_bytes, serialized_compare> values_set(element_type.as_less_comparator());
        for (managed_bytes& element : elements) {
            values_set.emplace(std::move(element));
        }

        return collection_type_impl::pack_fragmented(
            values_set.begin(),
            values_set.end(),
            values_set.size(), cql_serialization_format::internal()
        );
    }

    if (type.is_map()) {
        std::vector<std::pair<managed_bytes, managed_bytes>> elements = partially_deserialize_map(value_bytes, sf);

        const map_type_impl mapt = dynamic_cast<const map_type_impl&>(type);
        const abstract_type& key_type = mapt.get_keys_type()->without_reversed();
        const abstract_type& value_type = mapt.get_values_type()->without_reversed();

        if (key_type.bound_value_needs_to_be_reserialized(sf)) {
            for (std::pair<managed_bytes, managed_bytes>& element : elements) {
                element.first = reserialize_value(managed_bytes_view(element.first), key_type, sf);
            }
        }

        if (value_type.bound_value_needs_to_be_reserialized(sf)) {
            for (std::pair<managed_bytes, managed_bytes>& element : elements) {
                element.second = reserialize_value(managed_bytes_view(element.second), value_type, sf);
            }
        }

        std::map<managed_bytes, managed_bytes, serialized_compare> values_map(key_type.as_less_comparator());
        for (std::pair<managed_bytes, managed_bytes>& element : elements) {
            values_map.emplace(std::move(element));
        }

       return map_type_impl::serialize_to_managed_bytes(values_map);
    }

    if (type.is_tuple() || type.is_user_type()) {
        const tuple_type_impl& ttype = dynamic_cast<const tuple_type_impl&>(type);
        std::vector<managed_bytes_opt> elements = ttype.split_fragmented(value_bytes);

        for (std::size_t i = 0; i < elements.size(); i++) {
            const abstract_type& element_type = ttype.all_types().at(i)->without_reversed();
            if (elements[i].has_value() && element_type.bound_value_needs_to_be_reserialized(sf)) {
                elements[i] = reserialize_value(managed_bytes_view(*elements[i]), element_type, sf);
            }
        }

        return tuple_type_impl::build_value_fragmented(std::move(elements));
    }

    on_internal_error(expr_logger,
        fmt::format("Reserializing type that shouldn't need reserialization: {}", type.name()));
}

static cql3::raw_value evaluate(const bind_variable& bind_var, const evaluation_inputs& inputs) {
    if (bind_var.receiver.get() == nullptr) {
        on_internal_error(expr_logger,
            "evaluate(bind_variable) called with nullptr receiver, should be prepared first");
    }

    cql3::raw_value_view value = inputs.options->get_value_at(bind_var.bind_index);

    if (value.is_null()) {
        return cql3::raw_value::make_null();
    }

    if (value.is_unset_value()) {
        return cql3::raw_value::make_unset_value();
    }

    const abstract_type& value_type = bind_var.receiver->type->without_reversed();
    try {
        value.validate(value_type, inputs.options->get_cql_serialization_format());
    } catch (const marshal_exception& e) {
        throw exceptions::invalid_request_exception(format("Exception while binding column {:s}: {:s}",
                                                           bind_var.receiver->name->to_cql_string(), e.what()));
    }

    if (value_type.bound_value_needs_to_be_reserialized(inputs.options->get_cql_serialization_format())) {
        managed_bytes new_value = value.with_value([&] (const FragmentedView auto& value_bytes) {
            return reserialize_value(value_bytes, value_type, inputs.options->get_cql_serialization_format());
        });

        return raw_value::make_value(std::move(new_value));
    }

    return raw_value::make_value(value);
}

static cql3::raw_value evaluate(const tuple_constructor& tuple, const evaluation_inputs& inputs) {
    if (tuple.type.get() == nullptr) {
        on_internal_error(expr_logger,
            "evaluate(tuple_constructor) called with nullptr type, should be prepared first");
    }

    std::vector<managed_bytes_opt> tuple_elements;
    tuple_elements.reserve(tuple.elements.size());

    for (size_t i = 0; i < tuple.elements.size(); i++) {
        cql3::raw_value elem_val = evaluate(tuple.elements[i], inputs);
        if (elem_val.is_unset_value()) {
            throw exceptions::invalid_request_exception(format("Invalid unset value for tuple field number {:d}", i));
        }

        tuple_elements.emplace_back(std::move(elem_val).to_managed_bytes_opt());
    }

    cql3::raw_value raw_val =
        cql3::raw_value::make_value(tuple_type_impl::build_value_fragmented(std::move(tuple_elements)));

    return raw_val;
}

// Range of managed_bytes
template <typename Range>
requires requires (Range listlike_range) { {*listlike_range.begin()} -> std::convertible_to<const managed_bytes&>; }
static managed_bytes serialize_listlike(const Range& elements, const char* collection_name) {
    if (elements.size() > std::numeric_limits<int32_t>::max()) {
        throw exceptions::invalid_request_exception(fmt::format("{} size too large: {} > {}",
            collection_name, elements.size(), std::numeric_limits<int32_t>::max()));
    }

    for (const managed_bytes& element : elements) {
        if (element.size() > std::numeric_limits<int32_t>::max()) {
            throw exceptions::invalid_request_exception(fmt::format("{} element size too large: {} bytes > {}",
                collection_name, elements.size(), std::numeric_limits<int32_t>::max()));
        }
    }

    return collection_type_impl::pack_fragmented(
        elements.begin(),
        elements.end(),
        elements.size(),
        cql_serialization_format::internal()
    );
}

static cql3::raw_value evaluate_list(const collection_constructor& collection,
                              const evaluation_inputs& inputs,
                              bool skip_null = false) {
    std::vector<managed_bytes> evaluated_elements;
    evaluated_elements.reserve(collection.elements.size());

    for (const expression& element : collection.elements) {
        cql3::raw_value evaluated_element = evaluate(element, inputs);

        if (evaluated_element.is_unset_value()) {
            throw exceptions::invalid_request_exception("unset value is not supported inside collections");
        }

        if (evaluated_element.is_null()) {
            if (skip_null) {
                continue;
            }

            throw exceptions::invalid_request_exception("null is not supported inside collections");
        }

        evaluated_elements.emplace_back(std::move(evaluated_element).to_managed_bytes());
    }

    managed_bytes collection_bytes = serialize_listlike(evaluated_elements, "List");
    return raw_value::make_value(std::move(collection_bytes));
}

static cql3::raw_value evaluate_set(const collection_constructor& collection, const evaluation_inputs& inputs) {
    const set_type_impl& stype = dynamic_cast<const set_type_impl&>(collection.type->without_reversed());
    std::set<managed_bytes, serialized_compare> evaluated_elements(stype.get_elements_type()->as_less_comparator());

    for (const expression& element : collection.elements) {
        cql3::raw_value evaluated_element = evaluate(element, inputs);

        if (evaluated_element.is_null()) {
            throw exceptions::invalid_request_exception("null is not supported inside collections");
        }

        if (evaluated_element.is_unset_value()) {
            throw exceptions::invalid_request_exception("unset value is not supported inside collections");
        }

        if (evaluated_element.view().size_bytes() > std::numeric_limits<uint16_t>::max()) {
            // TODO: Behaviour copied from sets::delayed_value::bind(), but this seems incorrect
            // The original reasoning is:
            // "We don't support values > 64K because the serialization format encode the length as an unsigned short."
            // but CQL uses int32_t to encode length of a set value length
            throw exceptions::invalid_request_exception(format("Set value is too long. Set values are limited to {:d} bytes but {:d} bytes value provided",
                                                std::numeric_limits<uint16_t>::max(),
                                                evaluated_element.view().size_bytes()));
        }

        evaluated_elements.emplace(std::move(evaluated_element).to_managed_bytes());
    }

    managed_bytes collection_bytes = serialize_listlike(evaluated_elements, "Set");
    return raw_value::make_value(std::move(collection_bytes));
}

static cql3::raw_value evaluate_map(const collection_constructor& collection, const evaluation_inputs& inputs) {
    const map_type_impl& mtype = dynamic_cast<const map_type_impl&>(collection.type->without_reversed());
    std::map<managed_bytes, managed_bytes, serialized_compare> evaluated_elements(mtype.get_keys_type()->as_less_comparator());

    for (const expression& element : collection.elements) {
        if (auto tuple = expr::as_if<tuple_constructor>(&element)) {
            cql3::raw_value key = evaluate(tuple->elements.at(0), inputs);
            cql3::raw_value value = evaluate(tuple->elements.at(1), inputs);

            if (key.is_null() || value.is_null()) {
                throw exceptions::invalid_request_exception("null is not supported inside collections");
            }

            if (key.is_unset_value() || value.is_unset_value()) {
                throw exceptions::invalid_request_exception("unset value is not supported inside collections");
            }

            if (key.view().size_bytes() > std::numeric_limits<uint16_t>::max()) {
                // TODO: Behaviour copied from maps::delayed_value::bind(), but this seems incorrect
                // The original reasoning is:
                // "We don't support values > 64K because the serialization format encode the length as an unsigned short."
                // but CQL uses int32_t to encode length of a map key
                throw exceptions::invalid_request_exception(format("Map key is too long. Map keys are limited to {:d} bytes but {:d} bytes keys provided",
                                                   std::numeric_limits<uint16_t>::max(),
                                                   key.view().size_bytes()));
            }

            evaluated_elements.emplace(std::move(key).to_managed_bytes(),
                                       std::move(value).to_managed_bytes());
        } else {
            cql3::raw_value pair = evaluate(element, inputs);
            std::vector<managed_bytes_opt> map_pair = get_tuple_elements(pair, *type_of(element));

            if (!map_pair.at(0).has_value() || !map_pair.at(1).has_value()) {
                throw exceptions::invalid_request_exception("null is not supported inside collections");
            }

            evaluated_elements.emplace(std::move(*map_pair.at(0)), std::move(*map_pair.at(1)));
        }
    }

    managed_bytes serialized_map = map_type_impl::serialize_to_managed_bytes(evaluated_elements);
    return raw_value::make_value(std::move(serialized_map));
}

static cql3::raw_value evaluate(const collection_constructor& collection, const evaluation_inputs& inputs) {
    if (collection.type.get() == nullptr) {
        on_internal_error(expr_logger,
            "evaluate(collection_constructor) called with nullptr type, should be prepared first");
    }

    switch (collection.style) {
        case collection_constructor::style_type::list:
            return evaluate_list(collection, inputs);

        case collection_constructor::style_type::set:
            return evaluate_set(collection, inputs);

        case collection_constructor::style_type::map:
            return evaluate_map(collection, inputs);
    }
    std::abort();
}

static cql3::raw_value evaluate(const usertype_constructor& user_val, const evaluation_inputs& inputs) {
    if (user_val.type.get() == nullptr) {
        on_internal_error(expr_logger,
            "evaluate(usertype_constructor) called with nullptr type, should be prepared first");
    }

    const user_type_impl& utype = dynamic_cast<const user_type_impl&>(user_val.type->without_reversed());
    if (user_val.elements.size() != utype.size()) {
        on_internal_error(expr_logger,
            "evaluate(usertype_constructor) called with bad number of elements, should be prepared first");
    }

    std::vector<managed_bytes_opt> field_values;
    field_values.reserve(utype.size());

    for (std::size_t i = 0; i < utype.size(); i++) {
        column_identifier field_id(to_bytes(utype.field_name(i)), utf8_type);
        auto cur_field = user_val.elements.find(field_id);

        if (cur_field == user_val.elements.end()) {
            on_internal_error(expr_logger, fmt::format(
                "evaluate(usertype_constructor) called without a value for field {}, should be prepared first",
                utype.field_name_as_string(i)));
        }

        cql3::raw_value field_val = evaluate(cur_field->second, inputs);
        if (field_val.is_unset_value()) {
            throw exceptions::invalid_request_exception(format(
                "Invalid unset value for field '{}' of user defined type ", utype.field_name_as_string(i)));
        }

        field_values.emplace_back(std::move(field_val).to_managed_bytes_opt());
    }

    raw_value val_bytes = cql3::raw_value::make_value(tuple_type_impl::build_value_fragmented(field_values));
    return val_bytes;
}

static cql3::raw_value evaluate(const function_call& fun_call, const evaluation_inputs& inputs) {
    const shared_ptr<functions::function>* fun = std::get_if<shared_ptr<functions::function>>(&fun_call.func);
    if (fun == nullptr) {
        throw std::runtime_error("Can't evaluate function call with name only, should be prepared earlier");
    }

    // Can't use static_cast<> because function is a virtual base class of scalar_function
    functions::scalar_function* scalar_fun = dynamic_cast<functions::scalar_function*>(fun->get());
    if (scalar_fun == nullptr) {
        throw std::runtime_error("Only scalar functions can be evaluated using evaluate()");
    }

    std::vector<bytes_opt> arguments;
    arguments.reserve(fun_call.args.size());

    for (const expression& arg : fun_call.args) {
        cql3::raw_value arg_val = evaluate(arg, inputs);
        if (arg_val.is_null_or_unset()) {
            throw exceptions::invalid_request_exception(format("Invalid null or unset value for argument to {}", *scalar_fun));
        }

        arguments.emplace_back(to_bytes_opt(std::move(arg_val)));
    }

    bool has_cache_id = fun_call.lwt_cache_id.get() != nullptr && fun_call.lwt_cache_id->has_value();
    if (has_cache_id) {
        computed_function_values::mapped_type* cached_value =
            inputs.options->find_cached_pk_function_call(**fun_call.lwt_cache_id);
        if (cached_value != nullptr) {
            return raw_value::make_value(*cached_value);
        }
    }

    bytes_opt result = scalar_fun->execute(cql_serialization_format::internal(), arguments);

    if (has_cache_id) {
        inputs.options->cache_pk_function_call(**fun_call.lwt_cache_id, result);
    }

    if (!result.has_value()) {
        return cql3::raw_value::make_null();
    }

    try {
        scalar_fun->return_type()->validate(*result, cql_serialization_format::internal());
    } catch (marshal_exception&) {
        throw runtime_exception(format("Return of function {} ({}) is not a valid value for its declared return type {}",
                                       *scalar_fun, to_hex(result),
                                       scalar_fun->return_type()->as_cql3_type()
                                       ));
    }

    return raw_value::make_value(std::move(*result));
}

static void ensure_can_get_value_elements(const cql3::raw_value& val,
                                          const char* caller_name) {
    if (val.is_null()) {
        on_internal_error(expr_logger, fmt::format("{} called with null value", caller_name));
    }

    if (val.is_unset_value()) {
        on_internal_error(expr_logger, fmt::format("{} called with unset value", caller_name));
    }
}

utils::chunked_vector<managed_bytes> get_list_elements(const cql3::raw_value& val) {
    ensure_can_get_value_elements(val, "expr::get_list_elements");

    return val.view().with_value([](const FragmentedView auto& value_bytes) {
        return partially_deserialize_listlike(value_bytes, cql_serialization_format::internal());
    });
}

utils::chunked_vector<managed_bytes> get_set_elements(const cql3::raw_value& val) {
    ensure_can_get_value_elements(val, "expr::get_set_elements");

    return val.view().with_value([](const FragmentedView auto& value_bytes) {
        return partially_deserialize_listlike(value_bytes, cql_serialization_format::internal());
    });
}

std::vector<std::pair<managed_bytes, managed_bytes>> get_map_elements(const cql3::raw_value& val) {
    ensure_can_get_value_elements(val, "expr::get_map_elements");

    return val.view().with_value([](const FragmentedView auto& value_bytes) {
        return partially_deserialize_map(value_bytes, cql_serialization_format::internal());
    });
}

std::vector<managed_bytes_opt> get_tuple_elements(const cql3::raw_value& val, const abstract_type& type) {
    ensure_can_get_value_elements(val, "expr::get_tuple_elements");

    return val.view().with_value([&](const FragmentedView auto& value_bytes) {
        const tuple_type_impl& ttype = static_cast<const tuple_type_impl&>(type.without_reversed());
        return ttype.split_fragmented(value_bytes);
    });
}

std::vector<managed_bytes_opt> get_user_type_elements(const cql3::raw_value& val, const abstract_type& type) {
    ensure_can_get_value_elements(val, "expr::get_user_type_elements");

    return val.view().with_value([&](const FragmentedView auto& value_bytes) {
        const user_type_impl& utype = static_cast<const user_type_impl&>(type.without_reversed());
        return utype.split_fragmented(value_bytes);
    });
}

static std::vector<managed_bytes_opt> convert_listlike(utils::chunked_vector<managed_bytes>&& elements) {
    return std::vector<managed_bytes_opt>(std::make_move_iterator(elements.begin()),
                                          std::make_move_iterator(elements.end()));
}

std::vector<managed_bytes_opt> get_elements(const cql3::raw_value& val, const abstract_type& type) {
    const abstract_type& val_type = type.without_reversed();

    switch (val_type.get_kind()) {
        case abstract_type::kind::list:
            return convert_listlike(get_list_elements(val));

        case abstract_type::kind::set:
            return convert_listlike(get_set_elements(val));

        case abstract_type::kind::tuple:
            return get_tuple_elements(val, type);

        case abstract_type::kind::user:
            return get_user_type_elements(val, type);

        default:
            on_internal_error(expr_logger, fmt::format("expr::get_elements called on bad type: {}", type.name()));
    }
}

utils::chunked_vector<std::vector<managed_bytes_opt>> get_list_of_tuples_elements(const cql3::raw_value& val, const abstract_type& type) {
    utils::chunked_vector<managed_bytes> elements = get_list_elements(val);
    const list_type_impl& list_typ = dynamic_cast<const list_type_impl&>(type.without_reversed());
    const tuple_type_impl& tuple_typ = dynamic_cast<const tuple_type_impl&>(*list_typ.get_elements_type());

    utils::chunked_vector<std::vector<managed_bytes_opt>> tuples_list;
    tuples_list.reserve(elements.size());

    for (managed_bytes& element : elements) {
        std::vector<managed_bytes_opt> cur_tuple = tuple_typ.split_fragmented(managed_bytes_view(element));
        tuples_list.emplace_back(std::move(cur_tuple));
    }

    return tuples_list;
}

void fill_prepare_context(expression& e, prepare_context& ctx) {
    expr::visit(overloaded_functor {
        [&](bind_variable& bind_var) {
            ctx.add_variable_specification(bind_var.bind_index, bind_var.receiver);
        },
        [&](collection_constructor& c) {
            for (expr::expression& element : c.elements) {
                fill_prepare_context(element, ctx);
            }
        },
        [&](tuple_constructor& t) {
            for (expr::expression& element : t.elements) {
                fill_prepare_context(element, ctx);
            }
        },
        [&](usertype_constructor& u) {
            for (auto& [field_name, field_val] : u.elements) {
                fill_prepare_context(field_val, ctx);
            }
        },
        [&](function_call& f) {
            const shared_ptr<functions::function>& func = std::get<shared_ptr<functions::function>>(f.func);
            if (ctx.is_processing_pk_restrictions() && !func->is_pure()) {
                ctx.add_pk_function_call(f);
            }

            for (expr::expression& argument : f.args) {
                fill_prepare_context(argument, ctx);
            }
        },
        [&](binary_operator& binop) {
            fill_prepare_context(binop.lhs, ctx);
            fill_prepare_context(binop.rhs, ctx);
        },
        [&](conjunction& c) {
            for (expression& child : c.children) {
                fill_prepare_context(child, ctx);
            }
        },
        [&](token& tok) {
            for (expression& arg : tok.args) {
                fill_prepare_context(arg, ctx);
            }
        },
        [](unresolved_identifier&) {},
        [&](column_mutation_attribute& a) {
            fill_prepare_context(a.column, ctx);
        },
        [&](cast& c) {
            fill_prepare_context(c.arg, ctx);
        },
        [&](field_selection& fs) {
            fill_prepare_context(fs.structure, ctx);
        },
        [](column_value& cv) {},
        [&](subscript& s) {
            fill_prepare_context(s.val, ctx);
            fill_prepare_context(s.sub, ctx);
        },
        [](untyped_constant&) {},
        [](null&) {},
        [](constant&) {},
    }, e);
}

bool contains_bind_marker(const expression& e) {
    const bind_variable* search_res = find_in_expression<bind_variable>(e, [](const bind_variable&) { return true; });
    return search_res != nullptr;
}

size_t count_if(const expression& e, const noncopyable_function<bool (const binary_operator&)>& f) {
    size_t ret = 0;
    recurse_until(e, [&] (const expression& e) {
        if (auto op = as_if<binary_operator>(&e)) {
            ret += f(*op) ? 1 : 0;
        }
        return false;
    });
    return ret;
}

data_type
type_of(const expression& e) {
    return visit(overloaded_functor{
        [] (const conjunction& e) {
            return boolean_type;
        },
        [] (const binary_operator& e) {
            // All our binary operators are relations
            return boolean_type;
        },
        [] (const column_value& e) {
            return e.col->type;
        },
        [] (const token& e) {
            return long_type;
        },
        [] (const unresolved_identifier& e) -> data_type {
            on_internal_error(expr_logger, "evaluating type of unresolved_identifier");
        },
        [] (const column_mutation_attribute& e) {
            switch (e.kind) {
            case column_mutation_attribute::attribute_kind::writetime:
                return long_type;
            case column_mutation_attribute::attribute_kind::ttl:
                return int32_type;
            }
            on_internal_error(expr_logger, "evaluating type of illegal column mutation attribute kind");
        },
        [] (const function_call& e) {
            return std::visit(overloaded_functor{
                [] (const functions::function_name& unprepared) -> data_type {
                    on_internal_error(expr_logger, "evaluating type of unprepared function call");
                },
                [] (const shared_ptr<functions::function>& f) {
                    return f->return_type();
                },
            }, e.func);
        },
        [] (const bind_variable& e) {
            return e.receiver->type;
        },
        [] (const untyped_constant& e) -> data_type {
            on_internal_error(expr_logger, "evaluating type of untyped_constant call");
        },
        [] (const cast& e) {
            return std::visit(overloaded_functor{
                [] (const shared_ptr<cql3_type::raw>& unprepared) -> data_type {
                    on_internal_error(expr_logger, "evaluating type of unprepared cast");
                },
                [] (const data_type& t) {
                    return t;
                },
            }, e.type);
        },
        [] (const ExpressionElement auto& e) -> data_type {
            return e.type;
        }
    }, e);
}

static std::optional<std::reference_wrapper<const column_value>> get_single_column_restriction_column(const expression& e) {
    if (find_in_expression<unresolved_identifier>(e, [](const auto&) {return true;})) {
        on_internal_error(expr_logger,
            format("get_single_column_restriction_column expects a prepared expression, but it's not: {}}", e));
    }

    const column_value* the_only_column = nullptr;
    bool expression_is_single_column = false;

    for_each_expression<column_value>(e,
        [&](const column_value& cval) {
            if (the_only_column == nullptr) {
                // It's the first column_value we've encountered - set it as the only column
                the_only_column = &cval;
                expression_is_single_column = true;
                return;
            }

            if (cval.col != the_only_column->col) {
                // In case any other column is encountered the restriction
                // restricts more than one column.
                expression_is_single_column = false;
            }
        }
    );

    if (expression_is_single_column) {
        return std::cref(*the_only_column);
    } else {
        return std::nullopt;
    }
}

bool is_single_column_restriction(const expression& e) {
    return get_single_column_restriction_column(e).has_value();
}

const column_value& get_the_only_column(const expression& e) {
    std::optional<std::reference_wrapper<const column_value>> result = get_single_column_restriction_column(e);

    if (!result.has_value()) {
        on_internal_error(expr_logger,
            format("get_the_only_column - bad expression: {}", e));
    }

    return *result;
}


bool schema_pos_column_definition_comparator::operator()(const column_definition *def1, const column_definition *def2) const {
    auto column_pos = [](const column_definition* cdef) -> uint32_t {
        if (cdef->is_primary_key()) {
            return cdef->id;
        }

        return std::numeric_limits<uint32_t>::max();
    };

    uint32_t pos1 = column_pos(def1);
    uint32_t pos2 = column_pos(def2);
    if (pos1 != pos2) {
        return pos1 < pos2;
    }
    // FIXME: shouldn't we use regular column name comparator here? Origin does not...
    return less_unsigned(def1->name(), def2->name());
}

std::vector<const column_definition*> get_sorted_column_defs(const expression& e) {
    std::set<const column_definition*, schema_pos_column_definition_comparator> cols;

    for_each_expression<column_value>(e,
        [&](const column_value& cval) {
            cols.insert(cval.col);
        }
    );

    std::vector<const column_definition*> result;
    result.reserve(cols.size());
    for (const column_definition* col : cols) {
        result.push_back(col);
    }
    return result;
}

const column_definition* get_last_column_def(const expression& e) {
    std::vector<const column_definition*> sorted_defs = get_sorted_column_defs(e);

    if (sorted_defs.empty()) {
        return nullptr;
    }

    return sorted_defs.back();
}

single_column_restrictions_map get_single_column_restrictions_map(const expression& e) {
    single_column_restrictions_map result;

    std::vector<const column_definition*> sorted_defs = get_sorted_column_defs(e);
    for (const column_definition* cdef : sorted_defs) {
        expression col_restrictions = conjunction {
            .children = extract_single_column_restrictions_for_column(e, *cdef)
        };
        result.emplace(cdef, std::move(col_restrictions));
    }

    return result;
}

bool is_empty_restriction(const expression& e) {
    bool contains_non_conjunction = recurse_until(e, [&](const expression& e) -> bool {
        return !is<conjunction>(e);
    });

    return !contains_non_conjunction;
}

sstring get_columns_in_commons(const expression& a, const expression& b) {
    std::vector<const column_definition*> ours = get_sorted_column_defs(a);
    std::vector<const column_definition*> theirs = get_sorted_column_defs(b);

    std::sort(ours.begin(), ours.end());
    std::sort(theirs.begin(), theirs.end());
    std::vector<const column_definition*> common;
    std::set_intersection(ours.begin(), ours.end(), theirs.begin(), theirs.end(), std::back_inserter(common));

    sstring str;
    for (auto&& c : common) {
        if (!str.empty()) {
            str += " ,";
        }
        str += c->name_as_text();
    }
    return str;
}

bytes_opt value_for(const column_definition& cdef, const expression& e, const query_options& options) {
    value_set possible_vals = possible_lhs_values(&cdef, e, options);
    return std::visit(overloaded_functor {
        [&](const value_list& val_list) -> bytes_opt {
            if (val_list.empty()) {
                return std::nullopt;
            }

            if (val_list.size() != 1) {
                on_internal_error(expr_logger, format("expr::value_for - multiple possible values for column: {}", e));
            }

            return to_bytes(val_list.front());
        },
        [&](const nonwrapping_range<managed_bytes>&) -> bytes_opt {
            on_internal_error(expr_logger, format("expr::value_for - possible values are a range: {}", e));
        }
    }, possible_vals);
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
} // namespace expr
} // namespace cql3
