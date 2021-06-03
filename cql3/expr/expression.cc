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
#include "index/secondary_index_manager.hh"
#include "types/list.hh"
#include "types/map.hh"
#include "types/set.hh"
#include "utils/like_matcher.hh"

namespace cql3 {
namespace expr {

using boost::adaptors::filtered;
using boost::adaptors::transformed;

namespace {

static
managed_bytes_opt do_get_value(const schema& schema,
        const column_definition& cdef,
        const partition_key& key,
        const clustering_key_prefix& ckey,
        const row& cells,
        gc_clock::time_point now) {
    switch (cdef.kind) {
        case column_kind::partition_key:
            return managed_bytes(key.get_component(schema, cdef.component_index()));
        case column_kind::clustering_key:
            return managed_bytes(ckey.get_component(schema, cdef.component_index()));
        default:
            auto cell = cells.find_cell(cdef.id);
            if (!cell) {
                return std::nullopt;
            }
            assert(cdef.is_atomic());
            auto c = cell->as_atomic_cell(cdef);
            return c.is_dead(now) ? std::nullopt : managed_bytes_opt(c.value());
    }
}

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

/// Data used to derive cell values from a mutation.
struct row_data_from_mutation {
    // Underscores avoid name clashes.
    const partition_key& partition_key_;
    const clustering_key_prefix& clustering_key_;
    const row& other_columns;
    const schema& schema_;
    gc_clock::time_point now;
};

/// Everything needed to compute column values during restriction evaluation.
struct column_value_eval_bag {
    const query_options& options; // For evaluating subscript terms.
    std::variant<row_data_from_partition_slice, row_data_from_mutation> row_data;
};

/// Returns col's value from queried data.
managed_bytes_opt get_value_from_partition_slice(
        const column_value& col, row_data_from_partition_slice data, const query_options& options) {
    auto cdef = col.col;
    if (col.sub) {
        auto col_type = static_pointer_cast<const collection_type_impl>(cdef->type);
        if (!col_type->is_map()) {
            throw exceptions::invalid_request_exception(format("subscripting non-map column {}", cdef->name_as_text()));
        }
        const auto deserialized = cdef->type->deserialize(managed_bytes_view(*data.other_columns[data.sel.index_of(*cdef)]));
        const auto& data_map = value_cast<map_type_impl::native_type>(deserialized);
        const auto key = col.sub->bind_and_get(options);
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

/// Returns col's value from a mutation.
managed_bytes_opt get_value_from_mutation(const column_value& col, row_data_from_mutation data) {
    return do_get_value(
            data.schema_, *col.col, data.partition_key_, data.clustering_key_, data.other_columns, data.now);
}

/// Returns col's value from the fetched data.
managed_bytes_opt get_value(const column_value& col, const column_value_eval_bag& bag) {
    using std::placeholders::_1;
    return std::visit(overloaded_functor{
            std::bind(get_value_from_mutation, col, _1),
            std::bind(get_value_from_partition_slice, col, _1, bag.options),
        }, bag.row_data);
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

/// If t represents a tuple value, returns that value.  Otherwise, null.
///
/// Useful for checking binary_operator::rhs, which packs multiple values into a single term when lhs is itself
/// a tuple.  NOT useful for the IN operator, whose rhs is either a list or tuples::in_value.
::shared_ptr<tuples::value> get_tuple(term& t, const query_options& opts) {
    return dynamic_pointer_cast<tuples::value>(t.bind(opts));
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
    return equal(to_managed_bytes_opt(rhs.bind_and_get(bag.options)), lhs, bag);
}

/// True iff columns' values equal t.
bool equal(term& t, const column_value_tuple& columns_tuple, const column_value_eval_bag& bag) {
    const auto tup = get_tuple(t, bag.options);
    if (!tup) {
        throw exceptions::invalid_request_exception("multi-column equality has right-hand side that isn't a tuple");
    }
    const auto& rhs = tup->get_elements();
    if (rhs.size() != columns_tuple.elements.size()) {
        throw exceptions::invalid_request_exception(
                format("tuple equality size mismatch: {} elements on left-hand side, {} on right",
                       columns_tuple.elements.size(), rhs.size()));
    }
    return boost::equal(rhs, columns_tuple.elements, [&] (const managed_bytes_opt& b, const column_value& lhs) {
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
    const auto b = to_managed_bytes_opt(rhs.bind_and_get(bag.options));
    return b ? limits(*lhs, op, *b, *get_value_comparator(col)) : false;
}

/// True iff the column values are limited by t in the manner prescribed by op.
bool limits(const column_value_tuple& columns_tuple, const oper_t op, term& t,
            const column_value_eval_bag& bag) {
    if (!is_slice(op)) { // For EQ or NEQ, use equal().
        throw std::logic_error("limits() called on non-slice op");
    }
    const auto tup = get_tuple(t, bag.options);
    if (!tup) {
        throw exceptions::invalid_request_exception(
                "multi-column comparison has right-hand side that isn't a tuple");
    }
    const auto& rhs = tup->get_elements();
    if (rhs.size() != columns_tuple.elements.size()) {
        throw exceptions::invalid_request_exception(
                format("tuple comparison size mismatch: {} elements on left-hand side, {} on right",
                       columns_tuple.elements.size(), rhs.size()));
    }
    for (size_t i = 0; i < rhs.size(); ++i) {
        const auto cmp = get_value_comparator(columns_tuple.elements[i])->compare(
                // CQL dictates that columns[i] is a clustering column and non-null.
                *get_value(columns_tuple.elements[i], bag),
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
    // RHS is prepared differently for different CQL cases.  Cast it dynamically to discern which case this is.
    if (auto dv = dynamic_cast<lists::delayed_value*>(&rhs)) {
        // This is `a IN (1,2,3)`.  RHS elements are themselves terms.
        return boost::algorithm::any_of(dv->get_elements(), [&] (const ::shared_ptr<term>& t) {
                return equal(*t, col, bag);
            });
    } else if (auto mkr = dynamic_cast<lists::marker*>(&rhs)) {
        // This is `a IN ?`.  RHS elements are values representable as bytes_opt.
        const auto values = static_pointer_cast<lists::value>(mkr->bind(bag.options));
        statements::request_validations::check_not_null(
                values, "Invalid null value for column %s", col.col->name_as_text());
        return boost::algorithm::any_of(values->get_elements(), [&] (const managed_bytes_opt& b) {
            return equal(b, col, bag);
        });
    }
    throw std::logic_error("unexpected term type in is_one_of(single column)");
}

/// True iff the tuple of column values is in the set defined by rhs.
bool is_one_of(const column_value_tuple& tuple, term& rhs, const column_value_eval_bag& bag) {
    // RHS is prepared differently for different CQL cases.  Cast it dynamically to discern which case this is.
    if (auto dv = dynamic_cast<lists::delayed_value*>(&rhs)) {
        // This is `(a,b) IN ((1,1),(2,2),(3,3))`.  RHS elements are themselves terms.
        return boost::algorithm::any_of(dv->get_elements(), [&] (const ::shared_ptr<term>& t) {
                return equal(*t, tuple, bag);
            });
    } else if (auto mkr = dynamic_cast<tuples::in_marker*>(&rhs)) {
        // This is `(a,b) IN ?`.  RHS elements are themselves tuples, represented as vector<managed_bytes_opt>.
        const auto marker_value = static_pointer_cast<tuples::in_value>(mkr->bind(bag.options));
        return boost::algorithm::any_of(marker_value->get_split_values(), [&] (const std::vector<managed_bytes_opt>& el) {
                return boost::equal(tuple.elements, el, [&] (const column_value& c, const managed_bytes_opt& b) {
                    return equal(b, c, bag);
                });
            });
    }
    throw std::logic_error("unexpected term type in is_one_of(multi-column)");
}

/// True iff op means bnd type of bound.
bool matches(oper_t op, statements::bound bnd) {
    switch (op) {
    case oper_t::GT:
    case oper_t::GTE:
        return is_start(bnd); // These set a lower bound.
    case oper_t::LT:
    case oper_t::LTE:
        return is_end(bnd); // These set an upper bound.
    case oper_t::EQ:
        return true; // Bounds from both sides.
    default:
        return false;
    }
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
                    return contains(col, opr.rhs->bind_and_get(bag.options), bag);
                } else if (opr.op == oper_t::CONTAINS_KEY) {
                    return contains_key(col, opr.rhs->bind_and_get(bag.options), bag);
                } else if (opr.op == oper_t::LIKE) {
                    return like(col, opr.rhs->bind_and_get(bag.options), bag);
                } else if (opr.op == oper_t::IN) {
                    return is_one_of(col, *opr.rhs, bag);
                } else {
                    throw exceptions::unsupported_operation_exception(format("Unhandled binary_operator: {}", opr));
                }
            },
            [&] (const column_value_tuple& cvs) {
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
        }, opr.lhs);
}

bool is_satisfied_by(const expression& restr, const column_value_eval_bag& bag) {
    return std::visit(overloaded_functor{
            [&] (bool v) { return v; },
            [&] (const conjunction& conj) {
                return boost::algorithm::all_of(conj.children, [&] (const expression& c) {
                    return is_satisfied_by(c, bag);
                });
            },
            [&] (const binary_operator& opr) { return is_satisfied_by(opr, bag); },
        }, restr);
}

/// If t is a tuple, binds and gets its k-th element.  Otherwise, binds and gets t's whole value.
managed_bytes_opt get_kth(size_t k, const query_options& options, const ::shared_ptr<term>& t) {
    auto bound = t->bind(options);
    if (auto tup = dynamic_pointer_cast<tuples::value>(bound)) {
        return tup->get_elements()[k];
    } else {
        throw std::logic_error("non-tuple RHS for multi-column IN");
    }
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
    // RHS is prepared differently for different CQL cases.  Cast it dynamically to discern which case this is.
    if (auto dv = dynamic_pointer_cast<lists::delayed_value>(t)) {
        // Case `a IN (1,2,3)`.
        const auto result_range = dv->get_elements()
                | boost::adaptors::transformed([&] (const ::shared_ptr<term>& t) { return to_managed_bytes_opt(t->bind_and_get(options)); })
                | non_null | deref;
        return to_sorted_vector(std::move(result_range), comparator);
    } else if (auto mkr = dynamic_pointer_cast<lists::marker>(t)) {
        // Case `a IN ?`.  Collect all list-element values.
        const auto val = mkr->bind(options);
        if (val == constants::UNSET_VALUE) {
            throw exceptions::invalid_request_exception(format("Invalid unset value for column {}", column_name));
        }
        statements::request_validations::check_not_null(val, "Invalid null value for column %s", column_name);
        return to_sorted_vector(static_pointer_cast<lists::value>(val)->get_elements() | non_null | deref, comparator);
    }
    throw std::logic_error(format("get_IN_values(single column) on invalid term {}", *t));
}

/// Returns possible values for k-th column from t, which must be RHS of IN.
value_list get_IN_values(const ::shared_ptr<term>& t, size_t k, const query_options& options,
                         const serialized_compare& comparator) {
    // RHS is prepared differently for different CQL cases.  Cast it dynamically to discern which case this is.
    if (auto dv = dynamic_pointer_cast<lists::delayed_value>(t)) {
        // Case `(a,b) in ((1,1),(2,2),(3,3))`.  Get kth value from each term element.
        const auto result_range = dv->get_elements()
                | boost::adaptors::transformed(std::bind_front(get_kth, k, options)) | non_null | deref;
        return to_sorted_vector(std::move(result_range), comparator);
    } else if (auto mkr = dynamic_pointer_cast<tuples::in_marker>(t)) {
        // Case `(a,b) IN ?`.  Get kth value from each vector<bytes> element.
        const auto val = static_pointer_cast<tuples::in_value>(mkr->bind(options));
        const auto split_values = val->get_split_values(); // Need lvalue from which to make std::view.
        const auto result_range = split_values
                | boost::adaptors::transformed([k] (const std::vector<managed_bytes_opt>& v) { return v[k]; }) | non_null | deref;
        return to_sorted_vector(std::move(result_range), comparator);
    }
    throw std::logic_error(format("get_IN_values(multi-column) on invalid term {}", *t));
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

bool is_satisfied_by(
        const expression& restr,
        const schema& schema, const partition_key& key, const clustering_key_prefix& ckey, const row& cells,
        const query_options& options, gc_clock::time_point now) {
    return is_satisfied_by(restr, {options, row_data_from_mutation{key, ckey, cells, schema, now}});
}

std::vector<managed_bytes_opt> first_multicolumn_bound(
        const expression& restr, const query_options& options, statements::bound bnd) {
    auto found = find_atom(restr, [bnd] (const binary_operator& oper) {
        return matches(oper.op, bnd) && is_multi_column(oper);
    });
    if (found) {
        return static_pointer_cast<tuples::value>(found->rhs->bind(options))->get_elements();
    } else {
        return std::vector<managed_bytes_opt>{};
    }
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
            [] (bool b) {
                return b ? unbounded_value_set : empty_value_set;
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
                                managed_bytes_opt val = to_managed_bytes_opt(oper.rhs->bind_and_get(options));
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
                        [&] (const column_value_tuple& tuple) -> value_set {
                            if (!cdef) {
                                return unbounded_value_set;
                            }
                            const auto found = boost::find_if(
                                    tuple.elements, [&] (const column_value& c) { return c.col == cdef; });
                            if (found == tuple.elements.end()) {
                                return unbounded_value_set;
                            }
                            const auto column_index_on_lhs = std::distance(tuple.elements.begin(), found);
                            if (is_compare(oper.op)) {
                                // RHS must be a tuple due to upstream checks.
                                managed_bytes_opt val = get_tuple(*oper.rhs, options)->get_elements()[column_index_on_lhs];
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
                            const auto val = to_managed_bytes_opt(oper.rhs->bind_and_get(options));
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
                    }, oper.lhs);
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
                        [&] (const column_value_tuple& tuple) {
                            if (tuple.elements.size() == 1) {
                                return idx.supports_expression(*tuple.elements[0].col, oper.op);
                            }
                            // We don't use index table for multi-column restrictions, as it cannot avoid filtering.
                            return false;
                        },
                        [&] (const token&) { return false; },
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

std::ostream& operator<<(std::ostream& os, const column_value& cv) {
    os << cv.col->name_as_text();
    if (cv.sub) {
        os << '[' << *cv.sub << ']';
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, const expression& expr) {
    std::visit(overloaded_functor{
            [&] (bool b) { os << (b ? "TRUE" : "FALSE"); },
            [&] (const conjunction& conj) { fmt::print(os, "({})", fmt::join(conj.children, ") AND (")); },
            [&] (const binary_operator& opr) {
                std::visit(overloaded_functor{
                        [&] (const token& t) { os << "TOKEN"; },
                        [&] (const column_value& col) {
                            fmt::print(os, "{}", col);
                        },
                        [&] (const column_value_tuple& tuple) {
                            fmt::print(os, "({})", fmt::join(tuple.elements, ","));
                        },
                    }, opr.lhs);
                os << ' ' << opr.op << ' ' << *opr.rhs;
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
    if (auto tuple = std::get_if<column_value_tuple>(&b.lhs)) {
        return boost::algorithm::any_of(tuple->elements, [] (const column_value& v) { return v.sub; });
    }
    return false;
}

expression replace_column_def(const expression& expr, const column_definition* new_cdef) {
    return std::visit(overloaded_functor{
            [] (bool b){ return expression(b); },
            [&] (const conjunction& conj) {
                const auto applied = conj.children | transformed(
                        std::bind(replace_column_def, std::placeholders::_1, new_cdef));
                return expression(conjunction{std::vector(applied.begin(), applied.end())});
            },
            [&] (const binary_operator& oper) {
                return std::visit(overloaded_functor{
                        [&] (const column_value& col) {
                            return expression(binary_operator{column_value{new_cdef}, oper.op, oper.rhs});
                        },
                        [&] (const column_value_tuple& tuple) -> expression {
                            throw std::logic_error(format("replace_column_def invalid LHS: {}", to_string(oper)));
                        },
                        [&] (const token&) { return expr; },
                    }, oper.lhs);
            },
        }, expr);
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

} // namespace expr
} // namespace cql3


template <>
struct fmt::formatter<cql3::expr::expression> {
    constexpr auto parse(format_parse_context& ctx) {
        return ctx.end();
    }

    template <typename FormatContext>
    auto format(const cql3::expr::expression& expr, FormatContext& ctx) {
        std::ostringstream os;
        os << expr;
        return format_to(ctx.out(), "{}", os.str());
    }
};

template <>
struct fmt::formatter<cql3::expr::column_value> {
    constexpr auto parse(format_parse_context& ctx) {
        return ctx.end();
    }

    template <typename FormatContext>
    auto format(const cql3::expr::column_value& col, FormatContext& ctx) {
        std::ostringstream os;
        os << col;
        return format_to(ctx.out(), "{}", os.str());
    }
};
