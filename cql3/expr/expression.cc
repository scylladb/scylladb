/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "expression.hh"

#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expr-utils.hh"

#include <seastar/core/on_internal_error.hh>

#include <fmt/ostream.h>
#include <unordered_map>
#include <algorithm>
#include <ranges>

#include "cql3/selection/selection.hh"
#include "cql3/util.hh"
#include "index/secondary_index_manager.hh"
#include "types/list.hh"
#include "types/map.hh"
#include "types/set.hh"
#include "utils/like_matcher.hh"
#include "query-result-reader.hh"
#include "types/user.hh"
#include "cql3/functions/scalar_function.hh"
#include "cql3/functions/first_function.hh"
#include "cql3/prepare_context.hh"

namespace cql3 {
namespace expr {

logging::logger expr_logger("cql_expression");

bool operator==(const expression& e1, const expression& e2) {
    if (e1._v->v.index() != e2._v->v.index()) {
        return false;
    }
    return expr::visit([&] <typename T> (const T& e1_element) {
        const T& e2_element = expr::as<T>(e2);
        return e1_element == e2_element;
    }, e1);
}

expression::expression(const expression& o)
        : _v(std::make_unique<impl>(*o._v)) {
}

expression&
expression::operator=(const expression& o) {
    *this = expression(o);
    return *this;
}

binary_operator::binary_operator(expression lhs, oper_t op, expression rhs, comparison_order order)
            : lhs(std::move(lhs))
            , op(op)
            , rhs(std::move(rhs))
            , order(order) {
}

// Since column_identifier_raw is forward-declared in expression.hh, delay destructor instantiation here
unresolved_identifier::~unresolved_identifier() = default;

static cql3::raw_value do_evaluate(const bind_variable&, const evaluation_inputs&);
static cql3::raw_value do_evaluate(const tuple_constructor&, const evaluation_inputs&);
static cql3::raw_value do_evaluate(const collection_constructor&, const evaluation_inputs&);
static cql3::raw_value do_evaluate(const usertype_constructor&, const evaluation_inputs&);
static cql3::raw_value do_evaluate(const function_call&, const evaluation_inputs&);

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

} // anonymous

managed_bytes_opt
extract_column_value(const column_definition* cdef, const evaluation_inputs& inputs) {
    switch (cdef->kind) {
        case column_kind::partition_key:
            return managed_bytes(inputs.partition_key[cdef->id]);
        case column_kind::clustering_key:
            if (cdef->id >= inputs.clustering_key.size()) {
                // partial clustering key, or LWT non-existing row
                return std::nullopt;
            }
            return managed_bytes(inputs.clustering_key[cdef->id]);
        case column_kind::static_column:
            [[fallthrough]];
        case column_kind::regular_column: {
            int32_t index = inputs.selection->index_of(*cdef);
            if (index == -1) {
                throw std::runtime_error(
                        format("Column definition {} does not match any column in the query selection",
                        cdef->name_as_text()));
            }
            return managed_bytes_opt(inputs.static_and_regular_columns[index]);
        }
        default:
            throw exceptions::unsupported_operation_exception("Unknown column kind");
    }
}

namespace {

/// Returns col's value from queried data.
managed_bytes_opt get_value(const column_value& col, const evaluation_inputs& inputs) {
    return extract_column_value(col.col, inputs);
}

managed_bytes_opt
get_value(const subscript& s, const evaluation_inputs& inputs) {
    raw_value container_val = evaluate(s.val, inputs);
    auto serialized = std::move(container_val).to_managed_bytes_opt();
    if (!serialized) {
        // For null[i] we return null.
        return std::nullopt;
    }
    auto col_type = static_pointer_cast<const collection_type_impl>(type_of(s.val));
    const auto deserialized = type_of(s.val)->deserialize(managed_bytes_view(*serialized));
    const auto key = evaluate(s.sub, inputs);
    auto&& key_type = col_type->is_list() ? int32_type : col_type->name_comparator();
    if (key.is_null()) {
        // For m[null] return null.
        // This is different from Cassandra - which treats m[null]
        // as an invalid request error. But m[null] -> null is more
        // consistent with our usual null treatment (e.g., both
        // null[2] and null < 2 return null). It will also allow us
        // to support non-constant subscripts (e.g., m[a]) where "a"
        // may be null in some rows and non-null in others, and it's
        // not an error.
        return std::nullopt;
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
    } else if (col_type->is_set()) {
        const auto& data_set = value_cast<set_type_impl::native_type>(deserialized);
        const auto found = key.view().with_linearized([&] (bytes_view key_bv) {
            using entry = data_value;
            return std::find_if(data_set.cbegin(), data_set.cend(), [&] (const entry& element) {
                return key_type->compare(element.serialize_nonnull(), key_bv) == 0;
            });
        });
        return found == data_set.cend() ? std::nullopt : managed_bytes_opt(found->serialize_nonnull());
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
        throw exceptions::invalid_request_exception(fmt::format("subscripting non-map, non-list column {:user}", s.val));
    }
}

// This class represents a value that can be one of three things:
// false, true or null.
// It could be represented by std::optional<bool>, but optional
// can be implicitly casted to bool, which might cause mistakes.
// (bool)(std::make_optional<bool>(false)) will return true,
// despite the fact that the represented value is `false`.
// To avoid any such problems this class is introduced
// along with the is_true() method, which can be used
// to check if the value held is indeed `true`.
class bool_or_null {
    std::optional<bool> value;
public:
    bool_or_null(bool val) : value(val) {}
    bool_or_null(null_value) : value(std::nullopt) {}

    static bool_or_null null() {
        return bool_or_null(null_value{});
    }
    bool has_value() const {
        return value.has_value();
    }
    bool is_null() const {
        return !has_value();
    }
    const bool& get_value() const {
        return *value;
    }
    bool is_true() const {
        return has_value() && get_value();
    }
};

/// True iff lhs's value equals rhs.
bool_or_null equal(const expression& lhs, const managed_bytes_opt& rhs_bytes, const evaluation_inputs& inputs) {
    raw_value lhs_value = evaluate(lhs, inputs);
    if (lhs_value.is_null() || !rhs_bytes.has_value()) {
        return bool_or_null::null();
    }
    managed_bytes lhs_bytes = std::move(lhs_value).to_managed_bytes();

    return type_of(lhs)->equal(managed_bytes_view(lhs_bytes), managed_bytes_view(*rhs_bytes));
}

static std::pair<managed_bytes_opt, managed_bytes_opt> evaluate_binop_sides(const expression& lhs,
                                                                                   const expression& rhs,
                                                                                   const oper_t op,
                                                                                   const evaluation_inputs& inputs) {
    raw_value lhs_value = evaluate(lhs, inputs);
    raw_value rhs_value = evaluate(rhs, inputs);

    managed_bytes_opt lhs_bytes = std::move(lhs_value).to_managed_bytes_opt();
    managed_bytes_opt rhs_bytes = std::move(rhs_value).to_managed_bytes_opt();
    return std::pair(std::move(lhs_bytes), std::move(rhs_bytes));
}

bool_or_null equal(const expression& lhs, const expression& rhs, const evaluation_inputs& inputs, null_handling_style null_handling) {
    std::pair<managed_bytes_opt, managed_bytes_opt> sides_bytes =
        evaluate_binop_sides(lhs, rhs, oper_t::EQ, inputs);

    if (null_handling == null_handling_style::lwt_nulls && (!sides_bytes.first || !sides_bytes.second)) {
        return bool(sides_bytes.first) == bool(sides_bytes.second);
    }

    if (!sides_bytes.first || !sides_bytes.second) {
        return bool_or_null::null();
    }
    auto [lhs_bytes, rhs_bytes] = std::move(sides_bytes);

    return type_of(lhs)->equal(managed_bytes_view(*lhs_bytes), managed_bytes_view(*rhs_bytes));
}

bool_or_null not_equal(const expression& lhs, const expression& rhs, const evaluation_inputs& inputs, null_handling_style null_handling) {
    std::pair<managed_bytes_opt, managed_bytes_opt> sides_bytes =
        evaluate_binop_sides(lhs, rhs, oper_t::NEQ, inputs);

    if (null_handling == null_handling_style::lwt_nulls && (!sides_bytes.first || !sides_bytes.second)) {
        return bool(sides_bytes.first) != bool(sides_bytes.second);
    }

    if (!sides_bytes.first || !sides_bytes.second) {
        return bool_or_null::null();
    }
    auto [lhs_bytes, rhs_bytes] = std::move(sides_bytes);

    return !type_of(lhs)->equal(managed_bytes_view(*lhs_bytes), managed_bytes_view(*rhs_bytes));
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

bool list_contains_null(managed_bytes_view list) {
    return std::ranges::contains(partially_deserialize_listlike(list), managed_bytes_opt());
}

/// True iff the column value is limited by rhs in the manner prescribed by op.
bool_or_null limits(const expression& lhs, oper_t op, null_handling_style null_handling, const expression& rhs, const evaluation_inputs& inputs) {
    if (!is_slice(op)) { // For EQ or NEQ, use equal().
        throw std::logic_error("limits() called on non-slice op");
    }
    std::pair<managed_bytes_opt, managed_bytes_opt> sides_bytes =
        evaluate_binop_sides(lhs, rhs, op, inputs);

    if (!sides_bytes.first || !sides_bytes.second) {
        switch (null_handling) {
        case null_handling_style::sql:
            return bool_or_null::null();
        case null_handling_style::lwt_nulls:
            switch (op) {
            case oper_t::LT:
            case oper_t::LTE:
            case oper_t::GT:
            case oper_t::GTE:
                if (!sides_bytes.second) {
                    throw exceptions::invalid_request_exception(fmt::format("Invalid comparison with null for operator \"{}\"", op));
                } else {
                    // LWT < > <= >= throws only for NULL second operand, not first
                    return false;
                }
            case oper_t::CONTAINS:
            case oper_t::CONTAINS_KEY:
            case oper_t::LIKE:
            case oper_t::IS_NOT: // IS_NOT doesn't really belong here, luckily this is never reached.
                throw exceptions::invalid_request_exception(fmt::format("Invalid comparison with null for operator \"{}\"", op));
            case oper_t::EQ:
                return !sides_bytes.first == !sides_bytes.second;
            case oper_t::NEQ:
                return !sides_bytes.first != !sides_bytes.second;
            case oper_t::IN:
                if (!sides_bytes.second.has_value()) {
                    // Nothing can be IN a list if there is no list.
                    return false;
                } else {
                    return list_contains_null(*sides_bytes.second);
                }
            case oper_t::NOT_IN:
                on_internal_error(expr_logger, "NOT IN operator on limits(), despite being rejected via !is_slice()");
            }
        }
    }
    // at this point, both sides are non-NULL

    auto [lhs_bytes, rhs_bytes] = std::move(sides_bytes);

    return limits(*lhs_bytes, op, *rhs_bytes, type_of(lhs)->without_reversed());
}

/// True iff a column is a collection containing value.
bool_or_null contains(const expression& lhs, const expression& rhs, const evaluation_inputs& inputs) {
    std::pair<managed_bytes_opt, managed_bytes_opt> sides_bytes =
        evaluate_binop_sides(lhs, rhs, oper_t::CONTAINS, inputs);
    if (!sides_bytes.first || !sides_bytes.second) {
        return bool_or_null::null();
    }

    const abstract_type& lhs_type = type_of(lhs)->without_reversed();
    data_value lhs_collection = lhs_type.deserialize(managed_bytes_view(*sides_bytes.first));

    const collection_type_impl* collection_type = dynamic_cast<const collection_type_impl*>(&lhs_type);
    data_type element_type =
        collection_type->is_set() ? collection_type->name_comparator() : collection_type->value_comparator();

    auto exists_in = [&](auto&& range) {
        auto found = std::find_if(range.begin(), range.end(), [&](auto&& element) {
            return element_type->compare(managed_bytes_view(element.serialize_nonnull()), *sides_bytes.second) == 0;
        });
        return found != range.end();
    };
    if (collection_type->is_list()) {
        return exists_in(value_cast<list_type_impl::native_type>(lhs_collection));
    } else if (collection_type->is_set()) {
        return exists_in(value_cast<set_type_impl::native_type>(lhs_collection));
    } else if (collection_type->is_map()) {
        auto data_map = value_cast<map_type_impl::native_type>(lhs_collection);
        using entry = std::pair<data_value, data_value>;
        return exists_in(data_map | std::views::transform([](const entry& e) { return e.second; }));
    } else {
        on_internal_error(expr_logger, "unsupported collection type in a CONTAINS expression");
    }
}

/// True iff a column is a map containing \p key.
bool_or_null contains_key(const expression& lhs, const expression& rhs, const evaluation_inputs& inputs) {
    std::pair<managed_bytes_opt, managed_bytes_opt> sides_bytes =
        evaluate_binop_sides(lhs, rhs, oper_t::CONTAINS_KEY, inputs);
    if (!sides_bytes.first || !sides_bytes.second) {
        return bool_or_null::null();
    }
    auto [lhs_bytes, rhs_bytes] = std::move(sides_bytes);

    data_type lhs_type = type_of(lhs);
    const map_type_impl::native_type data_map =
        value_cast<map_type_impl::native_type>(lhs_type->deserialize(managed_bytes_view(*lhs_bytes)));
    data_type key_type = static_pointer_cast<const collection_type_impl>(lhs_type)->name_comparator();

    for (const std::pair<data_value, data_value>& map_element : data_map) {
        bytes serialized_element_key = map_element.first.serialize_nonnull();
        if (key_type->compare(managed_bytes_view(*rhs_bytes), managed_bytes_view(bytes_view(serialized_element_key))) ==
            0) {
            return true;
        };
    }

    return false;
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
bool_or_null like(const expression& lhs, const expression& rhs, const evaluation_inputs& inputs) {
    data_type lhs_type = type_of(lhs)->underlying_type();
    if (!lhs_type->is_string()) {
        throw exceptions::invalid_request_exception(
                format("LIKE is allowed only on string types, which {:user} is not", lhs));
    }
    std::pair<managed_bytes_opt, managed_bytes_opt> sides_bytes =
        evaluate_binop_sides(lhs, rhs, oper_t::LIKE, inputs);
    if (!sides_bytes.first || !sides_bytes.second) {
        return bool_or_null::null();
    }
    auto [lhs_managed_bytes, rhs_managed_bytes] = std::move(sides_bytes);

    bytes lhs_bytes = to_bytes(*lhs_managed_bytes);
    bytes rhs_bytes = to_bytes(*rhs_managed_bytes);

    return like_matcher(bytes_view(rhs_bytes))(bytes_view(lhs_bytes));
}

/// True iff the column value is in the set defined by rhs.
bool_or_null is_one_of(const expression& lhs, const expression& rhs, const evaluation_inputs& inputs, null_handling_style null_handling) {
    std::pair<managed_bytes_opt, managed_bytes_opt> sides_bytes =
        evaluate_binop_sides(lhs, rhs, oper_t::IN, inputs);
    if (!sides_bytes.first || !sides_bytes.second) {
        switch (null_handling) {
        case null_handling_style::sql:
            return bool_or_null::null();
        case null_handling_style::lwt_nulls:
            if (!sides_bytes.second) {
                return false;
            } else {
                return list_contains_null(*sides_bytes.second);
            }
        }
    }

    auto [lhs_bytes, rhs_bytes] = std::move(sides_bytes);

    expression lhs_constant = constant(raw_value::make_value(std::move(*lhs_bytes)), type_of(lhs));
    utils::chunked_vector<managed_bytes_opt> list_elems = get_list_elements(raw_value::make_value(std::move(*rhs_bytes)));
    for (const managed_bytes_opt& elem : list_elems) {
        if (equal(lhs_constant, elem, inputs).is_true()) {
            return true;
        }
    }
    return false;
}

/// True iff the column value is NOT in the set defined by rhs. Differs from !is_one_of() in handling NULLs.
bool_or_null is_none_of(const expression& lhs, const expression& rhs, const evaluation_inputs& inputs, null_handling_style null_handling) {
    std::pair<managed_bytes_opt, managed_bytes_opt> sides_bytes =
        evaluate_binop_sides(lhs, rhs, oper_t::IN, inputs);
    if (!sides_bytes.first || !sides_bytes.second) {
        switch (null_handling) {
        case null_handling_style::sql:
            return bool_or_null::null();
        case null_handling_style::lwt_nulls:
            if (!sides_bytes.second) {
                return true;
            } else {
                return !list_contains_null(*sides_bytes.second);
            }
        }
    }

    auto [lhs_bytes, rhs_bytes] = std::move(sides_bytes);

    expression lhs_constant = constant(raw_value::make_value(std::move(*lhs_bytes)), type_of(lhs));
    utils::chunked_vector<managed_bytes_opt> list_elems = get_list_elements(raw_value::make_value(std::move(*rhs_bytes)));
    bool saw_null = false;
    for (const managed_bytes_opt& elem : list_elems) {
        auto cmp_result = equal(lhs_constant, elem, inputs);
        if (cmp_result.is_null()) {
            // If we match another element in the list, we can return `false` with
            // confidence. If we don't, we return NULL to indicate we don't know.
            saw_null = true;
        } else if (cmp_result.is_true()) {
            return false;
        }
    }
    if (saw_null) {
            switch (null_handling) {
            case null_handling_style::sql:
                return bool_or_null::null();
            case null_handling_style::lwt_nulls:
                // In LWT, `3 NOT IN (NULL, 5)` counts as true
                return true;
            }
    }
    return true;
}

bool is_not_null(const expression& lhs, const expression& rhs, const evaluation_inputs& inputs) {
    cql3::raw_value lhs_val = evaluate(lhs, inputs);
    cql3::raw_value rhs_val = evaluate(rhs, inputs);
    if (!rhs_val.is_null()) {
        throw exceptions::invalid_request_exception("IS NOT operator accepts only NULL as its right side");
    }
    return !lhs_val.is_null();
}

} // anonymous namespace

bool is_satisfied_by(const expression& restr, const evaluation_inputs& inputs) {
    static auto true_value = managed_bytes_opt(data_value(true).serialize_nonnull());
    return evaluate(restr, inputs).to_managed_bytes_opt() == true_value;
}

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
    std::ranges::copy(explode_conjunction(std::move(b)), back_inserter(children));
    return conjunction{std::move(children)};
}

untyped_constant
make_untyped_null() {
    return {
        .partial_type = untyped_constant::type_class::null,
        .raw_text = "null",
    };
}

std::vector<expression>
boolean_factors(expression e) {
    std::vector<expression> ret;
    for_each_boolean_factor(e, [&](const expression& boolean_factor) {
        ret.push_back(boolean_factor);
    });
    return ret;
}

void for_each_boolean_factor(const expression& e, const noncopyable_function<void (const expression&)>& for_each_func) {
    if (auto conj = as_if<conjunction>(&e)) {
        for (const expression& child : conj->children) {
            for_each_boolean_factor(child, for_each_func);
        }
    } else {
        for_each_func(e);
    }
}

std::ostream& operator<<(std::ostream& os, const column_value& cv) {
    os << cv.col->name_as_text();
    return os;
}

}
}

template <typename FormatContext>
auto fmt::formatter<cql3::expr::expression::printer>::format(const cql3::expr::expression::printer& pr, FormatContext& ctx) const
        -> decltype(ctx.out()) {
    using namespace cql3::expr;
    // Wraps expression in expression::printer to forward formatting options
    auto to_printer = [&pr] (const expression& expr) -> expression::printer {
        return expression::printer {
            .expr_to_print = expr,
            .debug_mode = pr.debug_mode,
            .for_metadata = pr.for_metadata,
        };
    };

    auto out = ctx.out();
    cql3::expr::visit(overloaded_functor{
            [&] (const constant& v) {
                if (pr.debug_mode) {
                    out = fmt::format_to(out, "{}", v.view());
                } else {
                    if (v.value.is_null()) {
                        out = fmt::format_to(out, "null");
                    } else {
                        v.value.view().with_value([&](const FragmentedView auto& bytes_view) {
                            data_value deser_val = v.type->deserialize(bytes_view);
                            out = fmt::format_to(out, "{}", deser_val.to_parsable_string());
                        });
                    }
                }
            },
            [&] (const cql3::expr::conjunction& conj) {
                out = fmt::format_to(out, "({})", fmt::join(conj.children | std::views::transform(to_printer), ") AND ("));
            },
            [&] (const binary_operator& opr) {
                if (pr.debug_mode) {
                    out = fmt::format_to(out, "({}) {} {}", to_printer(opr.lhs), opr.op, to_printer(opr.rhs));
                    if (opr.null_handling == null_handling_style::lwt_nulls) {
                        out = fmt::format_to(out, " [[lwt_nulls]]");
                    }
                } else {
                    if ((opr.op == oper_t::IN || opr.op == oper_t::NOT_IN) && is<collection_constructor>(opr.rhs)) {
                        tuple_constructor rhs_tuple {
                            .elements = as<collection_constructor>(opr.rhs).elements
                        };
                        out = fmt::format_to(out, "{} {} {}", to_printer(opr.lhs), opr.op, to_printer(rhs_tuple));
                    } else if ((opr.op == oper_t::IN || opr.op == oper_t::NOT_IN) && is<constant>(opr.rhs) && as<constant>(opr.rhs).type->without_reversed().is_list()) {
                        tuple_constructor rhs_tuple;
                        const list_type_impl* list_typ = dynamic_cast<const list_type_impl*>(&as<constant>(opr.rhs).type->without_reversed());
                        for (const managed_bytes_opt& elem : get_list_elements(as<constant>(opr.rhs).value)) {
                            rhs_tuple.elements.push_back(constant(cql3::raw_value::make_value(elem), list_typ->get_elements_type()));
                        }
                        out = fmt::format_to(out, "{} {} {}", to_printer(opr.lhs), opr.op, to_printer(rhs_tuple));
                    } else {
                        out = fmt::format_to(out, "{} {} {}", to_printer(opr.lhs), opr.op, to_printer(opr.rhs));
                    }
                }
            },
            [&] (const column_value& col) {
                if (pr.for_metadata) {
                    out = fmt::format_to(out, "{}", col.col->name_as_text());
                } else {
                    out = fmt::format_to(out, "{}", col.col->name_as_cql_string());
                }
            },
            [&] (const subscript& sub) {
                out = fmt::format_to(out, "{}[{}]", to_printer(sub.val), to_printer(sub.sub));
            },
            [&] (const unresolved_identifier& ui) {
                if (pr.debug_mode) {
                    out = fmt::format_to(out, "unresolved({})", *ui.ident);
                } else {
                    out = fmt::format_to(out, "{}", cql3::util::maybe_quote(ui.ident->to_string()));
                }
            },
            [&] (const column_mutation_attribute& cma)  {
                if (!pr.for_metadata) {
                    out = fmt::format_to(out, "{}({})",
                            cma.kind,
                            to_printer(cma.column));
                } else {
                    auto kind = fmt::format("{}", cma.kind);
                    std::transform(kind.begin(), kind.end(), kind.begin(), [] (unsigned char c) { return std::tolower(c); });
                    out = fmt::format_to(out, "{}({})", kind, to_printer(cma.column));
                }
            },
            [&] (const function_call& fc)  {
                if (is_token_function(fc)) {
                    if (!pr.for_metadata) {
                        out = fmt::format_to(out, "token({})", fmt::join(fc.args | std::views::transform(to_printer), ", "));
                    } else {
                        out = fmt::format_to(out, "system.token({})", fmt::join(fc.args | std::views::transform(to_printer), ", "));
                    }
                } else {
                    std::visit(overloaded_functor{
                        [&] (const cql3::functions::function_name& named) {
                            out = fmt::format_to(out, "{}({})", named, fmt::join(fc.args | std::views::transform(to_printer), ", "));
                        },
                        [&] (const shared_ptr<cql3::functions::function>& fn) {
                            if (!pr.debug_mode && fn->name() == cql3::functions::aggregate_fcts::first_function_name()) {
                                // The "first" function is artificial, don't emit it
                                out = fmt::format_to(out, "{}", to_printer(fc.args[0]));
                            } else if (!pr.for_metadata) {
                                out = fmt::format_to(out, "{}({})", fn->name(), fmt::join(fc.args | std::views::transform(to_printer), ", "));
                            } else {
                                const std::string_view fn_name = fn->name().name;
                                if (fn->name().keyspace == "system" && fn_name.starts_with("castas")) {
                                    auto cast_type = fn_name.substr(6);
                                    out = fmt::format_to(out, "cast({} as {})", fmt::join(fc.args | std::views::transform(to_printer), ", "),
                                         cast_type);
                                } else {
                                    auto args = fc.args | std::views::transform(to_printer) | std::views::transform(fmt::to_string<expression::printer>)
                                            | std::ranges::to<std::vector<sstring>>();
                                    out = fmt::format_to(out, "{}", fn->column_name(args));
                                }
                            }
                        },
                    }, fc.func);
                }
            },
            [&] (const cast& c)  {
                auto type_str = std::visit(overloaded_functor{
                    [&] (const cql3::cql3_type& t) {
                        return fmt::format("{}", t);
                    },
                    [&] (const shared_ptr<cql3::cql3_type::raw>& t) {
                        return fmt::format("{}", t);
                    }}, c.type);

                switch (c.style) {
                case cast::cast_style::sql:
                    out = fmt::format_to(out, "({} AS {})", to_printer(c.arg), type_str);
                    return;
                case cast::cast_style::c:
                    out = fmt::format_to(out, "({}) {}", type_str, to_printer(c.arg));
                    return;
                }
                on_internal_error(expr_logger, "unexpected cast_style");
            },
            [&] (const field_selection& fs)  {
                if (pr.debug_mode) {
                    out = fmt::format_to(out, "({}.{})", to_printer(fs.structure), fs.field);
                } else {
                    out = fmt::format_to(out, "{}.{}", to_printer(fs.structure), fs.field);
                }
            },
            [&] (const bind_variable&) {
                // FIXME: store and present bind variable name
                out = fmt::format_to(out, "?");
            },
            [&] (const untyped_constant& uc) {
                if (uc.partial_type == untyped_constant::type_class::string) {
                    out = fmt::format_to(out, "'{}'", uc.raw_text);
                } else {
                    out = fmt::format_to(out, "{}", uc.raw_text);
                }
            },
            [&] (const tuple_constructor& tc) {
                out = fmt::format_to(out, "({})", fmt::join(tc.elements | std::views::transform(to_printer), ", "));
            },
            [&] (const collection_constructor& cc) {
                switch (cc.style) {
                case collection_constructor::style_type::list: {
                    out = fmt::format_to(out, "[{}]", fmt::join(cc.elements | std::views::transform(to_printer), ", "));
                    return;
                }
                case collection_constructor::style_type::set: {
                    out = fmt::format_to(out, "{{{}}}", fmt::join(cc.elements | std::views::transform(to_printer), ", "));
                    return;
                }
                case collection_constructor::style_type::map: {
                    out = fmt::format_to(out, "{{");
                    bool first = true;
                    for (auto& e : cc.elements) {
                        if (!first) {
                            out = fmt::format_to(out, ", ");
                        }
                        first = false;
                        auto& tuple = cql3::expr::as<tuple_constructor>(e);
                        if (tuple.elements.size() != 2) {
                            on_internal_error(expr_logger, "map constructor element is not a tuple of arity 2");
                        }
                        out = fmt::format_to(out, "{}:{}", to_printer(tuple.elements[0]), to_printer(tuple.elements[1]));
                    }
                    out = fmt::format_to(out, "}}");
                    return;
                }
                }
                on_internal_error(expr_logger, fmt::format("unexpected collection_constructor style {}", static_cast<unsigned>(cc.style)));
            },
            [&] (const usertype_constructor& uc) {
                out = fmt::format_to(out, "{{");
                bool first = true;
                for (auto& [k, v] : uc.elements) {
                    if (!first) {
                        out = fmt::format_to(out, ", ");
                    }
                    first = false;
                    out = fmt::format_to(out, "{}:{}", k, to_printer(v));
                }
                out = fmt::format_to(out, "}}");
            },
            [&] (const temporary& t) {
                out = fmt::format_to(out, "@temporary{}", t.index);
            }
        }, pr.expr_to_print);
    return out;
}

template auto
fmt::formatter<cql3::expr::expression::printer>::format<fmt::format_context>(const cql3::expr::expression::printer&, fmt::format_context& ctx) const
    -> decltype(ctx.out());
template auto
fmt::formatter<cql3::expr::expression::printer>::format<fmt::basic_format_context<std::back_insert_iterator<std::string>, char>>(const cql3::expr::expression::printer&, fmt::basic_format_context<std::back_insert_iterator<std::string>, char>& ctx) const
    -> decltype(ctx.out());

namespace cql3 {
namespace expr {

sstring to_string(const expression& expr) {
    return fmt::format("{}", expr);
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

expression replace_partition_token(const expression& expr, const column_definition* new_cdef, const schema& table_schema) {
    return search_and_replace(expr, [&] (const expression& expr) -> std::optional<expression> {
        if (is_partition_token_for_schema(expr, table_schema)) {
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
                            conj.children
                            | std::views::transform(recurse)
                            | std::ranges::to<std::vector>()
                    };
                },
                [&] (const binary_operator& oper) -> expression {
                    return binary_operator(recurse(oper.lhs), oper.op, recurse(oper.rhs), oper.order);
                },
                [&] (const column_mutation_attribute& cma) -> expression {
                    return column_mutation_attribute{cma.kind, recurse(cma.column)};
                },
                [&] (const tuple_constructor& tc) -> expression {
                    return tuple_constructor{
                        tc.elements
                            | std::views::transform(recurse)
                            | std::ranges::to<std::vector>(),
                        tc.type
                    };
                },
                [&] (const collection_constructor& c) -> expression {
                    return collection_constructor{
                        c.style,
                        c.elements
                            | std::views::transform(recurse)
                            | std::ranges::to<std::vector>(),
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
                        fc.args
                            | std::views::transform(recurse)
                            | std::ranges::to<std::vector>()
                    };
                },
                [&] (const cast& c) -> expression {
                    return cast{c.style, recurse(c.arg), c.type};
                },
                [&] (const field_selection& fs) -> expression {
                    return field_selection{recurse(fs.structure), fs.field};
                },
                [&] (const subscript& s) -> expression {
                    return subscript {
                        .val = recurse(s.val),
                        .sub = recurse(s.sub),
                        .type = s.type,
                    };
                },
                [&] (LeafExpression auto const& e) -> expression {
                    return e;
                },
            }, e);
    }
}


constant::constant(cql3::raw_value val, data_type typ)
    : value(std::move(val)), type(std::move(typ)) {
}

constant constant::make_null(data_type val_type) {
    return constant(cql3::raw_value::make_null(), std::move(val_type));
}

constant constant::make_bool(bool bool_val) {
    return constant(raw_value::make_value(boolean_type->decompose(bool_val)), boolean_type);
}

bool constant::is_null() const {
    return value.is_null();
}

bool constant::has_empty_value_bytes() const {
    if (is_null()) {
        return false;
    }

    return value.view().size_bytes() == 0;
}

cql3::raw_value_view constant::view() const {
    return value.view();
}

std::optional<bool> get_bool_value(const constant& constant_val) {
    if (constant_val.type->get_kind() != abstract_type::kind::boolean) {
        return std::nullopt;
    }

    if (constant_val.is_null()) {
        return std::nullopt;
    }

    if (constant_val.has_empty_value_bytes()) {
        return std::nullopt;
    }

    return constant_val.view().deserialize<bool>(*boolean_type);
}

static
cql3::raw_value do_evaluate(const binary_operator& binop, const evaluation_inputs& inputs) {
    if (binop.order == comparison_order::clustering) {
        throw exceptions::invalid_request_exception("Can't evaluate a binary operator with SCYLLA_CLUSTERING_BOUND");
    }

    bool_or_null binop_result(false);

    switch (binop.op) {
        case oper_t::EQ:
            binop_result = equal(binop.lhs, binop.rhs, inputs, binop.null_handling);
            break;
        case oper_t::NEQ:
            binop_result = not_equal(binop.lhs, binop.rhs, inputs, binop.null_handling);
            break;
        case oper_t::LT:
        case oper_t::LTE:
        case oper_t::GT:
        case oper_t::GTE:
            binop_result = limits(binop.lhs, binop.op, binop.null_handling, binop.rhs, inputs);
            break;
        case oper_t::CONTAINS:
            binop_result = contains(binop.lhs, binop.rhs, inputs);
            break;
        case oper_t::CONTAINS_KEY:
            binop_result = contains_key(binop.lhs, binop.rhs, inputs);
            break;
        case oper_t::LIKE:
            binop_result = like(binop.lhs, binop.rhs, inputs);
            break;
        case oper_t::IN:
            binop_result = is_one_of(binop.lhs, binop.rhs, inputs, binop.null_handling);
            break;
        case oper_t::NOT_IN:
            binop_result = is_none_of(binop.lhs, binop.rhs, inputs, binop.null_handling);
            break;
        case oper_t::IS_NOT:
            binop_result = is_not_null(binop.lhs, binop.rhs, inputs);
            break;
    };

    if (binop_result.is_null()) {
        return raw_value::make_null();
    }

    return raw_value::make_value(boolean_type->decompose(binop_result.get_value()));
}

// Evaluate a conjunction of elements separated by AND.
// NULL is treated as an "unknown value" - maybe true maybe false.
// `TRUE AND NULL` evaluates to NULL because it might be true but also might be false.
// `FALSE AND NULL` evaluates to FALSE because no matter what value NULL acts as, the result will still be FALSE.
// Empty values are not allowed.
//
// Usually in CQL the rule is that when NULL occurs in an operation the whole expression
// becomes NULL, but here we decided to deviate from this behavior.
// Treating NULL as an "unknown value" is the standard SQL way of handing NULLs in conjunctions.
// It works this way in MySQL and Postgres so we do it this way as well.
//
// The evaluation short-circuits. Once FALSE is encountered the function returns FALSE
// immediately without evaluating any further elements.
// It works this way in Postgres as well, for example:
// `SELECT true AND NULL AND 1/0 = 0` will throw a division by zero error
// but `SELECT false AND 1/0 = 0` will successfully evaluate to FALSE.
static
cql3::raw_value do_evaluate(const conjunction& conj, const evaluation_inputs& inputs) {
    bool has_null = false;

    for (const expression& element : conj.children) {
        cql3::raw_value element_val = evaluate(element, inputs);
        if (element_val.is_null()) {
            has_null = true;
            continue;
        }
        if (element_val.is_empty_value()) {
            throw exceptions::invalid_request_exception("empty value found inside AND conjunction");
        }
        bool element_val_bool = element_val.view().deserialize<bool>(*boolean_type);
        if (element_val_bool == false) {
            // The conjunction contains a false value, so the result must be false.
            // Don't evaluate other elements, short-circuit and return immediately.
            return raw_value::make_value(boolean_type->decompose(false));
        }
    }

    if (has_null) {
        return raw_value::make_null();
    }

    return raw_value::make_value(boolean_type->decompose(true));
}

static
cql3::raw_value do_evaluate(const field_selection& field_select, const evaluation_inputs& inputs) {
    cql3::raw_value udt_value = evaluate(field_select.structure, inputs);
    if (udt_value.is_null()) {
        // `<null>.field` should evaluate to NULL.
        return cql3::raw_value::make_null();
    }

    cql3::raw_value field_value = udt_value.view().with_value(
        [&](const FragmentedView auto& udt_serialized_bytes) -> cql3::raw_value {
            // std::optional<FragmentedView> read_field
            auto read_field = read_nth_user_type_field(udt_serialized_bytes, field_select.field_idx);

            return cql3::raw_value::make_value(managed_bytes_opt(read_field));
    });

    return field_value;
}

static
cql3::raw_value
do_evaluate(const column_mutation_attribute& cma, const evaluation_inputs& inputs) {
    auto col = expr::as_if<column_value>(&cma.column);
    if (!col) {
        on_internal_error(expr_logger, fmt::format("evaluating column_mutation_attribute of non-column {}", cma.column));
    }
    int32_t index = inputs.selection->index_of(*col->col);
    switch (cma.kind) {
    case column_mutation_attribute::attribute_kind::ttl: {
        auto ttl_v = inputs.static_and_regular_ttls[index];
        if (ttl_v <= 0) {
            return cql3::raw_value::make_null();
        }
        return raw_value::make_value(data_value(ttl_v).serialize());
    }
    case column_mutation_attribute::attribute_kind::writetime: {
        auto ts_v = inputs.static_and_regular_timestamps[index];
        if (ts_v == api::missing_timestamp) {
            return cql3::raw_value::make_null();
        }
        return raw_value::make_value(data_value(ts_v).serialize());
    }
    }
    on_internal_error(expr_logger, "evaluating column_mutation_attribute with unexpected kind");
}

static
cql3::raw_value
do_evaluate(const cast& c, const evaluation_inputs& inputs) {
    // std::invoke trick allows us to use "return" in switch can have the compiler warn us if we missed an enum
    std::invoke([&] {
        switch (c.style) {
        case cast::cast_style::c: return;
        case cast::cast_style::sql: on_internal_error(expr_logger, "SQL-style cast should have been converted to a function_call");
        }
        on_internal_error(expr_logger, "illegal cast_style");
    });

    auto ret = evaluate(c.arg, inputs);
    auto type = std::get_if<data_type>(&c.type);
    if (!type) {
        on_internal_error(expr_logger, "attempting to evaluate an unprepared cast");
    }
    return ret;
}

static
cql3::raw_value
do_evaluate(const unresolved_identifier& ui, const evaluation_inputs& inputs) {
    on_internal_error(expr_logger, "Can't evaluate unresolved_identifier");
}

static
cql3::raw_value
do_evaluate(const untyped_constant& uc, const evaluation_inputs& inputs) {
    on_internal_error(expr_logger, "Can't evaluate a untyped_constant ");
}

static
cql3::raw_value
do_evaluate(const column_value& cv, const evaluation_inputs& inputs) {
    return raw_value::make_value(get_value(cv, inputs));
}

static
cql3::raw_value
do_evaluate(const subscript& s, const evaluation_inputs& inputs) {
    return raw_value::make_value(get_value(s, inputs));
}

static
cql3::raw_value
do_evaluate(const constant& c, const evaluation_inputs& inputs) {
    return c.value;
}

static
cql3::raw_value
do_evaluate(const temporary& t, const evaluation_inputs& inputs) {
    return inputs.temporaries[t.index];
}

cql3::raw_value evaluate(const expression& e, const evaluation_inputs& inputs) {
    return expr::visit([&] (const ExpressionElement auto& ee) -> cql3::raw_value {
        return do_evaluate(ee, inputs);
    }, e);
}

cql3::raw_value evaluate(const expression& e, const query_options& options) {
    return evaluate(e, evaluation_inputs{.options = &options});
}

// Takes a value and reserializes it where needs_to_be_reserialized() says it's needed
template <FragmentedView View>
static managed_bytes reserialize_value(View value_bytes,
                                       const abstract_type& type) {
    if (type.is_list()) {
        utils::chunked_vector<managed_bytes_opt> elements = partially_deserialize_listlike(value_bytes);

        const abstract_type& element_type = dynamic_cast<const list_type_impl&>(type).get_elements_type()->without_reversed();
        if (element_type.bound_value_needs_to_be_reserialized()) {
            for (managed_bytes_opt& element_opt : elements) {
                if (element_opt) {
                    element_opt = reserialize_value(managed_bytes_view(*element_opt), element_type);
                }
            }
        }

        return collection_type_impl::pack_fragmented(
            elements.begin(),
            elements.end(),
            elements.size()
        );
    }

    if (type.is_set()) {
        utils::chunked_vector<managed_bytes_opt> elements = partially_deserialize_listlike(value_bytes);

        const abstract_type& element_type = dynamic_cast<const set_type_impl&>(type).get_elements_type()->without_reversed();
        if (element_type.bound_value_needs_to_be_reserialized()) {
            for (managed_bytes_opt& element_opt : elements) {
                if (element_opt) {
                    element_opt = reserialize_value(managed_bytes_view(*element_opt), element_type);
                }
            }
        }

        std::set<managed_bytes, serialized_compare> values_set(element_type.as_less_comparator());
        for (managed_bytes_opt& element_opt : elements) {
            if (!element_opt) {
                throw exceptions::invalid_request_exception("Invalid NULL value in set");
            }
            values_set.emplace(std::move(*element_opt));
        }

        return collection_type_impl::pack_fragmented(
            values_set.begin(),
            values_set.end(),
            values_set.size()
        );
    }

    if (type.is_map()) {
        std::vector<std::pair<managed_bytes, managed_bytes>> elements = partially_deserialize_map(value_bytes);

        const map_type_impl mapt = dynamic_cast<const map_type_impl&>(type);
        const abstract_type& key_type = mapt.get_keys_type()->without_reversed();
        const abstract_type& value_type = mapt.get_values_type()->without_reversed();

        if (key_type.bound_value_needs_to_be_reserialized()) {
            for (std::pair<managed_bytes, managed_bytes>& element : elements) {
                element.first = reserialize_value(managed_bytes_view(element.first), key_type);
            }
        }

        if (value_type.bound_value_needs_to_be_reserialized()) {
            for (std::pair<managed_bytes, managed_bytes>& element : elements) {
                element.second = reserialize_value(managed_bytes_view(element.second), value_type);
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
            if (elements[i].has_value() && element_type.bound_value_needs_to_be_reserialized()) {
                elements[i] = reserialize_value(managed_bytes_view(*elements[i]), element_type);
            }
        }

        return tuple_type_impl::build_value_fragmented(std::move(elements));
    }

    on_internal_error(expr_logger,
        fmt::format("Reserializing type that shouldn't need reserialization: {}", type.name()));
}

static cql3::raw_value do_evaluate(const bind_variable& bind_var, const evaluation_inputs& inputs) {
    if (bind_var.receiver.get() == nullptr) {
        on_internal_error(expr_logger,
            "evaluate(bind_variable) called with nullptr receiver, should be prepared first");
    }

    cql3::raw_value_view value = inputs.options->get_value_at(bind_var.bind_index);

    if (value.is_null()) {
        return cql3::raw_value::make_null();
    }

    const abstract_type& value_type = bind_var.receiver->type->without_reversed();
    try {
        value.validate(value_type);
    } catch (const marshal_exception& e) {
        throw exceptions::invalid_request_exception(format("Exception while binding column {:s}: {:s}",
                                                           bind_var.receiver->name->to_cql_string(), e.what()));
    }

    if (value_type.bound_value_needs_to_be_reserialized()) {
        managed_bytes new_value = value.with_value([&] (const FragmentedView auto& value_bytes) {
            return reserialize_value(value_bytes, value_type);
        });

        return raw_value::make_value(std::move(new_value));
    }

    return raw_value::make_value(value);
}

static cql3::raw_value do_evaluate(const tuple_constructor& tuple, const evaluation_inputs& inputs) {
    if (tuple.type.get() == nullptr) {
        on_internal_error(expr_logger,
            "evaluate(tuple_constructor) called with nullptr type, should be prepared first");
    }

    std::vector<managed_bytes_opt> tuple_elements;
    tuple_elements.reserve(tuple.elements.size());

    for (size_t i = 0; i < tuple.elements.size(); i++) {
        cql3::raw_value elem_val = evaluate(tuple.elements[i], inputs);
        tuple_elements.emplace_back(std::move(elem_val).to_managed_bytes_opt());
    }

    cql3::raw_value raw_val =
        cql3::raw_value::make_value(tuple_type_impl::build_value_fragmented(std::move(tuple_elements)));

    return raw_val;
}

// Range of managed_bytes
template <typename Range>
requires requires (Range listlike_range) { {*listlike_range.begin()} -> std::convertible_to<const managed_bytes_opt&>; }
static managed_bytes serialize_listlike(const Range& elements, const char* collection_name) {
    if (elements.size() > std::numeric_limits<int32_t>::max()) {
        throw exceptions::invalid_request_exception(fmt::format("{} size too large: {} > {}",
            collection_name, elements.size(), std::numeric_limits<int32_t>::max()));
    }

    for (const auto& element : elements) {
      const managed_bytes_opt& element_opt = element;
      if (element_opt) {
        auto& element = *element_opt;
        if (element.size() > std::numeric_limits<int32_t>::max()) {
            throw exceptions::invalid_request_exception(fmt::format("{} element size too large: {} bytes > {}",
                collection_name, elements.size(), std::numeric_limits<int32_t>::max()));
        }
      }
    }

    return collection_type_impl::pack_fragmented(
        elements.begin(),
        elements.end(),
        elements.size()
    );
}

static cql3::raw_value evaluate_list(const collection_constructor& collection,
                              const evaluation_inputs& inputs,
                              bool skip_null = false) {
    std::vector<managed_bytes_opt> evaluated_elements;
    evaluated_elements.reserve(collection.elements.size());

    for (const expression& element : collection.elements) {
        cql3::raw_value evaluated_element = evaluate(element, inputs);

        evaluated_elements.emplace_back(std::move(evaluated_element).to_managed_bytes_opt());
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

static cql3::raw_value do_evaluate(const collection_constructor& collection, const evaluation_inputs& inputs) {
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

static cql3::raw_value do_evaluate(const usertype_constructor& user_val, const evaluation_inputs& inputs) {
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

        field_values.emplace_back(std::move(field_val).to_managed_bytes_opt());
    }

    raw_value val_bytes = cql3::raw_value::make_value(tuple_type_impl::build_value_fragmented(field_values));
    return val_bytes;
}

static cql3::raw_value do_evaluate(const function_call& fun_call, const evaluation_inputs& inputs) {
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

    bytes_opt result = scalar_fun->execute(arguments);

    if (has_cache_id) {
        inputs.options->cache_pk_function_call(**fun_call.lwt_cache_id, result);
    }

    if (!result.has_value()) {
        return cql3::raw_value::make_null();
    }

    try {
        scalar_fun->return_type()->validate(*result);
    } catch (marshal_exception&) {
        throw runtime_exception(fmt::format("Return of function {} ({}) is not a valid value for its declared return type {}",
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
}

utils::chunked_vector<managed_bytes_opt> get_list_elements(const cql3::raw_value& val) {
    ensure_can_get_value_elements(val, "expr::get_list_elements");

    return val.view().with_value([](const FragmentedView auto& value_bytes) {
        return partially_deserialize_listlike(value_bytes);
    });
}

utils::chunked_vector<managed_bytes_opt> get_set_elements(const cql3::raw_value& val) {
    ensure_can_get_value_elements(val, "expr::get_set_elements");

    return val.view().with_value([](const FragmentedView auto& value_bytes) {
        return partially_deserialize_listlike(value_bytes);
    });
}

std::vector<std::pair<managed_bytes, managed_bytes>> get_map_elements(const cql3::raw_value& val) {
    ensure_can_get_value_elements(val, "expr::get_map_elements");

    return val.view().with_value([](const FragmentedView auto& value_bytes) {
        return partially_deserialize_map(value_bytes);
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

static std::vector<managed_bytes_opt> convert_listlike(utils::chunked_vector<managed_bytes_opt>&& elements) {
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
    utils::chunked_vector<managed_bytes_opt> elements = get_list_elements(val);
    const list_type_impl& list_typ = dynamic_cast<const list_type_impl&>(type.without_reversed());
    const tuple_type_impl& tuple_typ = dynamic_cast<const tuple_type_impl&>(*list_typ.get_elements_type());

    utils::chunked_vector<std::vector<managed_bytes_opt>> tuples_list;
    tuples_list.reserve(elements.size());

    for (managed_bytes_opt& element : elements) {
        if (!element) {
            // Note: just skipping would also be okay here, for our caller (maybe
            //       even better, but leaving that for later)
            throw exceptions::invalid_request_exception("Invalid NULL tuple in list of tuples");
        }
        std::vector<managed_bytes_opt> cur_tuple = tuple_typ.split_fragmented(managed_bytes_view(*element));
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
        [](constant&) {},
        [](temporary&) {},
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
column_mutation_attribute_type(const column_mutation_attribute& e) {
    switch (e.kind) {
    case column_mutation_attribute::attribute_kind::writetime:
        return long_type;
    case column_mutation_attribute::attribute_kind::ttl:
        return int32_type;
    }
    on_internal_error(expr_logger, "evaluating type of illegal column mutation attribute kind");
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
        [] (const unresolved_identifier& e) -> data_type {
            on_internal_error(expr_logger, "evaluating type of unresolved_identifier");
        },
        [] (const column_mutation_attribute& e) {
            return column_mutation_attribute_type(e);
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

unset_bind_variable_guard::unset_bind_variable_guard(const expr::expression& e) {
    if (auto bv = expr::as_if<expr::bind_variable>(&e)) {
        _var = *bv;
    }
}

unset_bind_variable_guard::unset_bind_variable_guard(const std::optional<expr::expression>& e) {
    if (!e) {
        return;
    }
    if (auto bv = expr::as_if<expr::bind_variable>(&*e)) {
        _var = *bv;
    }
}

bool
unset_bind_variable_guard::is_unset(const query_options& qo) const {
    if (!_var) {
        return false;
    }
    return qo.is_unset(_var->bind_index);
}

static
expression
convert_map_back_to_listlike(expression e) {
    class conversion_function : public functions::scalar_function {
        functions::function_name _name = { "system", "reserialize_listlike" };
        shared_ptr<const map_type_impl> _map_type;
        shared_ptr<const listlike_collection_type_impl> _listlike_type;
        std::vector<data_type> _arg_types;
    public:
        explicit conversion_function(shared_ptr<const map_type_impl> map_type,
                shared_ptr<const listlike_collection_type_impl> listlike_type)
                : _map_type(std::move(map_type))
                , _listlike_type(std::move(listlike_type)) {
            _arg_types.push_back(_listlike_type);
        }

        virtual const functions::function_name& name() const override {
            return _name;
        }

        virtual const std::vector<data_type>& arg_types() const override {
            return _arg_types;
        }

        virtual const data_type& return_type() const override {
            return _arg_types[0];
        }

        virtual bool is_pure() const override {
            return true;
        }

        virtual bool is_native() const override {
            return true;
        }

        virtual bool requires_thread() const override {
            return false;
        }

        virtual bool is_aggregate() const override {
            return false;
        }

        virtual void print(std::ostream& os) const override {
            os << "MAP_TO_LIST(internal)";
        }

        virtual sstring column_name(const std::vector<sstring>& column_names) const override {
            return "NONE";
        }

        virtual bytes_opt execute(std::span<const bytes_opt> parameters) override {
            auto& p = parameters[0];
            if (!p) {
                return std::nullopt;
            }
            auto v = _map_type->deserialize_value(*p);
            return _listlike_type->serialize_map(*_map_type, v);
        }
    };

    auto type = dynamic_pointer_cast<const listlike_collection_type_impl>(type_of(e));
    auto map_type = map_type_impl::get_instance(type->name_comparator(), type->value_comparator(), true);
    auto args = std::vector<expression>();
    args.push_back(std::move(e));

    return function_call{
        .func = make_shared<conversion_function>(std::move(map_type), std::move(type)),
        .args = std::move(args),
    };
}

expression
adjust_for_collection_as_maps(const expression& e) {
    return search_and_replace(e, [] (const expression& candidate) -> std::optional<expression> {
        if (auto* cval = as_if<column_value>(&candidate)) {
            if (cval->col->type->is_listlike() && cval->col->type->is_multi_cell()) {
                return convert_map_back_to_listlike(candidate);
            }
        }
        return std::nullopt;
    });
}

bool is_token_function(const function_call& fun_call) {
    static thread_local const functions::function_name token_function_name =
        functions::function_name::native_function("token");

    // Check that function name is "token"
    const functions::function_name& fun_name =
        std::visit(overloaded_functor{[](const functions::function_name& fname) { return fname; },
                                      [](const shared_ptr<functions::function>& fun) { return fun->name(); }},
                   fun_call.func);

    return fun_name.has_keyspace() ? fun_name == token_function_name : fun_name.name == token_function_name.name;
}

bool is_token_function(const expression& e) {
    const function_call* fun_call = as_if<function_call>(&e);
    if (fun_call == nullptr) {
        return false;
    }

    return is_token_function(*fun_call);
}

bool is_partition_token_for_schema(const function_call& fun_call, const schema& table_schema) {
    if (!is_token_function(fun_call)) {
        return false;
    }

    if (fun_call.args.size() != table_schema.partition_key_size()) {
        return false;
    }

    auto arguments_iter = fun_call.args.begin();
    for (const column_definition& partition_key_col : table_schema.partition_key_columns()) {
        const expression& cur_argument = *arguments_iter;

        const column_value* cur_col = as_if<column_value>(&cur_argument);
        if (cur_col == nullptr) {
            // A sanity check that we didn't call the function on an unprepared expression.
            if (is<unresolved_identifier>(cur_argument)) {
                on_internal_error(expr_logger,
                                  format("called is_partition_token with unprepared expression: {}", fun_call));
            }

            return false;
        }

        if (cur_col->col != &partition_key_col) {
            return false;
        }

        arguments_iter++;
    }

    return true;
}

bool is_partition_token_for_schema(const expression& maybe_token, const schema& table_schema) {
    const function_call* fun_call = as_if<function_call>(&maybe_token);
    if (fun_call == nullptr) {
        return false;
    }

    return is_partition_token_for_schema(*fun_call, table_schema);
}

void
verify_no_aggregate_functions(const expression& expr, std::string_view context_for_errors) {
    if (aggregation_depth(expr) > 0) {
        throw exceptions::invalid_request_exception(fmt::format("Aggregation functions are not supported in the {}", context_for_errors));
    }
}

unsigned
aggregation_depth(const cql3::expr::expression& e) {
    static constexpr auto max_over_range = [] (std::ranges::range auto&& rng) -> unsigned {
        return std::ranges::fold_left(rng | std::views::transform(aggregation_depth), 0u, std::ranges::max);
    };
    return visit(overloaded_functor{
        [] (const conjunction& c) {
            return max_over_range(c.children);
        },
        [] (const binary_operator& bo) {
            return std::max(aggregation_depth(bo.lhs), aggregation_depth(bo.rhs));
        },
        [] (const subscript& s) {
            return std::max(aggregation_depth(s.val), aggregation_depth(s.sub));
        },
        [] (const column_mutation_attribute& cma) {
            return aggregation_depth(cma.column);
        },
        [] (const function_call& fc) {
            unsigned this_function_depth = std::visit(overloaded_functor{
                [] (const functions::function_name& n) -> unsigned {
                    on_internal_error(expr_logger, "unprepared function_call in aggregation_depth()");
                },
                [] (const shared_ptr<functions::function>& fn) -> unsigned {
                    return unsigned(fn->is_aggregate());
                }
            }, fc.func);
            auto arg_depth = max_over_range(fc.args);
            return this_function_depth + arg_depth;
        },
        [] (const cast& c) {
            return aggregation_depth(c.arg);
        },
        [] (const field_selection& fs) {
            return aggregation_depth(fs.structure);
        },
        [] (const LeafExpression auto&) {
            return 0u;
        },
        [] (const tuple_constructor& tc) {
            return max_over_range(tc.elements);
        },
        [] (const collection_constructor& cc) {
            return max_over_range(cc.elements);
        },
        [] (const usertype_constructor& uc) {
            return max_over_range(uc.elements | std::views::values);
        }
    }, e);
}

cql3::expr::expression
levellize_aggregation_depth(const cql3::expr::expression& e, unsigned desired_depth) {
    auto recurse = [&] (expression& e) {
        e = levellize_aggregation_depth(e, desired_depth);
    };
    auto recurse_over_range = [&] (std::ranges::range auto&& rng) {
        for (auto& e : rng) {
            recurse(e);
        }
    };

    auto pad_with_calls_to_first_function = [&] (expression ret) {
        while (desired_depth) {
            ret = function_call({
                .func = functions::aggregate_fcts::make_first_function(type_of(ret)),
                .args = {std::move(ret)},
            });
            desired_depth -= 1;
        }
        return ret;
    };

    // Implementation trick: we're returning a new expression, so have the visitor
    // accept everything by value to generate a clean copy for us to mutate.
    return visit(overloaded_functor{
        [&] (column_value cv) -> expression {
            return pad_with_calls_to_first_function(std::move(cv));
        },
        [&] (conjunction c) -> expression {
            recurse_over_range(c.children);
            return c;
        },
        [&] (binary_operator bo) -> expression {
            recurse(bo.lhs);
            recurse(bo.rhs);
            return bo;
        },
        [&] (subscript s) -> expression {
            recurse(s.val);
            recurse(s.sub);
            return s;
        },
        // column_mutation_attribute doesn't want to be separated from its nested column_value,
        // so end the recursion if we reach it.
        [&] (column_mutation_attribute cma) -> expression {
            return pad_with_calls_to_first_function(std::move(cma));
        },
        [&] (function_call fc) -> expression {
            unsigned this_function_depth = std::visit(overloaded_functor{
                [] (const functions::function_name& n) -> unsigned {
                    on_internal_error(expr_logger, "unprepared function_call in aggregation_depth()");
                },
                [] (const shared_ptr<functions::function>& fn) -> unsigned {
                    return unsigned(fn->is_aggregate());
                }
            }, fc.func);
            if (this_function_depth > desired_depth) {
                on_internal_error(expr_logger, "expression aggregation depth exceeds expectations");
            }
            desired_depth -= this_function_depth;
            recurse_over_range(fc.args);
            return fc;
        },
        [&] (cast c) -> expression {
            recurse(c.arg);
            return c;
        },
        [&] (field_selection fs) -> expression {
            recurse(fs.structure);
            return fs;
        },
        [&] (LeafExpression auto leaf) -> expression {
            return leaf;
        },
        [&] (tuple_constructor tc) -> expression {
            recurse_over_range(tc.elements);
            return tc;
        },
        [&] (collection_constructor cc) -> expression {
            recurse_over_range(cc.elements);
            return cc;
        },
        [&] (usertype_constructor uc) -> expression {
            recurse_over_range(uc.elements | std::views::values);
            return uc;
        }
    }, e);
}

aggregation_split_result
split_aggregation(std::span<const expression> aggregation) {
    size_t nr_temporaries = 0;
    auto allocate_temporary = [&] () -> size_t {
        return nr_temporaries++;
    };
    std::vector<expression> inner_vec;
    std::vector<expression> outer_vec;
    std::vector<raw_value> initial_values_vec;
    for (auto& e : aggregation) {
        auto outer = search_and_replace(e, [&] (const expression& e) -> std::optional<expression> {
            auto fc = as_if<function_call>(&e);
            if (!fc) {
                return std::nullopt;
            }
            return std::visit(overloaded_functor{
                [] (const functions::function_name& n) -> std::optional<expression> {
                    on_internal_error(expr_logger, "unprepared function_call in split_aggregation()");
                },
                [&] (const shared_ptr<functions::function>& fn) -> std::optional<expression> {
                    if (!fn->is_aggregate()) {
                        return std::nullopt;
                    }
                    // Split the aggregate. The aggregation function becomes the inner loop, the final
                    // function becomes the outer loop, and they're connected with a temporary.
                    auto temp = allocate_temporary();
                    auto agg_fn = dynamic_pointer_cast<cql3::functions::aggregate_function>(fn);
                    auto& agg = agg_fn->get_aggregate();
                    auto inner_fn = agg.aggregation_function;
                    auto outer_fn = agg.state_to_result_function;
                    auto inner_args = std::vector<expression>();
                    inner_args.push_back(temporary{.index = temp, .type = agg.state_type});
                    inner_args.insert(inner_args.end(), fc->args.begin(), fc->args.end());
                    auto inner = function_call{
                        .func = std::move(inner_fn),
                        .args = std::move(inner_args),
                    };
                    // The result of evaluating inner should be stored in the same temporary.
                    auto outer_args = std::vector<expression>();
                    outer_args.push_back(temporary{.index = temp, .type = agg.state_type});
                    auto outer = std::invoke([&] () -> expression {
                        if (outer_fn) {
                            return function_call{
                                    .func = std::move(outer_fn),
                                    .args = std::move(outer_args),
                            };
                        } else {
                            // When executing automatically parallelized queries,
                            // we get no state_to_result_function since we have to return
                            // the state. Just return the temporary that holds the state.
                            return outer_args[0];
                        }
                    });
                    inner_vec.push_back(std::move(inner));
                    initial_values_vec.push_back(raw_value::make_value(agg.initial_state));
                    return outer;
                }
            }, fc->func);
        });
        outer_vec.push_back(std::move(outer));
    }
    // Whew!
    return aggregation_split_result{
        .inner_loop = std::move(inner_vec),
        .outer_loop = std::move(outer_vec),
        .initial_values_for_temporaries = std::move(initial_values_vec),
    };
}


} // namespace expr
} // namespace cql3

std::string_view fmt::formatter<cql3::expr::oper_t>::to_string(const cql3::expr::oper_t& op) {
    using cql3::expr::oper_t;

    switch (op) {
    case oper_t::EQ:
        return "=";
    case oper_t::NEQ:
        return "!=";
    case oper_t::LT:
        return "<";
    case oper_t::LTE:
        return "<=";
    case oper_t::GT:
        return ">";
    case oper_t::GTE:
        return ">=";
    case oper_t::IN:
        return "IN";
    case oper_t::NOT_IN:
        return "NOT IN";
    case oper_t::CONTAINS:
        return "CONTAINS";
    case oper_t::CONTAINS_KEY:
        return "CONTAINS KEY";
    case oper_t::IS_NOT:
        return "IS NOT";
    case oper_t::LIKE:
        return "LIKE";
    }
    __builtin_unreachable();
}
