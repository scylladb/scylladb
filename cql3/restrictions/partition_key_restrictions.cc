/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "partition_key_restrictions.hh"
#include "cartesian_product.hh"
#include "cql3/cql_config.hh"
#include "cql3/expr/expression.hh"
#include "cql3/query_options.hh"
#include "schema.hh"
#include "seastar/core/on_internal_error.hh"

namespace cql3 {
namespace restrictions {
extern logging::logger rlogger;

using namespace expr;

static void assert_contains_only_partition_key_columns(const expr::expression& e) {
    const column_value* non_partition_column =
        find_in_expression<column_value>(e, [](const column_value& cval) { return !cval.col->is_partition_key(); });

    if (non_partition_column != nullptr) {
        on_internal_error(
            rlogger,
            format("partition_key_restrictions were given an expression that contains a non-parition column: {}",
                   non_partition_column->col->name_as_text()));
    }
}

partition_key_restrictions::partition_key_restrictions(expr::expression partition_restrictions, schema_ptr table_schema)
    : _table_schema(std::move(table_schema)), _partition_restrictions(std::move(partition_restrictions)) {
    if (!is_prepared(_partition_restrictions)) {
        on_internal_error(rlogger, format("partition_key_restrictions were given an unprepared expression: {}",
                                          _partition_restrictions));
    }

    assert_contains_only_partition_key_columns(_partition_restrictions);

    _single_column_partition_key_restrictions = expr::get_single_column_restrictions_map(_partition_restrictions);
    analyze_restrictions();
}

const expr::expression& partition_key_restrictions::get_partition_key_restrictions() const {
    return _partition_restrictions;
}

const expr::single_column_restrictions_map& partition_key_restrictions::get_single_column_partition_key_restrictions()
    const {
    return _single_column_partition_key_restrictions;
}

// Check if there are no partition key restrictions.
bool partition_key_restrictions::partition_key_restrictions_is_empty() const {
    return is_empty_restriction(_partition_restrictions);
}

static bool contains_column(const expression& e) {
    return find_in_expression<column_value>(e, [](const column_value&) { return true; }) != nullptr;
}

void partition_key_restrictions::analyze_restrictions() {
    std::map<const column_definition*, std::vector<expression>> column_eq_restrictions;
    std::vector<expression> token_range_restrictions;
    std::vector<expression> filtering_restrictions;

    for_each_boolean_factor(_partition_restrictions, [&](const expression& e) {
        const binary_operator* binop = as_if<binary_operator>(&e);
        if (binop == nullptr) {
            filtering_restrictions.push_back(e);
            return;
        }

        if (auto lhs_col = as_if<column_value>(&binop->lhs)) {
            if (lhs_col->col->is_partition_key() && (binop->op == oper_t::EQ || binop->op == oper_t::IN) &&
                !contains_column(binop->rhs)) {
                column_eq_restrictions[lhs_col->col].push_back(e);
                return;
            }
        }

        if (auto lhs_token = as_if<token>(&binop->lhs)) {
            if ((binop->op == oper_t::EQ || binop->op == oper_t::IN || is_slice(binop->op)) &&
                !contains_column(binop->rhs)) {
                token_range_restrictions.push_back(e);
                return;
            }
        }

        filtering_restrictions.push_back(e);
    });

    for (auto& [col_def, col_def_eq_restrictions] : column_eq_restrictions) {
        _column_eq_restrictions[col_def] = conjunction{std::move(col_def_eq_restrictions)};
    }

    _token_range_restrictions = conjunction{std::move(token_range_restrictions)};

    if (all_columns_have_eq_restrictions()) {
        // _column_eq_restrictions will be used to generate partition ranges.
        // Add token range restrictions to filtering restrictions.
        // In the future it might be possible to do an intersection
        // of ranges generated using both of these restrictions,
        // but this isn't supported at the moment.
        for_each_boolean_factor(_token_range_restrictions,
                                [&](const expression& eq_restr) { filtering_restrictions.push_back(eq_restr); });
    } else {
        // Token range restrictions will be used to generate partition ranges.
        // Add column equality restrictions to filtering restrictions.
        for (auto&& [col_def, col_def_eq_restrictions] : _column_eq_restrictions) {
            for_each_boolean_factor(col_def_eq_restrictions,
                                    [&](const expression& eq_restr) { filtering_restrictions.push_back(eq_restr); });
        }
    }

    _filtering_restrictions = conjunction{std::move(filtering_restrictions)};
}

bool partition_key_restrictions::all_columns_have_eq_restrictions() const {
    return _column_eq_restrictions.size() == _table_schema->partition_key_size();
}

bool partition_key_restrictions::partition_key_restrictions_is_all_eq() const {
    bool result = true;
    for_each_boolean_factor(_partition_restrictions, [&](const expression& e) {
        const binary_operator* binop = as_if<binary_operator>(&e);
        if (binop == nullptr) {
            result = false;
        }
        if (binop->op != oper_t::EQ) {
            result = false;
        }
    });
    return result;
}

bool partition_key_restrictions::has_partition_key_unrestricted_components() const {
    std::vector<const column_definition*> pk_columns = expr::get_sorted_column_defs(_partition_restrictions);
    bool all_restricted = pk_columns.size() == _table_schema->partition_key_size();
    return !all_restricted;
}

bool partition_key_restrictions::has_token_restrictions() const {
    return has_token(_partition_restrictions);
}

bool partition_key_restrictions::is_key_range() const {
    return _column_eq_restrictions.size() != _table_schema->partition_key_size();
}

bool partition_key_restrictions::key_is_in_relation() const {
    bool result = false;
    for_each_boolean_factor(_partition_restrictions, [&](const expression& e) {
        if (auto binop = as_if<binary_operator>(&e)) {
            if (auto lhs_col = as_if<column_value>(&binop->lhs)) {
                if (lhs_col->col->is_partition_key() && binop->op == oper_t::IN) {
                    result = true;
                }
            }
        }
    });
    return result;
}

bool partition_key_restrictions::pk_restrictions_need_filtering() const {
    return !is_empty_restriction(_filtering_restrictions);
}

dht::partition_range_vector partition_key_restrictions::get_partition_key_ranges(const query_options& options) const {
    if (all_columns_have_eq_restrictions()) {
        return get_partition_key_ranges_from_column_eq_restrictions(options);
    }
    return get_partition_key_ranges_from_token_range_restrictions(options);
}

dht::partition_range_vector partition_key_restrictions::get_partition_key_ranges_from_token_range_restrictions(
    const query_options& options) const {
    const value_set token_values = possible_lhs_values(nullptr, _token_range_restrictions, options);

    return std::visit(overloaded_functor {
        [&](const value_list& token_values_list) {
            dht::partition_range_vector result;
            for (const managed_bytes& token_value : token_values_list) {
                const dht::token tok = token_value.with_linearized([](bytes_view bv) { return dht::token::from_bytes(bv); });
                result.emplace_back(dht::ring_position::starting_at(tok), dht::ring_position::ending_at(tok));
            }
            return result;
        },
        [&](const nonwrapping_range<managed_bytes>& token_range) -> dht::partition_range_vector {
            const dht::token start_token = token_range.start()
                ? token_range.start()->value().with_linearized([] (bytes_view bv) { return dht::token::from_bytes(bv); })
                : dht::minimum_token();
            const dht::token end_token = token_range.end()
                ? token_range.end()->value().with_linearized([] (bytes_view bv) { return dht::token::from_bytes(bv); })
                : dht::maximum_token();

            // This part is very weird.
            // It would make more sense to have something like:
            // bool include_start = token_range.start() ? token_range.start()->is_inclusive() : true;
            // dht::partition_range::bound start(dht::ring_position::starting_at(start_token), include_start);
            // but this fails the tests. I don't know why it's done this way, I just copied the existing code.
            const bool include_start = token_range.start() ? token_range.start()->is_inclusive() : false;
            const bool include_end = token_range.end() ? token_range.end()->is_inclusive() : false;
            auto start = dht::partition_range::bound(include_start
                                                    ? dht::ring_position::starting_at(start_token)
                                                    : dht::ring_position::ending_at(start_token));
            auto end = dht::partition_range::bound(include_end
                                                ? dht::ring_position::ending_at(end_token)
                                                : dht::ring_position::starting_at(end_token));
            return {{std::move(start), std::move(end)}};
        }
    }, token_values);
}

// Turns a partition-key value into a partition_range. pk_values must have elements for all partition columns.
static dht::partition_range partition_range_from_pk_values(const schema& schema, const std::vector<managed_bytes>& pk_values) {
    const partition_key pk = partition_key::from_exploded(pk_values);
    const dht::token tok = dht::get_token(schema, pk);
    const dht::ring_position pos(std::move(tok), std::move(pk));
    return dht::partition_range::make_singular(std::move(pos));
}

dht::partition_range_vector partition_key_restrictions::get_partition_key_ranges_from_column_eq_restrictions(
    const query_options& options) const {
    const size_t size_limit =
        options.get_cql_config().restrictions.partition_key_restrictions_max_cartesian_product_size;
    // Each element is a vector of that column's possible values:
    std::vector<std::vector<managed_bytes>> column_values(_table_schema->partition_key_size());
    size_t product_size = 1;
    for (auto&& [pk_col_def, pk_col_eq_restrictions] : _column_eq_restrictions) {
        const value_set vals = possible_lhs_values(pk_col_def, pk_col_eq_restrictions, options);
        if (auto lst = std::get_if<value_list>(&vals)) {
            if (lst->empty()) {
                return {};
            }
            product_size *= lst->size();
            if (product_size > size_limit) {
                throw std::runtime_error(fmt::format(
                    "clustering-key cartesian product size {} is greater than maximum {}", product_size, size_limit));
            }
            column_values[_table_schema->position(*pk_col_def)] = std::move(*lst);
        } else {
            on_internal_error(rlogger, "possible_lhs_values for column_eq_restrictions didnt produce a list of values");
        }
    }
    cartesian_product cp(column_values);
    dht::partition_range_vector ranges;
    ranges.reserve(product_size);
    for (const std::vector<managed_bytes>& pk_values : cp) {
        ranges.push_back(partition_range_from_pk_values(*_table_schema, pk_values));
    }
    return ranges;
}
}  // namespace restrictions
}  // namespace cql3
