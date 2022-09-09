/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "partition_key_restrictions.hh"
#include "cql3/expr/expression.hh"

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
}  // namespace restrictions
}  // namespace cql3
