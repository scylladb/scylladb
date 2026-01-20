/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "vector_search/filter.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "cql3/query_options.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/expr/evaluate.hh"
#include "types/json_utils.hh"

namespace vector_search {

namespace {

std::optional<sstring> to_single_column_op_string(cql3::expr::oper_t op) {
    switch (op) {
    case cql3::expr::oper_t::EQ:
        return "==";
    case cql3::expr::oper_t::LT:
        return "<";
    case cql3::expr::oper_t::LTE:
        return "<=";
    case cql3::expr::oper_t::GT:
        return ">";
    case cql3::expr::oper_t::GTE:
        return ">=";
    case cql3::expr::oper_t::IN:
        return "IN";
    default:
        return std::nullopt;
    }
}

std::optional<sstring> to_multi_column_op_string(cql3::expr::oper_t op) {
    switch (op) {
    case cql3::expr::oper_t::EQ:
        return "()==()";
    case cql3::expr::oper_t::LT:
        return "()<()";
    case cql3::expr::oper_t::LTE:
        return "()<=()";
    case cql3::expr::oper_t::GT:
        return "()>()";
    case cql3::expr::oper_t::GTE:
        return "()>=()";
    case cql3::expr::oper_t::IN:
        return "()IN()";
    default:
        return std::nullopt;
    }
}

rjson::value value_to_json(const data_type& type, const cql3::raw_value& val) {
    if (val.is_null()) {
        return rjson::null_value();
    }
    auto json_str = to_json_string(*type, to_bytes(val.view()));
    return rjson::parse(json_str);
}

void single_column_restriction_to_json(const cql3::expr::binary_operator& binop, const cql3::expr::column_value& col,
        const cql3::query_options& options, rjson::value& restrictions_arr) {
    auto op_str = to_single_column_op_string(binop.op);
    if (!op_str) {
        throw exceptions::unsupported_operation_exception(
            sstring("Unsupported operator in restriction on column ") + col.col->name_as_text()
        );
    }

    auto obj = rjson::empty_object();
    rjson::add(obj, "type", rjson::from_string(*op_str));
    rjson::add(obj, "lhs", rjson::from_string(col.col->name_as_text()));

    auto rhs_type = cql3::expr::type_of(binop.rhs);
    auto rhs_val = cql3::expr::evaluate(binop.rhs, options);
    rjson::add(obj, "rhs", value_to_json(rhs_type, rhs_val));

    rjson::push_back(restrictions_arr, std::move(obj));
}

rjson::value column_names_to_json(const cql3::expr::tuple_constructor& lhs_tuple) {
    auto arr = rjson::empty_array();
    for (const auto& elem : lhs_tuple.elements) {
        if (auto* cv = cql3::expr::as_if<cql3::expr::column_value>(&elem)) {
            rjson::push_back(arr, rjson::from_string(cv->col->name_as_text()));
        }
    }
    return arr;
}

void multi_column_restriction_to_json(const cql3::expr::binary_operator& binop, const cql3::expr::tuple_constructor& lhs_tuple,
        const cql3::query_options& options, rjson::value& restrictions_arr) {
    auto op_str = to_multi_column_op_string(binop.op);
    if (!op_str) {
        throw exceptions::unsupported_operation_exception(
            sstring("Unsupported operator in restriction on columns ")+ to_string(lhs_tuple)
        );
    }

    auto obj = rjson::empty_object();
    rjson::add(obj, "type", rjson::from_string(*op_str));
    rjson::add(obj, "lhs", column_names_to_json(lhs_tuple));

    auto rhs_type = cql3::expr::type_of(binop.rhs);
    auto rhs_val = cql3::expr::evaluate(binop.rhs, options);
    rjson::add(obj, "rhs", value_to_json(rhs_type, rhs_val));

    rjson::push_back(restrictions_arr, std::move(obj));
}

void binary_operator_to_json(const cql3::expr::binary_operator& binop, const cql3::query_options& options, rjson::value& restrictions_arr) {
    if (auto* cv = cql3::expr::as_if<cql3::expr::column_value>(&binop.lhs)) {
        single_column_restriction_to_json(binop, *cv, options, restrictions_arr);
        return;
    }

    if (auto* tuple = cql3::expr::as_if<cql3::expr::tuple_constructor>(&binop.lhs)) {
        multi_column_restriction_to_json(binop, *tuple, options, restrictions_arr);
        return;
    }
}

void expression_to_json(const cql3::expr::expression& expr, const cql3::query_options& options, rjson::value& restrictions_arr) {
    cql3::expr::for_each_expression<cql3::expr::binary_operator>(expr, [&](const cql3::expr::binary_operator& binop) {
        binary_operator_to_json(binop, options, restrictions_arr);
    });
}

} // anonymous namespace

rjson::value to_json(const cql3::restrictions::statement_restrictions& restrictions, const cql3::query_options& options, bool allow_filtering) {
    auto result = rjson::empty_object();

    if (restrictions.is_empty() && !allow_filtering) {
        return result;
    }

    auto restrictions_arr = rjson::empty_array();
    expression_to_json(restrictions.get_partition_key_restrictions(), options, restrictions_arr);
    expression_to_json(restrictions.get_clustering_columns_restrictions(), options, restrictions_arr);

    rjson::add(result, "restrictions", std::move(restrictions_arr));
    rjson::add(result, "allow_filtering", rjson::value(allow_filtering));

    return result;
}

} // namespace vector_search
