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

namespace cql3::restrictions {

namespace {

std::optional<sstring> to_single_column_op_string(expr::oper_t op) {
    switch (op) {
    case expr::oper_t::EQ:
        return "==";
    case expr::oper_t::LT:
        return "<";
    case expr::oper_t::LTE:
        return "<=";
    case expr::oper_t::GT:
        return ">";
    case expr::oper_t::GTE:
        return ">=";
    case expr::oper_t::IN:
        return "IN";
    default:
        return std::nullopt;
    }
}

std::optional<sstring> to_multi_column_op_string(expr::oper_t op) {
    switch (op) {
    case expr::oper_t::EQ:
        return "()==()";
    case expr::oper_t::LT:
        return "()<()";
    case expr::oper_t::LTE:
        return "()<=()";
    case expr::oper_t::GT:
        return "()>()";
    case expr::oper_t::GTE:
        return "()>=()";
    case expr::oper_t::IN:
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

void process_single_column_restriction(const expr::binary_operator& binop, const expr::column_value& col,
        const query_options& options, rjson::value& restrictions_arr) {
    auto op_str = to_single_column_op_string(binop.op);
    if (!op_str) {
        throw exceptions::unsupported_operation_exception(
            sstring("Unsupported operator in restriction on column ") + col.col->name_as_text()
        );
    }

    auto obj = rjson::empty_object();
    rjson::add(obj, "type", rjson::from_string(*op_str));
    rjson::add(obj, "lhs", rjson::from_string(col.col->name_as_text()));

    auto rhs_type = expr::type_of(binop.rhs);
    auto rhs_val = expr::evaluate(binop.rhs, options);
    rjson::add(obj, "rhs", value_to_json(rhs_type, rhs_val));

    rjson::push_back(restrictions_arr, std::move(obj));
}

rjson::value extract_column_names_json(const expr::tuple_constructor& lhs_tuple) {
    auto arr = rjson::empty_array();
    for (const auto& elem : lhs_tuple.elements) {
        if (auto* cv = expr::as_if<expr::column_value>(&elem)) {
            rjson::push_back(arr, rjson::from_string(cv->col->name_as_text()));
        }
    }
    return arr;
}

void process_multi_column_restriction(const expr::binary_operator& binop, const expr::tuple_constructor& lhs_tuple,
        const query_options& options, rjson::value& restrictions_arr) {
    auto op_str = to_multi_column_op_string(binop.op);
    if (!op_str) {
        throw exceptions::unsupported_operation_exception(
            sstring("Unsupported operator in restriction on columns ")+ to_string(lhs_tuple)
        );
    }

    auto obj = rjson::empty_object();
    rjson::add(obj, "type", rjson::from_string(*op_str));
    rjson::add(obj, "lhs", extract_column_names_json(lhs_tuple));

    auto rhs_type = expr::type_of(binop.rhs);
    auto rhs_val = expr::evaluate(binop.rhs, options);
    rjson::add(obj, "rhs", value_to_json(rhs_type, rhs_val));

    rjson::push_back(restrictions_arr, std::move(obj));
}

void process_binary_operator(const expr::binary_operator& binop, const query_options& options, rjson::value& restrictions_arr) {
    if (auto* cv = expr::as_if<expr::column_value>(&binop.lhs)) {
        process_single_column_restriction(binop, *cv, options, restrictions_arr);
        return;
    }

    if (auto* tuple = expr::as_if<expr::tuple_constructor>(&binop.lhs)) {
        process_multi_column_restriction(binop, *tuple, options, restrictions_arr);
        return;
    }
}

void extract_restrictions_from_expression(const expr::expression& expr, const query_options& options, rjson::value& restrictions_arr) {
    expr::for_each_expression<expr::binary_operator>(expr, [&](const expr::binary_operator& binop) {
        process_binary_operator(binop, options, restrictions_arr);
    });
}

} // anonymous namespace

rjson::value to_json(const statement_restrictions& restrictions, const query_options& options, bool allow_filtering) {
    auto result = rjson::empty_object();

    if (restrictions.is_empty() && !allow_filtering) {
        return result;
    }

    auto restrictions_arr = rjson::empty_array();
    extract_restrictions_from_expression(restrictions.get_partition_key_restrictions(), options, restrictions_arr);
    extract_restrictions_from_expression(restrictions.get_clustering_columns_restrictions(), options, restrictions_arr);

    rjson::add(result, "restrictions", std::move(restrictions_arr));
    rjson::add(result, "allow_filtering", rjson::value(allow_filtering));

    return result;
}

} // namespace cql3::restrictions
