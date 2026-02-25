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

rjson::value lhs_to_json(const cql3::expr::column_value& col) {
    return rjson::from_string(col.col->name_as_text());
}

rjson::value lhs_to_json(const cql3::expr::tuple_constructor& lhs_tuple) {
    auto arr = rjson::empty_array();
    for (const auto& elem : lhs_tuple.elements) {
        if (auto* cv = cql3::expr::as_if<cql3::expr::column_value>(&elem)) {
            rjson::push_back(arr, rjson::from_string(cv->col->name_as_text()));
        }
    }
    return arr;
}

prepared_restriction make_prepared_restriction(const sstring& op_str, rjson::value lhs_json, const cql3::expr::expression& rhs_expr) {
    auto rhs_type = cql3::expr::type_of(rhs_expr);
    if (cql3::expr::contains_bind_marker(rhs_expr)) {
        return prepared_restriction{
                .type_json = rjson::from_string(op_str), .lhs_json = std::move(lhs_json), .rhs = prepared_rhs{std::move(rhs_type), rhs_expr}};
    } else {
        auto rhs_val = cql3::expr::evaluate(rhs_expr, cql3::query_options({}));
        return prepared_restriction{.type_json = rjson::from_string(op_str), .lhs_json = std::move(lhs_json), .rhs = value_to_json(rhs_type, rhs_val)};
    }
}

void single_column_restriction_to_prepared(
        const cql3::expr::binary_operator& binop, const cql3::expr::column_value& col, std::vector<prepared_restriction>& restrictions) {
    auto op_str = to_single_column_op_string(binop.op);
    if (!op_str) {
        throw exceptions::unsupported_operation_exception(sstring("Unsupported operator in restriction on column ") + col.col->name_as_text());
    }

    restrictions.push_back(make_prepared_restriction(*op_str, lhs_to_json(col), binop.rhs));
}

void multi_column_restriction_to_prepared(
        const cql3::expr::binary_operator& binop, const cql3::expr::tuple_constructor& lhs_tuple, std::vector<prepared_restriction>& restrictions) {
    auto op_str = to_multi_column_op_string(binop.op);
    if (!op_str) {
        throw exceptions::unsupported_operation_exception(sstring("Unsupported operator in restriction on columns ") + to_string(lhs_tuple));
    }

    restrictions.push_back(make_prepared_restriction(*op_str, lhs_to_json(lhs_tuple), binop.rhs));
}

void binary_operator_to_prepared(const cql3::expr::binary_operator& binop, std::vector<prepared_restriction>& restrictions) {
    if (auto* cv = cql3::expr::as_if<cql3::expr::column_value>(&binop.lhs)) {
        single_column_restriction_to_prepared(binop, *cv, restrictions);
        return;
    }

    if (auto* tuple = cql3::expr::as_if<cql3::expr::tuple_constructor>(&binop.lhs)) {
        multi_column_restriction_to_prepared(binop, *tuple, restrictions);
        return;
    }
}

void expression_to_prepared(const cql3::expr::expression& expr, std::vector<prepared_restriction>& restrictions) {
    cql3::expr::for_each_expression<cql3::expr::binary_operator>(expr, [&](const cql3::expr::binary_operator& binop) {
        binary_operator_to_prepared(binop, restrictions);
    });
}

rjson::value restriction_to_json(const prepared_restriction& r, const cql3::query_options& options) {
    auto obj = rjson::empty_object();
    rjson::add(obj, "type", rjson::copy(r.type_json));
    rjson::add(obj, "lhs", rjson::copy(r.lhs_json));
    rjson::add(obj, "rhs", r.rhs_to_json(options));
    return obj;
}

rjson::value restrictions_to_json(const std::vector<prepared_restriction>& restrictions, bool allow_filtering, const cql3::query_options& options) {
    auto result = rjson::empty_object();

    if (restrictions.empty() && !allow_filtering) {
        return result;
    }

    auto restrictions_arr = rjson::empty_array();
    for (const auto& r : restrictions) {
        rjson::push_back(restrictions_arr, restriction_to_json(r, options));
    }

    rjson::add(result, "restrictions", std::move(restrictions_arr));
    rjson::add(result, "allow_filtering", rjson::value(allow_filtering));

    return result;
}

} // anonymous namespace

rjson::value prepared_restriction::rhs_to_json(const cql3::query_options& options) const {
    return std::visit(
            [&](const auto& v) -> rjson::value {
                using T = std::decay_t<decltype(v)>;
                if constexpr (std::is_same_v<T, rjson::value>) {
                    return rjson::copy(v);
                } else {
                    const auto& [type, expr] = v;
                    auto val = cql3::expr::evaluate(expr, options);
                    return value_to_json(type, val);
                }
            },
            rhs);
}

rjson::value prepared_filter::to_json(const cql3::query_options& options) const {
    if (_cached_json) {
        return rjson::copy(_cached_json.value());
    }

    return restrictions_to_json(_restrictions, _allow_filtering, options);
}

prepared_filter prepare_filter(const cql3::restrictions::statement_restrictions& restrictions, bool allow_filtering) {
    if (restrictions.is_empty()) {
        return prepared_filter({}, allow_filtering);
    }

    std::vector<prepared_restriction> prepared_restrictions;

    auto& partition_key_restrictions = restrictions.get_partition_key_restrictions();
    auto& clustering_columns_restrictions = restrictions.get_clustering_columns_restrictions();

    expression_to_prepared(partition_key_restrictions, prepared_restrictions);
    expression_to_prepared(clustering_columns_restrictions, prepared_restrictions);

    bool has_bind_markers = cql3::expr::contains_bind_marker(partition_key_restrictions) || cql3::expr::contains_bind_marker(clustering_columns_restrictions);

    if (!has_bind_markers) {
        auto cached_json = restrictions_to_json(prepared_restrictions, allow_filtering, cql3::query_options({}));
        return prepared_filter(std::move(prepared_restrictions), allow_filtering, std::move(cached_json));
    }

    return prepared_filter(std::move(prepared_restrictions), allow_filtering);
}

} // namespace vector_search
