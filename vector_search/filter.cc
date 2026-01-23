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

sstring value_to_json(const data_type& type, const cql3::raw_value& val) {
    if (val.is_null()) {
        return "null";
    }
    return to_json_string(*type, to_bytes(val.view()));
}

void append(prepared_filter::cache& c, std::string_view s) {
    if (!c.empty() && std::holds_alternative<sstring>(c.back())) {
        std::get<sstring>(c.back()).append(s.data(), s.size());
    } else {
        c.emplace_back(sstring(s));
    }
}

void append_lhs(prepared_filter::cache& c, const cql3::expr::column_value& col) {
    append(c, "\"");
    append(c, col.col->name_as_text());
    append(c, "\"");
}

void append_lhs(prepared_filter::cache& c, const cql3::expr::tuple_constructor& lhs_tuple) {
    append(c, "[");
    bool first = true;
    for (const auto& elem : lhs_tuple.elements) {
        if (auto* cv = cql3::expr::as_if<cql3::expr::column_value>(&elem)) {
            if (!first) {
                append(c, ",");
            }
            first = false;
            append_lhs(c, *cv);
        }
    }
    append(c, "]");
}

void append_rhs(prepared_filter::cache& c, const cql3::expr::expression& rhs_expr) {
    auto rhs_type = cql3::expr::type_of(rhs_expr);
    if (cql3::expr::contains_bind_marker(rhs_expr)) {
        c.emplace_back(bind_marker_metadata{std::move(rhs_type), rhs_expr});
    } else {
        auto rhs_val = cql3::expr::evaluate(rhs_expr, cql3::query_options({}));
        append(c, value_to_json(rhs_type, rhs_val));
    }
}

void append_separator(prepared_filter::cache& c, bool& needs_comma) {
    if (needs_comma) {
        append(c, ",");
    }
    needs_comma = true;
}

void append_restriction(prepared_filter::cache& c, const sstring& op_str, const cql3::expr::column_value& col, const cql3::expr::expression& rhs_expr) {
    append(c, "{\"type\":\"");
    append(c, op_str);
    append(c, "\",\"lhs\":");
    append_lhs(c, col);
    append(c, ",\"rhs\":");
    append_rhs(c, rhs_expr);
    append(c, "}");
}

void append_restriction(prepared_filter::cache& c, const sstring& op_str, const cql3::expr::tuple_constructor& lhs_tuple, const cql3::expr::expression& rhs_expr) {
    append(c, "{\"type\":\"");
    append(c, op_str);
    append(c, "\",\"lhs\":");
    append_lhs(c, lhs_tuple);
    append(c, ",\"rhs\":");
    append_rhs(c, rhs_expr);
    append(c, "}");
}

void single_column_restriction_to_json(const cql3::expr::binary_operator& binop, const cql3::expr::column_value& col, prepared_filter::cache& c, bool& needs_comma) {
    auto op_str = to_single_column_op_string(binop.op);
    if (!op_str) {
        throw exceptions::unsupported_operation_exception(sstring("Unsupported operator in restriction on column ") + col.col->name_as_text());
    }

    append_separator(c, needs_comma);
    append_restriction(c, *op_str, col, binop.rhs);
}

void multi_column_restriction_to_json(const cql3::expr::binary_operator& binop, const cql3::expr::tuple_constructor& lhs_tuple, prepared_filter::cache& c, bool& needs_comma) {
    auto op_str = to_multi_column_op_string(binop.op);
    if (!op_str) {
        throw exceptions::unsupported_operation_exception(sstring("Unsupported operator in restriction on columns ") + to_string(lhs_tuple));
    }

    append_separator(c, needs_comma);
    append_restriction(c, *op_str, lhs_tuple, binop.rhs);
}

void binary_operator_to_json(const cql3::expr::binary_operator& binop, prepared_filter::cache& c, bool& needs_comma) {
    if (auto* cv = cql3::expr::as_if<cql3::expr::column_value>(&binop.lhs)) {
        single_column_restriction_to_json(binop, *cv, c, needs_comma);
        return;
    }

    if (auto* tuple = cql3::expr::as_if<cql3::expr::tuple_constructor>(&binop.lhs)) {
        multi_column_restriction_to_json(binop, *tuple, c, needs_comma);
        return;
    }
}

void expression_to_json(const cql3::expr::expression& expr, prepared_filter::cache& c, bool& needs_comma) {
    cql3::expr::for_each_expression<cql3::expr::binary_operator>(expr, [&](const cql3::expr::binary_operator& binop) {
        binary_operator_to_json(binop, c, needs_comma);
    });
}

} // anonymous namespace

bytes_ostream prepared_filter::to_json(const cql3::query_options& options) const {
    bytes_ostream result;
    for (const auto& entry : _cache) {
        std::visit(overloaded_functor{
            [&](const sstring& s) {
                result.write(s.data(), s.size());
            },
            [&](const bind_marker_metadata& marker) {
                auto val = cql3::expr::evaluate(marker.expr, options);
                auto json_str = value_to_json(marker.type, val);
                result.write(json_str.data(), json_str.size());
            }
        }, entry);
    }
    return result;
}

prepared_filter prepare_filter(const cql3::restrictions::statement_restrictions& restrictions, bool allow_filtering) {
    prepared_filter::cache c;

    if (restrictions.is_empty() && !allow_filtering) {
        return prepared_filter(std::move(c));
    }

    auto& partition_key_restrictions = restrictions.get_partition_key_restrictions();
    auto& clustering_columns_restrictions = restrictions.get_clustering_columns_restrictions();

    append(c, "{\"restrictions\":[");

    bool needs_comma = false;
    expression_to_json(partition_key_restrictions, c, needs_comma);
    expression_to_json(clustering_columns_restrictions, c, needs_comma);

    append(c, "],\"allow_filtering\":");
    append(c, allow_filtering ? "true" : "false");
    append(c, "}");

    return prepared_filter(std::move(c));
}

} // namespace vector_search
