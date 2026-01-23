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

void write_to_json(bytes_ostream& out, std::string_view s) {
    out.write(s.data(), s.size());
}

void write_lhs_json(bytes_ostream& out, const cql3::expr::column_value& col) {
    write_to_json(out, "\"");
    write_to_json(out, col.col->name_as_text());
    write_to_json(out, "\"");
}

void write_lhs_json(bytes_ostream& out, const cql3::expr::tuple_constructor& lhs_tuple) {
    write_to_json(out, "[");
    bool first = true;
    for (const auto& elem : lhs_tuple.elements) {
        if (auto* cv = cql3::expr::as_if<cql3::expr::column_value>(&elem)) {
            if (!first) {
                write_to_json(out, ",");
            }
            first = false;
            write_lhs_json(out, *cv);
        }
    }
    write_to_json(out, "]");
}

void write_rhs_json(bytes_ostream& out, const cql3::expr::expression& rhs_expr, std::vector<bind_marker_metadata>& bind_markers) {
    auto rhs_type = cql3::expr::type_of(rhs_expr);
    if (cql3::expr::contains_bind_marker(rhs_expr)) {
        bind_markers.push_back(bind_marker_metadata{out.size(), std::move(rhs_type), rhs_expr});
    } else {
        auto rhs_val = cql3::expr::evaluate(rhs_expr, cql3::query_options({}));
        auto json_str = value_to_json(rhs_type, rhs_val);
        write_to_json(out, json_str);
    }
}

class json_array_builder {
    bytes_ostream& _out;
    bool _first = true;

public:
    explicit json_array_builder(bytes_ostream& out)
        : _out(out) {
    }

    template <typename Func>
    void add_element(Func&& write_func) {
        if (!_first) {
            write_to_json(_out, ",");
        }
        _first = false;
        write_func(_out);
    }
};

void write_restriction_json(bytes_ostream& out, const sstring& op_str, const cql3::expr::column_value& col, const cql3::expr::expression& rhs_expr,
        std::vector<bind_marker_metadata>& bind_markers) {
    write_to_json(out, R"({"type":")");
    write_to_json(out, op_str);
    write_to_json(out, R"(","lhs":)");
    write_lhs_json(out, col);
    write_to_json(out, R"(,"rhs":)");
    write_rhs_json(out, rhs_expr, bind_markers);
    write_to_json(out, "}");
}

void write_restriction_json(bytes_ostream& out, const sstring& op_str, const cql3::expr::tuple_constructor& lhs_tuple, const cql3::expr::expression& rhs_expr,
        std::vector<bind_marker_metadata>& bind_markers) {
    write_to_json(out, R"({"type":")");
    write_to_json(out, op_str);
    write_to_json(out, R"(","lhs":)");
    write_lhs_json(out, lhs_tuple);
    write_to_json(out, R"(,"rhs":)");
    write_rhs_json(out, rhs_expr, bind_markers);
    write_to_json(out, "}");
}

void single_column_restriction_to_json(const cql3::expr::binary_operator& binop, const cql3::expr::column_value& col,
        std::vector<bind_marker_metadata>& bind_markers, json_array_builder& builder) {
    auto op_str = to_single_column_op_string(binop.op);
    if (!op_str) {
        throw exceptions::unsupported_operation_exception(sstring("Unsupported operator in restriction on column ") + col.col->name_as_text());
    }

    builder.add_element([&](bytes_ostream& out) {
        write_restriction_json(out, *op_str, col, binop.rhs, bind_markers);
    });
}

void multi_column_restriction_to_json(const cql3::expr::binary_operator& binop, const cql3::expr::tuple_constructor& lhs_tuple,
        std::vector<bind_marker_metadata>& bind_markers, json_array_builder& builder) {
    auto op_str = to_multi_column_op_string(binop.op);
    if (!op_str) {
        throw exceptions::unsupported_operation_exception(sstring("Unsupported operator in restriction on columns ") + to_string(lhs_tuple));
    }

    builder.add_element([&](bytes_ostream& out) {
        write_restriction_json(out, *op_str, lhs_tuple, binop.rhs, bind_markers);
    });
}

void binary_operator_to_json(const cql3::expr::binary_operator& binop, std::vector<bind_marker_metadata>& bind_markers, json_array_builder& builder) {
    if (auto* cv = cql3::expr::as_if<cql3::expr::column_value>(&binop.lhs)) {
        single_column_restriction_to_json(binop, *cv, bind_markers, builder);
        return;
    }

    if (auto* tuple = cql3::expr::as_if<cql3::expr::tuple_constructor>(&binop.lhs)) {
        multi_column_restriction_to_json(binop, *tuple, bind_markers, builder);
        return;
    }
}

void expression_to_json(const cql3::expr::expression& expr, std::vector<bind_marker_metadata>& bind_markers, json_array_builder& builder) {
    cql3::expr::for_each_expression<cql3::expr::binary_operator>(expr, [&](const cql3::expr::binary_operator& binop) {
        binary_operator_to_json(binop, bind_markers, builder);
    });
}

bytes_ostream substitute_bind_markers(
        const bytes_ostream& cached_json, const std::vector<bind_marker_metadata>& bind_markers, const cql3::query_options& options) {
    auto frag = cached_json.begin();
    ser::buffer_view<bytes_ostream::fragment_iterator> view(*frag, cached_json.size(), std::next(frag));

    bytes_ostream result;
    size_t current_offset = 0;

    for (const auto& marker : bind_markers) {
        // Copy bytes from current_offset to marker.offset
        if (marker.offset > current_offset) {
            auto prefix = view.prefix(marker.offset - current_offset);
            for (bytes_view frag : prefix) {
                result.write(frag);
            }
            view.remove_prefix(marker.offset - current_offset);
        }

        // Write the bind marker value
        auto val = cql3::expr::evaluate(marker.expr, options);
        auto json_str = value_to_json(marker.type, val);
        result.write(json_str.data(), json_str.size());

        current_offset = marker.offset;
    }

    // Copy remaining bytes after last bind marker
    for (bytes_view frag : view) {
        result.write(frag);
    }

    return result;
}

} // anonymous namespace

bytes_ostream prepared_filter::to_json(const cql3::query_options& options) const {
    if (_bind_markers.empty()) {
        return _cached_json;
    }

    return substitute_bind_markers(_cached_json, _bind_markers, options);
}

prepared_filter prepare_filter(const cql3::restrictions::statement_restrictions& restrictions, bool allow_filtering) {
    bytes_ostream out;
    std::vector<bind_marker_metadata> bind_markers;

    if (restrictions.is_empty() && !allow_filtering) {
        return prepared_filter(std::move(out), std::move(bind_markers));
    }

    auto& partition_key_restrictions = restrictions.get_partition_key_restrictions();
    auto& clustering_columns_restrictions = restrictions.get_clustering_columns_restrictions();

    write_to_json(out, R"({"restrictions":[)");

    json_array_builder builder(out);
    expression_to_json(partition_key_restrictions, bind_markers, builder);
    expression_to_json(clustering_columns_restrictions, bind_markers, builder);

    write_to_json(out, R"(],"allow_filtering":)");
    write_to_json(out, allow_filtering ? "true" : "false");
    write_to_json(out, "}");

    return prepared_filter(std::move(out), std::move(bind_markers));
}

} // namespace vector_search
