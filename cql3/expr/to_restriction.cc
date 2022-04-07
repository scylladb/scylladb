/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "expression.hh"
#include "cql3/restrictions/single_column_restriction.hh"
#include "cql3/restrictions/multi_column_restriction.hh"
#include "cql3/restrictions/token_restriction.hh"
#include "cql3/prepare_context.hh"

namespace cql3 {
namespace expr {
extern logging::logger expr_logger;

namespace {

bool is_legal_relation_for_non_frozen_collection(oper_t oper, bool is_lhs_col_indexed) {
    return oper == oper_t::CONTAINS_KEY || oper == oper_t::CONTAINS || (oper == oper_t::EQ && is_lhs_col_indexed);
}

bool oper_is_slice(oper_t oper) {
    switch(oper) {
        case oper_t::LT:
        case oper_t::LTE:
        case oper_t::GT:
        case oper_t::GTE:
            return true;
        case oper_t::EQ:
        case oper_t::NEQ:
        case oper_t::IN:
        case oper_t::CONTAINS:
        case oper_t::CONTAINS_KEY:
        case oper_t::IS_NOT:
        case oper_t::LIKE:
            return false;
    }
    on_internal_error(expr_logger, format("oper_is_slice: Unhandled oper_t: {}", oper));
}

void validate_single_column_relation(const column_value& lhs, oper_t oper, const schema& schema, bool is_lhs_subscripted) {
    using namespace statements::request_validations; // used for check_false and check_true

    const abstract_type& lhs_col_type = lhs.col->column_specification->type->without_reversed();
    const ::shared_ptr<column_identifier>& lhs_col_name = lhs.col->column_specification->name;

    if (schema.is_dense() && lhs.col->is_regular()) {
        throw exceptions::invalid_request_exception(
            format("Predicates on the non-primary-key column ({}) of a COMPACT table are not yet supported",
                   lhs.col->name_as_text()));
    }

    if (oper_is_slice(oper) && lhs_col_type.references_duration()) {
        using statements::request_validations::check_false;

        check_false(lhs_col_type.is_collection(), "Slice restrictions are not supported on collections containing durations");
        check_false(lhs_col_type.is_user_type(), "Slice restrictions are not supported on UDTs containing durations");
        check_false(lhs_col_type.is_tuple(), "Slice restrictions are not supported on tuples containing durations");

        // We're a duration.
        throw exceptions::invalid_request_exception("Slice restrictions are not supported on duration columns");
    }

    if (is_lhs_subscripted) {
        check_false(lhs_col_type.is_list(),
                    "Indexes on list entries ({}[index] = value) are not currently supported.",
                    lhs_col_name);
        check_true(lhs_col_type.is_map(), "Column {} cannot be used as a map", lhs_col_name);
        check_true(lhs_col_type.is_multi_cell(),
                   "Map-entry equality predicates on frozen map column {} are not supported",
                   lhs_col_name);
        check_true(oper == oper_t::EQ, "Only EQ relations are supported on map entries");
    }

    if (lhs_col_type.is_collection()) {
        // We don't support relations against entire collections (unless they're frozen), like "numbers = {1, 2, 3}"
        check_false(lhs_col_type.is_multi_cell()
                    && !is_legal_relation_for_non_frozen_collection(oper, is_lhs_subscripted),
                    "Collection column '{}' ({}) cannot be restricted by a '{}' relation",
                    lhs_col_name,
                    lhs_col_type.as_cql3_type(),
                    oper);
    }
}

::shared_ptr<restrictions::restriction> new_single_column_restriction(column_value&& lhs_col,
                                                                      oper_t oper,
                                                                      expression&& prepared_rhs,
                                                                      comparison_order order,
                                                                      const schema& schema) {
    validate_single_column_relation(lhs_col, oper, schema, false);

    ::shared_ptr<restrictions::restriction> r = ::make_shared<restrictions::single_column_restriction>(*lhs_col.col);
    r->expression = binary_operator(std::move(lhs_col), oper, std::move(prepared_rhs), order);
    return r;
}

::shared_ptr<restrictions::restriction> new_subscripted_column_restriction(subscript&& lhs_sub,
                                                                           oper_t oper,
                                                                           expression&& prepared_rhs,
                                                                           comparison_order order,
                                                                           const schema& schema) {
    const column_value& sub_col = get_subscripted_column(lhs_sub);
    validate_single_column_relation(sub_col, oper, schema, true);

    ::shared_ptr<restrictions::restriction> r = ::make_shared<restrictions::single_column_restriction>(*sub_col.col);
    r->expression = binary_operator(std::move(lhs_sub), oper, std::move(prepared_rhs));
    return r;
}

} // anonymous namespace

::shared_ptr<restrictions::restriction> to_restriction(const expression& e,
                                                       data_dictionary::database db,
                                                       schema_ptr schema,
                                                       prepare_context& ctx) {
    if (is<constant>(e)) {
        throw exceptions::invalid_request_exception("Constant value by itself is not supported as a restriction yet");
    }

    if (!is<binary_operator>(e)) {
        throw exceptions::invalid_request_exception("Restriction must be a binary operator");
    }
    binary_operator binop_to_prepare = as<binary_operator>(e);

    // Convert single element IN relation to an EQ relation
    if (binop_to_prepare.op == oper_t::IN && is<collection_constructor>(binop_to_prepare.rhs)) {
        const std::vector<expression>& elements = as<collection_constructor>(binop_to_prepare.rhs).elements;
        if (elements.size() == 1) {
            binop_to_prepare.op = oper_t::EQ;
            binop_to_prepare.rhs = elements[0];
        }
    }

    binary_operator prepared_binop = prepare_binary_operator(binop_to_prepare, db, schema, ctx);

    const column_value* lhs_pk_col_search_res = find_in_expression<column_value>(prepared_binop.lhs,
        [](const column_value& col) {
            return col.col->is_partition_key();
        }
    );
    auto reset_processing_pk_column = defer([&ctx] () noexcept { ctx.set_processing_pk_restrictions(false); });
    if (lhs_pk_col_search_res != nullptr) {
        ctx.set_processing_pk_restrictions(true);
    }
    fill_prepare_context(prepared_binop.lhs, ctx);
    fill_prepare_context(prepared_binop.rhs, ctx);

    if (prepared_binop.op == oper_t::NEQ) {
        throw exceptions::invalid_request_exception(format("Unsupported \"!=\" relation: {}", prepared_binop));
    }

    if (prepared_binop.op == oper_t::IS_NOT && !is<null>(prepared_binop.rhs)) {
        throw exceptions::invalid_request_exception(format("Unsupported \"IS NOT\" relation: {}", prepared_binop));
    }

    if (auto col_val = as_if<column_value>(&prepared_binop.lhs)) {
        return new_single_column_restriction(std::move(*col_val), prepared_binop.op, std::move(prepared_binop.rhs), prepared_binop.order, *schema);
    }

    if (auto sub = as_if<subscript>(&prepared_binop.lhs)) {
        return new_subscripted_column_restriction(std::move(*sub), prepared_binop.op, std::move(prepared_binop.rhs), prepared_binop.order, *schema);
    }

    on_internal_error(expr_logger, format("expr::to_restriction unhandled case: {}", e));
}

} // namespace expr
} // namespace cql3