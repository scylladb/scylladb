/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "cql3/column_specification.hh"
#include "expression.hh"
#include "cql3/restrictions/single_column_restriction.hh"
#include "cql3/restrictions/multi_column_restriction.hh"
#include "cql3/restrictions/token_restriction.hh"
#include "cql3/prepare_context.hh"
#include "schema.hh"

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
        check_true(lhs_col_type.is_map() || lhs_col_type.is_list(), "Column {} cannot be subscripted", lhs_col_name);
        check_true(!lhs_col_type.is_map() || lhs_col_type.is_multi_cell(),
                   "Map-entry equality predicates on frozen map column {} are not supported",
                   lhs_col_name);
        check_true(!lhs_col_type.is_list() || lhs_col_type.is_multi_cell(),
                   "List element equality predicates on frozen list column {} are not supported",
                   lhs_col_name);
        check_true(oper == oper_t::EQ, "Only EQ relations are supported on map/list entries");
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

std::vector<const column_definition*> to_column_definitions(const std::vector<expression>& cols) {
    std::vector<const column_definition*> result;
    result.reserve(cols.size());

    for (const expression& col : cols) {
        if (auto col_val = as_if<column_value>(&col)) {
            result.push_back(col_val->col);
        } else {
            on_internal_error(expr_logger, format("to_column_definitions bad expression: {}", col));
        }
    }

    return result;
}

void validate_multi_column_relation(const std::vector<const column_definition*>& lhs, oper_t oper) {
    using namespace statements::request_validations;
    int previous_position = -1;
    for (const column_definition* col_def : lhs) {
        check_true(col_def->is_clustering_key(),
                   "Multi-column relations can only be applied to clustering columns but was applied to: {}",
                   col_def->name_as_text());

        // FIXME: the following restriction should be removed (CASSANDRA-8613)
        if (col_def->position() != unsigned(previous_position + 1)) {
            check_false(previous_position == -1, "Clustering columns may not be skipped in multi-column relations. "
                                                 "They should appear in the PRIMARY KEY order"); // TODO: More detailed messages?
            throw exceptions::invalid_request_exception(format("Clustering columns must appear in the PRIMARY KEY order in multi-column relations"));
        }
        previous_position = col_def->position();
    }
}

::shared_ptr<restrictions::restriction> new_multi_column_restriction(const tuple_constructor& prepared_lhs_tuple,
                                                                     oper_t oper,
                                                                     expression&& prepared_rhs,
                                                                     comparison_order order,
                                                                     const schema_ptr& schema) {
    std::vector<const column_definition*> lhs_cols = to_column_definitions(prepared_lhs_tuple.elements);
    std::vector<lw_shared_ptr<column_specification>> lhs_col_specs;
    lhs_col_specs.reserve(prepared_lhs_tuple.elements.size());

    for (const column_definition* col_def : lhs_cols) {
        lhs_col_specs.push_back(col_def->column_specification);
    }

    validate_multi_column_relation(lhs_cols, oper);

    if (oper == oper_t::EQ) {
        return ::make_shared<restrictions::multi_column_restriction::EQ>(schema,
                                                                         std::move(lhs_cols),
                                                                         std::move(prepared_rhs));
    }

    if (oper == oper_t::LT) {
        return ::make_shared<restrictions::multi_column_restriction::slice>(schema,
                                                                            std::move(lhs_cols),
                                                                            statements::bound::END,
                                                                            false,
                                                                            std::move(prepared_rhs),
                                                                            order);
    }

    if (oper == oper_t::LTE) {
        return ::make_shared<restrictions::multi_column_restriction::slice>(schema,
                                                                            std::move(lhs_cols),
                                                                            statements::bound::END,
                                                                            true,
                                                                            std::move(prepared_rhs),
                                                                            order);
    }

    if (oper == oper_t::GT) {
        return ::make_shared<restrictions::multi_column_restriction::slice>(schema,
                                                                            std::move(lhs_cols),
                                                                            statements::bound::START,
                                                                            false,
                                                                            std::move(prepared_rhs),
                                                                            order);
    }

    if (oper == oper_t::GTE) {
        return ::make_shared<restrictions::multi_column_restriction::slice>(schema,
                                                                            std::move(lhs_cols),
                                                                            statements::bound::START,
                                                                            true,
                                                                            std::move(prepared_rhs),
                                                                            order);
    }

    if (oper == oper_t::IN) {
        if (auto rhs_marker = as_if<bind_variable>(&prepared_rhs)) {
            return ::make_shared<restrictions::multi_column_restriction::IN_with_marker>(schema,
                                                                                         std::move(lhs_cols),
                                                                                         std::move(*rhs_marker));
        }

        if (auto rhs_list = as_if<collection_constructor>(&prepared_rhs)) {
            return ::make_shared<restrictions::multi_column_restriction::IN_with_values>(schema,
                                                                                         std::move(lhs_cols),
                                                                                         std::move(rhs_list->elements));
        }

        if (auto rhs_list = as_if<constant>(&prepared_rhs)) {
            const list_type_impl* list_type = dynamic_cast<const list_type_impl*>(&rhs_list->type->without_reversed());
            const data_type& elements_type = list_type->get_elements_type();

            utils::chunked_vector<managed_bytes> raw_elems = get_list_elements(*rhs_list);
            std::vector<expression> list_elems;
            list_elems.reserve(raw_elems.size());

            for (managed_bytes elem : std::move(raw_elems)) {
                raw_value elem_val = raw_value::make_value(std::move(elem));
                list_elems.emplace_back(constant(elem_val, elements_type));
            }

            return ::make_shared<restrictions::multi_column_restriction::IN_with_values>(schema,
                                                                                         std::move(lhs_cols),
                                                                                         std::move(list_elems));
        }
    }

    on_internal_error(expr_logger,
                      fmt::format("new_multi_column_restriction operation type: {} not handled", oper));
}

void validate_token_relation(const std::vector<const column_definition*> column_defs, oper_t oper, const schema& schema) {
    auto pk = schema.partition_key_columns();
    if (!std::equal(column_defs.begin(), column_defs.end(), pk.begin(),
            pk.end(), [](auto* c1, auto& c2) {
                return c1 == &c2; // same, not "equal".
        })) {

        throw exceptions::invalid_request_exception(
                format("The token function arguments must be in the partition key order: {}",
                        std::to_string(column_defs)));
    }
}

::shared_ptr<restrictions::restriction> new_token_restriction(token&& prepared_lhs_token,
                                                              oper_t oper,
                                                              expression&& prepared_rhs,
                                                              comparison_order order,
                                                              const schema& schema) {
    std::vector<const column_definition*> column_defs = to_column_definitions(prepared_lhs_token.args);
    validate_token_relation(column_defs, oper, schema);

    ::shared_ptr<restrictions::restriction> r = ::make_shared<restrictions::token_restriction>(column_defs);
    r->expression = binary_operator(std::move(prepared_lhs_token), oper, std::move(prepared_rhs));

    return r;
}

void preliminary_binop_vaidation_checks(const binary_operator& binop) {
    if (auto lhs_tup = as_if<tuple_constructor>(&binop.lhs)) {
        if (binop.op == oper_t::CONTAINS) {
            throw exceptions::invalid_request_exception("CONTAINS cannot be used for Multi-column relations");
        }

        if (binop.op == oper_t::CONTAINS_KEY) {
            throw exceptions::invalid_request_exception("CONTAINS_KEY cannot be used for Multi-column relations");
        }

        if (binop.op == oper_t::LIKE) {
            throw exceptions::invalid_request_exception("LIKE cannot be used for Multi-column relations");
        }

        if (auto rhs_tup = as_if<tuple_constructor>(&binop.rhs)) {
            if (lhs_tup->elements.size() != rhs_tup->elements.size()) {
                throw exceptions::invalid_request_exception(
                    format("Expected {} elements in value tuple, but got {}: {}",
                                  lhs_tup->elements.size(), rhs_tup->elements.size(), *rhs_tup));
            }
        }
    }

    if (is<token>(binop.lhs)) {
        if (binop.op == oper_t::IN) {
            throw exceptions::invalid_request_exception("IN cannot be used with the token function");
        }

        if (binop.op == oper_t::LIKE) {
            throw exceptions::invalid_request_exception("LIKE cannot be used with the token function");
        }

        if (binop.op == oper_t::CONTAINS) {
            throw exceptions::invalid_request_exception("CONTAINS cannot be used with the token function");
        }

        if (binop.op == oper_t::CONTAINS_KEY) {
            throw exceptions::invalid_request_exception("CONTAINS_KEY cannot be used with the token function");
        }
    }
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

    preliminary_binop_vaidation_checks(binop_to_prepare);

    // Convert single element IN relation to an EQ relation
    if (binop_to_prepare.op == oper_t::IN && is<collection_constructor>(binop_to_prepare.rhs)) {
        const std::vector<expression>& elements = as<collection_constructor>(binop_to_prepare.rhs).elements;
        if (elements.size() == 1) {
            binop_to_prepare.op = oper_t::EQ;
            binop_to_prepare.rhs = elements[0];
        }
    }

    binary_operator prepared_binop = prepare_binary_operator(binop_to_prepare, db, schema);

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

    if (auto multi_col_tuple = as_if<tuple_constructor>(&prepared_binop.lhs)) {
        return new_multi_column_restriction(*multi_col_tuple, prepared_binop.op, std::move(prepared_binop.rhs), prepared_binop.order, schema);
    }

    if (auto lhs_token = as_if<token>(&prepared_binop.lhs)) {
        return new_token_restriction(std::move(*lhs_token), prepared_binop.op, std::move(prepared_binop.rhs), prepared_binop.order, *schema);
    }

    on_internal_error(expr_logger, format("expr::to_restriction unhandled case: {}", e));
}

} // namespace expr
} // namespace cql3