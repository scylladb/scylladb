/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "restrictions.hh"
#include "cql3/statements/request_validations.hh"
#include "seastar/util/defer.hh"
#include "cql3/prepare_context.hh"
#include "cql3/restrictions/multi_column_restriction.hh"
#include "cql3/restrictions/token_restriction.hh"

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

binary_operator validate_and_prepare_new_restriction(const binary_operator& restriction,
                                                     data_dictionary::database db,
                                                     schema_ptr schema,
                                                     prepare_context& ctx) {
    // Perform basic initial checks
    preliminary_binop_vaidation_checks(restriction);

    // Prepare the restriction
    binary_operator prepared_binop = prepare_binary_operator(restriction, db, schema);

    // Fill prepare context
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

    // Check for disallowed operators
    if (prepared_binop.op == oper_t::NEQ) {
        throw exceptions::invalid_request_exception(format("Unsupported \"!=\" relation: {}", prepared_binop));
    }

    if (prepared_binop.op == oper_t::IS_NOT) {
        bool rhs_is_null = is<constant>(prepared_binop.rhs) && as<constant>(prepared_binop.rhs).is_null();
        if (!rhs_is_null) {
            throw exceptions::invalid_request_exception(format("Unsupported \"IS NOT\" relation: {}", prepared_binop));
        }
    }

    // Perform more throughout validation depending on restriction type
    if (auto col_val = as_if<column_value>(&prepared_binop.lhs)) {
        // Simple single column restriction
        validate_single_column_relation(*col_val, prepared_binop.op, *schema, false);
    } else if (auto sub = as_if<subscript>(&prepared_binop.lhs)) {
        // Subscripted single column restriction
        const column_value& sub_col = get_subscripted_column(*sub);
        validate_single_column_relation(sub_col, prepared_binop.op, *schema, true);
    } else if (auto multi_col_tuple = as_if<tuple_constructor>(&prepared_binop.lhs)) {
        // Multi column restriction
        std::vector<const column_definition*> lhs_cols = to_column_definitions(multi_col_tuple->elements);
        std::vector<lw_shared_ptr<column_specification>> lhs_col_specs;
        lhs_col_specs.reserve(multi_col_tuple->elements.size());

        for (const column_definition* col_def : lhs_cols) {
            lhs_col_specs.push_back(col_def->column_specification);
        }

        validate_multi_column_relation(lhs_cols, prepared_binop.op);
    } else if (auto lhs_token = as_if<token>(&prepared_binop.lhs)) {
        // Token restriction
        std::vector<const column_definition*> column_defs = to_column_definitions(lhs_token->args);
        validate_token_relation(column_defs, prepared_binop.op, *schema);
    } else {
        // Anything else
        throw exceptions::invalid_request_exception(
            format("expr::validate_and_prepare_new_restriction unhandled restriction: {}", prepared_binop));
    }

    return prepared_binop;
}

namespace {

::shared_ptr<restrictions::restriction> new_multi_column_restriction(const tuple_constructor& prepared_lhs_tuple,
                                                                     oper_t oper,
                                                                     expression prepared_rhs,
                                                                     comparison_order order,
                                                                     const schema_ptr& schema) {
    std::vector<const column_definition*> lhs_cols = to_column_definitions(prepared_lhs_tuple.elements);

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

            utils::chunked_vector<managed_bytes> raw_elems = get_list_elements(rhs_list->value);
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

} // anonymous namespace

::shared_ptr<restrictions::restriction> convert_to_restriction(const binary_operator& prepared_binop, const schema_ptr& schema) {
    if (is<column_value>(prepared_binop.lhs) || is<subscript>(prepared_binop.lhs)) {
        ::shared_ptr<restrictions::restriction> r = ::make_shared<restrictions::restriction>();
        r->expression = prepared_binop;
        return r;
    } else if (auto lhs_tuple = as_if<tuple_constructor>(&prepared_binop.lhs)) {
        return new_multi_column_restriction(*lhs_tuple, prepared_binop.op, prepared_binop.rhs, prepared_binop.order, schema);
    } else if (auto lhs_token = as_if<token>(&prepared_binop.lhs)) {
        std::vector<const column_definition*> column_defs = to_column_definitions(lhs_token->args);
        ::shared_ptr<restrictions::restriction> r = ::make_shared<restrictions::token_restriction>(std::move(column_defs));
        r->expression = prepared_binop;
        return r;
    } else {
        throw exceptions::invalid_request_exception(
            format("expr::validate_and_prepare_new_restriction unhandled restriction: {}", prepared_binop));
    }
}
} // namespace expr
} // namespace cql3