/*
 * Copyright (C) 2022-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */


#include "cql3/statements/strongly_consistent_select_statement.hh"

#include <seastar/core/future.hh>

#include "cql3/restrictions/statement_restrictions.hh"
#include "cql3/query_processor.hh"
#include "service/broadcast_tables/experimental/lang.hh"

namespace cql3 {

namespace statements {

static
bytes get_key(const cql3::expr::expression& partition_key_restrictions) {
    const auto* conjunction = cql3::expr::as_if<cql3::expr::conjunction>(&partition_key_restrictions);

    if (!conjunction || conjunction->children.size() != 1) {
        throw service::broadcast_tables::unsupported_operation_error(fmt::format(
            "partition key restriction: {}", partition_key_restrictions));
    }

    const auto* key_restriction = cql3::expr::as_if<cql3::expr::binary_operator>(&conjunction->children[0]);

    if (!key_restriction) {
        throw service::broadcast_tables::unsupported_operation_error(fmt::format("partition key restriction: {}", *conjunction));
    }

    // FIXME: add support for bind variables
    const auto* column = cql3::expr::as_if<cql3::expr::column_value>(&key_restriction->lhs);
    const auto* value = cql3::expr::as_if<cql3::expr::constant>(&key_restriction->rhs);

    if (!column || column->col->kind != column_kind::partition_key ||
        !value || key_restriction->op != cql3::expr::oper_t::EQ) {
        throw service::broadcast_tables::unsupported_operation_error(fmt::format("key restriction: {}", *key_restriction));
    }

    return to_bytes(value->view());
}

static
bool is_selecting_only_value(const cql3::selection::selection& selection) {
    return selection.is_trivial() &&
           selection.get_column_count() == 1 &&
           selection.get_columns()[0]->name() == "value";
}

strongly_consistent_select_statement::strongly_consistent_select_statement(schema_ptr schema, uint32_t bound_terms,
                                                                           lw_shared_ptr<const parameters> parameters,
                                                                           ::shared_ptr<selection::selection> selection,
                                                                           ::shared_ptr<const restrictions::statement_restrictions> restrictions,
                                                                           ::shared_ptr<std::vector<size_t>> group_by_cell_indices,
                                                                           bool is_reversed,
                                                                           ordering_comparator_type ordering_comparator,
                                                                           std::optional<expr::expression> limit,
                                                                           std::optional<expr::expression> per_partition_limit,
                                                                           cql_stats &stats,
                                                                           std::unique_ptr<attributes> attrs)
    : select_statement{schema, bound_terms, parameters, selection, restrictions, group_by_cell_indices, is_reversed, ordering_comparator, std::move(limit), std::move(per_partition_limit), stats, std::move(attrs)},
      _query{prepare_query()}
{ }

service::broadcast_tables::select_query strongly_consistent_select_statement::prepare_query() const {
    if (!is_selecting_only_value(*_selection)) {
        throw service::broadcast_tables::unsupported_operation_error("only 'value' selector is allowed");
    }

    return {
        .key = get_key(_restrictions->get_partition_key_restrictions())
    };
}

future<::shared_ptr<cql_transport::messages::result_message>>
strongly_consistent_select_statement::execute_without_checking_exception_message(query_processor& qp, service::query_state& qs, const query_options& options) const {
    return service::broadcast_tables::execute(qp.get_group0_client(), { _query })
        .then(make_ready_future<::shared_ptr<cql_transport::messages::result_message>>);
}

}

}
