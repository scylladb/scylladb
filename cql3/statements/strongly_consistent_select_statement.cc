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
#include <seastar/core/on_internal_error.hh>

#include "cql3/restrictions/statement_restrictions.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/query_processor.hh"
#include "service/broadcast_tables/experimental/lang.hh"
#include "db/system_keyspace.hh"

namespace cql3 {

namespace statements {

static logging::logger logger("strongly_consistent_select_statement");

static
expr::expression get_key(const cql3::expr::expression& partition_key_restrictions) {
    const auto* conjunction = cql3::expr::as_if<cql3::expr::conjunction>(&partition_key_restrictions);

    if (!conjunction || conjunction->children.size() != 1) {
        throw service::broadcast_tables::unsupported_operation_error(fmt::format(
            "partition key restriction: {}", partition_key_restrictions));
    }

    const auto* key_restriction = cql3::expr::as_if<cql3::expr::binary_operator>(&conjunction->children[0]);

    if (!key_restriction) {
        throw service::broadcast_tables::unsupported_operation_error(fmt::format("partition key restriction: {}", *conjunction));
    }

    const auto* column = cql3::expr::as_if<cql3::expr::column_value>(&key_restriction->lhs);

    if (!column || column->col->kind != column_kind::partition_key ||
        key_restriction->op != cql3::expr::oper_t::EQ) {
        throw service::broadcast_tables::unsupported_operation_error(fmt::format("key restriction: {}", *key_restriction));
    }

    return key_restriction->rhs;
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

broadcast_tables::prepared_select strongly_consistent_select_statement::prepare_query() const {
    if (!is_selecting_only_value(*_selection)) {
        throw service::broadcast_tables::unsupported_operation_error("only 'value' selector is allowed");
    }

    return {
        .key = get_key(_restrictions->get_partition_key_restrictions())
    };
}

static
service::broadcast_tables::select_query
evaluate_prepared(
    const broadcast_tables::prepared_select& query,
    const query_options& options) {
    return service::broadcast_tables::select_query{
        .key = expr::evaluate(query.key, options).to_bytes()
    };
}

future<::shared_ptr<cql_transport::messages::result_message>>
strongly_consistent_select_statement::execute_without_checking_exception_message(query_processor& qp, service::query_state& qs, const query_options& options, std::optional<service::group0_guard> guard) const {
    if (this_shard_id() != 0) {
        co_return ::make_shared<cql_transport::messages::result_message::bounce_to_shard>(0, cql3::computed_function_values{});
    }

    auto result = co_await qp.execute_broadcast_table_query(
        { evaluate_prepared(_query, options) }
    );

    auto query_result = std::get_if<service::broadcast_tables::query_result_select>(&result);

    if (!query_result) {
        on_internal_error(logger, "incorrect query result ");
    }

    auto result_set = std::make_unique<cql3::result_set>(std::vector{
        make_lw_shared<cql3::column_specification>(
            db::system_keyspace::NAME,
            db::system_keyspace::BROADCAST_KV_STORE,
            ::make_shared<cql3::column_identifier>("value", true),
            utf8_type
        )
    });

    if (query_result->value) {
        result_set->add_row({ managed_bytes_opt(query_result->value) });
    }

    co_return ::make_shared<cql_transport::messages::result_message::rows>(cql3::result{std::move(result_set)});
}

}

}
