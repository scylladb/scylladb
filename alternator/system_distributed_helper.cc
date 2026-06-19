/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "alternator/system_distributed_helper.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/consistency_level_type.hh"
#include "timeout_config.hh"
#include "service/query_state.hh"
#include "service_permit.hh"
#include <seastar/core/coroutine.hh>

namespace alternator {

static export_row row_to_export(const cql3::untyped_result_set_row& row) {
    return export_row{
        .export_arn = row.get_as<sstring>("export_arn"),
        .client_token = row.get_or<sstring>("client_token", sstring()),
        .request = row.get_or<sstring>("request", sstring()),
        .export_status = row.get_or<sstring>("export_status", sstring()),
        .failure_code = row.get_or<sstring>("failure_code", sstring()),
        .failure_message = row.get_or<sstring>("failure_message", sstring()),
        .item_count = row.get_or<int64_t>("item_count", 0),
        .export_id_token = row.get_or<sstring>("export_id_token", sstring()),
        .accepted_at = row.get_or<db_clock::time_point>("accepted_at", db_clock::time_point()),
        .completed_at = row.get_or<db_clock::time_point>("completed_at", db_clock::time_point()),
        .node_id = row.get_or<sstring>("node_id", sstring()),
    };
}

future<std::tuple<client_row, bool>> get_or_insert_client_row(cql3::query_processor& qp, const sstring& client_token) {
    static const sstring query = format(
        "INSERT INTO {}.{} (client_token) VALUES (?) IF NOT EXISTS",
        db::system_distributed_keyspace::NAME, db::system_distributed_keyspace::ALTERNATOR_EXPORT_TO_S3_CLIENT_TOKENS);
    bool was_applied = false;
    client_row result;
    co_await qp.query_internal(
        query,
        db::consistency_level::QUORUM,
        { std::move(client_token) },
        1, // batch size
        [&](const cql3::untyped_result_set_row& row) -> future<stop_iteration> {
            was_applied = row.get_as<bool>("[applied]");
            result = client_row{
                .client_token = row.get_or<sstring>("client_token", sstring()),
                .export_arn = row.get_or<sstring>("export_arn", sstring()),
                .request = row.get_or<sstring>("request", sstring()),
                .node_id = row.get_or<sstring>("node_id", sstring()),
            };
            co_return stop_iteration::yes;
        });
    co_return std::make_tuple(std::move(result), was_applied);
}

future<bool> insert_export(cql3::query_processor& qp, const export_row& row) {
    static const sstring query = format(
        "INSERT INTO {}.{} (export_arn, client_token, request, export_status,"
        " failure_code, failure_message, item_count, export_id_token,"
        " accepted_at, completed_at, node_id)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS",
        db::system_distributed_keyspace::NAME, db::system_distributed_keyspace::ALTERNATOR_EXPORT_TO_S3_EXPORTS);
    bool was_applied = false;
    co_await qp.query_internal(
        query,
        db::consistency_level::QUORUM,
        { row.export_arn, row.client_token, row.request, row.export_status,
          row.failure_code, row.failure_message, row.item_count, row.export_id_token,
          row.accepted_at, row.completed_at, row.node_id },
        1, // batch size
        [&](const cql3::untyped_result_set_row& row) -> future<stop_iteration> {
            was_applied = row.get_as<bool>("[applied]");
            co_return stop_iteration::yes;
        });
    co_return was_applied;
}

future<bool> update_export(cql3::query_processor& qp, const export_row& row, const sstring& old_export_status, const sstring& old_node_id) {
    static const sstring query = format(
        "UPDATE {}.{} SET export_status = ?, failure_code = ?, failure_message = ?,"
        " item_count = ?, completed_at = ?, node_id = ?"
        " WHERE export_arn = ? IF export_status = ? AND node_id = ?",
        db::system_distributed_keyspace::NAME, db::system_distributed_keyspace::ALTERNATOR_EXPORT_TO_S3_EXPORTS);
    bool was_applied = false;
    co_await qp.query_internal(
        query,
        db::consistency_level::QUORUM,
        { row.export_status, row.failure_code, row.failure_message,
          row.item_count, row.completed_at, row.node_id,
          row.export_arn, old_export_status, old_node_id },
        1, // batch size
        [&](const cql3::untyped_result_set_row& row) -> future<stop_iteration> {
            was_applied = row.get_as<bool>("[applied]");
            co_return stop_iteration::yes;
        });
    co_return was_applied;
}

future<std::optional<export_row>> get_export(cql3::query_processor& qp, sstring export_arn) {
    static const sstring query = format(
        "SELECT export_arn, client_token, request, export_status,"
        " failure_code, failure_message, item_count, export_id_token,"
        " accepted_at, completed_at, node_id"
        " FROM {}.{} WHERE export_arn = ?",
        db::system_distributed_keyspace::NAME, db::system_distributed_keyspace::ALTERNATOR_EXPORT_TO_S3_EXPORTS);
    std::optional<export_row> result;
    co_await qp.query_internal(
        query,
        db::consistency_level::QUORUM,
        { std::move(export_arn) },
        1, // batch size
        [&](const cql3::untyped_result_set_row& row) -> future<stop_iteration> {
            result = row_to_export(row);
            co_return stop_iteration::yes;
        });
    co_return result;
}

future<utils::chunked_vector<client_row>> get_all_client_tokens(cql3::query_processor& db) {
    static const sstring query = format(
        "SELECT client_token, export_arn, request, node_id"
        " FROM {}.{}",
        db::system_distributed_keyspace::NAME, db::system_distributed_keyspace::ALTERNATOR_EXPORT_TO_S3_CLIENT_TOKENS);
    utils::chunked_vector<client_row> results;
    co_await db.query_internal(
        query,
        db::consistency_level::QUORUM,
        {},
        1000, // batch size
        [&](const cql3::untyped_result_set_row& row) -> future<stop_iteration> {
            results.push_back(client_row{
                .client_token = row.get_as<sstring>("client_token"),
                .export_arn = row.get_or<sstring>("export_arn", sstring()),
                .request = row.get_or<sstring>("request", sstring()),
                .node_id = row.get_or<sstring>("node_id", sstring()),
            });
            co_return stop_iteration::no;
        });
    co_return results;
}

future<utils::chunked_vector<export_row>> get_all_exports(cql3::query_processor& qp) {
    static const sstring query = format(
        "SELECT export_arn, client_token, request, export_status,"
        " failure_code, failure_message, item_count, export_id_token,"
        " accepted_at, completed_at, node_id"
        " FROM {}.{}",
        db::system_distributed_keyspace::NAME, db::system_distributed_keyspace::ALTERNATOR_EXPORT_TO_S3_EXPORTS);
    utils::chunked_vector<export_row> results;
    co_await qp.query_internal(
        query,
        db::consistency_level::QUORUM,
        {},
        1000, // batch size
        [&](const cql3::untyped_result_set_row& row) -> future<stop_iteration> {
            results.push_back(row_to_export(row));
            co_return stop_iteration::no;
        });
    co_return results;
}

} // namespace alternator
