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
#include "service/storage_proxy.hh"
#include "locator/token_metadata.hh"
#include "service/cas_shard.hh"
#include "keys/keys.hh"
#include "types/types.hh"
#include <seastar/core/coroutine.hh>
#include <charconv>
#include <stdexcept>
#include <fmt/format.h>

namespace alternator {

live_node_identifier live_node_identifier::from_string(std::string_view str) {
    if (str.empty()) {
        return live_node_identifier{};
    }
    auto pos = str.rfind(':');
    if (pos == std::string_view::npos || pos == 0) {
        throw std::invalid_argument(fmt::format("Invalid node_id format: {}", str));
    }
    std::uint32_t generation = 0;
    auto gen_str = str.substr(pos + 1);
    auto [ptr, ec] = std::from_chars(gen_str.data(), gen_str.data() + gen_str.size(), generation);
    if (ec != std::errc() || ptr != gen_str.data() + gen_str.size()) {
        throw std::invalid_argument(fmt::format("Invalid node_id format: {}", str));
    }
    return live_node_identifier{ .host_id = sstring(str.substr(0, pos)), .gossip_generation = generation };
}

// system_distributed uses SimpleStrategy with RF=3, but in a cluster with a
// single token owner there is only one replica, so QUORUM (computed from RF)
// can never be reached. Use ONE in that case, like the rest of
// system_distributed_keyspace does (see quorum_if_many()).
static db::consistency_level get_consistency_level(cql3::query_processor& qp) {
    return qp.proxy().get_token_metadata_ptr()->count_normal_token_owners() > 1
            ? db::consistency_level::QUORUM : db::consistency_level::ONE;
}

// Conditional (LWT) statements must be executed on the shard owning the
// partition's Paxos state - otherwise they return a bounce_to_shard message,
// which query_internal() doesn't handle. Run `func` on the owning shard of
// partition `key` (a single text partition key column) of the given table.
template <typename Func>
static future<bool> invoke_on_cas_shard(cql3::query_processor& qp, std::string_view table, const sstring& key, Func func) {
    auto schema = qp.db().find_schema(db::system_distributed_keyspace::NAME, table);
    auto pk = partition_key::from_single_value(*schema, utf8_type->decompose(key));
    auto shard = service::cas_shard(*schema, dht::get_token(*schema, pk)).shard();
    return qp.container().invoke_on(shard, std::move(func));
}

static live_node_identifier get_node_id(const cql3::untyped_result_set_row& row) {
    return live_node_identifier::from_string(row.get_or<sstring>("node_id", sstring()));
}

static export_row row_to_export(const cql3::untyped_result_set_row& row) {
    return export_row{
        .export_arn = row.get_as<sstring>("export_arn"),
        .client_token = row.get_or<sstring>("client_token", sstring()),
        .request = row.get_or<sstring>("request", sstring()),
        .export_manifest = row.get_or<sstring>("export_manifest", ""),
        .export_status = row.get_or<sstring>("export_status", sstring()),
        .failure_code = row.get_or<sstring>("failure_code", sstring()),
        .failure_message = row.get_or<sstring>("failure_message", sstring()),
        .item_count = row.get_or<int64_t>("item_count", 0),
        .export_id_token = row.get_or<sstring>("export_id_token", sstring()),
        .accepted_at = row.get_or<db_clock::time_point>("accepted_at", db_clock::time_point()),
        .completed_at = row.get_or<db_clock::time_point>("completed_at", db_clock::time_point()),
        .node_id = get_node_id(row),
    };
}

future<std::optional<client_row>> get_client_row(cql3::query_processor& qp, const sstring& client_token) {
    static const sstring query = format(
        "SELECT client_token, export_arn, request, node_id"
        " FROM {}.{} WHERE client_token = ?",
        db::system_distributed_keyspace::NAME, db::system_distributed_keyspace::ALTERNATOR_EXPORT_TO_S3_CLIENT_TOKENS);
    std::optional<client_row> result;
    co_await qp.query_internal(
        query,
        get_consistency_level(qp),
        { client_token },
        1, // batch size
        [&](const cql3::untyped_result_set_row& row) -> future<stop_iteration> {
            result = client_row{
                .client_token = row.get_as<sstring>("client_token"),
                .export_arn = row.get_or<sstring>("export_arn", sstring()),
                .request = row.get_or<sstring>("request", sstring()),
                .node_id = get_node_id(row),
            };
            co_return stop_iteration::yes;
        });
    co_return result;
}

future<bool> insert_client_row(cql3::query_processor& qp, const client_row& row) {
    static const sstring query = format(
        "INSERT INTO {}.{} (client_token, export_arn, request, node_id) VALUES (?, ?, ?, ?) IF NOT EXISTS",
        db::system_distributed_keyspace::NAME, db::system_distributed_keyspace::ALTERNATOR_EXPORT_TO_S3_CLIENT_TOKENS);
    co_return co_await invoke_on_cas_shard(qp, db::system_distributed_keyspace::ALTERNATOR_EXPORT_TO_S3_CLIENT_TOKENS, row.client_token, [&] (cql3::query_processor& local_qp) -> future<bool> {
        bool was_applied = false;
        co_await local_qp.query_internal(
            query,
            get_consistency_level(local_qp),
            { row.client_token, row.export_arn, row.request, row.node_id.to_string() },
            1, // batch size
            [&](const cql3::untyped_result_set_row& row) -> future<stop_iteration> {
                was_applied = row.get_as<bool>("[applied]");
                co_return stop_iteration::yes;
            });
        co_return was_applied;
    });
}

future<bool> insert_export(cql3::query_processor& qp, const export_row& row) {
    static const sstring query = format(
        "INSERT INTO {}.{} (export_arn, client_token, request, export_status,"
        " failure_code, failure_message, item_count, export_id_token,"
        " accepted_at, completed_at, node_id, export_manifest)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS",
        db::system_distributed_keyspace::NAME, db::system_distributed_keyspace::ALTERNATOR_EXPORT_TO_S3_EXPORTS);
    co_return co_await invoke_on_cas_shard(qp, db::system_distributed_keyspace::ALTERNATOR_EXPORT_TO_S3_EXPORTS, row.export_arn, [&] (cql3::query_processor& local_qp) -> future<bool> {
        bool was_applied = false;
        co_await local_qp.query_internal(
            query,
            get_consistency_level(local_qp),
            { row.export_arn, row.client_token, row.request, row.export_status,
              row.failure_code, row.failure_message, row.item_count, row.export_id_token,
              row.accepted_at, row.completed_at, row.node_id.to_string(), row.export_manifest },
            1, // batch size
            [&](const cql3::untyped_result_set_row& row) -> future<stop_iteration> {
                was_applied = row.get_as<bool>("[applied]");
                co_return stop_iteration::yes;
            });
        co_return was_applied;
    });
}

future<bool> update_export(cql3::query_processor& qp, const export_row& row, const sstring& old_export_status, const live_node_identifier& old_node_id) {
    static const sstring query = format(
        "UPDATE {}.{} SET export_status = ?, failure_code = ?, failure_message = ?,"
        " item_count = ?, completed_at = ?, node_id = ?, export_manifest = ?"
        " WHERE export_arn = ? IF export_status = ? AND node_id = ?",
        db::system_distributed_keyspace::NAME, db::system_distributed_keyspace::ALTERNATOR_EXPORT_TO_S3_EXPORTS);
    co_return co_await invoke_on_cas_shard(qp, db::system_distributed_keyspace::ALTERNATOR_EXPORT_TO_S3_EXPORTS, row.export_arn, [&] (cql3::query_processor& local_qp) -> future<bool> {
        bool was_applied = false;
        co_await local_qp.query_internal(
            query,
            get_consistency_level(local_qp),
            { row.export_status, row.failure_code, row.failure_message,
              row.item_count, row.completed_at, row.node_id.to_string(), row.export_manifest,
              row.export_arn, old_export_status, old_node_id.to_string() },
            1, // batch size
            [&](const cql3::untyped_result_set_row& row) -> future<stop_iteration> {
                was_applied = row.get_as<bool>("[applied]");
                co_return stop_iteration::yes;
            });
        co_return was_applied;
    });
}

future<std::optional<export_row>> get_export(cql3::query_processor& qp, sstring export_arn) {
    static const sstring query = format(
        "SELECT export_arn, client_token, request, export_status,"
        " failure_code, failure_message, item_count, export_id_token,"
        " accepted_at, completed_at, node_id, export_manifest"
        " FROM {}.{} WHERE export_arn = ?",
        db::system_distributed_keyspace::NAME, db::system_distributed_keyspace::ALTERNATOR_EXPORT_TO_S3_EXPORTS);
    std::optional<export_row> result;
    co_await qp.query_internal(
        query,
        get_consistency_level(qp),
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
        get_consistency_level(db),
        {},
        1000, // batch size
        [&](const cql3::untyped_result_set_row& row) -> future<stop_iteration> {
            results.push_back(client_row{
                .client_token = row.get_as<sstring>("client_token"),
                .export_arn = row.get_or<sstring>("export_arn", sstring()),
                .request = row.get_or<sstring>("request", sstring()),
                .node_id = get_node_id(row),
            });
            co_return stop_iteration::no;
        });
    co_return results;
}

future<utils::chunked_vector<export_row>> get_all_exports(cql3::query_processor& qp) {
    static const sstring query = format(
        "SELECT export_arn, client_token, request, export_status,"
        " failure_code, failure_message, item_count, export_id_token,"
        " accepted_at, completed_at, node_id, export_manifest"
        " FROM {}.{}",
        db::system_distributed_keyspace::NAME, db::system_distributed_keyspace::ALTERNATOR_EXPORT_TO_S3_EXPORTS);
    utils::chunked_vector<export_row> results;
    co_await qp.query_internal(
        query,
        get_consistency_level(qp),
        {},
        1000, // batch size
        [&](const cql3::untyped_result_set_row& row) -> future<stop_iteration> {
            results.push_back(row_to_export(row));
            co_return stop_iteration::no;
        });
    co_return results;
}

} // namespace alternator
