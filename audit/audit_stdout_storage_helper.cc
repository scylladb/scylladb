/*
 * Copyright (C) 2025 ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "audit/audit_stdout_storage_helper.hh"

#include <cstdio>

#include <seastar/core/coroutine.hh>

#include <fmt/chrono.h>

#include "cql3/query_processor.hh"

namespace cql3 {

class query_processor;

}

namespace audit {

namespace {

/// Escape `"`, `\`, and whitespace/control characters inside a field value
/// so that each audit record occupies exactly one line in the log.
static std::string stdout_json_escape(std::string_view str) {
    std::string result;
    result.reserve(str.size());
    for (auto c : str) {
        if (c == '"' || c == '\\') {
            result.push_back('\\');
            result.push_back(c);
        } else if (c == '\n') {
            result.push_back('\\');
            result.push_back('n');
        } else if (c == '\r') {
            result.push_back('\\');
            result.push_back('r');
        } else if (c == '\t') {
            result.push_back('\\');
            result.push_back('t');
        } else {
            result.push_back(c);
        }
    }
    return result;
}

} // namespace

audit_stdout_storage_helper::audit_stdout_storage_helper(cql3::query_processor& /*qp*/, service::migration_manager& /*mm*/) {
}

future<> audit_stdout_storage_helper::start(const db::config& /*cfg*/) {
    if (this_shard_id() != 0) {
        co_return;
    }
    logger.info("Initializing stdout audit backend.");
    co_return;
}

future<> audit_stdout_storage_helper::stop() {
    co_return;
}

future<> audit_stdout_storage_helper::write(
        const audit_info* ai, socket_address node_ip, socket_address client_ip, db::consistency_level cl, const sstring& username, bool error) {
    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    tm time;
    localtime_r(&now, &time);
    sstring msg = seastar::format(
            R"({:%h %e %T} scylla-audit: node="{}", category="{}", cl="{}", error="{}", keyspace="{}", query="{}", client_ip="{}", table="{}", username="{}")",
            time, node_ip, ai->category_string(), cl, (error ? "true" : "false"), ai->keyspace(), stdout_json_escape(ai->query()), client_ip, ai->table(),
            username);

    fmt::print("{}\n", msg);
    std::fflush(stdout);
    co_return;
}

future<> audit_stdout_storage_helper::write_login(const sstring& username, socket_address node_ip, socket_address client_ip, bool error) {
    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    tm time;
    localtime_r(&now, &time);
    sstring msg = seastar::format(
            R"({:%h %e %T} scylla-audit: node="{}", category="AUTH", cl="", error="{}", keyspace="", query="", client_ip="{}", table="", username="{}")", time,
            node_ip, (error ? "true" : "false"), client_ip, username);

    fmt::print("{}\n", msg);
    std::fflush(stdout);
    co_return;
}

} // namespace audit
