/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "audit/audit_syslog_storage_helper.hh"

#include <algorithm>
#include <sys/socket.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <syslog.h>
#include <utility>

#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/net/api.hh>

#include <fmt/chrono.h>

#include "cql3/query_processor.hh"

namespace cql3 {

class query_processor;

}

namespace audit {

namespace {

static auto syslog_address_helper(const db::config& cfg)
{
    return cfg.audit_unix_socket_path.is_set()
        ? unix_domain_addr(cfg.audit_unix_socket_path())
        : unix_domain_addr(_PATH_LOG);
}

size_t escaped_syslog_field_size(std::string_view str) {
    return str.size() + std::ranges::count_if(str, [] (char c) {
        return c == '"' || c == '\\';
    });
}

template <typename OutputIterator>
OutputIterator escape_syslog_field_to(OutputIterator out, std::string_view str) {
    for (char c : str) {
        if (c == '"' || c == '\\') {
            *out++ = '\\';
        }
        *out++ = c;
    }
    return out;
}

static sstring format_syslog_message(const tm& time,
        socket_address node_ip,
        std::string_view category,
        std::string_view cl,
        bool error,
        std::string_view keyspace,
        std::string_view query,
        socket_address client_ip,
        std::string_view table,
        std::string_view username) {
    constexpr std::string_view query_separator = R"(", query=")";
    constexpr std::string_view client_ip_separator = R"(", client_ip=")";
    constexpr std::string_view table_separator = R"(", table=")";
    constexpr std::string_view username_separator = R"(", username=")";
    constexpr std::string_view closing_quote = R"(")";
    constexpr auto prefix_format = R"(<{}>{:%h %e %T} scylla-audit: node="{}", category="{}", cl="{}", error="{}", keyspace=")";

    const size_t message_size = fmt::formatted_size(prefix_format,
            LOG_NOTICE | LOG_USER, time, node_ip, category, cl, error ? "true" : "false")
            + keyspace.size()
            + query_separator.size()
            + escaped_syslog_field_size(query)
            + client_ip_separator.size()
            + fmt::formatted_size("{}", client_ip)
            + table_separator.size()
            + table.size()
            + username_separator.size()
            + username.size()
            + closing_quote.size();

    sstring result(sstring::initialized_later(), message_size);
    auto out = fmt::format_to(result.begin(), prefix_format,
            LOG_NOTICE | LOG_USER, time, node_ip, category, cl, error ? "true" : "false");
    out = std::ranges::copy(keyspace, out).out;
    out = std::ranges::copy(query_separator, out).out;
    out = escape_syslog_field_to(out, query);
    out = std::ranges::copy(client_ip_separator, out).out;
    out = fmt::format_to(out, "{}", client_ip);
    out = std::ranges::copy(table_separator, out).out;
    out = std::ranges::copy(table, out).out;
    out = std::ranges::copy(username_separator, out).out;
    out = std::ranges::copy(username, out).out;
    std::ranges::copy(closing_quote, out);
    return result;
}

}

std::string escape_syslog_field(std::string_view str) {
    std::string result(escaped_syslog_field_size(str), '\0');
    escape_syslog_field_to(result.begin(), str);
    return result;
}

[[noreturn]] void audit_syslog_storage_helper::throw_syslog_error(const std::exception& error) const {
    auto error_msg = seastar::format(
        "Syslog audit backend failed (sending a message to {} resulted in {}).",
        _syslog_address,
        error
    );
    logger.error("{}", error_msg);
    throw audit_exception(std::move(error_msg));
}

future<semaphore_units<>> audit_syslog_storage_helper::acquire_syslog_unit() {
    try {
        co_return co_await get_units(_semaphore, 1, std::chrono::hours(1));
    } catch (const std::exception& error) {
        throw_syslog_error(error);
    }
}

future<> audit_syslog_storage_helper::syslog_send_helper(temporary_buffer<char> msg, semaphore_units<>) {
    try {
        co_await _sender.send(_syslog_address, std::span(&msg, 1));
    } catch (const std::exception& error) {
        throw_syslog_error(error);
    }
}

audit_syslog_storage_helper::audit_syslog_storage_helper(cql3::query_processor& qp, service::migration_manager&) :
    _syslog_address(syslog_address_helper(qp.db().get_config())),
    _sender(make_unbound_datagram_channel(AF_UNIX)),
    _semaphore(1) {
}

audit_syslog_storage_helper::~audit_syslog_storage_helper() {
}

/*
 * We don't use openlog and syslog directly because it's already used by logger.
 * Audit needs to use different ident so than logger but syslog.h uses a global ident
 * and it's not possible to use more than one in a program.
 *
 * To work around it we directly communicate with the socket.
 */
future<> audit_syslog_storage_helper::start(const db::config& cfg) {
    if (this_shard_id() != 0) {
        co_return;
    }

    auto unit = co_await acquire_syslog_unit();
    co_await syslog_send_helper(temporary_buffer<char>::copy_of("Initializing syslog audit backend."), std::move(unit));
}

future<> audit_syslog_storage_helper::stop() {
    _sender.shutdown_output();
    co_return;
}

future<> audit_syslog_storage_helper::write(audit_sink_set sinks,
                                            const audit_info* audit_info,
                                            socket_address node_ip,
                                            socket_address client_ip,
                                            std::optional<db::consistency_level> cl,
                                            const sstring& username,
                                            bool error) {
    if (!sinks.contains(audit_sink::syslog)) {
        co_return;
    }
    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    tm time;
    localtime_r(&now, &time);
    auto cl_str = cl ? format("{}", *cl) : sstring("");
    auto category = audit_info->category_string();
    auto unit = co_await acquire_syslog_unit();
    auto msg = format_syslog_message(time, node_ip, category, cl_str, error,
            audit_info->keyspace(), audit_info->query(), client_ip, audit_info->table(), username);
    co_await syslog_send_helper(std::move(msg).release(), std::move(unit));
}

future<> audit_syslog_storage_helper::write_login(audit_sink_set sinks,
                                                  const sstring& username,
                                                  socket_address node_ip,
                                                  socket_address client_ip,
                                                  bool error) {
    if (!sinks.contains(audit_sink::syslog)) {
        co_return;
    }

    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    tm time;
    localtime_r(&now, &time);
    auto unit = co_await acquire_syslog_unit();
    auto msg = format_syslog_message(time, node_ip, "AUTH", "", error,
            "", "", client_ip, "", username);
    co_await syslog_send_helper(std::move(msg).release(), std::move(unit));
}

}
