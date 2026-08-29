/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "audit/audit_syslog_storage_helper.hh"

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

}

std::string escape_syslog_field(std::string_view str) {
    size_t escaped_size = str.size();
    for (unsigned char c : str) {
        if (c == '"' || c == '\\' || c == '\b' || c == '\f' || c == '\n' || c == '\r' || c == '\t') {
            ++escaped_size;
        } else if (c < 0x20) {
            escaped_size += 5;
        }
    }

    std::string result;
    result.reserve(escaped_size);
    static constexpr char hex[] = "0123456789ABCDEF";
    for (unsigned char c : str) {
        switch (c) {
        case '"': result += "\\\""; break;
        case '\\': result += "\\\\"; break;
        case '\b': result += "\\b"; break;
        case '\f': result += "\\f"; break;
        case '\n': result += "\\n"; break;
        case '\r': result += "\\r"; break;
        case '\t': result += "\\t"; break;
        default:
            if (c < 0x20) {
                result += "\\u00";
                result.push_back(hex[c >> 4]);
                result.push_back(hex[c & 0x0f]);
            } else {
                result.push_back(static_cast<char>(c));
            }
        }
    }
    return result;
}

namespace {

template <typename... Args>
static sstring format_exactly(fmt::format_string<Args...> format_string, Args&&... args) {
    auto size = fmt::formatted_size(format_string, std::forward<Args>(args)...);
    sstring result(sstring::initialized_later(), size);
    fmt::format_to(result.begin(), format_string, std::forward<Args>(args)...);
    return result;
}

}

future<> audit_syslog_storage_helper::syslog_send_helper(temporary_buffer<char> msg) {
    try {
        auto lock = co_await get_units(_semaphore, 1, std::chrono::hours(1));
        co_await _sender.send(_syslog_address, std::span(&msg, 1));
    }
    catch (const std::exception& e) {
        auto error_msg = seastar::format(
            "Syslog audit backend failed (sending a message to {} resulted in {}).",
            _syslog_address,
            e
        );
        logger.error("{}", error_msg);
        throw audit_exception(std::move(error_msg));
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

    co_await syslog_send_helper(temporary_buffer<char>::copy_of("Initializing syslog audit backend."));
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
    sstring msg = format_exactly(R"(<{}>{:%h %e %T} scylla-audit: node="{}", category="{}", cl="{}", error="{}", keyspace="{}", query="{}", client_ip="{}", table="{}", username="{}")",
                                    LOG_NOTICE | LOG_USER,
                                    time,
                                    node_ip,
                                    audit_info->category_string(),
                                    cl_str,
                                    (error ? "true" : "false"),
                                    escape_syslog_field(audit_info->keyspace()),
                                    escape_syslog_field(audit_info->query()),
                                    client_ip,
                                    escape_syslog_field(audit_info->table()),
                                    escape_syslog_field(username));

    co_await syslog_send_helper(std::move(msg).release());
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
    sstring msg = format_exactly(R"(<{}>{:%h %e %T} scylla-audit: node="{}", category="AUTH", cl="", error="{}", keyspace="", query="", client_ip="{}", table="", username="{}")",
                                    LOG_NOTICE | LOG_USER,
                                    time,
                                    node_ip,
                                    (error ? "true" : "false"),
                                    client_ip,
                                    escape_syslog_field(username));

    co_await syslog_send_helper(std::move(msg).release());
}

}
