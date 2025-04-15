/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "audit/audit_syslog_storage_helper.hh"

#include <sys/socket.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <syslog.h>

#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/net/api.hh>

#include <fmt/chrono.h>

#include "cql3/query_processor.hh"
#include "utils/class_registrator.hh"

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

static std::string json_escape(std::string_view str) {
    std::string result;
    result.reserve(str.size() * 1.2);
    for (auto c : str) {
        if (c == '"' || c == '\\') {
            result.push_back('\\');
        }
        result.push_back(c);
    }
    return result;
}

}

future<> audit_syslog_storage_helper::syslog_send_helper(const sstring& msg) {
    try {
        auto lock = co_await get_units(_semaphore, 1, std::chrono::hours(1));
        co_await _sender.send(_syslog_address, net::packet{msg.data(), msg.size()});
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

    co_await syslog_send_helper("Initializing syslog audit backend.");
}

future<> audit_syslog_storage_helper::stop() {
    _sender.shutdown_output();
    co_return;
}

future<> audit_syslog_storage_helper::write(const audit_info* audit_info,
                                            socket_address node_ip,
                                            socket_address client_ip,
                                            db::consistency_level cl,
                                            const sstring& username,
                                            bool error) {
    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    tm time;
    localtime_r(&now, &time);
    sstring msg = seastar::format(R"(<{}>{:%h %e %T} scylla-audit: node="{}" category="{}" cl="{}" error="{}" keyspace="{}" query="{}" client_ip="{}" table="{}" username="{}")",
                                    LOG_NOTICE | LOG_USER,
                                    time,
                                    node_ip,
                                    audit_info->category_string(),
                                    cl,
                                    (error ? "true" : "false"),
                                    audit_info->keyspace(),
                                    json_escape(audit_info->query()),
                                    client_ip,
                                    audit_info->table(),
                                    username);

    co_await syslog_send_helper(msg);
}

future<> audit_syslog_storage_helper::write_login(const sstring& username,
                                                  socket_address node_ip,
                                                  socket_address client_ip,
                                                  bool error) {

    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    tm time;
    localtime_r(&now, &time);
    sstring msg = seastar::format(R"(<{}>{:%h %e %T} scylla-audit: node="{}", category="AUTH", cl="", error="{}", keyspace="", query="", client_ip="{}", table="", username="{}")",
                                    LOG_NOTICE | LOG_USER,
                                    time,
                                    node_ip,
                                    (error ? "true" : "false"),
                                    client_ip,
                                    username);

    co_await syslog_send_helper(msg.c_str());
}

using registry = class_registrator<storage_helper, audit_syslog_storage_helper, cql3::query_processor&, service::migration_manager&>;
static registry registrator1("audit_syslog_storage_helper");

}
