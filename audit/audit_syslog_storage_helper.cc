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

future<> syslog_send_helper(net::datagram_channel& sender,
                            const socket_address& address,
                            const sstring& msg) {
    return sender.send(address, net::packet{msg.data(), msg.size()}).handle_exception([address](auto&& exception_ptr) {
        auto error_msg = seastar::format(
            "Syslog audit backend failed (sending a message to {} resulted in {}).",
            address,
            exception_ptr
        );
        logger.error("{}", error_msg);
        throw audit_exception(std::move(error_msg));
    });
}

static auto syslog_address_helper(const db::config& cfg)
{
    return cfg.audit_unix_socket_path.is_set()
        ? unix_domain_addr(cfg.audit_unix_socket_path())
        : unix_domain_addr(_PATH_LOG);
}

}

audit_syslog_storage_helper::audit_syslog_storage_helper(cql3::query_processor& qp, service::migration_manager&) :
    _syslog_address(syslog_address_helper(qp.db().get_config())),
    _sender(make_unbound_datagram_channel(AF_UNIX)) {
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
        return make_ready_future();
    }

    return syslog_send_helper(_sender, _syslog_address, "Initializing syslog audit backend.");
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
    sstring msg = seastar::format("<{}>{:%h %e %T} scylla-audit: \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\"",
                                    LOG_NOTICE | LOG_USER,
                                    time,
                                    node_ip,
                                    audit_info->category_string(),
                                    cl,
                                    (error ? "true" : "false"),
                                    audit_info->keyspace(),
                                    audit_info->query(),
                                    client_ip,
                                    audit_info->table(),
                                    username);

    return syslog_send_helper(_sender, _syslog_address, msg);
}

future<> audit_syslog_storage_helper::write_login(const sstring& username,
                                                  socket_address node_ip,
                                                  socket_address client_ip,
                                                  bool error) {

    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    tm time;
    localtime_r(&now, &time);
    sstring msg = seastar::format("<{}>{:%h %e %T} scylla-audit: \"{}\", \"AUTH\", \"\", \"\", \"\", \"\", \"{}\", \"{}\", \"{}\"",
                                    LOG_NOTICE | LOG_USER,
                                    time,
                                    node_ip,
                                    client_ip,
                                    username,
                                    (error ? "true" : "false"));

    co_await syslog_send_helper(_sender, _syslog_address, msg.c_str());
}

using registry = class_registrator<storage_helper, audit_syslog_storage_helper, cql3::query_processor&, service::migration_manager&>;
static registry registrator1("audit_syslog_storage_helper");

}
