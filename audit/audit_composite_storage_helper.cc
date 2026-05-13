/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/core/loop.hh>
#include <seastar/core/future-util.hh>

#include "audit/audit_composite_storage_helper.hh"

#include "utils/class_registrator.hh"

namespace audit {

audit_composite_storage_helper::audit_composite_storage_helper(std::vector<std::unique_ptr<storage_helper>>&& storage_helpers)
    : _storage_helpers(std::move(storage_helpers))
{}

future<> audit_composite_storage_helper::start(const db::config& cfg) {
    auto res = seastar::parallel_for_each(
        _storage_helpers,
        [&cfg] (std::unique_ptr<storage_helper>& h) {
            return h->start(cfg);
        }
    );
    return res;
}

future<> audit_composite_storage_helper::stop() {
    auto res = seastar::parallel_for_each(
        _storage_helpers,
        [] (std::unique_ptr<storage_helper>& h) {
            return h->stop();
        }
    );
    return res;
}

future<> audit_composite_storage_helper::write(audit_sink_set sinks,
                                               const audit_info* audit_info,
                                               socket_address node_ip,
                                               socket_address client_ip,
                                               std::optional<db::consistency_level> cl,
                                               const sstring& username,
                                               bool error) {
    return seastar::parallel_for_each(
        _storage_helpers,
        [sinks, audit_info, node_ip, client_ip, cl, &username, error](std::unique_ptr<storage_helper>& h) {
            return h->write(sinks, audit_info, node_ip, client_ip, cl, username, error);
        }
    );
}

future<> audit_composite_storage_helper::write_login(audit_sink_set sinks,
                                                     const sstring& username,
                                                     socket_address node_ip,
                                                     socket_address client_ip,
                                                     bool error) {
    return seastar::parallel_for_each(
        _storage_helpers,
        [sinks, &username, node_ip, client_ip, error](std::unique_ptr<storage_helper>& h) {
            return h->write_login(sinks, username, node_ip, client_ip, error);
        }
    );
}

} // namespace audit
