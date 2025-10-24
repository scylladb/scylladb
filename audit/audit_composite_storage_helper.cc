/*
 * Copyright (C) 2025 ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/loop.hh>
#include <seastar/core/future-util.hh>

#include "audit/audit_composite_storage_helper.hh"

#include "utils/class_registrator.hh"

namespace audit {

audit_composite_storage_helper::audit_composite_storage_helper(const std::vector<sstring>& storage_helper_class_names, cql3::query_processor& qp, service::migration_manager& mm) {
    for (auto& storage_helper_class_name : storage_helper_class_names) {
        try {
            auto _storage_helper_ptr = create_object<storage_helper>(storage_helper_class_name, qp, mm);
            _storage_helpers.push_back(std::move(_storage_helper_ptr));
        } catch (no_such_class& e) {
            logger.warn("Can't create audit storage helper {}: not supported", storage_helper_class_name);
        } catch (std::exception& e) {
            logger.error("Can't create audit storage helper {}: {}", storage_helper_class_name, e);
        }
    }
    if (_storage_helpers.empty()) {
        throw audit_exception("No audit storage helper could be created");
    }
}

audit_composite_storage_helper::~audit_composite_storage_helper() {
}

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

future<> audit_composite_storage_helper::write(const audit_info* audit_info,
                                               socket_address node_ip,
                                               socket_address client_ip,
                                               db::consistency_level cl,
                                               const sstring& username,
                                               bool error) {
    return seastar::parallel_for_each(
        _storage_helpers,
        [audit_info, node_ip, client_ip, cl, username, error](std::unique_ptr<storage_helper>& h) {
            return h->write(audit_info, node_ip, client_ip, cl, username, error);
        }
    );
}

future<> audit_composite_storage_helper::write_login(const sstring& username,
                                                     socket_address node_ip,
                                                     socket_address client_ip,
                                                     bool error) {
    return seastar::parallel_for_each(
        _storage_helpers,
        [username, node_ip, client_ip, error](std::unique_ptr<storage_helper>& h) {
            return h->write_login(username, node_ip, client_ip, error);
        }
    );
}

} // namespace audit
