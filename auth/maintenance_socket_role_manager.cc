/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "auth/maintenance_socket_role_manager.hh"

#include <seastar/core/future.hh>
#include <stdexcept>
#include <string_view>
#include "auth/cache.hh"
#include "cql3/description.hh"
#include "utils/log.hh"
#include "utils/on_internal_error.hh"

namespace auth {

static logging::logger log("maintenance_socket_role_manager");

future<> maintenance_socket_role_manager::ensure_role_operations_are_enabled() {
    if (_is_maintenance_mode) {
        on_internal_error(log, "enabling role operations not allowed in maintenance mode");
    }

    if (_std_mgr.has_value()) {
        on_internal_error(log, "role operations are already enabled");
    }

    _std_mgr.emplace(_qp, _group0_client, _migration_manager, _cache);
    return _std_mgr->start();
}

void maintenance_socket_role_manager::set_maintenance_mode() {
    if (_std_mgr.has_value()) {
        on_internal_error(log, "cannot enter maintenance mode after role operations have been enabled");
    }
    _is_maintenance_mode = true;
}

maintenance_socket_role_manager::maintenance_socket_role_manager(
        cql3::query_processor& qp,
        ::service::raft_group0_client& rg0c,
        ::service::migration_manager& mm,
        cache& c)
    : _qp(qp)
    , _group0_client(rg0c)
    , _migration_manager(mm)
    , _cache(c)
    , _std_mgr(std::nullopt)
    , _is_maintenance_mode(false) {
}

std::string_view maintenance_socket_role_manager::qualified_java_name() const noexcept {
    return "com.scylladb.auth.MaintenanceSocketRoleManager";
}

const resource_set& maintenance_socket_role_manager::protected_resources() const {
    static const resource_set resources{};

    return resources;
}

future<> maintenance_socket_role_manager::start() {
    return make_ready_future<>();
}

future<> maintenance_socket_role_manager::stop() {
    return _std_mgr ? _std_mgr->stop() : make_ready_future<>();
}

future<> maintenance_socket_role_manager::ensure_superuser_is_created() {
    return _std_mgr ? _std_mgr->ensure_superuser_is_created() : make_ready_future<>();
}

template<typename T = void>
future<T> operation_not_supported_exception(std::string_view operation) {
    return make_exception_future<T>(
        std::runtime_error(fmt::format("role manager: {} operation not supported through maintenance socket", operation)));
}

template<typename T = void>
future<T> operation_not_available_in_maintenance_mode_exception(std::string_view operation) {
    return make_exception_future<T>(
        std::runtime_error(fmt::format("role manager: {} operation not available through maintenance socket in maintenance mode", operation)));
}

template<typename T = void>
future<T> manager_not_ready_exception(std::string_view operation) {
    return make_exception_future<T>(
        std::runtime_error(fmt::format("role manager: {} operation not available because manager not ready yet (role operations not enabled)", operation)));
}

future<> maintenance_socket_role_manager::create(std::string_view role_name, const role_config& c, ::service::group0_batch& mc) {
    return validate_operation("CREATE").then([this, role_name, c, &mc] {
        return _std_mgr->create(role_name, c, mc);
    });
}

future<> maintenance_socket_role_manager::drop(std::string_view role_name, ::service::group0_batch& mc) {
    return validate_operation("DROP").then([this, role_name, &mc] {
        return _std_mgr->drop(role_name, mc);
    });
}

future<> maintenance_socket_role_manager::alter(std::string_view role_name, const role_config_update& u, ::service::group0_batch& mc) {
    return validate_operation("ALTER").then([this, role_name, u, &mc] {
        return _std_mgr->alter(role_name, u, mc);
    });
}

future<> maintenance_socket_role_manager::grant(std::string_view grantee_name, std::string_view role_name, ::service::group0_batch& mc) {
    return validate_operation("GRANT").then([this, grantee_name, role_name, &mc] {
        return _std_mgr->grant(grantee_name, role_name, mc);
    });
}

future<> maintenance_socket_role_manager::revoke(std::string_view revokee_name, std::string_view role_name, ::service::group0_batch& mc) {
    return validate_operation("REVOKE").then([this, revokee_name, role_name, &mc] {
        return _std_mgr->revoke(revokee_name, role_name, mc);
    });
}

future<role_set> maintenance_socket_role_manager::query_granted(std::string_view grantee_name, recursive_role_query m) {
    return validate_operation("QUERY GRANTED").then([this, grantee_name, m] {
        return _std_mgr->query_granted(grantee_name, m);
    });
}

future<role_to_directly_granted_map> maintenance_socket_role_manager::query_all_directly_granted(::service::query_state& qs) {
    return validate_operation("QUERY ALL DIRECTLY GRANTED").then([this, &qs] {
        return _std_mgr->query_all_directly_granted(qs);
    });
}

future<role_set> maintenance_socket_role_manager::query_all(::service::query_state& qs) {
    return validate_operation("QUERY ALL").then([this, &qs] {
        return _std_mgr->query_all(qs);
    });
}

future<bool> maintenance_socket_role_manager::exists(std::string_view role_name) {
    return validate_operation("EXISTS").then([this, role_name] {
        return _std_mgr->exists(role_name);
    });
}

future<bool> maintenance_socket_role_manager::is_superuser(std::string_view role_name) {
    return validate_operation("IS SUPERUSER").then([this, role_name] {
        return _std_mgr->is_superuser(role_name);
    });
}

future<bool> maintenance_socket_role_manager::can_login(std::string_view role_name) {
    return validate_operation("CAN LOGIN").then([this, role_name] {
        return _std_mgr->can_login(role_name);
    });
}

future<std::optional<sstring>> maintenance_socket_role_manager::get_attribute(std::string_view role_name, std::string_view attribute_name, ::service::query_state& qs) {
    return validate_operation("GET ATTRIBUTE").then([this, role_name, attribute_name, &qs] {
        return _std_mgr->get_attribute(role_name, attribute_name, qs);
    });
}

future<role_manager::attribute_vals> maintenance_socket_role_manager::query_attribute_for_all(std::string_view attribute_name, ::service::query_state& qs) {
    return validate_operation("QUERY ATTRIBUTE FOR ALL").then([this, attribute_name, &qs] {
        return _std_mgr->query_attribute_for_all(attribute_name, qs);
    });
}

future<> maintenance_socket_role_manager::set_attribute(std::string_view role_name, std::string_view attribute_name, std::string_view attribute_value, ::service::group0_batch& mc) {
    return validate_operation("SET ATTRIBUTE").then([this, role_name, attribute_name, attribute_value, &mc] {
        return _std_mgr->set_attribute(role_name, attribute_name, attribute_value, mc);
    });
}

future<> maintenance_socket_role_manager::remove_attribute(std::string_view role_name, std::string_view attribute_name, ::service::group0_batch& mc) {
    return validate_operation("REMOVE ATTRIBUTE").then([this, role_name, attribute_name, &mc] {
        return _std_mgr->remove_attribute(role_name, attribute_name, mc);
    });
}

future<std::vector<cql3::description>> maintenance_socket_role_manager::describe_role_grants() {
    return validate_operation("DESCRIBE ROLE GRANTS").then([this] {
         return _std_mgr->describe_role_grants();
    });
}

future<> maintenance_socket_role_manager::validate_operation(std::string_view name) {
    if (_is_maintenance_mode) {
        return operation_not_available_in_maintenance_mode_exception(name);
    }
    if (!_std_mgr) {
        return manager_not_ready_exception(name);
    }
    return make_ready_future<>();
}

} // namespace auth
