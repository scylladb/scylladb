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
#include "cql3/description.hh"
#include "utils/class_registrator.hh"

namespace auth {

constexpr std::string_view maintenance_socket_role_manager_name = "com.scylladb.auth.MaintenanceSocketRoleManager";

static const class_registrator<
        role_manager,
        maintenance_socket_role_manager,
        cql3::query_processor&,
        ::service::raft_group0_client&,
        ::service::migration_manager&> registration(sstring{maintenance_socket_role_manager_name});

future<> maintenance_socket_role_manager::enable_role_operations() {
    if (_is_maintenance_mode) {
        return make_exception_future<>(std::runtime_error(
            "role manager: enabling role operations not allowed in maintenance mode"));
    }

    if (_std_mgr.has_value()) {
        return make_ready_future<>();
    }

    _std_mgr.emplace(_qp, _group0_client, _migration_manager);
    return _std_mgr->start();
}

bool maintenance_socket_role_manager::is_maintenance_mode() const {
    return _is_maintenance_mode;
}

void maintenance_socket_role_manager::set_maintenance_mode() {
    SCYLLA_ASSERT(!_std_mgr.has_value() && "Cannot enable maintenance mode after role operations have been enabled");
    _is_maintenance_mode = true;
}

maintenance_socket_role_manager::maintenance_socket_role_manager(
        cql3::query_processor& qp,
        ::service::raft_group0_client& rg0c,
        ::service::migration_manager& mm)
    : _qp(qp)
    , _group0_client(rg0c)
    , _migration_manager(mm)
    , _std_mgr(std::nullopt)
    , _is_maintenance_mode(false) {
}

std::string_view maintenance_socket_role_manager::qualified_java_name() const noexcept {
    return maintenance_socket_role_manager_name;
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
    return make_ready_future<>();
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
future<T> operation_not_ready_exception(std::string_view operation) {
    return make_exception_future<T>(
        std::runtime_error(fmt::format("role manager: {} operation not available yet (role operations not enabled)", operation)));
}

future<> maintenance_socket_role_manager::create(std::string_view role_name, const role_config& config, ::service::group0_batch& mc) {
    if (_is_maintenance_mode) {
        return operation_not_available_in_maintenance_mode_exception("CREATE");
    }
    if (!_std_mgr) {
        return operation_not_ready_exception("CREATE");
    }
    return _std_mgr->create(role_name, config, mc);
}

future<> maintenance_socket_role_manager::drop(std::string_view role_name, ::service::group0_batch& mc) {
    if (_is_maintenance_mode) {
        return operation_not_available_in_maintenance_mode_exception("DROP");
    }
    if (!_std_mgr) {
        return operation_not_ready_exception("DROP");
    }
    return _std_mgr->drop(role_name, mc);
}

future<> maintenance_socket_role_manager::alter(std::string_view role_name, const role_config_update& config_update, ::service::group0_batch& mc) {
    if (_is_maintenance_mode) {
        return operation_not_available_in_maintenance_mode_exception("ALTER");
    }
    if (!_std_mgr) {
        return operation_not_ready_exception("ALTER");
    }
    return _std_mgr->alter(role_name, config_update, mc);
}

future<> maintenance_socket_role_manager::grant(std::string_view grantee_name, std::string_view role_name, ::service::group0_batch& mc) {
    return operation_not_supported_exception("GRANT");
}

future<> maintenance_socket_role_manager::revoke(std::string_view revokee_name, std::string_view role_name, ::service::group0_batch& mc) {
    return operation_not_supported_exception("REVOKE");
}

future<role_set> maintenance_socket_role_manager::query_granted(std::string_view grantee_name, recursive_role_query) {
    return operation_not_supported_exception<role_set>("QUERY GRANTED");
}

future<role_to_directly_granted_map> maintenance_socket_role_manager::query_all_directly_granted(::service::query_state&) {
    return operation_not_supported_exception<role_to_directly_granted_map>("QUERY ALL DIRECTLY GRANTED");
}

future<role_set> maintenance_socket_role_manager::query_all(::service::query_state&) {
    return operation_not_supported_exception<role_set>("QUERY ALL");
}

future<bool> maintenance_socket_role_manager::exists(std::string_view role_name) {
    return operation_not_supported_exception<bool>("EXISTS");
}

future<bool> maintenance_socket_role_manager::is_superuser(std::string_view role_name) {
    return make_ready_future<bool>(true);
}

future<bool> maintenance_socket_role_manager::can_login(std::string_view role_name) {
    return make_ready_future<bool>(true);
}

future<std::optional<sstring>> maintenance_socket_role_manager::get_attribute(std::string_view role_name, std::string_view attribute_name, ::service::query_state&) {
    return operation_not_supported_exception<std::optional<sstring>>("GET ATTRIBUTE");
}

future<role_manager::attribute_vals> maintenance_socket_role_manager::query_attribute_for_all(std::string_view attribute_name, ::service::query_state&) {
    return operation_not_supported_exception<role_manager::attribute_vals>("QUERY ATTRIBUTE");
}

future<> maintenance_socket_role_manager::set_attribute(std::string_view role_name, std::string_view attribute_name, std::string_view attribute_value, ::service::group0_batch& mc) {
    return operation_not_supported_exception("SET ATTRIBUTE");
}

future<> maintenance_socket_role_manager::remove_attribute(std::string_view role_name, std::string_view attribute_name, ::service::group0_batch& mc) {
    return operation_not_supported_exception("REMOVE ATTRIBUTE");
}

future<std::vector<cql3::description>> maintenance_socket_role_manager::describe_role_grants() {
    return operation_not_supported_exception<std::vector<cql3::description>>("DESCRIBE SCHEMA WITH INTERNALS");
}

} // namespace auth
