/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "auth/maintenance_socket_role_manager.hh"

#include <seastar/core/future.hh>
#include <stdexcept>
#include <string_view>
#include "utils/class_registrator.hh"

namespace auth {

constexpr std::string_view maintenance_socket_role_manager_name = "com.scylladb.auth.MaintenanceSocketRoleManager";

static const class_registrator<
        role_manager,
        maintenance_socket_role_manager,
        cql3::query_processor&,
        ::service::raft_group0_client&,
        ::service::migration_manager&> registration(sstring{maintenance_socket_role_manager_name});


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
    return make_ready_future<>();
}

template<typename T = void>
future<T> operation_not_supported_exception(std::string_view operation) {
    return make_exception_future<T>(
        std::runtime_error(format("role manager: {} operation not supported through maintenance socket", operation)));
}

future<> maintenance_socket_role_manager::create(std::string_view role_name, const role_config&, ::service::group0_batch&) {
    return operation_not_supported_exception("CREATE");
}

future<> maintenance_socket_role_manager::drop(std::string_view role_name, ::service::group0_batch& mc) {
    return operation_not_supported_exception("DROP");
}

future<> maintenance_socket_role_manager::alter(std::string_view role_name, const role_config_update&, ::service::group0_batch&) {
    return operation_not_supported_exception("ALTER");
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

future<role_to_directly_granted_map> maintenance_socket_role_manager::query_all_directly_granted() {
    return operation_not_supported_exception<role_to_directly_granted_map>("QUERY ALL DIRECTLY GRANTED");
}

future<role_set> maintenance_socket_role_manager::query_all() {
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

future<std::optional<sstring>> maintenance_socket_role_manager::get_attribute(std::string_view role_name, std::string_view attribute_name) {
    return operation_not_supported_exception<std::optional<sstring>>("GET ATTRIBUTE");
}

future<role_manager::attribute_vals> maintenance_socket_role_manager::query_attribute_for_all(std::string_view attribute_name) {
    return operation_not_supported_exception<role_manager::attribute_vals>("QUERY ATTRIBUTE");
}

future<> maintenance_socket_role_manager::set_attribute(std::string_view role_name, std::string_view attribute_name, std::string_view attribute_value, ::service::group0_batch& mc) {
    return operation_not_supported_exception("SET ATTRIBUTE");
}

future<> maintenance_socket_role_manager::remove_attribute(std::string_view role_name, std::string_view attribute_name, ::service::group0_batch& mc) {
    return operation_not_supported_exception("REMOVE ATTRIBUTE");
}

}
