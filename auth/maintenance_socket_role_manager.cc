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
#include "auth/authenticated_user.hh"
#include "auth/cache.hh"
#include "cql3/description.hh"
#include "utils/class_registrator.hh"
#include "utils/log.hh"
#include "utils/on_internal_error.hh"

namespace auth {

static logging::logger log("maintenance_socket_role_manager");

constexpr std::string_view maintenance_socket_role_manager_name = "com.scylladb.auth.MaintenanceSocketRoleManager";

static const class_registrator<
        role_manager,
        maintenance_socket_role_manager,
        cql3::query_processor&,
        ::service::raft_group0_client&,
        ::service::migration_manager&,
        cache&> registration(sstring{maintenance_socket_role_manager_name});

future<> maintenance_socket_role_manager::enable_role_operations() {
    if (_is_maintenance_mode) {
        return make_exception_future<>(std::runtime_error(
            "role manager: enabling role operations not allowed in maintenance mode"));
    }

    if (_std_mgr.has_value()) {
        return make_ready_future<>();
    }

    _std_mgr.emplace(_qp, _group0_client, _migration_manager, _cache);
    return _std_mgr->start();
}

bool maintenance_socket_role_manager::is_maintenance_mode() const {
    return _is_maintenance_mode;
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
future<T> operation_not_ready_exception(std::string_view operation) {
    return make_exception_future<T>(
        std::runtime_error(fmt::format("role manager: {} operation not available yet (role operations not enabled)", operation)));
}

future<> maintenance_socket_role_manager::create(std::string_view role_name, const role_config& c, ::service::group0_batch& mc) {
    if (role_name == maintenance_user().name) {
        on_internal_error(log, "cannot create maintenance user role");
    }
    if (_is_maintenance_mode) {
        return operation_not_available_in_maintenance_mode_exception("CREATE");
    }
    if (!_std_mgr) {
        return operation_not_ready_exception("CREATE");
    }
    return _std_mgr->create(role_name, c, mc);
}

future<> maintenance_socket_role_manager::drop(std::string_view role_name, ::service::group0_batch& mc) {
    if (role_name == maintenance_user().name) {
        on_internal_error(log, "cannot drop maintenance user role");
    }
    if (_is_maintenance_mode) {
        return operation_not_available_in_maintenance_mode_exception("DROP");
    }
    if (!_std_mgr) {
        return operation_not_ready_exception("DROP");
    }
    return _std_mgr->drop(role_name, mc);
}

future<> maintenance_socket_role_manager::alter(std::string_view role_name, const role_config_update& u, ::service::group0_batch& mc) {
    if (role_name == maintenance_user().name) {
        on_internal_error(log, "cannot alter maintenance user role");
    }
    if (_is_maintenance_mode) {
        return operation_not_available_in_maintenance_mode_exception("ALTER");
    }
    if (!_std_mgr) {
        return operation_not_ready_exception("ALTER");
    }
    return _std_mgr->alter(role_name, u, mc);
}

future<> maintenance_socket_role_manager::grant(std::string_view grantee_name, std::string_view role_name, ::service::group0_batch& mc) {
    if (grantee_name == maintenance_user().name || role_name == maintenance_user().name) {
        on_internal_error(log, "cannot grant maintenance user role");
    }
    if (_is_maintenance_mode) {
        return operation_not_available_in_maintenance_mode_exception("GRANT");
    }
    if (!_std_mgr) {
        return operation_not_ready_exception("GRANT");
    }
    return _std_mgr->grant(grantee_name, role_name, mc);
}

future<> maintenance_socket_role_manager::revoke(std::string_view revokee_name, std::string_view role_name, ::service::group0_batch& mc) {
    if (revokee_name == maintenance_user().name || role_name == maintenance_user().name) {
        on_internal_error(log, "cannot revoke maintenance user role");
    }
    if (_is_maintenance_mode) {
        return operation_not_available_in_maintenance_mode_exception("REVOKE");
    }
    if (!_std_mgr) {
        return operation_not_ready_exception("REVOKE");
    }
    return _std_mgr->revoke(revokee_name, role_name, mc);
}

future<role_set> maintenance_socket_role_manager::query_granted(std::string_view grantee_name, recursive_role_query m) {
    if (grantee_name == maintenance_user().name) {
        return make_ready_future<role_set>(role_set{sstring{grantee_name}});
    }
    if (_is_maintenance_mode) {
        return operation_not_available_in_maintenance_mode_exception<role_set>("QUERY GRANTED");
    }
    if (!_std_mgr) {
        return operation_not_ready_exception<role_set>("QUERY GRANTED");
    }
    return _std_mgr->query_granted(grantee_name, m);
}

future<role_to_directly_granted_map> maintenance_socket_role_manager::query_all_directly_granted(::service::query_state& qs) {
    if (_is_maintenance_mode) {
        return operation_not_available_in_maintenance_mode_exception<role_to_directly_granted_map>("QUERY ALL DIRECTLY GRANTED");
    }
    if (!_std_mgr) {
        return operation_not_ready_exception<role_to_directly_granted_map>("QUERY ALL DIRECTLY GRANTED");
    }
    return _std_mgr->query_all_directly_granted(qs);
}

future<role_set> maintenance_socket_role_manager::query_all(::service::query_state& qs) {
    if (_is_maintenance_mode) {
        return operation_not_available_in_maintenance_mode_exception<role_set>("QUERY ALL");
    }
    if (!_std_mgr) {
        return operation_not_ready_exception<role_set>("QUERY ALL");
    }
    return _std_mgr->query_all(qs);
}

future<bool> maintenance_socket_role_manager::exists(std::string_view role_name) {
    if (role_name == maintenance_user().name) {
        return make_ready_future<bool>(true);
    }
    if (_is_maintenance_mode) {
        return operation_not_available_in_maintenance_mode_exception<bool>("EXISTS");
    }
    if (!_std_mgr) {
        return operation_not_ready_exception<bool>("EXISTS");
    }
    return _std_mgr->exists(role_name);
}

future<bool> maintenance_socket_role_manager::is_superuser(std::string_view role_name) {
    if (role_name == maintenance_user().name) {
        return make_ready_future<bool>(true);
    }
    if (_is_maintenance_mode) {
        return operation_not_available_in_maintenance_mode_exception<bool>("IS SUPERUSER");
    }
    if (!_std_mgr) {
        return operation_not_ready_exception<bool>("IS SUPERUSER");
    }
    return _std_mgr->is_superuser(role_name);
}

future<bool> maintenance_socket_role_manager::can_login(std::string_view role_name) {
    if (role_name == maintenance_user().name) {
        return make_ready_future<bool>(true);
    }
    if (_is_maintenance_mode) {
        return operation_not_available_in_maintenance_mode_exception<bool>("CAN LOGIN");
    }
    if (!_std_mgr) {
        return operation_not_ready_exception<bool>("CAN LOGIN");
    }
    return _std_mgr->can_login(role_name);
}

future<std::optional<sstring>> maintenance_socket_role_manager::get_attribute(std::string_view role_name, std::string_view attribute_name, ::service::query_state& qs) {
    if (role_name == maintenance_user().name) {
        on_internal_error(log, "cannot get attribute of maintenance user role");
    }
    if (_is_maintenance_mode) {
        return operation_not_available_in_maintenance_mode_exception<std::optional<sstring>>("GET ATTRIBUTE");
    }
    if (!_std_mgr) {
        return operation_not_ready_exception<std::optional<sstring>>("GET ATTRIBUTE");
    }
    return _std_mgr->get_attribute(role_name, attribute_name, qs);
}

future<role_manager::attribute_vals> maintenance_socket_role_manager::query_attribute_for_all(std::string_view attribute_name, ::service::query_state& qs) {
    if (_is_maintenance_mode) {
        return operation_not_available_in_maintenance_mode_exception<role_manager::attribute_vals>("QUERY ATTRIBUTE");
    }
    if (!_std_mgr) {
        return operation_not_ready_exception<role_manager::attribute_vals>("QUERY ATTRIBUTE");
    }
    return _std_mgr->query_attribute_for_all(attribute_name, qs);
}

future<> maintenance_socket_role_manager::set_attribute(std::string_view role_name, std::string_view attribute_name, std::string_view attribute_value, ::service::group0_batch& mc) {
    if (role_name == maintenance_user().name) {
        on_internal_error(log, "cannot set attribute on maintenance user role");
    }
    if (_is_maintenance_mode) {
        return operation_not_available_in_maintenance_mode_exception("SET ATTRIBUTE");
    }
    if (!_std_mgr) {
        return operation_not_ready_exception("SET ATTRIBUTE");
    }
    return _std_mgr->set_attribute(role_name, attribute_name, attribute_value, mc);
}

future<> maintenance_socket_role_manager::remove_attribute(std::string_view role_name, std::string_view attribute_name, ::service::group0_batch& mc) {
    if (role_name == maintenance_user().name) {
        on_internal_error(log, "cannot remove attribute from maintenance user role");
    }
    if (_is_maintenance_mode) {
        return operation_not_available_in_maintenance_mode_exception("REMOVE ATTRIBUTE");
    }
    if (!_std_mgr) {
        return operation_not_ready_exception("REMOVE ATTRIBUTE");
    }
    return _std_mgr->remove_attribute(role_name, attribute_name, mc);
}

future<std::vector<cql3::description>> maintenance_socket_role_manager::describe_role_grants() {
    if (_is_maintenance_mode) {
        return operation_not_available_in_maintenance_mode_exception<std::vector<cql3::description>>("DESCRIBE ROLE GRANTS");
    }
    if (!_std_mgr) {
        return operation_not_ready_exception<std::vector<cql3::description>>("DESCRIBE ROLE GRANTS");
    }
    return _std_mgr->describe_role_grants();
}

} // namespace auth
