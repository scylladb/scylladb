/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "auth/cache.hh"
#include "auth/resource.hh"
#include "auth/role_manager.hh"
#include "auth/standard_role_manager.hh"
#include <seastar/core/future.hh>

namespace cql3 {
class query_processor;
}

namespace service {
class migration_manager;
class raft_group0_client;
}

namespace auth {

// This role manager is used by the maintenance socket. It has disabled all role management operations
// in maintenance mode. In normal mode it delegates all operations to a standard_role_manager,
// which is created on demand when the node joins the cluster.
class maintenance_socket_role_manager final : public role_manager {
    cql3::query_processor& _qp;
    ::service::raft_group0_client& _group0_client;
    ::service::migration_manager& _migration_manager;
    cache& _cache;
    std::optional<standard_role_manager> _std_mgr;
    bool _is_maintenance_mode;

public:
    void set_maintenance_mode() override;

    // Ensures role management operations are enabled.
    // It must be called once the node has joined the cluster.
    // In the meantime all role management operations will fail.
    future<> ensure_role_operations_are_enabled() override;

    maintenance_socket_role_manager(cql3::query_processor&, ::service::raft_group0_client&, ::service::migration_manager&, cache&);

    virtual std::string_view qualified_java_name() const noexcept override;

    virtual const resource_set& protected_resources() const override ;

    virtual future<> start() override;

    virtual future<> stop() override;

    virtual future<> ensure_superuser_is_created() override;

    virtual future<> create(std::string_view role_name, const role_config& c, ::service::group0_batch& mc) override;

    virtual future<> drop(std::string_view role_name, ::service::group0_batch& mc) override;

    virtual future<> alter(std::string_view role_name, const role_config_update& u, ::service::group0_batch& mc) override;

    virtual future<> grant(std::string_view grantee_name, std::string_view role_name, ::service::group0_batch& mc) override;

    virtual future<> revoke(std::string_view revokee_name, std::string_view role_name, ::service::group0_batch& mc) override;

    virtual future<role_set> query_granted(std::string_view grantee_name, recursive_role_query m) override;

    virtual future<role_to_directly_granted_map> query_all_directly_granted(::service::query_state& qs) override;

    virtual future<role_set> query_all(::service::query_state& qs) override;

    virtual future<bool> exists(std::string_view role_name) override;

    virtual future<bool> is_superuser(std::string_view role_name) override;

    virtual future<bool> can_login(std::string_view role_name) override;

    virtual future<std::optional<sstring>> get_attribute(std::string_view role_name, std::string_view attribute_name, ::service::query_state& qs) override;

    virtual future<role_manager::attribute_vals> query_attribute_for_all(std::string_view attribute_name, ::service::query_state& qs) override;

    virtual future<> set_attribute(std::string_view role_name, std::string_view attribute_name, std::string_view attribute_value, ::service::group0_batch& mc) override;

    virtual future<> remove_attribute(std::string_view role_name, std::string_view attribute_name, ::service::group0_batch& mc) override;

    virtual future<std::vector<cql3::description>> describe_role_grants() override;

private:
    future<> validate_operation(std::string_view name);

};

}
