/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "auth/resource.hh"
#include "auth/role_manager.hh"
#include <seastar/core/future.hh>

namespace cql3 {
class query_processor;
}

namespace service {
class migration_manager;
class raft_group0_client;
}

namespace auth {

extern const std::string_view maintenance_socket_role_manager_name;

// This role manager is used by the maintenance socket. It has disabled all role management operations to not depend on
// system_auth keyspace, which may be not yet created when the maintenance socket starts listening.
class maintenance_socket_role_manager final : public role_manager {
public:
    maintenance_socket_role_manager(cql3::query_processor&, ::service::raft_group0_client&, ::service::migration_manager&) {}

    virtual std::string_view qualified_java_name() const noexcept override;

    virtual const resource_set& protected_resources() const override ;

    virtual future<> start() override;

    virtual future<> stop() override;

    virtual future<> create(std::string_view role_name, const role_config&, ::service::group0_batch&) override;

    virtual future<> drop(std::string_view role_name, ::service::group0_batch& mc) override;

    virtual future<> alter(std::string_view role_name, const role_config_update&, ::service::group0_batch&) override;

    virtual future<> grant(std::string_view grantee_name, std::string_view role_name, ::service::group0_batch& mc) override;

    virtual future<> revoke(std::string_view revokee_name, std::string_view role_name, ::service::group0_batch& mc) override;

    virtual future<role_set> query_granted(std::string_view grantee_name, recursive_role_query) override;

    virtual future<role_to_directly_granted_map> query_all_directly_granted() override;

    virtual future<role_set> query_all() override;

    virtual future<bool> exists(std::string_view role_name) override;

    virtual future<bool> is_superuser(std::string_view role_name) override;

    virtual future<bool> can_login(std::string_view role_name) override;

    virtual future<std::optional<sstring>> get_attribute(std::string_view role_name, std::string_view attribute_name) override;

    virtual future<role_manager::attribute_vals> query_attribute_for_all(std::string_view attribute_name) override;

    virtual future<> set_attribute(std::string_view role_name, std::string_view attribute_name, std::string_view attribute_value, ::service::group0_batch& mc) override;

    virtual future<> remove_attribute(std::string_view role_name, std::string_view attribute_name, ::service::group0_batch& mc) override;
};

}
