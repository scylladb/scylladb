/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "auth/common.hh"
#include "auth/role_manager.hh"

#include <string_view>

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include "seastarx.hh"
#include "service/raft/raft_group0_client.hh"

namespace cql3 {
class query_processor;
}

namespace service {
class migration_manager;
}

namespace auth {

class standard_role_manager final : public role_manager {
    cql3::query_processor& _qp;
    ::service::raft_group0_client& _group0_client;
    ::service::migration_manager& _migration_manager;
    future<> _stopped;
    abort_source _as;
    std::string _superuser;

public:
    standard_role_manager(cql3::query_processor&, ::service::raft_group0_client&, ::service::migration_manager&);

    virtual std::string_view qualified_java_name() const noexcept override;

    virtual const resource_set& protected_resources() const override;

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

private:
    enum class membership_change { add, remove };

    future<> create_legacy_metadata_tables_if_missing() const;

    bool legacy_metadata_exists();

    future<> migrate_legacy_metadata();

    future<> create_default_role_if_missing();

    future<> create_or_replace(std::string_view role_name, const role_config&, ::service::group0_batch&);

    future<> legacy_modify_membership(std::string_view role_name, std::string_view grantee_name, membership_change);

    future<> modify_membership(std::string_view role_name, std::string_view grantee_name, membership_change, ::service::group0_batch& mc);
};

}
