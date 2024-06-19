/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <string_view>
#include <memory>
#include <optional>

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/bool_class.hh>
#include <seastar/core/sharded.hh>

#include "auth/authenticator.hh"
#include "auth/authorizer.hh"
#include "auth/permission.hh"
#include "auth/permissions_cache.hh"
#include "auth/role_manager.hh"
#include "auth/common.hh"
#include "seastarx.hh"
#include "service/raft/raft_group0_client.hh"
#include "utils/observable.hh"
#include "utils/serialized_action.hh"
#include "service/maintenance_mode.hh"

namespace cql3 {
class query_processor;
}

namespace service {
class migration_manager;
class migration_notifier;
class migration_listener;
}

namespace auth {

class role_or_anonymous;

struct service_config final {
    sstring authorizer_java_name;
    sstring authenticator_java_name;
    sstring role_manager_java_name;
};

///
/// Due to poor (in this author's opinion) decisions of Apache Cassandra, certain choices of one role-manager,
/// authenticator, or authorizer imply restrictions on the rest.
///
/// This exception is thrown when an invalid combination of modules is selected, with a message explaining the
/// incompatibility.
///
class incompatible_module_combination : public std::invalid_argument {
public:
    using std::invalid_argument::invalid_argument;
};

///
/// Client for access-control in the system.
///
/// Access control encompasses user/role management, authentication, and authorization. This client provides access to
/// the dynamically-loaded implementations of these modules (through the `underlying_*` member functions), but also
/// builds on their functionality with caching and abstractions for common operations.
///
/// All state associated with access-control is stored externally to any particular instance of this class.
///
/// peering_sharded_service inheritance is needed to be able to access shard local authentication service
/// given an object from another shard. Used for bouncing lwt requests to correct shard.
class service final : public seastar::peering_sharded_service<service> {
    utils::loading_cache_config _loading_cache_config;
    std::unique_ptr<permissions_cache> _permissions_cache;

    cql3::query_processor& _qp;

    ::service::raft_group0_client& _group0_client;

    ::service::migration_notifier& _mnotifier;

    authorizer::ptr_type _authorizer;

    authenticator::ptr_type _authenticator;

    role_manager::ptr_type _role_manager;

    // Only one of these should be registered, so we end up with some unused instances. Not the end of the world.
    std::unique_ptr<::service::migration_listener> _migration_listener;

    std::function<void(uint32_t)> _permissions_cache_cfg_cb;
    serialized_action _permissions_cache_config_action;

    utils::observer<uint32_t> _permissions_cache_max_entries_observer;
    utils::observer<uint32_t> _permissions_cache_update_interval_in_ms_observer;
    utils::observer<uint32_t> _permissions_cache_validity_in_ms_observer;

    maintenance_socket_enabled _used_by_maintenance_socket;

    abort_source _as;

public:
    service(
            utils::loading_cache_config,
            cql3::query_processor&,
            ::service::raft_group0_client&,
            ::service::migration_notifier&,
            std::unique_ptr<authorizer>,
            std::unique_ptr<authenticator>,
            std::unique_ptr<role_manager>,
            maintenance_socket_enabled);

    ///
    /// This constructor is intended to be used when the class is sharded via \ref seastar::sharded. In that case, the
    /// arguments must be copyable, which is why we delay construction with instance-construction instructions instead
    /// of the instances themselves.
    ///
    service(
            utils::loading_cache_config,
            cql3::query_processor&,
            ::service::raft_group0_client&,
            ::service::migration_notifier&,
            ::service::migration_manager&,
            const service_config&,
            maintenance_socket_enabled);

    future<> start(::service::migration_manager&, db::system_keyspace&);

    future<> stop();

    void update_cache_config();

    void reset_authorization_cache();

    ///
    /// \returns an exceptional future with \ref nonexistant_role if the named role does not exist.
    ///
    future<permission_set> get_permissions(const role_or_anonymous&, const resource&) const;

    ///
    /// Like \ref get_permissions, but never returns cached permissions.
    ///
    future<permission_set> get_uncached_permissions(const role_or_anonymous&, const resource&) const;

    ///
    /// Query whether the named role has been granted a role that is a superuser.
    ///
    /// A role is always granted to itself. Therefore, a role that "is" a superuser also "has" superuser.
    ///
    /// \returns an exceptional future with \ref nonexistant_role if the role does not exist.
    ///
    future<bool> has_superuser(std::string_view role_name) const;

    ///
    /// Create a role with optional authentication information.
    ///
    /// \returns an exceptional future with \ref role_already_exists if the user or role exists.
    ///
    /// \returns an exceptional future with \ref unsupported_authentication_option if an unsupported option is included.
    ///
    future<> create_role(std::string_view name,
            const role_config& config,
            const authentication_options& options,
            ::service::group0_batch& mc) const;

    ///
    /// Return the set of all roles granted to the given role, including itself and roles granted through other roles.
    ///
    /// \returns an exceptional future with \ref nonexistent_role if the role does not exist.
    future<role_set> get_roles(std::string_view role_name) const;

    future<bool> exists(const resource&) const;

    authenticator& underlying_authenticator() const {
        return *_authenticator;
    }

    authorizer& underlying_authorizer() const {
        return *_authorizer;
    }

    role_manager& underlying_role_manager() const {
        return *_role_manager;
    }

    cql3::query_processor& query_processor() const noexcept {
        return _qp;
    }

    future<> commit_mutations(::service::group0_batch&& mc) {
        return std::move(mc).commit(_group0_client, _as, ::service::raft_timeout{});
    }

private:
    future<> create_legacy_keyspace_if_missing(::service::migration_manager& mm) const;
    future<bool> has_superuser(std::string_view role_name, const role_set& roles) const;
};

future<bool> has_superuser(const service&, const authenticated_user&);

future<role_set> get_roles(const service&, const authenticated_user&);

future<permission_set> get_permissions(const service&, const authenticated_user&, const resource&);

/// A description of a CQL command from which auth::service can tell whether or not this command could endanger
/// internal data on which auth::service depends.
struct command_desc {
    auth::permission permission; ///< Nature of the command's alteration.
    const ::auth::resource& resource; ///< Resource impacted by this command.
    enum class type {
        ALTER_WITH_OPTS, ///< Command is ALTER ... WITH ...
        OTHER
    } type_ = type::OTHER;
};

///
/// Protected resources cannot be modified even if the performer has permissions to do so.
///
bool is_protected(const service&, command_desc) noexcept;

///
/// Create a role with optional authentication information.
///
/// \returns an exceptional future with \ref role_already_exists if the user or role exists.
///
/// \returns an exceptional future with \ref unsupported_authentication_option if an unsupported option is included.
///
future<> create_role(
        const service&,
        std::string_view name,
        const role_config&,
        const authentication_options&,
        ::service::group0_batch&);

///
/// Alter an existing role and its authentication information.
///
/// \returns an exceptional future with \ref nonexistant_role if the named role does not exist.
///
/// \returns an exceptional future with \ref unsupported_authentication_option if an unsupported option is included.
///
future<> alter_role(
        const service&,
        std::string_view name,
        const role_config_update&,
        const authentication_options&,
        ::service::group0_batch& mc);

///
/// Drop a role from the system, including all permissions and authentication information.
///
/// \returns an exceptional future with \ref nonexistant_role if the named role does not exist.
///
future<> drop_role(const service&, std::string_view name, ::service::group0_batch& mc);

///
/// Grant `role_name` to `grantee_name`.
///
/// \returns an exceptional future with \ref nonexistant_role if either the role or the grantee do not exist.
///
/// \returns an exceptional future with \ref role_already_included if granting the role would be redundant, or
/// create a cycle.
///
future<> grant_role(const service&, std::string_view grantee_name, std::string_view role_name, ::service::group0_batch& mc);

///
/// Revoke `role_name` from `revokee_name`.
///
/// \returns an exceptional future with \ref nonexistant_role if either the role or the revokee do not exist.
///
/// \returns an exceptional future with \ref revoke_ungranted_role if the role was not granted.
///
future<> revoke_role(const service&, std::string_view revokee_name, std::string_view role_name, ::service::group0_batch& mc);

///
/// Check if `grantee` has been granted the named role.
///
/// \returns an exceptional future with \ref nonexistent_role if `grantee` or `name` do not exist.
///
future<bool> has_role(const service&, std::string_view grantee, std::string_view name);
///
/// Check if the authenticated user has been granted the named role.
///
/// \returns an exceptional future with \ref nonexistent_role if the user or `name` do not exist.
///
future<bool> has_role(const service&, const authenticated_user&, std::string_view name);


/// Sets `attribute_name` with `attribute_value` for `role_name`.
/// \returns an exceptional future with nonexistant_role if the role does not exist.
///
future<> set_attribute(const service&, std::string_view role_name, std::string_view attribute_name, std::string_view attribute_value, ::service::group0_batch& mc);

/// Removes `attribute_name` for `role_name`.
/// \returns an exceptional future with nonexistant_role if the role does not exist.
/// \note: This is a no-op if the role does not have the named attribute set.
///
future<> remove_attribute(const service&, std::string_view role_name, std::string_view attribute_name, ::service::group0_batch& mc);

///
/// \returns an exceptional future with \ref nonexistent_role if the named role does not exist.
///
/// \returns an exceptional future with \ref unsupported_authorization_operation if granting permissions is not
/// supported.
///
future<> grant_permissions(
        const service&,
        std::string_view role_name,
        permission_set,
        const resource&,
        ::service::group0_batch&);

///
/// Like \ref grant_permissions, but grants all applicable permissions on the resource.
///
/// \returns an exceptional future with \ref nonexistent_role if the named role does not exist.
///
/// \returns an exceptional future with \ref unsupported_authorization_operation if granting permissions is not
/// supported.
///
future<> grant_applicable_permissions(const service&, std::string_view role_name, const resource&, ::service::group0_batch&);
future<> grant_applicable_permissions(const service&, const authenticated_user&, const resource&, ::service::group0_batch&);

///
/// \returns an exceptional future with \ref nonexistent_role if the named role does not exist.
///
/// \returns an exceptional future with \ref unsupported_authorization_operation if revoking permissions is not
/// supported.
///
future<> revoke_permissions(
        const service&,
        std::string_view role_name,
        permission_set,
        const resource&,
        ::service::group0_batch&);

///
/// Revoke all permissions granted to any role for a particular resource.
///
/// \throws \ref unsupported_authorization_operation if revoking permissions is not supported.
///
future<> revoke_all(const service&, const resource&, ::service::group0_batch&);

using recursive_permissions = bool_class<struct recursive_permissions_tag>;

///
/// Query for all granted permissions according to filtering criteria.
///
/// Only permissions included in the provided set are included.
///
/// If a role name is provided, only permissions granted (directly or recursively) to the role are included.
///
/// If a resource filter is provided, only permissions granted on the resource are included. When \ref
/// recursive_permissions is `true`, permissions on a parent resource are included.
///
/// \returns an exceptional future with \ref nonexistent_role if a role name is included which refers to a role that
/// does not exist.
///
/// \returns an exceptional future with \ref unsupported_authorization_operation if listing permissions is not
/// supported.
///
future<std::vector<permission_details>> list_filtered_permissions(
        const service&,
        permission_set,
        std::optional<std::string_view> role_name,
        const std::optional<std::pair<resource, recursive_permissions>>& resource_filter);


// Finalizes write operations performed in auth by committing mutations via raft group0.
future<> commit_mutations(service& ser, ::service::group0_batch&& mc);

// Migrates data from old keyspace to new one which supports linearizable writes via raft.
future<> migrate_to_auth_v2(db::system_keyspace& sys_ks, ::service::raft_group0_client& g0, start_operation_func_t start_operation_func, abort_source& as);

}
