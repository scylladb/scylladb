/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */


#pragma once

#include <seastar/core/abort_source.hh>
#include <stdexcept>

#include "ent/ldap/ldap_connection.hh"
#include "standard_role_manager.hh"

namespace auth {

/// Queries an LDAP server for roles.
///
/// Since LDAP grants and revokes roles, calling grant() and revoke() is disallowed.
///
/// We query LDAP for a list of a particular user's roles, and the results must match roles that exist in the
/// database.  Furthermore, the user must have already authenticated to Scylla, meaning it, too, exists in the
/// database.  Therefore, some of the role_manager functionality is provided by a standard_role_manager under
/// the hood.  For example, listing all roles or checking if the user can login cannot currently be determined
/// by querying LDAP, so they are delegated to the standard_role_manager.
class ldap_role_manager : public role_manager {
    standard_role_manager _std_mgr;
    ::service::raft_group0_client& _group0_client;
    seastar::sstring _query_template; ///< LDAP URL dictating which query to make.
    seastar::sstring _target_attr; ///< LDAP entry attribute containing the Scylla role name.
    seastar::sstring _bind_name; ///< Username for LDAP simple bind.
    seastar::sstring _bind_password; ///< Password for LDAP simple bind.
    mutable ldap_reuser _connection_factory; // Potentially modified by query_granted().
    seastar::abort_source _as;
  public:
    ldap_role_manager(
            std::string_view query_template, ///< LDAP query template as described in Scylla documentation.
            std::string_view target_attr, ///< LDAP entry attribute containing the Scylla role name.
            std::string_view bind_name, ///< LDAP bind credentials.
            std::string_view bind_password, ///< LDAP bind credentials.
            cql3::query_processor& qp, ///< Passed to standard_role_manager.
            ::service::raft_group0_client& rg0c, ///< Passed to standard_role_manager.
            ::service::migration_manager& mm ///< Passed to standard_role_manager.
    );

    /// Retrieves LDAP configuration entries from qp and invokes the other constructor.  Required by
    /// class_registrator<role_manager>.
    ldap_role_manager(cql3::query_processor& qp, ::service::raft_group0_client& rg0c, ::service::migration_manager& mm);

    /// Thrown when query-template parsing fails.
    struct url_error : public std::runtime_error {
        using runtime_error::runtime_error;
    };

    std::string_view qualified_java_name() const noexcept override;

    const resource_set& protected_resources() const override;

    future<> start() override;

    future<> stop() override;

    future<> create(std::string_view, const role_config&, ::service::group0_batch& mc) override;

    future<> drop(std::string_view, ::service::group0_batch& mc) override;

    future<> alter(std::string_view, const role_config_update&, ::service::group0_batch& mc) override;

    future<> grant(std::string_view, std::string_view, ::service::group0_batch& mc) override;

    future<> revoke(std::string_view, std::string_view, ::service::group0_batch& mc) override;

    future<role_set> query_granted(std::string_view, recursive_role_query) override;

    future<role_to_directly_granted_map> query_all_directly_granted() override;

    future<role_set> query_all() override;

    future<bool> exists(std::string_view) override;

    future<bool> is_superuser(std::string_view) override;

    future<bool> can_login(std::string_view) override;

    future<std::optional<sstring>> get_attribute(std::string_view, std::string_view) override;

    future<role_manager::attribute_vals> query_attribute_for_all(std::string_view) override;

    future<> set_attribute(std::string_view, std::string_view, std::string_view, ::service::group0_batch& mc) override;

    future<> remove_attribute(std::string_view, std::string_view, ::service::group0_batch& mc) override;

    future<std::vector<cql3::description>> describe_role_grants() override;
  private:
    /// Connects to the LDAP server indicated by _query_template and executes LDAP bind using _bind_name and
    /// _bind_password.  Returns the resulting ldap_connection.
    future<lw_shared_ptr<ldap_connection>> connect();

    /// Invokes connect() repeatedly with backoff, until it succeeds or retry limit is reached.
    future<seastar::lw_shared_ptr<ldap_connection>> reconnect();

    /// Macro-expands _query_template, returning the result.
    sstring get_url(std::string_view user) const;

    /// Used to auto-create roles returned by ldap.
    future<> create_role(std::string_view role_name);

    future<> ensure_superuser_is_created() override;
};

} // namespace auth
