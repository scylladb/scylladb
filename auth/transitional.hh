/*
 * Copyright (C) 2026-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "auth/authenticator.hh"
#include "auth/authorizer.hh"
#include "auth/cache.hh"

namespace cql3 {
class query_processor;
}

namespace service {
class raft_group0_client;
class migration_manager;
}

namespace auth {

///
/// Transitional authenticator that allows anonymous access when credentials are not provided
/// or authentication fails. Used for migration scenarios.
///
class transitional_authenticator : public authenticator {
    std::unique_ptr<authenticator> _authenticator;

public:
    transitional_authenticator(cql3::query_processor& qp, ::service::raft_group0_client& g0, ::service::migration_manager& mm, cache& cache);
    transitional_authenticator(std::unique_ptr<authenticator> a);

    virtual future<> start() override;
    virtual future<> stop() override;
    virtual std::string_view qualified_java_name() const override;
    virtual bool require_authentication() const override;
    virtual authentication_option_set supported_options() const override;
    virtual authentication_option_set alterable_options() const override;
    virtual future<authenticated_user> authenticate(const credentials_map& credentials) const override;
    virtual future<> create(std::string_view role_name, const authentication_options& options, ::service::group0_batch& mc) override;
    virtual future<> alter(std::string_view role_name, const authentication_options& options, ::service::group0_batch& mc) override;
    virtual future<> drop(std::string_view role_name, ::service::group0_batch& mc) override;
    virtual future<custom_options> query_custom_options(std::string_view role_name) const override;
    virtual bool uses_password_hashes() const override;
    virtual future<std::optional<sstring>> get_password_hash(std::string_view role_name) const override;
    virtual const resource_set& protected_resources() const override;
    virtual ::shared_ptr<sasl_challenge> new_sasl_challenge() const override;
    virtual future<> ensure_superuser_is_created() const override;
};

///
/// Transitional authorizer that grants a fixed set of permissions to all users.
/// Used for migration scenarios.
///
class transitional_authorizer : public authorizer {
    std::unique_ptr<authorizer> _authorizer;

public:
    transitional_authorizer(cql3::query_processor& qp, ::service::raft_group0_client& g0, ::service::migration_manager& mm);
    transitional_authorizer(std::unique_ptr<authorizer> a);
    ~transitional_authorizer();

    virtual future<> start() override;
    virtual future<> stop() override;
    virtual std::string_view qualified_java_name() const override;
    virtual future<permission_set> authorize(const role_or_anonymous&, const resource&) const override;
    virtual future<> grant(std::string_view s, permission_set ps, const resource& r, ::service::group0_batch& mc) override;
    virtual future<> revoke(std::string_view s, permission_set ps, const resource& r, ::service::group0_batch& mc) override;
    virtual future<std::vector<permission_details>> list_all() const override;
    virtual future<> revoke_all(std::string_view s, ::service::group0_batch& mc) override;
    virtual future<> revoke_all(const resource& r, ::service::group0_batch& mc) override;
    virtual const resource_set& protected_resources() const override;
};

} // namespace auth
