/*
 * Copyright (C) 2022-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "auth/authenticator.hh"

namespace cql3 {

class query_processor;

} // namespace cql3

namespace service {
class migration_manager;
class raft_group0_client;
}

namespace auth {

extern const std::string_view certificate_authenticator_name;

class certificate_authenticator : public authenticator {
    enum class query_source;
    std::vector<std::pair<query_source, boost::regex>> _queries;
public:
    certificate_authenticator(cql3::query_processor&, ::service::raft_group0_client&, ::service::migration_manager&);
    ~certificate_authenticator();

    future<> start() override;
    future<> stop() override;

    std::string_view qualified_java_name() const override;

    bool require_authentication() const override;

    authentication_option_set supported_options() const override;
    authentication_option_set alterable_options() const override;

    future<authenticated_user> authenticate(const credentials_map& credentials) const override;
    future<std::optional<authenticated_user>> authenticate(session_dn_func) const override;

    future<> create(std::string_view role_name, const authentication_options& options, ::service::group0_batch& mc) override;
    future<> alter(std::string_view role_name, const authentication_options& options, ::service::group0_batch&) override;
    future<> drop(std::string_view role_name, ::service::group0_batch&) override;

    future<custom_options> query_custom_options(std::string_view role_name) const override;

    const resource_set& protected_resources() const override;

    ::shared_ptr<sasl_challenge> new_sasl_challenge() const override;
private:
};

}

