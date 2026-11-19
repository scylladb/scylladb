/*
 * Copyright 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#include "revoke_statement.hh"
#include "auth/authorizer.hh"
#include "cql3/statements/prepared_statement.hh"
#include "service/query_state.hh"
#include "service/raft/raft_group0_client.hh"

std::unique_ptr<cql3::statements::prepared_statement> cql3::statements::revoke_statement::prepare(
                data_dictionary::database db, cql_stats& stats, const cql_config& cfg) {
    return std::make_unique<prepared_statement>(audit_info(), ::make_shared<revoke_statement>(*this));
}

future<::shared_ptr<cql_transport::messages::result_message>>
cql3::statements::revoke_statement::execute(query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const {
    auto& auth_service = *state.get_client_state().get_auth_service();
    try {
        service::group0_batch mc{std::move(guard)};
        co_await for_each_resource([&] (const auth::resource& r) {
            return auth::revoke_permissions(auth_service, _role_name, _permissions, r, mc);
        });
        const auto configure = auth::permission_set::of<auth::permission::CONFIGURE>();
        // GRANT ALL does not grant CONFIGURE, but REVOKE ALL leaves nothing behind when the
        // revoking role holds CONFIGURE itself. A role without it (the creator of a table,
        // for one) cannot take away what an operator was granted there.
        const bool all_with_configure = _all_permissions && _resource.applicable_permissions().contains(auth::permission::CONFIGURE)
                && co_await state.get_client_state().check_has_config_permission(auth::permission::CONFIGURE, _resource, std::nullopt);
        if (all_with_configure) {
            co_await auth::revoke_permissions(auth_service, _role_name, configure, _resource, mc);
        }
        if (all_with_configure || (_config_options.empty() && _permissions.contains(auth::permission::CONFIGURE))) {
            // Revoking CONFIGURE on a resource leaves no per-option grant on it behind.
            co_await auth::revoke_config_option_permissions(auth_service, _role_name, _resource, mc);
        }
        co_await auth::commit_mutations(auth_service, std::move(mc));
    } catch (const auth::nonexistant_role& e) {
        throw exceptions::invalid_request_exception(e.what());
    } catch (const auth::unsupported_authorization_operation& e) {
        throw exceptions::invalid_request_exception(e.what());
    }

    co_return ::shared_ptr<cql_transport::messages::result_message>();
}
