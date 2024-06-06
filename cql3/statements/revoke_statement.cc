/*
 * Copyright 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "revoke_statement.hh"
#include "auth/authorizer.hh"
#include "cql3/statements/prepared_statement.hh"
#include "service/query_state.hh"

std::unique_ptr<cql3::statements::prepared_statement> cql3::statements::revoke_statement::prepare(
                data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(::make_shared<revoke_statement>(*this));
}

future<::shared_ptr<cql_transport::messages::result_message>>
cql3::statements::revoke_statement::execute(query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const {
    auto& auth_service = *state.get_client_state().get_auth_service();
    try {
        service::group0_batch mc{std::move(guard)};
        co_await auth::revoke_permissions(auth_service, _role_name, _permissions, _resource, mc);
        co_await auth::commit_mutations(auth_service, std::move(mc));
    } catch (const auth::nonexistant_role& e) {
        throw exceptions::invalid_request_exception(e.what());
    } catch (const auth::unsupported_authorization_operation& e) {
        throw exceptions::invalid_request_exception(e.what());
    }

    co_return ::shared_ptr<cql_transport::messages::result_message>();
}
