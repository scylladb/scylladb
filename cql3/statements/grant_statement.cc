/*
 * Copyright 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "grant_statement.hh"
#include "auth/authorizer.hh"
#include "cql3/statements/prepared_statement.hh"
#include "service/query_state.hh"

std::unique_ptr<cql3::statements::prepared_statement> cql3::statements::grant_statement::prepare(
                data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(::make_shared<grant_statement>(*this));
}

future<::shared_ptr<cql_transport::messages::result_message>>
cql3::statements::grant_statement::execute(query_processor&, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const {
    auto& auth_service = *state.get_client_state().get_auth_service();

    return auth::grant_permissions(auth_service, _role_name, _permissions, _resource).then([] {
        return make_ready_future<::shared_ptr<cql_transport::messages::result_message>>();
    }).handle_exception_type([](const auth::nonexistant_role& e) {
        return make_exception_future<::shared_ptr<cql_transport::messages::result_message>>(
                exceptions::invalid_request_exception(e.what()));
    }).handle_exception_type([](const auth::unsupported_authorization_operation& e) {
        return make_exception_future<::shared_ptr<cql_transport::messages::result_message>>(
                exceptions::invalid_request_exception(e.what()));
    });
}
