/*
 * Copyright 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "authorization_statement.hh"
#include "transport/messages/result_message.hh"
#include "service/client_state.hh"
#include "auth/resource.hh"

uint32_t cql3::statements::authorization_statement::get_bound_terms() const {
    return 0;
}

bool cql3::statements::authorization_statement::depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const {
    return false;
}

void cql3::statements::authorization_statement::validate(
                query_processor&,
                const service::client_state& state) const {
}

future<> cql3::statements::authorization_statement::check_access(query_processor& qp, const service::client_state& state) const {
    return make_ready_future<>();
}

void cql3::statements::authorization_statement::maybe_correct_resource(auth::resource& resource, const service::client_state& state){
    if (resource.kind() == auth::resource_kind::data) {
        const auto data_view = auth::data_resource_view(resource);
        const auto keyspace = data_view.keyspace();
        const auto table = data_view.table();

        if (table && keyspace->empty()) {
            resource = auth::make_data_resource(state.get_keyspace(), *table);
        }
    }
}

