/*
 * Copyright 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "authorization_statement.hh"
#include "service/client_state.hh"
#include "auth/resource.hh"
#include "cql3/query_processor.hh"
#include "exceptions/exceptions.hh"
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include "db/cql_type_parser.hh"
#include "auth/common.hh"

uint32_t cql3::statements::authorization_statement::get_bound_terms() const {
    return 0;
}

bool cql3::statements::authorization_statement::depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const {
    return false;
}

future<> cql3::statements::authorization_statement::check_access(query_processor& qp, const service::client_state& state) const {
    return make_ready_future<>();
}

void cql3::statements::authorization_statement::maybe_correct_resource(auth::resource& resource, const service::client_state& state, query_processor& qp) {
    if (resource.kind() == auth::resource_kind::data) {
        const auto data_view = auth::data_resource_view(resource);
        const auto keyspace = data_view.keyspace();
        const auto table = data_view.table();

        if (table && keyspace->empty()) {
            resource = auth::make_data_resource(state.get_keyspace(), *table);
        }
    } else if (resource.kind() == auth::resource_kind::functions) {
        // Maybe correct the resource for a specific function.
        const auto functions_view = auth::functions_resource_view(resource);
        const auto keyspace = functions_view.keyspace();
        if (!keyspace) {
            // This is an "ALL FUNCTIONS" resource.
            return;
        }
        if (!qp.db().has_keyspace(*keyspace)) {
            throw exceptions::invalid_request_exception(format("{} doesn't exist.", resource));
        }
        if (functions_view.function_signature()) {
            // The resource is already corrected.
            return;
        }
        if (!functions_view.function_name()) {
            // This is an "ALL FUNCTIONS IN KEYSPACE" resource.
            return;
        }
        auto ks = qp.db().find_keyspace(*keyspace);
        const auto& utm = ks.user_types();
        auto function_name = *functions_view.function_name();
        auto function_args = functions_view.function_args();
        std::vector<data_type> parsed_types;
        if (function_args) {
            parsed_types = boost::copy_range<std::vector<data_type>>(
                *function_args | boost::adaptors::transformed([&] (std::string_view raw_type) {
                    auto parsed = db::cql_type_parser::parse(sstring(keyspace->data(), keyspace->size()), sstring(raw_type.data(), raw_type.size()), utm);
                    return parsed->is_user_type() ? parsed->freeze() : parsed;
                })
            );
        }
        resource = auth::make_functions_resource(*keyspace, auth::encode_signature(function_name, parsed_types));
    }
}

bool cql3::statements::authorization_altering_statement::needs_guard(
                query_processor& qp, service::query_state&) const {
    return !auth::legacy_mode(qp);
};
