/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright 2017 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <algorithm>

#include "auth/common.hh"
#include "auth/role_manager.hh"
#include "cql3/column_specification.hh"
#include "cql3/query_processor.hh"
#include "cql3/statements/list_roles_statement.hh"
#include "cql3/statements/request_validations.hh"
#include "exceptions/exceptions.hh"
#include "transport/messages/result_message.hh"
#include "unimplemented.hh"

namespace cql3 {

namespace statements {

static future<::shared_ptr<cql_transport::messages::result_message>> void_result_message() {
    return make_ready_future<::shared_ptr<cql_transport::messages::result_message>>(nullptr);
}

//
// `list_roles_statement`
//

future<> list_roles_statement::check_access(const service::client_state& state) {
    state.ensure_not_anonymous();

    return async([this, &state] {
        const auto user_has_grantee = [this, &state] {
            auto& rm = state.get_auth_service()->underlying_role_manager();
            const auto roles = rm.query_granted(state.user()->name(), auth::recursive_role_query::yes).get0();
            return roles.count(*_grantee) != 0;
        };

        if (!auth::is_super_user(*state.get_auth_service(), *state.user()).get0() && _grantee && !user_has_grantee()) {
            throw exceptions::unauthorized_exception(
                sprint("You are not authorized to view the roles granted to role '%s'.", *_grantee));
        }
    }).handle_exception_type([](const auth::roles_argument_exception& e) {
        throw exceptions::invalid_request_exception(e.what());
    });
}

future<::shared_ptr<cql_transport::messages::result_message>>
list_roles_statement::execute(distributed<service::storage_proxy>&, service::query_state& state, const query_options&) {
    unimplemented::warn(unimplemented::cause::ROLES);

    static const auth::data_resource virtual_table(auth::meta::AUTH_KS, "role");

    static const auto make_column_spec = [](const sstring& name, const ::shared_ptr<const abstract_type>& ty) {
        return ::make_shared<column_specification>(
                virtual_table.keyspace(),
                virtual_table.column_family(),
                ::make_shared<column_identifier>(name, true),
                ty);
    };

    static const auto metadata = ::make_shared<cql3::metadata>(
            std::vector<::shared_ptr<column_specification>>{
                    make_column_spec("role", utf8_type),
                    make_column_spec("super", boolean_type),
                    make_column_spec("login", boolean_type)});

    static const auto make_results = [](auth::role_manager& rm, std::unordered_set<sstring>&& roles)
            -> future<::shared_ptr<cql_transport::messages::result_message>> {
        using cql_transport::messages::result_message;

        auto results = std::make_unique<result_set>(metadata);

        if (roles.empty()) {
            return make_ready_future<::shared_ptr<result_message>>(
                ::make_shared<result_message::rows>(std::move(results)));
        }

        std::vector<sstring> sorted_roles(roles.cbegin(), roles.cend());
        std::sort(sorted_roles.begin(), sorted_roles.end());

        return do_with(
                std::move(sorted_roles),
                std::move(results),
                [&rm](const std::vector<sstring>& sorted_roles, std::unique_ptr<result_set>& results) {
            return do_for_each(sorted_roles, [&results, &rm](const sstring& role) {
                return when_all_succeed(
                        rm.can_login(role),
                        rm.is_superuser(role)).then([&results, &role](bool login, bool super) {
                    results->add_column_value(utf8_type->decompose(role));
                    results->add_column_value(boolean_type->decompose(super));
                    results->add_column_value(boolean_type->decompose(login));
                });
            }).then([&results] {
                return make_ready_future<::shared_ptr<result_message>>(
                        ::make_shared<result_message::rows>(std::move(results)));
            });
        });
    };

    auto& cs = state.get_client_state();
    auto& as = *cs.get_auth_service();

    return auth::is_super_user(as, *cs.user()).then([this, &state, &cs, &as](bool super) {
        auto& rm = as.underlying_role_manager();
        const auto query_mode = _recursive ? auth::recursive_role_query::yes : auth::recursive_role_query::no;

        if (!_grantee) {
            if (super) {
                return rm.query_all().then([&rm](auto&& roles) { return make_results(rm, std::move(roles)); });
            }

            return rm.query_granted(cs.user()->name(), query_mode).then([&rm](std::unordered_set<sstring> roles) {
                return make_results(rm, std::move(roles));
            });
        }

        return rm.query_granted(*_grantee, query_mode).then([&rm](std::unordered_set<sstring> roles) {
            return make_results(rm, std::move(roles));
        });
    }).handle_exception_type([](const auth::roles_argument_exception& e) {
        throw exceptions::invalid_request_exception(e.what());
        return void_result_message();
    });
}

}

}
