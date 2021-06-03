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
 * Copyright 2016-present ScyllaDB
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

#include "list_users_statement.hh"
#include "cql3/query_processor.hh"
#include "cql3/query_options.hh"
#include "auth/common.hh"
#include "transport/messages/result_message.hh"

std::unique_ptr<cql3::statements::prepared_statement> cql3::statements::list_users_statement::prepare(
                database& db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(::make_shared<list_users_statement>(*this));
}

void cql3::statements::list_users_statement::validate(service::storage_proxy& proxy, const service::client_state& state) const {
}

future<> cql3::statements::list_users_statement::check_access(service::storage_proxy& proxy, const service::client_state& state) const {
    state.ensure_not_anonymous();
    return make_ready_future();
}

future<::shared_ptr<cql_transport::messages::result_message>>
cql3::statements::list_users_statement::execute(query_processor& qp, service::query_state& state, const query_options& options) const {
    static const sstring virtual_table_name("users");

    static const auto make_column_spec = [](const sstring& name, const ::shared_ptr<const abstract_type>& ty) {
        return make_lw_shared<column_specification>(
            auth::meta::AUTH_KS,
            virtual_table_name,
            ::make_shared<column_identifier>(name, true),
            ty);
    };

    static thread_local const auto metadata = ::make_shared<cql3::metadata>(
        std::vector<lw_shared_ptr<column_specification>>{
                make_column_spec("name", utf8_type),
                make_column_spec("super", boolean_type)});

    static const auto make_results = [](const auth::service& as, std::unordered_set<sstring>&& roles) {
        using cql_transport::messages::result_message;

        auto results = std::make_unique<result_set>(metadata);

        std::vector<sstring> sorted_roles(roles.cbegin(), roles.cend());
        std::sort(sorted_roles.begin(), sorted_roles.end());

        return do_with(
                std::move(sorted_roles),
                std::move(results),
                [&as](const std::vector<sstring>& sorted_roles, std::unique_ptr<result_set>& results) {
            return do_for_each(sorted_roles, [&as, &results](const sstring& role) {
                return when_all_succeed(
                        as.has_superuser(role),
                        as.underlying_role_manager().can_login(role)).then_unpack([&results, &role](bool super, bool login) {
                    if (login) {
                        results->add_column_value(utf8_type->decompose(role));
                        results->add_column_value(boolean_type->decompose(super));
                    }
                });
            }).then([&results] {
                return make_ready_future<::shared_ptr<result_message>>(::make_shared<result_message::rows>(
                        result(std::move(results))));
            });
        });
    };

    const auto& cs = state.get_client_state();
    const auto& as = *cs.get_auth_service();

    return auth::has_superuser(as, *cs.user()).then([&cs, &as](bool has_superuser) {
        if (has_superuser) {
            return as.underlying_role_manager().query_all().then([&as](std::unordered_set<sstring> roles) {
                return make_results(as, std::move(roles));
            });
        }

        return auth::get_roles(as, *cs.user()).then([&as](std::unordered_set<sstring> granted_roles) {
            return make_results(as, std::move(granted_roles));
        });
    });
}
