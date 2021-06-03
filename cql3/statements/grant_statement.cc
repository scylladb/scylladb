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

#include "grant_statement.hh"
#include "auth/authorizer.hh"

std::unique_ptr<cql3::statements::prepared_statement> cql3::statements::grant_statement::prepare(
                database& db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(::make_shared<grant_statement>(*this));
}

future<::shared_ptr<cql_transport::messages::result_message>>
cql3::statements::grant_statement::execute(query_processor&, service::query_state& state, const query_options& options) const {
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
