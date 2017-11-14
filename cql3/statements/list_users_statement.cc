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
 * Copyright 2016 ScyllaDB
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

void cql3::statements::list_users_statement::validate(distributed<service::storage_proxy>& proxy, const service::client_state& state) {
}

future<> cql3::statements::list_users_statement::check_access(const service::client_state& state) {
    state.ensure_not_anonymous();
    return make_ready_future();
}

future<::shared_ptr<cql_transport::messages::result_message>>
cql3::statements::list_users_statement::execute(distributed<service::storage_proxy>& proxy, service::query_state& state, const query_options& options) {
    auto is = std::make_unique<service::query_state>(service::client_state::for_internal_calls());
    auto io = std::make_unique<query_options>(db::consistency_level::QUORUM, std::vector<cql3::raw_value>{});
    auto f = get_local_query_processor().process(
                    sprint("SELECT * FROM %s.%s", auth::meta::AUTH_KS,
                                    auth::meta::USERS_CF), *is, *io);
    return f.finally([is = std::move(is), io = std::move(io)] {});
}
