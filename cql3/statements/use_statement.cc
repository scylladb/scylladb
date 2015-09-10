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
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#include "cql3/statements/use_statement.hh"

#include "transport/messages/result_message.hh"

namespace cql3 {

namespace statements {

future<::shared_ptr<transport::messages::result_message>>
use_statement::execute(distributed<service::storage_proxy>& proxy, service::query_state& state, const query_options& options) {
    state.get_client_state().set_keyspace(proxy.local().get_db(), _keyspace);
    auto result =::make_shared<transport::messages::result_message::set_keyspace>(_keyspace);
    return make_ready_future<::shared_ptr<transport::messages::result_message>>(result);
}

future<::shared_ptr<transport::messages::result_message>>
use_statement::execute_internal(distributed<service::storage_proxy>& proxy, service::query_state& state, const query_options& options) {
    // Internal queries are exclusively on the system keyspace and 'use' is thus useless
    throw std::runtime_error("unsupported operation");
}

}

}
