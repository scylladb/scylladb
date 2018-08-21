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
 * Copyright (C) 2015 ScyllaDB
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

#include "service/migration_task.hh"

#include "message/messaging_service.hh"
#include "gms/failure_detector.hh"
#include "db/schema_tables.hh"
#include "frozen_mutation.hh"
#include "migration_manager.hh"

namespace service {

static logging::logger mlogger("migration_task");

future<> migration_task::run_may_throw(distributed<service::storage_proxy>& proxy, const gms::inet_address& endpoint)
{
    if (!gms::get_failure_detector().local().is_alive(endpoint)) {
        mlogger.warn("Can't send migration request: node {} is down.", endpoint);
        return make_ready_future<>();
    }
    netw::messaging_service::msg_addr id{endpoint, 0};
    return service::get_local_migration_manager().merge_schema_from(id).handle_exception([](std::exception_ptr e) {
        try {
            std::rethrow_exception(e);
        } catch (const exceptions::configuration_exception& e) {
            mlogger.error("Configuration exception merging remote schema: {}", e.what());
        }
    });
}

}
