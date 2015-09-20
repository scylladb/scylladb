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

namespace service {

static logging::logger logger("Migration Task");

future<> migration_task::run_may_throw(distributed<service::storage_proxy>& proxy, const gms::inet_address& endpoint)
{
    if (!gms::get_failure_detector().local().is_alive(endpoint)) {
        logger.error("Can't send migration request: node {} is down.", endpoint);
        return make_ready_future<>();
    }
    net::messaging_service::shard_id id{endpoint, 0};
    auto& ms = net::get_local_messaging_service();
    return ms.send_migration_request(std::move(id), endpoint, engine().cpu_id()).then([&proxy](const std::vector<frozen_mutation>& mutations) {
        try {
            std::vector<mutation> schema;
            for (auto& m : mutations) {
                schema_ptr s = proxy.local().get_db().local().find_schema(m.column_family_id());
                schema.emplace_back(m.unfreeze(s));
            }
            return db::schema_tables::merge_schema(proxy, std::move(schema));
        } catch (const exceptions::configuration_exception& e) {
            logger.error("Configuration exception merging remote schema: {}", e.what());
        }
        return make_ready_future<>();
    });
}

}
