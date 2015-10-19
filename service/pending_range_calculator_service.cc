/**
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
 * Modified by ScyllaDB
 * Copyright 2015 ScyllaDB
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

#include "service/pending_range_calculator_service.hh"
#include "service/storage_service.hh"
#include "database.hh"
#include <seastar/core/sleep.hh>

namespace service {

distributed<pending_range_calculator_service> _the_pending_range_calculator_service;

void pending_range_calculator_service::run() {
    // long start = System.currentTimeMillis();
    auto keyspaces = _db.local().get_non_system_keyspaces();
    for (auto& keyspace_name : keyspaces) {
        auto& ks = _db.local().find_keyspace(keyspace_name);
        calculate_pending_ranges(ks.get_replication_strategy(), keyspace_name);
    }
    _update_jobs--;
    // logger.debug("finished calculation for {} keyspaces in {}ms", keyspaces.size(), System.currentTimeMillis() - start);
}

void pending_range_calculator_service::calculate_pending_ranges(locator::abstract_replication_strategy& strategy, const sstring& keyspace_name) {
    get_local_storage_service().get_token_metadata().calculate_pending_ranges(strategy, keyspace_name);
}

future<> pending_range_calculator_service::stop() {
    return make_ready_future<>();
}

future<> pending_range_calculator_service::update() {
    return smp::submit_to(0, [] {
        get_local_pending_range_calculator_service()._update_jobs++;
        get_local_pending_range_calculator_service().run();
    });
}

future<> pending_range_calculator_service::block_until_finished() {
    // We want to be sure the job we're blocking for is actually finished and we can't trust the TPE's active job count
    return smp::submit_to(0, [] {
        return do_until(
            [] { return !(get_local_pending_range_calculator_service()._update_jobs > 0); },
            [] { return sleep(std::chrono::milliseconds(100)); });
    });
}

}
