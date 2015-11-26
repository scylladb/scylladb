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
#include "locator/abstract_replication_strategy.hh"
#include "database.hh"
#include <seastar/core/sharded.hh>

namespace service {

class pending_range_calculator_service {
private:
    int _update_jobs{0};
    distributed<database>& _db;
    void calculate_pending_ranges(locator::abstract_replication_strategy& strategy, const sstring& keyspace_name);
    void run();
public:
    pending_range_calculator_service(distributed<database>& db) : _db(db) {}
    void do_update();
    future<> update();
    future<> block_until_finished();
    future<> stop();
};

extern distributed<pending_range_calculator_service> _the_pending_range_calculator_service;

inline distributed<pending_range_calculator_service>& get_pending_range_calculator_service() {
    return _the_pending_range_calculator_service;
}

inline pending_range_calculator_service& get_local_pending_range_calculator_service() {
    return _the_pending_range_calculator_service.local();
}

} // service
