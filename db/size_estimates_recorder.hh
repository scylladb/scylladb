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

#pragma once

#include <chrono>
#include <seastar/core/timer.hh>
#include "database.hh"
#include "service/migration_manager.hh"

namespace db {

/**
 * A very simplistic/crude partition count/size estimator.
 *
 * Exposing per-primary-range estimated partitions count and size in CQL form,
 * as a direct CQL alternative to Thrift's describe_splits_ex().
 *
 * Estimates (per primary range) are calculated and dumped into a system table (system.size_estimates) every 5 minutes.
 *
 * See CASSANDRA-7688.
 */
class size_estimates_recorder : public service::migration_listener {
    static constexpr std::chrono::minutes RECORD_INTERVAL{5};
    timer<> _timer;
    seastar::gate _gate;

public:
    size_estimates_recorder();

    future<> record_size_estimates();
    void on_drop_column_family(const sstring& ks_name, const sstring& cf_name) override;
    future<> stop();

    void on_create_keyspace(const sstring& ks_name) override { }
    void on_create_column_family(const sstring& ks_name, const sstring& cf_name) override { }
    void on_create_user_type(const sstring& ks_name, const sstring& type_name) override { }
    void on_create_function(const sstring& ks_name, const sstring& function_name) override { }
    void on_create_aggregate(const sstring& ks_name, const sstring& aggregate_name) override { }

    void on_update_keyspace(const sstring& ks_name) override { }
    void on_update_column_family(const sstring& ks_name, const sstring& cf_name, bool) override { }
    void on_update_user_type(const sstring& ks_name, const sstring& type_name) override { }
    void on_update_function(const sstring& ks_name, const sstring& function_name) override { }
    void on_update_aggregate(const sstring& ks_name, const sstring& aggregate_name) override { }

    void on_drop_keyspace(const sstring& ks_name) override { }
    void on_drop_user_type(const sstring& ks_name, const sstring& type_name) override { }
    void on_drop_function(const sstring& ks_name, const sstring& function_name) override { }
    void on_drop_aggregate(const sstring& ks_name, const sstring& aggregate_name) override { }
};

extern distributed<size_estimates_recorder> _recorder;
inline size_estimates_recorder& get_local_size_estimates_recorder() {
    return _recorder.local();
}
inline distributed<size_estimates_recorder>& get_size_estimates_recorder() {
    return _recorder;
}

}
