/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Copyright (C) 2016-present ScyllaDB
 */

#pragma once

#include <memory>
#include <seastar/core/seastar.hh>

#include "schema.hh"
#include "schema_builder.hh"
#include "row_cache.hh"
#include "database.hh"
#include "cell_locking.hh"
#include "compaction/compaction_manager.hh"
#include "db/large_data_handler.hh"
#include "sstables/sstables_manager.hh"

extern db::nop_large_data_handler nop_lp_handler;
extern db::config test_db_config;
extern gms::feature_service test_feature_service;

column_family::config column_family_test_config(sstables::sstables_manager& sstables_manager, reader_concurrency_semaphore& compaction_semaphore);

struct column_family_for_tests {
    struct data {
        schema_ptr s;
        reader_concurrency_semaphore semaphore;
        cache_tracker tracker;
        column_family::config cfg;
        cell_locker_stats cl_stats;
        compaction_manager cm;
        lw_shared_ptr<column_family> cf;

        data();
    };
    lw_shared_ptr<data> _data;

    explicit column_family_for_tests(sstables::sstables_manager& sstables_manager, schema_registry& registry);

    explicit column_family_for_tests(sstables::sstables_manager& sstables_manager, schema_ptr s);

    schema_ptr schema() { return _data->s; }

    operator lw_shared_ptr<column_family>() { return _data->cf; }

    column_family& operator*() { return *_data->cf; }
    column_family* operator->() { return _data->cf.get(); }

    future<> stop() { return _data->semaphore.stop(); }

    future<> stop_and_keep_alive() {
        return stop().finally([cf = *this] {});
    }
};

dht::token create_token_from_key(const dht::i_partitioner&, sstring key);
range<dht::token> create_token_range_from_keys(const dht::sharder& sharder, const dht::i_partitioner&, sstring start_key, sstring end_key);
