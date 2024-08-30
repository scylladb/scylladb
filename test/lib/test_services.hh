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

#include "schema/schema.hh"
#include "schema/schema_builder.hh"
#include "row_cache.hh"
#include "replica/database.hh"
#include "cell_locking.hh"
#include "compaction/compaction_manager.hh"
#include "compaction/table_state.hh"
#include "sstables/sstables_manager.hh"

struct table_for_tests {
    class table_state;
    struct data {
        schema_ptr s;
        replica::cf_stats cf_stats{0};
        cell_locker_stats cl_stats;
        lw_shared_ptr<replica::column_family> cf;
        std::unique_ptr<table_state> table_s;
        data_dictionary::storage_options storage;
        data();
        ~data();
    };
    lw_shared_ptr<data> _data;

    static schema_ptr make_default_schema();

    explicit table_for_tests(sstables::sstables_manager& sstables_manager, compaction_manager& cm, schema_ptr s, replica::table::config cfg, data_dictionary::storage_options storage = {});

    schema_ptr schema() { return _data->s; }

    const replica::cf_stats& cf_stats() const noexcept { return _data->cf_stats; }

    operator lw_shared_ptr<replica::column_family>() { return _data->cf; }

    replica::column_family& operator*() { return *_data->cf; }
    replica::column_family* operator->() { return _data->cf.get(); }
    const replica::column_family* operator->() const { return _data->cf.get(); }

    compaction::table_state& as_table_state() noexcept;

    future<> stop();

    void set_tombstone_gc_enabled(bool tombstone_gc_enabled) noexcept;
};
