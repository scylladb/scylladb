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
        reader_concurrency_semaphore semaphore;
        replica::cf_stats cf_stats{0};
        replica::column_family::config cfg;
        cell_locker_stats cl_stats;
        tasks::task_manager tm;
        compaction_manager cm{tm, compaction_manager::for_testing_tag{}};
        lw_shared_ptr<replica::column_family> cf;
        std::unique_ptr<table_state> table_s;
        data_dictionary::storage_options storage;
        data();
        ~data();
    };
    lw_shared_ptr<data> _data;

    static schema_ptr make_default_schema();

    explicit table_for_tests(sstables::sstables_manager& sstables_manager, schema_ptr s, std::optional<sstring> datadir = {}, data_dictionary::storage_options storage = {});

    schema_ptr schema() { return _data->s; }

    const replica::cf_stats& cf_stats() const noexcept { return _data->cf_stats; }

    operator lw_shared_ptr<replica::column_family>() { return _data->cf; }

    replica::column_family& operator*() { return *_data->cf; }
    replica::column_family* operator->() { return _data->cf.get(); }

    compaction_manager& get_compaction_manager() noexcept { return _data->cm; }

    compaction::table_state& as_table_state() noexcept;

    future<> stop();

    sstables::shared_sstable make_sstable() {
        auto& table = *_data->cf;
        auto& sstables_manager = table.get_sstables_manager();
        return sstables_manager.make_sstable(_data->s, _data->cfg.datadir, _data->storage, table.calculate_generation_for_new_table());
    }

    sstables::shared_sstable make_sstable(sstables::sstable_version_types version) {
        auto& table = *_data->cf;
        auto& sstables_manager = table.get_sstables_manager();
        return sstables_manager.make_sstable(_data->s, _data->cfg.datadir, _data->storage, table.calculate_generation_for_new_table(), sstables::sstable_state::normal, version);
    }

    std::function<sstables::shared_sstable()> make_sst_factory() {
        return [this] {
            return make_sstable();
        };
    }

    std::function<sstables::shared_sstable()> make_sst_factory(sstables::sstable_version_types version) {
        return [this, version] {
            return make_sstable(version);
        };
    }

    void set_tombstone_gc_enabled(bool tombstone_gc_enabled) noexcept;
};
