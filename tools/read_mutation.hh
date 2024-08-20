/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <filesystem>
#include <functional>
#include <string_view>
#include <optional>

#include "db/cache_tracker.hh"
#include "db/config.hh"
#include "db/large_data_handler.hh"
#include "gms/feature_service.hh"
#include "schema/schema_fwd.hh"
#include "sstables/sstable_directory.hh"
#include "sstables/sstables_manager.hh"
#include "types/types.hh"
#include "reader_permit.hh"

future<std::filesystem::path> get_table_directory(std::filesystem::path scylla_data_path,
                                                  std::string_view keyspace_name,
                                                  std::string_view table_name);

struct sstable_manager_service {
    db::nop_large_data_handler large_data_handler;
    gms::feature_service feature_service;
    cache_tracker tracker;
    sstables::directory_semaphore dir_sem;
    sstables::sstables_manager sst_man;
    abort_source abort;

    explicit sstable_manager_service(const db::config& dbcfg)
        : feature_service(gms::feature_config_from_db_config(dbcfg))
        , dir_sem(1)
        , sst_man("schema_loader", large_data_handler, dbcfg, feature_service, tracker, memory::stats().total_memory(), dir_sem, []{ return locator::host_id{}; }, abort) {
    }

    future<> stop() {
        return sst_man.close();
    }
};

mutation_opt read_mutation_from_table_offline(sharded<sstable_manager_service>& sst_man,
                                              reader_permit permit,
                                              std::filesystem::path table_path,
                                              std::string_view keyspace,
                                              std::function<schema_ptr()> table_schema,
                                              data_value primary_key,
                                              std::optional<data_value> clustering_key);
