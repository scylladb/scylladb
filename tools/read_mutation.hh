/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <filesystem>
#include <functional>
#include <string_view>
#include <optional>

#include "init.hh"
#include "db/cache_tracker.hh"
#include "db/config.hh"
#include "db/large_data_handler.hh"
#include "db/corrupt_data_handler.hh"
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
    db::nop_corrupt_data_handler corrupt_data_handler;
    std::unique_ptr<gms::feature_service> feature_service_impl;
    gms::feature_service& feature_service = *feature_service_impl;
    cache_tracker tracker;
    sstables::directory_semaphore dir_sem;
    sstables::sstables_manager sst_man;
    abort_source abort;

    explicit sstable_manager_service(const db::config& dbcfg, sstable_compressor_factory& scf);
    ~sstable_manager_service();

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
