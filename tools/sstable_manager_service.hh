/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "db/large_data_handler.hh"
#include "db/cache_tracker.hh"
#include "db/config.hh"
#include "gms/feature_service.hh"
#include "sstables/sstable_directory.hh"
#include "sstables/sstables_manager.hh"

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
