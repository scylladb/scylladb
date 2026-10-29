/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstddef>
#include <seastar/core/scheduling.hh>

namespace replica {

struct database_config {
    seastar::scheduling_group memtable_scheduling_group;
    seastar::scheduling_group memtable_to_cache_scheduling_group; // FIXME: merge with memtable_scheduling_group
    seastar::scheduling_group compaction_scheduling_group;
    seastar::scheduling_group maintenance_compaction_scheduling_group;
    seastar::scheduling_group memory_compaction_scheduling_group;
    seastar::scheduling_group logstor_compaction_scheduling_group;
    seastar::scheduling_group statement_scheduling_group;
    seastar::scheduling_group streaming_scheduling_group;
    seastar::scheduling_group maintenance_scheduling_group;
    seastar::scheduling_group gossip_scheduling_group;
    seastar::scheduling_group commitlog_scheduling_group;
    seastar::scheduling_group schema_commitlog_scheduling_group;
    seastar::scheduling_group backup_scheduling_group;
    // Shared by the Alternator export to S3 and import from S3 jobs.
    seastar::scheduling_group export_import_scheduling_group;
    size_t available_memory;
};

} // namespace replica
