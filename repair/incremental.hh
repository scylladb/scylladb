/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "sstables/sstables.hh"
#include "sstables/sstable_set.hh"
#include <optional>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/log-level.hh>
#include "sstables/shared_sstable.hh"
#include "utils/log.hh"

extern logging::logger rlogger;

struct incremental_repair_meta {
    lw_shared_ptr<sstables::sstable_set> sst_set;
    int64_t sstables_repaired_at = 0;
};

namespace repair {

// Whether incremental repair has covered the sstable: its repaired_at is set and is no later
// than the tablet's sstables_repaired_at, which the last repair of every replica advanced to.
//
// Each sstable looked at is logged at log_level, which by default suits a repair, looking at
// the sstables of one tablet once. A caller that walks every sstable of every tablet
// periodically passes nullopt, or it would bury the repair logs under its own lines.
inline bool is_repaired(int64_t sstables_repaired_at, const sstables::shared_sstable& sst,
        std::optional<log_level> level = log_level::debug) {
    auto& stats = sst->get_stats_metadata();
    bool repaired = stats.repaired_at != 0 && stats.repaired_at <= sstables_repaired_at;
    if (level) {
        rlogger.log(*level, "Checking sst={} repaired_at={} sstables_repaired_at={} repaired={}",
                sst->toc_filename(), stats.repaired_at, sstables_repaired_at, repaired);
    }
    return repaired;
}

}
