/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "repair/incremental.hh"
#include "utils/log.hh"
#include <algorithm>
#include <ranges>

extern logging::logger rlogger;

namespace repair {

bool is_repaired(int64_t sstables_repaired_at, int64_t sstable_stats_repaired_at) {
    return sstable_stats_repaired_at !=0 && sstable_stats_repaired_at <= sstables_repaired_at;
}

bool is_repaired(int64_t sstables_repaired_at, const sstables::shared_sstable& sst) {
    auto& stats = sst->get_stats_metadata();
    bool repaired = is_repaired(sstables_repaired_at, stats.repaired_at);
    rlogger.debug("Checking sst={} repaired_at={} sstables_repaired_at={} repaired={}",
            sst->toc_filename(), stats.repaired_at, sstables_repaired_at, repaired);
    return repaired;
}

}
