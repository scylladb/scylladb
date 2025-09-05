/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "timestamp.hh"
#include "sstables/sstable_set.hh"
#include "compaction/compaction_group_view.hh"
#include "compaction/compaction_garbage_collector.hh"

namespace compaction {

class sstables_max_purgeable {
    const compaction_group_view& _table_s;
    lw_shared_ptr<const sstables::sstable_set> _sstable_set;
    std::optional<sstables::sstable_set::incremental_selector> _selector;

public:
    // sstable_set_snapshot_override can be null, in which case table_s.sstable_set_for_tombstone_gc() is used. 
    explicit sstables_max_purgeable(const compaction_group_view& table_s, lw_shared_ptr<const sstables::sstable_set> sstable_set_snapshot_override = nullptr);

    void reset_selector(lw_shared_ptr<const sstables::sstable_set> sst_set);

    struct result {
        api::timestamp_type timestamp = api::max_timestamp;
        uint64_t bloom_filter_checks = 0;
    };

    result operator()(const dht::decorated_key& dk, const is_shadowable is_shadowable,
            const std::unordered_set<sstables::shared_sstable>& compacting_set, api::timestamp_type timestamp);

    max_purgeable operator()(const dht::decorated_key& dk, const is_shadowable is_shadowable);
};

} // namespace compaction
