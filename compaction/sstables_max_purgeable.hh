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

class table_state;

class sstables_max_purgeable {
    const compaction_group_view& _table_s;
    std::optional<sstables::sstable_set::incremental_selector> _selector;

public:
    sstables_max_purgeable(const compaction_group_view& table_s, const std::optional<sstables::sstable_set>& sstable_set_snapshot_override);

    void reset_selector(const sstables::sstable_set& sst_set);

    struct result {
        api::timestamp_type timestamp = api::max_timestamp;
        uint64_t bloom_filter_checks = 0;
    };

    result operator()(const dht::decorated_key& dk, const is_shadowable is_shadowable,
            const std::unordered_set<sstables::shared_sstable>& compacting_set, api::timestamp_type timestamp);

    max_purgeable operator()(const dht::decorated_key& dk, const is_shadowable is_shadowable);
};

} // namespace compaction
