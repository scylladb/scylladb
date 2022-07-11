/*
 * Copyright (C) 2021-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "schema_fwd.hh"
#include "sstables/sstable_set.hh"

class reader_permit;

namespace sstables {
class compaction_strategy;
struct sstable_writer_config;
}

namespace compaction {

class table_state {
public:
    virtual ~table_state() {}
    virtual const schema_ptr& schema() const noexcept = 0;
    // min threshold as defined by table.
    virtual unsigned min_compaction_threshold() const noexcept = 0;
    virtual bool compaction_enforce_min_threshold() const noexcept = 0;
    virtual const sstables::sstable_set& main_sstable_set() const = 0;
    virtual const sstables::sstable_set& maintenance_sstable_set() const = 0;
    virtual std::unordered_set<sstables::shared_sstable> fully_expired_sstables(const std::vector<sstables::shared_sstable>& sstables, gc_clock::time_point compaction_time) const = 0;
    virtual const std::vector<sstables::shared_sstable>& compacted_undeleted_sstables() const noexcept = 0;
    virtual sstables::compaction_strategy& get_compaction_strategy() const noexcept = 0;
    virtual reader_permit make_compaction_reader_permit() const = 0;
    virtual sstables::sstable_writer_config configure_writer(sstring origin) const = 0;
    virtual api::timestamp_type min_memtable_timestamp() const = 0;
    virtual future<> update_compaction_history(utils::UUID compaction_id, sstring ks_name, sstring cf_name, std::chrono::milliseconds ended_at, int64_t bytes_in, int64_t bytes_out) = 0;
};

}

