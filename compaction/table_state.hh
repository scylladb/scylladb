/*
 * Copyright (C) 2021-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/condition-variable.hh>

#include "schema/schema_fwd.hh"
#include "compaction_descriptor.hh"

class reader_permit;
class compaction_backlog_tracker;

namespace sstables {
class sstable_set;
class compaction_strategy;
class sstables_manager;
struct sstable_writer_config;
}

namespace compaction {
class compaction_strategy_state;
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
    virtual compaction_strategy_state& get_compaction_strategy_state() noexcept = 0;
    virtual reader_permit make_compaction_reader_permit() const = 0;
    virtual sstables::sstables_manager& get_sstables_manager() noexcept = 0;
    virtual sstables::shared_sstable make_sstable() const = 0;
    virtual sstables::sstable_writer_config configure_writer(sstring origin) const = 0;
    virtual api::timestamp_type min_memtable_timestamp() const = 0;
    virtual bool memtable_has_key(const dht::decorated_key& key) const = 0;
    virtual future<> on_compaction_completion(sstables::compaction_completion_desc desc, sstables::offstrategy offstrategy) = 0;
    virtual bool is_auto_compaction_disabled_by_user() const noexcept = 0;
    virtual bool tombstone_gc_enabled() const noexcept = 0;
    virtual const tombstone_gc_state& get_tombstone_gc_state() const noexcept = 0;
    virtual compaction_backlog_tracker& get_backlog_tracker() = 0;
    virtual const std::string get_group_id() const noexcept = 0;
    virtual seastar::condition_variable& get_staging_done_condition() noexcept = 0;
};

} // namespace compaction

namespace fmt {

template <>
struct formatter<compaction::table_state> : formatter<string_view> {
    template <typename FormatContext>
    auto format(const compaction::table_state& t, FormatContext& ctx) const {
        auto s = t.schema();
        return fmt::format_to(ctx.out(), "{}.{} compaction_group={}", s->ks_name(), s->cf_name(), t.get_group_id());
    }
};

} // namespace fmt
