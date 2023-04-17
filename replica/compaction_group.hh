/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "database_fwd.hh"
#include "compaction/compaction_descriptor.hh"
#include "compaction/compaction_backlog_manager.hh"
#include "compaction/compaction_strategy_state.hh"
#include "sstables/sstable_set.hh"
#include "sstables/sstable_set_impl.hh"
#include "compaction/compaction_fwd.hh"
#include <span>

#pragma once

namespace replica {

using enable_backlog_tracker = bool_class<class enable_backlog_tracker_tag>;

// Compaction group is a set of SSTables which are eligible to be compacted together.
// By this definition, we can say:
//      - A group contains SSTables that are owned by the same shard.
//      - Also, a group will be owned by a single table. Different tables own different groups.
//      - Each group can be thought of an isolated LSM tree, where Memtable(s) and SSTable(s) are
//          isolated from other groups.
// Usually, a table T in shard S will own a single compaction group. With compaction_group, a
// table T will be able to own as many groups as it wishes.
class compaction_group {
    table& _t;
    class table_state;
    std::unique_ptr<table_state> _table_state;
    // Tokens included in this compaction_groups
    dht::token_range _token_range;
    compaction::compaction_strategy_state _compaction_strategy_state;
    // Holds list of memtables for this group
    lw_shared_ptr<memtable_list> _memtables;
    // SSTable set which contains all non-maintenance sstables
    lw_shared_ptr<sstables::sstable_set> _main_sstables;
    // Holds SSTables created by maintenance operations, which need reshaping before integration into the main set
    lw_shared_ptr<sstables::sstable_set> _maintenance_sstables;
    // sstables that have been compacted (so don't look up in query) but
    // have not been deleted yet, so must not GC any tombstones in other sstables
    // that may delete data in these sstables:
    std::vector<sstables::shared_sstable> _sstables_compacted_but_not_deleted;
    uint64_t _main_set_disk_space_used = 0;
    uint64_t _maintenance_set_disk_space_used = 0;
private:
    // Adds new sstable to the set of sstables
    // Doesn't update the cache. The cache must be synchronized in order for reads to see
    // the writes contained in this sstable.
    // Cache must be synchronized atomically with this, otherwise write atomicity may not be respected.
    // Doesn't trigger compaction.
    // Strong exception guarantees.
    lw_shared_ptr<sstables::sstable_set>
    do_add_sstable(lw_shared_ptr<sstables::sstable_set> sstables, sstables::shared_sstable sstable,
                   enable_backlog_tracker backlog_tracker);
    // Update compaction backlog tracker with the same changes applied to the underlying sstable set.
    void backlog_tracker_adjust_charges(const std::vector<sstables::shared_sstable>& old_sstables, const std::vector<sstables::shared_sstable>& new_sstables);
    static uint64_t calculate_disk_space_used_for(const sstables::sstable_set& set);

    future<> delete_sstables_atomically(std::vector<sstables::shared_sstable> sstables_to_remove);
public:
    compaction_group(table& t, dht::token_range token_range);

    // Will stop ongoing compaction on behalf of this group, etc.
    future<> stop() noexcept;

    // Clear sstable sets
    void clear_sstables();

    // Clear memtable(s) content
    future<> clear_memtables();

    future<> flush();
    bool can_flush() const;

    const dht::token_range& token_range() const noexcept {
        return _token_range;
    }

    void set_compaction_strategy_state(compaction::compaction_strategy_state compaction_strategy_state) noexcept;

    lw_shared_ptr<memtable_list>& memtables() noexcept;
    size_t memtable_count() const noexcept;
    // Returns minimum timestamp from memtable list
    api::timestamp_type min_memtable_timestamp() const;
    // Add sstable to main set
    void add_sstable(sstables::shared_sstable sstable);
    // Add sstable to maintenance set
    void add_maintenance_sstable(sstables::shared_sstable sst);

    // Update main sstable set based on info in completion descriptor, where input sstables
    // will be replaced by output ones, row cache ranges are possibly invalidated and
    // statistics are updated.
    future<> update_main_sstable_list_on_compaction_completion(sstables::compaction_completion_desc desc);

    // This will update sstable lists on behalf of off-strategy compaction, where
    // input files will be removed from the maintenance set and output files will
    // be inserted into the main set.
    future<> update_sstable_lists_on_off_strategy_completion(sstables::compaction_completion_desc desc);

    const lw_shared_ptr<sstables::sstable_set>& main_sstables() const noexcept;
    void set_main_sstables(lw_shared_ptr<sstables::sstable_set> new_main_sstables);

    const lw_shared_ptr<sstables::sstable_set>& maintenance_sstables() const noexcept;
    void set_maintenance_sstables(lw_shared_ptr<sstables::sstable_set> new_maintenance_sstables);

    // Makes a compound set, which includes main and maintenance sets
    lw_shared_ptr<sstables::sstable_set> make_compound_sstable_set();

    const std::vector<sstables::shared_sstable>& compacted_undeleted_sstables() const noexcept;
    // Triggers regular compaction.
    void trigger_compaction();

    compaction_backlog_tracker& get_backlog_tracker();

    size_t live_sstable_count() const noexcept;
    uint64_t live_disk_space_used() const noexcept;
    uint64_t total_disk_space_used() const noexcept;

    compaction::table_state& as_table_state() const noexcept;

    class sstable_set;
};

class compaction_group::sstable_set : public sstable_set_impl {
    using sstable_sets_t = std::vector<std::pair<const dht::token_range, lw_shared_ptr<sstables::sstable_set>>>;

    schema_ptr _schema;
    sstable_sets_t _cg_sstable_sets;
private:
    static unsigned x_log2_compaction_groups(size_t number_of_cgs);
    sstable_sets_t initialize_sstable_sets(const std::vector<std::unique_ptr<compaction_group>>& cgs) const;
    static unsigned group_of(const sstable_sets_t& sets, const dht::token& t);
    unsigned group_of(const dht::token& t) const;
    std::pair<unsigned, unsigned> groups_of(const dht::token_range& tr) const;

    auto select_group_sets(const dht::token_range& tr) const {
        auto [start, end] = groups_of(tr);
        // TODO: switch to std::ranges::subrange(...), it's now broken in clang 15.
        return std::span(_cg_sstable_sets.begin() + start, _cg_sstable_sets.begin() + end + 1);
    }
public:
    explicit sstable_set(schema_ptr schema, sstable_sets_t sets) noexcept
            : _schema(std::move(schema))
            , _cg_sstable_sets(std::move(sets)) {}

    explicit sstable_set(schema_ptr schema, const std::vector<std::unique_ptr<compaction_group>>& cgs)
            : _schema(std::move(schema))
            , _cg_sstable_sets(initialize_sstable_sets(cgs)) {}

    virtual std::unique_ptr<sstable_set_impl> clone() const override;
    virtual std::vector<shared_sstable> select(const dht::partition_range& pr = query::full_partition_range) const override;
    virtual std::vector<sstable_run> select_sstable_runs(const std::vector<shared_sstable>& sstables) const override;
    virtual lw_shared_ptr<const sstable_list> all() const override;
    virtual stop_iteration for_each_sstable_until(std::function<stop_iteration(const shared_sstable&)> func) const override;
    virtual void insert(shared_sstable sst) override;
    virtual void erase(shared_sstable sst) override;
    virtual size_t size() const noexcept override;
    virtual std::unique_ptr<incremental_selector_impl> make_incremental_selector() const override;

    virtual flat_mutation_reader_v2 create_single_key_sstable_reader(
            replica::table*,
            schema_ptr,
            reader_permit,
            utils::estimated_histogram&,
            const dht::partition_range&,
            const query::partition_slice&,
            const io_priority_class&,
            tracing::trace_state_ptr,
            streamed_mutation::forwarding,
            mutation_reader::forwarding) const override;

    static sstables::sstable_set make(schema_ptr schema, const std::vector<std::unique_ptr<compaction_group>>& cgs);

    class incremental_selector;
};

// Used by the tests to increase the default number of compaction groups by increasing the minimum to X.
void set_minimum_x_log2_compaction_groups(unsigned x_log2_compaction_groups);

}
