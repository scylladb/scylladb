/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>

#include "database_fwd.hh"
#include "compaction/compaction_descriptor.hh"
#include "compaction/compaction_backlog_manager.hh"
#include "compaction/compaction_strategy_state.hh"
#include "sstables/sstable_set.hh"
#include "utils/chunked_vector.hh"
#include <boost/intrusive/list.hpp>

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
    const size_t _group_id;
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
    seastar::condition_variable _staging_done_condition;
    // Gates async operations confined to a single group.
    seastar::gate _async_gate;
    using list_hook_t = boost::intrusive::list_member_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>;
    list_hook_t _list_hook;
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
    // Input SSTables that weren't added to any SSTable set, are considered unused and can be unlinked.
    // An input SSTable remains linked if it wasn't actually compacted, yet compaction manager wants
    // it to be moved from its original sstable set (e.g. maintenance) into a new one (e.g. main).
    future<> delete_unused_sstables(sstables::compaction_completion_desc desc);
public:
    using list_t = boost::intrusive::list<compaction_group,
        boost::intrusive::member_hook<compaction_group, compaction_group::list_hook_t, &compaction_group::_list_hook>,
        boost::intrusive::constant_time_size<false>>;

    compaction_group(table& t, size_t gid, dht::token_range token_range);

    size_t group_id() const noexcept {
        return _group_id;
    }

    // Stops all activity in the group, synchronizes with in-flight writes, before
    // flushing memtable(s), so all data can be found in the SSTable set.
    future<> stop() noexcept;

    // This removes all the storage belonging to the group. In order to avoid data
    // resurrection, makes sure that all data is flushed into SSTables before
    // proceeding with atomic deletion on them.
    future<> cleanup();

    // Clear sstable sets
    void clear_sstables();

    // Clear memtable(s) content
    future<> clear_memtables();

    future<> flush() noexcept;
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

    seastar::condition_variable& get_staging_done_condition() noexcept {
        return _staging_done_condition;
    }

    seastar::gate& async_gate() noexcept {
        return _async_gate;
    }
};

using compaction_group_ptr = std::unique_ptr<compaction_group>;
using compaction_group_vector = utils::chunked_vector<compaction_group_ptr>;
using compaction_group_list = compaction_group::list_t;

// Storage group is responsible for storage that belongs to a single tablet.
// A storage group can manage 1 or more compaction groups, each of which can be compacted independently.
// If a tablet needs splitting, the storage group can be put in splitting mode, allowing the storage
// in main compaction groups to be split into two new compaction groups, all of which will be managed
// by the same storage group.
class storage_group {
    compaction_group_ptr _main_cg;
public:
    storage_group(compaction_group_ptr cg, compaction_group_list& list);

    const dht::token_range& token_range() const noexcept;

    compaction_group_ptr& main_compaction_group() noexcept;

    utils::small_vector<compaction_group*, 3> compaction_groups() noexcept;
};

using storage_group_vector = utils::chunked_vector<std::unique_ptr<storage_group>>;

// TODO: will be changed into storage_group_manager. Not doing it now to reduce the change size.
class compaction_group_manager {
public:
    virtual ~compaction_group_manager() {}
    virtual compaction_group_vector make_compaction_groups() const = 0;
    virtual size_t compaction_group_of(dht::token) const = 0;
    virtual size_t log2_compaction_groups() const = 0;
};

}
