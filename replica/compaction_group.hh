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
#include "locator/tablets.hh"
#include "sstables/sstable_set.hh"
#include "utils/chunked_vector.hh"
#include <boost/intrusive/list.hpp>

#pragma once

namespace locator {
class effective_replication_map;
}

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
    size_t _group_id;
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

    void update_id_and_range(size_t id, dht::token_range token_range) {
        _group_id = id;
        _token_range = std::move(token_range);
    }

    size_t group_id() const noexcept {
        return _group_id;
    }

    // Stops all activity in the group, synchronizes with in-flight writes, before
    // flushing memtable(s), so all data can be found in the SSTable set.
    future<> stop() noexcept;

    bool empty() const noexcept;

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
    lw_shared_ptr<sstables::sstable_set> make_compound_sstable_set() const;

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

    compaction_manager& get_compaction_manager() noexcept;

    friend class storage_group;
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
    std::vector<compaction_group_ptr> _split_ready_groups;
private:
    bool splitting_mode() const {
        return !_split_ready_groups.empty();
    }
    size_t to_idx(locator::tablet_range_side) const;
public:
    storage_group(compaction_group_ptr cg, compaction_group_list* list);

    const dht::token_range& token_range() const noexcept;

    size_t memtable_count() const noexcept;

    compaction_group_ptr& main_compaction_group() noexcept;
    std::vector<compaction_group_ptr> split_ready_compaction_groups() &&;
    compaction_group_ptr& select_compaction_group(locator::tablet_range_side) noexcept;

    uint64_t live_disk_space_used() const noexcept;

    utils::small_vector<compaction_group*, 3> compaction_groups() noexcept;

    // Puts the storage group in split mode, in which it internally segregates data
    // into two sstable sets and two memtable sets corresponding to the two adjacent
    // tablets post-split.
    // Preexisting sstables and memtables are not split yet.
    // Returns true if post-conditions for split() are met.
    bool set_split_mode(compaction_group_list&);

    // Like set_split_mode() but triggers splitting for old sstables and memtables and waits
    // for it:
    //  1) Flushes all memtables which were created in non-split mode, and waits for that to complete.
    //  2) Compacts all sstables which overlap with the split point
    // Returns a future which resolves when this process is complete.
    future<> split(compaction_group_list&, sstables::compaction_type_options::split opt);
};

using storage_group_vector = utils::chunked_vector<std::unique_ptr<storage_group>>;

class storage_group_manager {
protected:
    // The compaction group list is only a helper for accessing the groups managed by the storage groups.
    // The list entries are unlinked automatically when the storage group, they belong to, is removed.
    compaction_group_list _compaction_groups;
    storage_group_vector _storage_groups;

public:
    virtual ~storage_group_manager();

    const compaction_group_list& compaction_groups() const noexcept {
        return _compaction_groups;
    }
    compaction_group_list& compaction_groups() noexcept {
        return _compaction_groups;
    }

    const storage_group_vector& storage_groups() const noexcept {
        return _storage_groups;
    }
    storage_group_vector& storage_groups() noexcept {
        return _storage_groups;
    }

    compaction_group* single_compaction_group_if_available() noexcept {
        return _compaction_groups.size() == 1 ? &_compaction_groups.front() : nullptr;
    }

    // Caller must keep the current effective_replication_map_ptr valid
    // until the storage_group_manager finishes update_effective_replication_map
    virtual future<> update_effective_replication_map(const locator::effective_replication_map& erm) = 0;

    virtual compaction_group& compaction_group_for_token(dht::token token) const noexcept = 0;
    virtual utils::chunked_vector<compaction_group*> compaction_groups_for_token_range(dht::token_range tr) const = 0;
    virtual compaction_group& compaction_group_for_key(partition_key_view key, const schema_ptr& s) const noexcept = 0;
    virtual compaction_group& compaction_group_for_sstable(const sstables::shared_sstable& sst) const noexcept = 0;

    virtual std::pair<size_t, locator::tablet_range_side> storage_group_of(dht::token) const = 0;
    virtual size_t log2_storage_groups() const = 0;
    virtual size_t storage_group_id_for_token(dht::token) const noexcept = 0;
    virtual storage_group* storage_group_for_token(dht::token) const noexcept = 0;

    virtual locator::resize_decision::seq_number_t split_ready_seq_number() const noexcept = 0;
    virtual bool all_storage_groups_split() = 0;
    virtual future<> split_all_storage_groups() = 0;
    virtual future<> maybe_split_compaction_group_of(size_t idx) = 0;
};

}
