/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/rwlock.hh>

#include "database_fwd.hh"
#include "compaction/compaction_descriptor.hh"
#include "compaction/compaction_backlog_manager.hh"
#include "compaction/compaction_strategy_state.hh"
#include "locator/tablets.hh"
#include "sstables/sstable_set.hh"
#include "utils/chunked_vector.hh"
#include <boost/intrusive/list.hpp>
#include <absl/container/flat_hash_map.h>

#pragma once

class compaction_manager;

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
    compaction_group(table& t, size_t gid, dht::token_range token_range);
    ~compaction_group();

    void update_id_and_range(size_t id, dht::token_range token_range) {
        _group_id = id;
        _token_range = std::move(token_range);
    }

    size_t group_id() const noexcept {
        return _group_id;
    }

    // Stops all activity in the group, synchronizes with in-flight writes, before
    // flushing memtable(s), so all data can be found in the SSTable set.
    future<> stop(sstring reason) noexcept;

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
    // Returns true if memtable(s) contains key.
    bool memtable_has_key(const dht::decorated_key& key) const;
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

    // Makes a sstable set, which includes all sstables managed by this group
    lw_shared_ptr<sstables::sstable_set> make_sstable_set() const;

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
    const compaction_manager& get_compaction_manager() const noexcept;

    friend class storage_group;
};

using compaction_group_ptr = lw_shared_ptr<compaction_group>;
using const_compaction_group_ptr = lw_shared_ptr<const compaction_group>;

// Storage group is responsible for storage that belongs to a single tablet.
// A storage group can manage 1 or more compaction groups, each of which can be compacted independently.
// If a tablet needs splitting, the storage group can be put in splitting mode, allowing the storage
// in main compaction groups to be split into two new compaction groups, all of which will be managed
// by the same storage group.
//
// With vnodes, a table instance in a given shard will have a single group. With tablets, a table in a
// shard will have as many groups as there are tablet replicas owned by that shard.
class storage_group {
    compaction_group_ptr _main_cg;
    std::vector<compaction_group_ptr> _split_ready_groups;
    seastar::gate _async_gate;
private:
    bool splitting_mode() const {
        return !_split_ready_groups.empty();
    }
    size_t to_idx(locator::tablet_range_side) const;
public:
    storage_group(compaction_group_ptr cg);

    seastar::gate& async_gate() {
        return _async_gate;
    }

    const dht::token_range& token_range() const noexcept;

    size_t memtable_count() const noexcept;

    const compaction_group_ptr& main_compaction_group() const noexcept;
    const std::vector<compaction_group_ptr>& split_ready_compaction_groups() const;
    compaction_group_ptr& select_compaction_group(locator::tablet_range_side) noexcept;

    uint64_t live_disk_space_used() const noexcept;

    void for_each_compaction_group(std::function<void(const compaction_group_ptr&)> action) const noexcept;
    utils::small_vector<compaction_group_ptr, 3> compaction_groups() noexcept;
    utils::small_vector<const_compaction_group_ptr, 3> compaction_groups() const noexcept;

    // Puts the storage group in split mode, in which it internally segregates data
    // into two sstable sets and two memtable sets corresponding to the two adjacent
    // tablets post-split.
    // Preexisting sstables and memtables are not split yet.
    // Returns true if post-conditions for split() are met.
    bool set_split_mode();

    // Like set_split_mode() but triggers splitting for old sstables and memtables and waits
    // for it:
    //  1) Flushes all memtables which were created in non-split mode, and waits for that to complete.
    //  2) Compacts all sstables which overlap with the split point
    // Returns a future which resolves when this process is complete.
    future<> split(sstables::compaction_type_options::split opt);

    // Make an sstable set spanning all sstables in the storage_group
    lw_shared_ptr<const sstables::sstable_set> make_sstable_set() const;

    // Flush all memtables.
    future<> flush() noexcept;
    bool can_flush() const;
    api::timestamp_type min_memtable_timestamp() const;

    bool compaction_disabled() const;
    // Returns true when all compacted sstables were already deleted.
    bool no_compacted_sstable_undeleted() const;

    future<> stop(sstring reason = "table removal") noexcept;
};

using storage_group_ptr = lw_shared_ptr<storage_group>;
using storage_group_map = absl::flat_hash_map<size_t, storage_group_ptr, absl::Hash<size_t>>;

class storage_group_manager {
protected:
    storage_group_map _storage_groups;
public:
    virtual ~storage_group_manager();

    //    How concurrent loop and updates on the group map works without a lock:
    //
    //    Firstly, all yielding loops will work on a copy of map, to prevent a
    //    concurrent update to the map from interfering with it.
    //
    //    scenario 1:
    //    T
    //    1   loop on the map
    //    2                               storage group X is stopped on cleanup
    //    3   loop reaches X
    //
    //    Here, X is stopped before it is reached. This is handled by teaching
    //    iteration to skip groups that were stopped by cleanup (implemented
    //    using gate).
    //    X survives its removal from the map since it is a lw_shared_ptr.
    //
    //
    //    scenario 2:
    //    T
    //    1   loop on the map
    //    2   loop reaches X
    //    3                               storage group X is stopped on cleanup
    //
    //    Here, X is stopped while being used, but that also happens during shutdown.
    //    When X is stopped, flush happens and compactions are all stopped (exception
    //    is not propagated upwards) and new ones cannot start afterward.
    //
    //
    //    scenario 3:
    //    T
    //    1   loop on the map
    //    2                               storage groups are split
    //    3   loop reaches old groups
    //
    //    Here, the loop continues post storage group split, which rebuilds the old
    //    map into a new one. This is handled by allowing the old map to still access
    //    the compaction groups that were reassigned according to the new tablet count.
    //    We don't move the compaction groups, but rather they're still visible by old
    //    and new storage groups.

    // Important to not return storage_group_id in yielding variants, since ids can be
    // invalidated when storage group count changes (e.g. split or merge).
    future<> parallel_foreach_storage_group(std::function<future<>(storage_group&)> f);
    future<> for_each_storage_group_gently(std::function<future<>(storage_group&)> f);
    void for_each_storage_group(std::function<void(size_t, storage_group&)> f) const;
    const storage_group_map& storage_groups() const;

    future<> stop_storage_groups() noexcept;
    void remove_storage_group(size_t id);
    storage_group& storage_group_for_id(const schema_ptr&, size_t i) const;

    // Caller must keep the current effective_replication_map_ptr valid
    // until the storage_group_manager finishes update_effective_replication_map
    //
    // refresh_mutation_source must be called when there are changes to data source
    // structures but logical state of data is not changed (e.g. when state for a
    // new tablet replica is allocated).
    virtual future<> update_effective_replication_map(const locator::effective_replication_map& erm, noncopyable_function<void()> refresh_mutation_source) = 0;

    virtual compaction_group& compaction_group_for_token(dht::token token) const noexcept = 0;
    virtual utils::chunked_vector<compaction_group*> compaction_groups_for_token_range(dht::token_range tr) const = 0;
    virtual compaction_group& compaction_group_for_key(partition_key_view key, const schema_ptr& s) const noexcept = 0;
    virtual compaction_group& compaction_group_for_sstable(const sstables::shared_sstable& sst) const noexcept = 0;

    virtual size_t log2_storage_groups() const = 0;
    virtual storage_group& storage_group_for_token(dht::token) const noexcept = 0;

    virtual locator::table_load_stats table_load_stats(std::function<bool(const locator::tablet_map&, locator::global_tablet_id)> tablet_filter) const noexcept = 0;
    virtual bool all_storage_groups_split() = 0;
    virtual future<> split_all_storage_groups() = 0;
    virtual future<> maybe_split_compaction_group_of(size_t idx) = 0;
    virtual future<std::vector<sstables::shared_sstable>> maybe_split_sstable(const sstables::shared_sstable& sst) = 0;

    virtual lw_shared_ptr<sstables::sstable_set> make_sstable_set() const = 0;
};

}
