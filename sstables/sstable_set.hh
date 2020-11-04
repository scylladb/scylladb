/*
 * Copyright (C) 2016 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "flat_mutation_reader.hh"
#include "sstables/progress_monitor.hh"
#include "shared_sstable.hh"
#include "sstables/sstables.hh"
#include "dht/i_partitioner.hh"
#include <seastar/core/shared_ptr.hh>
#include <vector>

namespace utils {
class estimated_histogram;
}

namespace sstables {

// This is an implementation of a set of sstables. The sstable_set allows:
// - selecting sstables from a given partition_range
// - selecting sstables with the same run identifiers
// - creating incremental_selectors, used to incrementally select sstables from the set using ring-position
// - many utilities of the std::set (inserting, erasing, checking membership, checking emptiness, iterating)
// - creating copies

// The main use of the sstable_set is in the table class. The sstable_set that is stored there needs to be copied
// every time it is modified, to allow existing sstable readers to continue using the old version. For this reason
// the sstable_set is implemented in a way that allows fast copying.

// This is achieved by associating each copy of an sstable_set with an sstable_set_version which contains all changes
// made to that copy. Each sstable_set_version has a reference to the sstable_set_version associated with the sstable_set
// that was copied. We say that the copied version is a parent, and that the copy version is a child, or that it is "based"
// on the copied version.
// This allows easy checking if an sstable is an element of an sstable_set copy - the answer is yes if the sstable was added
// in this copy or if it wasn't erased in it but it was an element of the parent version.
// It's worth adding that this solution makes a copied sstable_set immutable. To support modifying a copied sstable_set anyway,
// any modification applied to it replaces its associated sstable_set_version with a new one, based on the immutable version.
// The new version hasn't been copied, co it can be modified.

// With the ability to check whether an sstable is an element of an sstable_set, all the methods that select sstables
// from the set can follow the same rule: get results that include all sstables from any copy, and filter out those sstables
// that aren't elements of current copy. These results are received from the class called sstable_set_data structure.

// This solution requires a special way of finding out when an sstable should be removed from sstable_set_data. We achieve this
// by counting, for each sstable, in how many different versions has it been added. If that number is zero - the sstable should
// be completely removed. This number is decreased when deleting versions, when erasing in the same version it was added, and
// in one other case, as a result of propagating a change from a versions children to their parent, explained further in following
// paragraphs.

// With this approach, the length of the longest chain of sstable_set_versions that are based on one another should be as
// short as possible. To achieve that, we are merging pairs of versions. If a version is not referenced by any sstable_sets or
// sstable_lists, we know that it won't be modified or read from, so we can merge its changes into versions that are based on it.
// We don't want to copy these changes though, so we wait with merging until there is only one child version.

// This waiting causes one more problem: an sstable may have been added in a version that has no reference from an sstable_set or
// an sstable_list, and it may have been erased in all its child versions. In such scenario, the sstable can't be selected from
// any version, so it should be removed from the set completely. To handle such situations, if a change has been made to an
// sstable in all children of some version that has no referenece from an sstable_set or sstable_list, the change is removed
// in all children and added to the parent instead.
// This solution guarantees that when a version is ready for merging, the version it is being merged into contains no changes,
// because if it had any, they would be propagated to this version instead. As a result, merging versions is simply reassigning
// sets of changes. The number of times an sstable change gets propagated into a parent version is limited by the number of ancestors
// of a version.

class incremental_selector_impl;
class sstable_set_impl;

// Structure holds all sstables (a.k.a. fragments) that belong to same run identifier, which is an UUID.
// SStables in that same run will not overlap with one another.
class sstable_run {
    std::unordered_set<shared_sstable> _all;
public:
    sstable_run() = default;
    sstable_run(std::unordered_set<shared_sstable> all) : _all(std::move(all)) {}
    void insert(shared_sstable sst);
    void erase(shared_sstable sst);
    // Data size of the whole run, meaning it's a sum of the data size of all its fragments.
    uint64_t data_size() const;
    const std::unordered_set<shared_sstable>& all() const { return _all; }
};

// The very base class of an sstable_set. Stores all sstables that were added in
// any version of the set. Its methods return supersets of values returned by
// any single version.
class sstable_set_data {
    std::unique_ptr<sstable_set_impl> _impl;
    std::unordered_map<utils::UUID, std::unordered_set<shared_sstable>> _all_runs;
    // For each sstable, stores in how many different versions has it been inserted
    std::map<shared_sstable, unsigned> _sstables_and_times_added;
public:
    sstable_set_data(std::unique_ptr<sstable_set_impl> impl);
    void insert(shared_sstable sst);
    void erase(shared_sstable sst);
    std::vector<shared_sstable> select(const dht::partition_range& range) const;
    std::unordered_set<shared_sstable> select_by_run_id(utils::UUID run_id) const;
    const std::map<shared_sstable, unsigned>& all() const;
    void remove(shared_sstable sst);
    std::unique_ptr<incremental_selector_impl> make_incremental_selector();
};

class sstable_set_version;

// Manages a pointer to an sstable_set_version - the when sstable_set_version gets removed when all
// sstable_set_version_references are removed, or when there is only one sstable_set_version_reference
// and that reference is owned by another sstable_set_version.
// In the second case the data from the managed version is merged into the other version before removal.
class sstable_set_version_reference {
    mutable sstable_set_version* _p = nullptr;

    explicit sstable_set_version_reference(sstable_set_version* p);
public:
    ~sstable_set_version_reference();
    sstable_set_version_reference() = default;
    sstable_set_version_reference(const sstable_set_version_reference& ref);
    sstable_set_version_reference(sstable_set_version_reference&&) noexcept;
    sstable_set_version_reference& operator=(const sstable_set_version_reference& x);
    sstable_set_version_reference& operator=(sstable_set_version_reference&&) noexcept;
    sstable_set_version& operator*() const noexcept { return *_p; }
    sstable_set_version* operator->() const noexcept { return _p; }
    sstable_set_version* get() const noexcept { return _p; }
    explicit operator bool() const noexcept { return bool(_p); }
    friend class sstable_set_version;
};

// The data storage for an sstable_set. Can be used like a std::set<shared_sstable> (although with slightly
// costlier operations).
class sstable_list {
    sstable_set_version_reference _version;
public:
    sstable_list(std::unique_ptr<sstable_set_impl> impl, schema_ptr s);
    sstable_list(const sstable_list& sstl);
    sstable_list(sstable_list&& sstl) noexcept;
    sstable_list& operator=(const sstable_list& sstl);
    sstable_list& operator=(sstable_list&& sstl) noexcept;
public:
    class const_iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = const shared_sstable;
        using difference_type = std::ptrdiff_t;
        using pointer = const shared_sstable*;
        using reference = const shared_sstable&;
    private:
        std::map<shared_sstable, unsigned>::const_iterator _it;
        const sstable_set_version* _ver;
        void advance();
    public:
        const_iterator(std::map<shared_sstable, unsigned>::const_iterator it, const sstable_set_version* ver);
        const_iterator& operator++();
        const_iterator operator++(int);
        const shared_sstable& operator*() const;
        bool operator==(const const_iterator& it) const;
    };
    using iterator = const_iterator;
    const_iterator begin() const;
    const_iterator end() const;

    size_t size() const;
    void insert(shared_sstable sst);
    void erase(shared_sstable sst);
    bool contains(shared_sstable sst) const;
    bool empty() const;
    const sstable_set_version& version() const;
};

// A set of sstables associated with a table.
class sstable_set : public enable_lw_shared_from_this<sstable_set> {
    // used to support column_family::get_sstable(), which wants to return an sstable_list
    // that has a reference somewhere
    lw_shared_ptr<sstable_list> _all;
public:
    sstable_set(std::unique_ptr<sstable_set_impl> impl, schema_ptr s);
    sstable_set(const sstable_set&);
    sstable_set(sstable_set&&) noexcept;
    sstable_set& operator=(const sstable_set&);
    sstable_set& operator=(sstable_set&&) noexcept;
    std::vector<shared_sstable> select(const dht::partition_range& range) const;
    // Return all runs which contain any of the input sstables.
    std::vector<sstable_run> select_sstable_runs(const std::vector<shared_sstable>& sstables) const;
    lw_shared_ptr<const sstable_list> all() const;
    void insert(shared_sstable sst);
    void erase(shared_sstable sst);

    // Used to incrementally select sstables from sstable set using ring-position.
    // sstable set must be alive during the lifetime of the selector.
    class incremental_selector {
        std::unique_ptr<incremental_selector_impl> _impl;
        dht::ring_position_comparator _cmp;
        mutable std::optional<dht::partition_range> _current_range;
        mutable std::optional<nonwrapping_range<dht::ring_position_view>> _current_range_view;
        mutable std::vector<shared_sstable> _current_sstables;
        mutable dht::ring_position_view _current_next_position = dht::ring_position_view::min();
        lw_shared_ptr<sstable_list> _sstl;
    public:
        ~incremental_selector();
        incremental_selector(std::unique_ptr<incremental_selector_impl> impl, const schema& s, lw_shared_ptr<sstable_list> sstl);
        incremental_selector(incremental_selector&&) noexcept;

        struct selection {
            const std::vector<shared_sstable>& sstables;
            dht::ring_position_view next_position;
        };

        // Return the sstables that intersect with `pos` and the next
        // position where the intersecting sstables change.
        // To walk through the token range incrementally call `select()`
        // with `dht::ring_position_view::min()` and then pass back the
        // returned `next_position` on each next call until
        // `next_position` becomes `dht::ring_position::max()`.
        //
        // Successive calls to `select()' have to pass weakly monotonic
        // positions (incrementability).
        //
        // NOTE: both `selection.sstables` and `selection.next_position`
        // are only guaranteed to be valid until the next call to
        // `select()`.
        selection select(const dht::ring_position_view& pos) const;
    };
    incremental_selector make_incremental_selector() const;

    flat_mutation_reader create_single_key_sstable_reader(
        column_family*,
        schema_ptr,
        reader_permit,
        utils::estimated_histogram&,
        const dht::ring_position&, // must contain a key
        const query::partition_slice&,
        const io_priority_class&,
        tracing::trace_state_ptr,
        streamed_mutation::forwarding,
        mutation_reader::forwarding) const;

    /// Read a range from the sstable set.
    ///
    /// The reader is unrestricted, but will account its resource usage on the
    /// semaphore belonging to the passed-in permit.
    flat_mutation_reader make_range_sstable_reader(
        schema_ptr,
        reader_permit,
        const dht::partition_range&,
        const query::partition_slice&,
        const io_priority_class&,
        tracing::trace_state_ptr,
        streamed_mutation::forwarding,
        mutation_reader::forwarding,
        read_monitor_generator& rmg = default_read_monitor_generator()) const;

    // Filters out mutations that don't belong to the current shard.
    flat_mutation_reader make_local_shard_sstable_reader(
        schema_ptr,
        reader_permit,
        const dht::partition_range&,
        const query::partition_slice&,
        const io_priority_class&,
        tracing::trace_state_ptr,
        streamed_mutation::forwarding,
        mutation_reader::forwarding,
        read_monitor_generator& rmg = default_read_monitor_generator()) const;
};

/// Read a range from the passed-in sstables.
///
/// The reader is restricted, that is it will wait for admission on the semaphore
/// belonging to the passed-in permit, before starting to read.
flat_mutation_reader make_restricted_range_sstable_reader(
    lw_shared_ptr<sstable_set> sstables,
    schema_ptr,
    reader_permit,
    const dht::partition_range&,
    const query::partition_slice&,
    const io_priority_class&,
    tracing::trace_state_ptr,
    streamed_mutation::forwarding,
    mutation_reader::forwarding,
    read_monitor_generator& rmg = default_read_monitor_generator());

class sstable_set_version {
    // shared by all sstable_set_versions that were based on the same original set
    lw_shared_ptr<sstable_set_data> _base_set;
    schema_ptr _schema;
    sstable_set_version_reference _prev;
    mutable std::unordered_set<sstable_set_version*> _next;
    std::unordered_set<shared_sstable> _added;
    std::unordered_set<shared_sstable> _erased;
    // is equal to the number of sstable_set_versions based on this version increased by one if there is
    // an sstable_list that references this version
    unsigned _reference_count = 0;
public:
    ~sstable_set_version();
    sstable_set_version(std::unique_ptr<sstable_set_impl> impl, schema_ptr schema);
    explicit sstable_set_version(sstable_set_version* ver);
private:
    void propagate_inserted_sstable(const shared_sstable& sst) noexcept;
    void propagate_erased_sstable(const shared_sstable& sst) noexcept;
    void propagate_changes_from_next_versions() noexcept;
public:
    const sstable_set_version* get_previous_version() const;
    bool can_merge_with_next() const noexcept;
    void merge_with_next() noexcept;
    bool can_delete() const noexcept;
    void add_reference() noexcept;
    void remove_reference() noexcept;
    schema_ptr get_schema() const;
    std::vector<shared_sstable> select(const dht::partition_range& range) const;
    // Return all runs which contain any of the input sstables.
    std::vector<sstable_run> select_sstable_runs(const std::vector<shared_sstable>& sstables) const;
    const std::map<shared_sstable, unsigned>& all() const;
    sstable_set_version_reference insert(shared_sstable sst);
    sstable_set_version_reference erase(shared_sstable sst);
    bool contains(shared_sstable sst) const;
    size_t size() const;
    std::unique_ptr<incremental_selector_impl> make_incremental_selector() const;
public:
    sstable_set_version_reference get_reference_to_this();
    sstable_set_version_reference get_reference_to_new_copy();
};

sstable_set make_partitioned_sstable_set(schema_ptr schema, bool use_level_metadata = true);

std::ostream& operator<<(std::ostream& os, const sstables::sstable_run& run);

}
