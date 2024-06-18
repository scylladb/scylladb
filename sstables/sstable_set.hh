/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "readers/mutation_reader_fwd.hh"
#include "readers/mutation_reader.hh"
#include "sstables/progress_monitor.hh"
#include "sstables/types_fwd.hh"
#include "shared_sstable.hh"
#include "dht/ring_position.hh"
#include <seastar/core/shared_ptr.hh>
#include <type_traits>
#include <vector>
#include <tuple>

namespace utils {
class estimated_histogram;
}

namespace sstables {

struct sstable_first_key_less_comparator {
    bool operator()(const shared_sstable& s1, const shared_sstable& s2) const;
};

// Structure holds all sstables (a.k.a. fragments) that belong to same run identifier, which is an UUID.
// SStables in that same run will not overlap with one another.
class sstable_run {
public:
    using sstable_set = std::set<shared_sstable, sstable_first_key_less_comparator>;
private:
    sstable_set _all;
private:
    bool will_introduce_overlapping(const shared_sstable& sst) const;
public:
    sstable_run() = default;
    sstable_run(const sstable_run&) = default;
    // Builds a sstable run with single fragment. It bypasses overlapping check done in insert().
    sstable_run(shared_sstable);
    // Returns false if sstable being inserted cannot satisfy the disjoint invariant. Then caller should pick another run for it.
    [[nodiscard]] bool insert(shared_sstable sst);
    void erase(shared_sstable sst);
    bool empty() const noexcept {
        return _all.empty();
    }
    // Data size of the whole run, meaning it's a sum of the data size of all its fragments.
    uint64_t data_size() const;
    const sstable_set& all() const { return _all; }
    double estimate_droppable_tombstone_ratio(const gc_clock::time_point& compaction_time, const tombstone_gc_state& gc_state, const schema_ptr& s) const;
    run_id run_identifier() const;
};

using shared_sstable_run = lw_shared_ptr<sstable_run>;
using frozen_sstable_run = lw_shared_ptr<const sstable_run>;

class incremental_selector_impl {
public:
    virtual ~incremental_selector_impl() {}
    struct selector_pos {
        const dht::ring_position_view& pos;
        const dht::partition_range* range = nullptr;
    };
    virtual std::tuple<dht::partition_range, std::vector<shared_sstable>, dht::ring_position_ext> select(const selector_pos&) = 0;
};

using sstable_predicate = noncopyable_function<bool(const sstable&)>;
// Default predicate includes everything
const sstable_predicate& default_sstable_predicate();

class sstable_set_impl {
protected:
    uint64_t _bytes_on_disk = 0;

    // for cloning
    explicit sstable_set_impl(uint64_t bytes_on_disk) noexcept : _bytes_on_disk(bytes_on_disk) {}
public:
    sstable_set_impl() = default;
    sstable_set_impl(const sstable_set_impl&) = default;
    virtual ~sstable_set_impl() {}
    virtual std::unique_ptr<sstable_set_impl> clone() const = 0;
    virtual std::vector<shared_sstable> select(const dht::partition_range& range) const = 0;
    virtual std::vector<frozen_sstable_run> all_sstable_runs() const;
    virtual lw_shared_ptr<const sstable_list> all() const = 0;
    virtual stop_iteration for_each_sstable_until(std::function<stop_iteration(const shared_sstable&)> func) const = 0;
    virtual future<stop_iteration> for_each_sstable_gently_until(std::function<future<stop_iteration>(const shared_sstable&)> func) const = 0;
    // Return true iff sst was inserted
    virtual bool insert(shared_sstable sst) = 0;
    // Return true iff sst was erased
    virtual bool erase(shared_sstable sst) = 0;
    virtual size_t size() const noexcept = 0;
    virtual uint64_t bytes_on_disk() const noexcept {
        return _bytes_on_disk;
    }
    uint64_t add_bytes_on_disk(uint64_t delta) noexcept {
        return _bytes_on_disk += delta;
    }
    uint64_t sub_bytes_on_disk(uint64_t delta) noexcept {
        return _bytes_on_disk -= delta;
    }
    using selector_and_schema_t = std::tuple<std::unique_ptr<incremental_selector_impl>, const schema&>;
    virtual selector_and_schema_t make_incremental_selector() const = 0;

    virtual mutation_reader create_single_key_sstable_reader(
        replica::column_family*,
        schema_ptr,
        reader_permit,
        utils::estimated_histogram&,
        const dht::partition_range&,
        const query::partition_slice&,
        tracing::trace_state_ptr,
        streamed_mutation::forwarding,
        mutation_reader::forwarding,
        const sstable_predicate&) const;
};

class sstable_set : public enable_lw_shared_from_this<sstable_set> {
    std::unique_ptr<sstable_set_impl> _impl;
public:
    ~sstable_set();
    sstable_set(std::unique_ptr<sstable_set_impl> impl);
    sstable_set(const sstable_set&);
    sstable_set(sstable_set&&) noexcept;
    sstable_set& operator=(const sstable_set&);
    sstable_set& operator=(sstable_set&&) noexcept;
    std::vector<shared_sstable> select(const dht::partition_range& range) const;
    // Return all runs which contain any of the input sstables.
    std::vector<frozen_sstable_run> all_sstable_runs() const;
    // Return all sstables. It's not guaranteed that sstable_set will keep a reference to the returned list, so user should keep it.
    lw_shared_ptr<const sstable_list> all() const;
    // Prefer for_each_sstable() over all() for iteration purposes, as the latter may have to copy all sstables into a temporary
    void for_each_sstable(std::function<void(const shared_sstable&)> func) const;
    template <typename Func>
    requires std::same_as<typename futurize<std::invoke_result_t<Func, shared_sstable>>::type, future<>>
    future<> for_each_sstable_gently(Func&& func) const {
        using futurator = futurize<std::invoke_result_t<Func, shared_sstable>>;
        co_await _impl->for_each_sstable_gently_until([func = std::forward<Func>(func)] (const shared_sstable& sst) -> future<stop_iteration> {
            co_await futurator::invoke(func, sst);
            co_return stop_iteration::no;
        });
    }
    // Calls func for each sstable or until it returns stop_iteration::yes
    // Returns the last stop_iteration value.
    stop_iteration for_each_sstable_until(std::function<stop_iteration(const shared_sstable&)> func) const;
    template <typename Func>
    requires std::same_as<typename futurize<std::invoke_result_t<Func, shared_sstable>>::type, future<stop_iteration>>
    future<stop_iteration> for_each_sstable_gently_until(Func&& func) const {
        return _impl->for_each_sstable_gently_until([func = std::forward<Func>(func)] (const shared_sstable& sst) -> future<stop_iteration> {
            using futurator = futurize<std::invoke_result_t<Func, shared_sstable>>;
            return futurator::invoke(func, sst);
        });
    }
    // Return true iff sst was inserted
    bool insert(shared_sstable sst);
    // Return true iff sst was erase
    bool erase(shared_sstable sst);
    size_t size() const noexcept;
    uint64_t bytes_on_disk() const noexcept;

    // Used to incrementally select sstables from sstable set using ring-position.
    // sstable set must be alive during the lifetime of the selector.
    class incremental_selector {
        std::unique_ptr<incremental_selector_impl> _impl;
        dht::ring_position_comparator _cmp;
        mutable std::optional<dht::partition_range> _current_range;
        mutable std::optional<interval<dht::ring_position_view>> _current_range_view;
        mutable std::vector<shared_sstable> _current_sstables;
        mutable dht::ring_position_ext _current_next_position = dht::ring_position_view::min();
    public:
        ~incremental_selector();
        incremental_selector(std::unique_ptr<incremental_selector_impl> impl, const schema& s);
        incremental_selector(incremental_selector&&) noexcept;

        struct selection {
            const std::vector<shared_sstable>& sstables;
            dht::ring_position_view next_position;
        };

        using selector_pos = incremental_selector_impl::selector_pos;

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
        selection select(selector_pos pos) const;
        selection select(const dht::ring_position_view& pos) const {
            return select(selector_pos{pos});
        }
    };
    incremental_selector make_incremental_selector() const;

    mutation_reader create_single_key_sstable_reader(
        replica::column_family*,
        schema_ptr,
        reader_permit,
        utils::estimated_histogram&,
        const dht::partition_range&, // must be singular and contain a key
        const query::partition_slice&,
        tracing::trace_state_ptr,
        streamed_mutation::forwarding,
        mutation_reader::forwarding,
        const sstable_predicate& p = default_sstable_predicate()) const;

    /// Read a range from the sstable set.
    ///
    /// The reader is unrestricted, but will account its resource usage on the
    /// semaphore belonging to the passed-in permit.
    mutation_reader make_range_sstable_reader(
        schema_ptr,
        reader_permit,
        const dht::partition_range&,
        const query::partition_slice&,
        tracing::trace_state_ptr,
        streamed_mutation::forwarding,
        mutation_reader::forwarding,
        read_monitor_generator& rmg = default_read_monitor_generator()) const;

    // Filters out mutations that don't belong to the current shard.
    mutation_reader make_local_shard_sstable_reader(
        schema_ptr,
        reader_permit,
        const dht::partition_range&,
        const query::partition_slice&,
        tracing::trace_state_ptr,
        streamed_mutation::forwarding,
        mutation_reader::forwarding,
        read_monitor_generator& rmg = default_read_monitor_generator(),
        const sstable_predicate& p = default_sstable_predicate()) const;

    mutation_reader make_crawling_reader(
            schema_ptr,
            reader_permit,
            tracing::trace_state_ptr,
            read_monitor_generator& rmg = default_read_monitor_generator()) const;

    friend class compound_sstable_set;
};

sstable_set make_partitioned_sstable_set(schema_ptr schema, bool use_level_metadata = true);

sstable_set make_compound_sstable_set(schema_ptr schema, std::vector<lw_shared_ptr<sstable_set>> sets);

std::ostream& operator<<(std::ostream& os, const sstables::sstable_run& run);

using offstrategy = bool_class<class offstrategy_tag>;

/// Return the amount of overlapping in a set of sstables. 0 is returned if set is disjoint.
///
/// The 'sstables' parameter must be a set of sstables sorted by first key.
unsigned sstable_set_overlapping_count(const schema_ptr& schema, const std::vector<shared_sstable>& sstables);

}
