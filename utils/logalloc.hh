/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <memory>
#include <seastar/core/memory.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/expiring_fifo.hh>
#include "allocation_strategy.hh"
#include "seastarx.hh"
#include "db/timeout_clock.hh"
#include "utils/assert.hh"
#include "utils/entangled.hh"
#include "utils/memory_limit_reached.hh"

namespace logalloc {

struct occupancy_stats;
class region;
class region_impl;
class allocating_section;

constexpr int segment_size_shift = 17; // 128K; see #151, #152
constexpr size_t segment_size = 1 << segment_size_shift;
constexpr size_t max_zone_segments = 256;

//
// Frees some amount of objects from the region to which it's attached.
//
// This should eventually stop given no new objects are added:
//
//     while (eviction_fn() == memory::reclaiming_result::reclaimed_something) ;
//
using eviction_fn = std::function<memory::reclaiming_result()>;

// Listens for events from a region
class region_listener {
public:
    virtual ~region_listener();
    virtual void add(region* r) = 0;
    virtual void del(region* r) = 0;
    virtual void moved(region* old_address, region* new_address) = 0;
    virtual void increase_usage(region* r, ssize_t delta) = 0;
    virtual void decrease_evictable_usage(region* r) = 0;
    virtual void decrease_usage(region* r, ssize_t delta) = 0;
};

// Controller for all LSA regions. There's one per shard.
class tracker {
public:
    class impl;

    struct config {
        bool defragment_on_idle;
        bool abort_on_lsa_bad_alloc;
        bool sanitizer_report_backtrace = false; // Better reports but slower
        size_t lsa_reclamation_step;
        scheduling_group background_reclaim_sched_group;
    };

    struct stats {
        size_t segments_compacted;
        size_t lsa_buffer_segments;
        uint64_t memory_allocated;
        uint64_t memory_freed;
        uint64_t memory_compacted;
        uint64_t memory_evicted;
        uint64_t num_allocations;

        friend stats operator+(const stats& s1, const stats& s2) {
            stats result(s1);
            result += s2;
            return result;
        }
        friend stats operator-(const stats& s1, const stats& s2) {
            stats result(s1);
            result -= s2;
            return result;
        }
        stats& operator+=(const stats& other) {
            segments_compacted += other.segments_compacted;
            lsa_buffer_segments += other.lsa_buffer_segments;
            memory_allocated += other.memory_allocated;
            memory_freed += other.memory_freed;
            memory_compacted += other.memory_compacted;
            memory_evicted += other.memory_evicted;
            num_allocations += other.num_allocations;
            return *this;
        }
        stats& operator-=(const stats& other) {
            segments_compacted -= other.segments_compacted;
            lsa_buffer_segments -= other.lsa_buffer_segments;
            memory_allocated -= other.memory_allocated;
            memory_freed -= other.memory_freed;
            memory_compacted -= other.memory_compacted;
            memory_evicted -= other.memory_evicted;
            num_allocations -= other.num_allocations;
            return *this;
        }
    };

    void configure(const config& cfg);
    future<> stop();

private:
    std::unique_ptr<impl> _impl;
    memory::reclaimer _reclaimer;
    friend class region;
    friend class region_impl;
    memory::reclaiming_result reclaim(seastar::memory::reclaimer::request);

public:
    tracker();
    ~tracker();

    stats statistics() const;

    //
    // Tries to reclaim given amount of bytes in total using all compactible
    // and evictable regions. Returns the number of bytes actually reclaimed.
    // That value may be smaller than requested when evictable pools are empty
    // and compactible pools can't compact any more.
    //
    // Invalidates references to objects in all compactible and evictable regions.
    //
    size_t reclaim(size_t bytes);

    // Compacts as much as possible. Very expensive, mainly for testing.
    // Guarantees that every live object from reclaimable regions will be moved.
    // Invalidates references to objects in all compactible and evictable regions.
    void full_compaction();

    void reclaim_all_free_segments();

    occupancy_stats global_occupancy() const noexcept;

    // Returns aggregate statistics for all pools.
    occupancy_stats region_occupancy() const noexcept;

    // Returns statistics for all segments allocated by LSA on this shard.
    occupancy_stats occupancy() const noexcept;

    // Returns amount of allocated memory not managed by LSA
    size_t non_lsa_used_space() const noexcept;

    impl& get_impl() noexcept { return *_impl; }

    // Returns the minimum number of segments reclaimed during single reclamation cycle.
    size_t reclamation_step() const noexcept;

    bool should_abort_on_bad_alloc() const noexcept;
};

class tracker_reclaimer_lock {
    tracker::impl& _tracker_impl;
public:
    tracker_reclaimer_lock(tracker::impl& impl) noexcept;
    tracker_reclaimer_lock(tracker& t) noexcept : tracker_reclaimer_lock(t.get_impl()) { }
    ~tracker_reclaimer_lock();
};

tracker& shard_tracker() noexcept;

class segment_descriptor;

/// A unique pointer to a chunk of memory allocated inside an LSA region.
///
/// The pointer can be in disengaged state in which case it doesn't point at any buffer (nullptr state).
/// When the pointer points at some buffer, it is said to be engaged.
///
/// The pointer owns the object.
/// When the pointer is destroyed or it transitions from engaged to disengaged state, the buffer is freed.
/// The buffer is never leaked when operating by the API of lsa_buffer.
/// The pointer object can be safely destroyed in any allocator context.
///
/// The pointer object is never invalidated.
/// The pointed-to buffer can be moved around by LSA, so the pointer returned by get() can be
/// invalidated, but the pointer object itself is updated automatically and get() always returns
/// a pointer which is valid at the time of the call.
///
/// Must not outlive the region.
class lsa_buffer {
    friend class region_impl;
    entangled _link;           // Paired with segment_descriptor::_buf_pointers[...]
    segment_descriptor* _desc; // Valid only when engaged
    char* _buf = nullptr;      // Valid only when engaged
    size_t _size = 0;
public:
    using char_type = char;

    lsa_buffer() = default;
    lsa_buffer(lsa_buffer&&) noexcept = default;
    ~lsa_buffer();

    /// Makes this instance point to the buffer pointed to by the other pointer.
    /// If this pointer was engaged before, the owned buffer is freed.
    /// The other pointer will be in disengaged state after this.
    lsa_buffer& operator=(lsa_buffer&& other) noexcept {
        if (this != &other) {
            this->~lsa_buffer();
            new (this) lsa_buffer(std::move(other));
        }
        return *this;
    }

    /// Disengages the pointer.
    /// If the pointer was engaged before, the owned buffer is freed.
    /// Postcondition: !bool(*this)
    lsa_buffer& operator=(std::nullptr_t) noexcept {
        this->~lsa_buffer();
        return *this;
    }

    /// Returns a pointer to the first element of the buffer.
    /// Valid only when engaged.
    char_type* get() noexcept { return _buf; }
    const char_type* get() const noexcept { return _buf; }

    /// Returns the number of bytes in the buffer.
    size_t size() const noexcept { return _size; }

    /// Returns true iff the pointer is engaged.
    explicit operator bool() const noexcept { return bool(_link); }
};

// Monoid representing pool occupancy statistics.
// Naturally ordered so that sparser pools come fist.
// All sizes in bytes.
class occupancy_stats {
    size_t _free_space;
    size_t _total_space;
public:
    occupancy_stats() noexcept : _free_space(0), _total_space(0) {}

    occupancy_stats(size_t free_space, size_t total_space) noexcept
        : _free_space(free_space), _total_space(total_space) { }

    bool operator<(const occupancy_stats& other) const noexcept {
        return used_fraction() < other.used_fraction();
    }

    friend occupancy_stats operator+(const occupancy_stats& s1, const occupancy_stats& s2) noexcept {
        occupancy_stats result(s1);
        result += s2;
        return result;
    }

    friend occupancy_stats operator-(const occupancy_stats& s1, const occupancy_stats& s2) noexcept {
        occupancy_stats result(s1);
        result -= s2;
        return result;
    }

    occupancy_stats& operator+=(const occupancy_stats& other) noexcept {
        _total_space += other._total_space;
        _free_space += other._free_space;
        return *this;
    }

    occupancy_stats& operator-=(const occupancy_stats& other) noexcept {
        _total_space -= other._total_space;
        _free_space -= other._free_space;
        return *this;
    }

    size_t used_space() const noexcept {
        return _total_space - _free_space;
    }

    size_t free_space() const noexcept {
        return _free_space;
    }

    size_t total_space() const noexcept {
        return _total_space;
    }

    float used_fraction() const noexcept {
        return _total_space ? float(used_space()) / total_space() : 0;
    }

    explicit operator bool() const noexcept {
        return _total_space > 0;
    }
};

class basic_region_impl : public allocation_strategy {
protected:
    tracker& _tracker;
    bool _reclaiming_enabled = true;
    seastar::shard_id _cpu = this_shard_id();
public:
    basic_region_impl(tracker& tracker) : _tracker(tracker)
    { }

    tracker& get_tracker() { return _tracker; }

    void set_reclaiming_enabled(bool enabled) noexcept {
        SCYLLA_ASSERT(this_shard_id() == _cpu);
        _reclaiming_enabled = enabled;
    }

    bool reclaiming_enabled() const noexcept {
        return _reclaiming_enabled;
    }
};

//
// Log-structured allocator region.
//
// Objects allocated using this region are said to be owned by this region.
// Objects must be freed only using the region which owns them. Ownership can
// be transferred across regions using the merge() method. Region must be live
// as long as it owns any objects.
//
// Each region has separate memory accounting and can be compacted
// independently from other regions. To reclaim memory from all regions use
// shard_tracker().
//
// Region is automatically added to the set of
// compactible regions when constructed.
//
class region {
public:
    using impl = region_impl;
private:
    shared_ptr<basic_region_impl> _impl;
private:
    region_impl& get_impl() noexcept;
    const region_impl& get_impl() const noexcept;
public:
    region();
    ~region();
    region(region&& other) noexcept;
    region& operator=(region&& other) noexcept;
    region(const region& other) = delete;

    void listen(region_listener* listener);
    void unlisten();

    occupancy_stats occupancy() const noexcept;

    tracker& get_tracker() const {
        return _impl->get_tracker();
    }

    allocation_strategy& allocator() noexcept {
        return *_impl;
    }
    const allocation_strategy& allocator() const noexcept {
        return *_impl;
    }

    // Allocates a buffer of a given size.
    // The buffer's pointer will be aligned to 4KB.
    // Note: it is wasteful to allocate buffers of sizes which are not a multiple of the alignment.
    lsa_buffer alloc_buf(size_t buffer_size);

    // Merges another region into this region. The other region is left empty.
    // Doesn't invalidate references to allocated objects.
    void merge(region& other) noexcept;

    // Compacts everything. Mainly for testing.
    // Invalidates references to allocated objects.
    void full_compaction();

    // Runs eviction function once. Mainly for testing.
    memory::reclaiming_result evict_some();

    // Changes the reclaimability state of this region. When region is not
    // reclaimable, it won't be considered by tracker::reclaim(). By default region is
    // reclaimable after construction.
    void set_reclaiming_enabled(bool e) noexcept { _impl->set_reclaiming_enabled(e); }

    // Returns the reclaimability state of this region.
    bool reclaiming_enabled() const noexcept { return _impl->reclaiming_enabled(); }

    // Returns a value which is increased when this region is either compacted or
    // evicted from, which invalidates references into the region.
    // When the value returned by this method doesn't change, references remain valid.
    uint64_t reclaim_counter() const noexcept {
        return allocator().invalidate_counter();
    }

    // Will cause subsequent calls to evictable_occupancy() to report empty occupancy.
    void ground_evictable_occupancy();

    // Follows region's occupancy in the parent region group. Less fine-grained than occupancy().
    // After ground_evictable_occupancy() is called returns 0.
    occupancy_stats evictable_occupancy() const noexcept;

    // Makes this region an evictable region. Supplied function will be called
    // when data from this region needs to be evicted in order to reclaim space.
    // The function should free some space from this region.
    void make_evictable(eviction_fn) noexcept;

    const eviction_fn& evictor() const noexcept;

    uint64_t id() const noexcept;

    std::unordered_map<std::string, uint64_t> collect_stats() const;

    friend class allocating_section;
};

// Forces references into the region to remain valid as long as this guard is
// live by disabling compaction and eviction.
// Can be nested.
struct reclaim_lock {
    region& _region;
    bool _prev;
    reclaim_lock(region& r) noexcept
        : _region(r)
        , _prev(r.reclaiming_enabled())
    {
        _region.set_reclaiming_enabled(false);
    }
    ~reclaim_lock() {
        _region.set_reclaiming_enabled(_prev);
    }
};

// Utility for running critical sections which need to lock some region and
// also allocate LSA memory. The object learns from failures how much it
// should reserve up front in order to not cause allocation failures.
class allocating_section {
    // Do not decay below these minimal values
    static constexpr size_t s_min_lsa_reserve = 1;
    static constexpr size_t s_min_std_reserve = 1024;
    static constexpr uint64_t s_bytes_per_decay = 10'000'000'000;
    static constexpr unsigned s_segments_per_decay = 100'000;
    size_t _lsa_reserve = s_min_lsa_reserve; // in segments
    size_t _std_reserve = s_min_std_reserve; // in bytes
    size_t _minimum_lsa_emergency_reserve = 0;
    int64_t _remaining_std_bytes_until_decay = s_bytes_per_decay;
    int _remaining_lsa_segments_until_decay = s_segments_per_decay;
private:
    struct guard {
        tracker::impl& _tracker;
        size_t _prev;
        explicit guard(tracker::impl& tracker) noexcept;
        ~guard();
    };
    void reserve(tracker::impl& tracker);
    void maybe_decay_reserve() noexcept;
    void on_alloc_failure(logalloc::region&);
public:

    void set_lsa_reserve(size_t) noexcept;
    void set_std_reserve(size_t) noexcept;

    //
    // Reserves standard allocator and LSA memory for subsequent operations that
    // have to be performed with memory reclamation disabled.
    //
    // Throws std::bad_alloc when reserves can't be increased to a sufficient level.
    //
    template<typename Func>
    decltype(auto) with_reserve(region& r, Func&& fn) {
        auto prev_lsa_reserve = _lsa_reserve;
        auto prev_std_reserve = _std_reserve;
        try {
            guard g(r.get_tracker().get_impl());
            _minimum_lsa_emergency_reserve = g._prev;
            reserve(r.get_tracker().get_impl());
            return fn();
        } catch (const std::bad_alloc&) {
            // roll-back limits to protect against pathological requests
            // preventing future requests from succeeding.
            _lsa_reserve = prev_lsa_reserve;
            _std_reserve = prev_std_reserve;
            throw;
        }
    }

    //
    // Invokes func with reclaim_lock on region r. If LSA allocation fails
    // inside func it is retried after increasing LSA segment reserve. The
    // memory reserves are increased with region lock off allowing for memory
    // reclamation to take place in the region.
    //
    // References in the region are invalidated when allocating section is re-entered
    // on allocation failure.
    //
    // Throws std::bad_alloc when reserves can't be increased to a sufficient level.
    //
    template<typename Func>
    decltype(auto) with_reclaiming_disabled(logalloc::region& r, Func&& fn) {
        SCYLLA_ASSERT(r.reclaiming_enabled());
        maybe_decay_reserve();
        while (true) {
            try {
                logalloc::reclaim_lock _(r);
                memory::disable_abort_on_alloc_failure_temporarily dfg;
                return fn();
            } catch (const utils::memory_limit_reached&) {
                // Do not retry, bumping reserves won't help.
                // The read reached a memory limit in the semaphore and is being
                // terminated.
                throw;
            } catch (const std::bad_alloc&) {
                on_alloc_failure(r);
            }
        }
    }

    //
    // Reserves standard allocator and LSA memory and
    // invokes func with reclaim_lock on region r. If LSA allocation fails
    // inside func it is retried after increasing LSA segment reserve. The
    // memory reserves are increased with region lock off allowing for memory
    // reclamation to take place in the region.
    //
    // References in the region are invalidated when allocating section is re-entered
    // on allocation failure.
    //
    // Throws std::bad_alloc when reserves can't be increased to a sufficient level.
    //
    template<typename Func>
    decltype(auto) operator()(logalloc::region& r, Func&& func) {
        return with_reserve(r, [this, &r, &func] {
            return with_reclaiming_disabled(r, func);
        });
    }
};

future<> prime_segment_pool(size_t available_memory, size_t min_free_memory);

// Use the segment pool appropriate for the standard allocator.
//
// In debug mode, this will use the release standard allocator store.
// Call once, when initializing the application, before any LSA allocation takes place.
future<> use_standard_allocator_segment_pool_backend(size_t available_memory);

}

template <> struct fmt::formatter<logalloc::occupancy_stats> : fmt::formatter<string_view> {
    auto format(const logalloc::occupancy_stats& stats, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{:.2f}%, {:d} / {:d} [B]",
                              stats.used_fraction() * 100, stats.used_space(), stats.total_space());
    }
};
