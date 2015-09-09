/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include <bits/unique_ptr.h>
#include <seastar/core/scollectd.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/shared_ptr.hh>
#include "allocation_strategy.hh"

namespace logalloc {

struct occupancy_stats;
class region;
class region_impl;
class allocating_section;

constexpr int segment_size_shift = 18; // 256K; see #151, #152
constexpr size_t segment_size = 1 << segment_size_shift;

//
// Frees some amount of objects from the region to which it's attached.
//
// This should eventually stop given no new objects are added:
//
//     while (eviction_fn() == memory::reclaiming_result::reclaimed_something) ;
//
using eviction_fn = std::function<memory::reclaiming_result()>;

// Groups regions for the purpose of statistics.  Can be nested.
class region_group {
    region_group* _parent = nullptr;
    size_t _total_memory = 0;
    std::vector<region_group*> _subgroups;
    std::vector<region_impl*> _regions;
public:
    region_group() = default;
    region_group(region_group* parent) : _parent(parent) {
        if (_parent) {
            _parent->add(this);
        }
    }
    region_group(region_group&& o) noexcept;
    region_group(const region_group&) = delete;
    ~region_group() {
        if (_parent) {
            _parent->del(this);
        }
    }
    region_group& operator=(const region_group&) = delete;
    region_group& operator=(region_group&&) = delete;
    size_t memory_used() const {
        return _total_memory;
    }
    void update(ssize_t delta) {
        auto rg = this;
        while (rg) {
            rg->_total_memory += delta;
            rg = rg->_parent;
        }
    }
private:
    void add(region_group* child);
    void del(region_group* child);
    void add(region_impl* child);
    void del(region_impl* child);
    friend class region_impl;
};

// Controller for all LSA regions. There's one per shard.
class tracker {
public:
    class impl;
private:
    std::unique_ptr<impl> _impl;
    memory::reclaimer _reclaimer;
    friend class region;
    friend class region_impl;
public:
    tracker();
    ~tracker();

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
    // Invalidates references to objects in all compactible and evictable regions.
    void full_compaction();

    // Returns aggregate statistics for all pools.
    occupancy_stats occupancy();
};

tracker& shard_tracker();

// Monoid representing pool occupancy statistics.
// Naturally ordered so that sparser pools come fist.
// All sizes in bytes.
class occupancy_stats {
    size_t _free_space;
    size_t _total_space;
public:
    occupancy_stats() : _free_space(0), _total_space(0) {}

    occupancy_stats(size_t free_space, size_t total_space)
        : _free_space(free_space), _total_space(total_space) { }

    bool operator<(const occupancy_stats& other) const {
        return used_fraction() < other.used_fraction();
    }

    friend occupancy_stats operator+(const occupancy_stats& s1, const occupancy_stats& s2) {
        occupancy_stats result(s1);
        result += s2;
        return result;
    }

    friend occupancy_stats operator-(const occupancy_stats& s1, const occupancy_stats& s2) {
        occupancy_stats result(s1);
        result -= s2;
        return result;
    }

    occupancy_stats& operator+=(const occupancy_stats& other) {
        _total_space += other._total_space;
        _free_space += other._free_space;
        return *this;
    }

    occupancy_stats& operator-=(const occupancy_stats& other) {
        _total_space -= other._total_space;
        _free_space -= other._free_space;
        return *this;
    }

    size_t used_space() const {
        return _total_space - _free_space;
    }

    size_t free_space() const {
        return _free_space;
    }

    size_t total_space() const {
        return _total_space;
    }

    float used_fraction() const {
        return _total_space ? float(used_space()) / total_space() : 0;
    }

    friend std::ostream& operator<<(std::ostream&, const occupancy_stats&);
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
    shared_ptr<impl> _impl;
public:
    region();
    explicit region(region_group& group);
    ~region();
    region(region&& other);
    region& operator=(region&& other);
    region(const region& other) = delete;

    occupancy_stats occupancy() const;

    allocation_strategy& allocator();

    // Merges another region into this region. The other region is left empty.
    // Doesn't invalidate references to allocated objects.
    void merge(region& other);

    // Compacts everything. Mainly for testing.
    // Invalidates references to allocated objects.
    void full_compaction();

    // Changes the reclaimability state of this region. When region is not
    // reclaimable, it won't be considered by tracker::reclaim(). By default region is
    // reclaimable after construction.
    void set_reclaiming_enabled(bool);

    // Returns the reclaimability state of this region.
    bool reclaiming_enabled() const;

    // Returns a value which is increased when this region is either compacted or
    // evicted from, which invalidates references into the region.
    // When the value returned by this method doesn't change, references remain valid.
    uint64_t reclaim_counter() const;

    // Makes this region an evictable region. Supplied function will be called
    // when data from this region needs to be evicted in order to reclaim space.
    // The function should free some space from this region.
    void make_evictable(eviction_fn);

    friend class region_group;
    friend class allocating_section;
};

// Forces references into the region to remain valid as long as this guard is
// live by disabling compaction and eviction.
// Can be nested.
struct reclaim_lock {
    region& _region;
    bool _prev;
    reclaim_lock(region& r)
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
    size_t _lsa_reserve = 10; // in segments
    size_t _std_reserve = 1024; // in bytes
private:
    struct guard {
        size_t _prev;
        guard();
        ~guard();
        void enter(allocating_section&);
    };
    void on_alloc_failure();
public:
    //
    // Invokes func with reclaim_lock on region r. If LSA allocation fails
    // inside func it is retried after increasing LSA segment reserve. The
    // memory reserves are increased with region lock off allowing for memory
    // reclamation to take place in the region.
    //
    // Throws std::bad_alloc when reserves can't be increased to a sufficient level.
    //
    template<typename Func>
    auto operator()(logalloc::region& r, Func&& func) {
        auto prev_lsa_reserve = _lsa_reserve;
        auto prev_std_reserve = _std_reserve;
        try {
            while (true) {
                assert(r.reclaiming_enabled());
                guard g;
                g.enter(*this);
                try {
                    logalloc::reclaim_lock _(r);
                    return func();
                } catch (const std::bad_alloc&) {
                    on_alloc_failure();
                }
            }
        } catch (const std::bad_alloc&) {
            // roll-back limits to protect against pathological requests
            // preventing future requests from succeeding.
            _lsa_reserve = prev_lsa_reserve;
            _std_reserve = prev_std_reserve;
            throw;
        }
    }
};

}
