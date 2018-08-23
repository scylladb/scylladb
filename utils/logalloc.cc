/*
 * Copyright (C) 2015 ScyllaDB
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

#include <boost/range/algorithm/heap_algorithm.hpp>
#include <boost/range/algorithm/remove.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/heap/binomial_heap.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/intrusive/slist.hpp>
#include <boost/range/adaptors.hpp>
#include <stack>

#include <seastar/core/memory.hh>
#include <seastar/core/align.hh>
#include <seastar/core/print.hh>
#include <seastar/core/metrics.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/backtrace.hh>

#include "utils/logalloc.hh"
#include "log.hh"
#include "utils/dynamic_bitset.hh"
#include "utils/log_heap.hh"

#include <random>

namespace bi = boost::intrusive;

standard_allocation_strategy standard_allocation_strategy_instance;

namespace {

#ifdef DEBUG_LSA_SANITIZER

class migrators : public enable_lw_shared_from_this<migrators> {
public:
    static constexpr uint32_t maximum_id_value = std::numeric_limits<uint32_t>::max() / 2;
private:
    struct migrator_entry {
        const migrate_fn_type* _migrator;
        saved_backtrace _registration;
        saved_backtrace _deregistration;
    };
    std::unordered_map<uint32_t, migrator_entry> _migrators;
    std::default_random_engine _random_engine { std::random_device()() };
    std::uniform_int_distribution<uint32_t> _id_distribution { 0, maximum_id_value };

    static logging::logger _logger;
private:
    void on_error() { abort(); }
public:
    uint32_t add(const migrate_fn_type* m) {
        while (true) {
            auto id = _id_distribution(_random_engine);
            if (_migrators.count(id)) {
                continue;
            }
            _migrators.emplace(id, migrator_entry { m, current_backtrace(), {} });
            return id;
        }
    }
    void remove(uint32_t idx) {
        auto it = _migrators.find(idx);
        if (it == _migrators.end()) {
            _logger.error("Attempting to deregister migrator id {} which was never registered:\n{}",
                          idx, current_backtrace());
            on_error();
        }
        if (!it->second._migrator) {
            _logger.error("Attempting to double deregister migrator id {}:\n{}\n"
                          "Previously deregistered at:\n{}\nRegistered at:\n{}",
                          idx, current_backtrace(), it->second._deregistration,
                          it->second._registration);
            on_error();
        }
        it->second._migrator = nullptr;
        it->second._deregistration = current_backtrace();
    }
    const migrate_fn_type*& operator[](uint32_t idx) {
        auto it = _migrators.find(idx);
        if (it == _migrators.end()) {
            _logger.error("Attempting to use migrator id {} that was never registered:\n{}",
                          idx, current_backtrace());
            on_error();
        }
        if (!it->second._migrator) {
            _logger.error("Attempting to use deregistered migrator id {}:\n{}\n"
                          "Deregistered at:\n{}\nRegistered at:\n{}",
                          idx, current_backtrace(), it->second._deregistration,
                          it->second._registration);
            on_error();
        }
        return it->second._migrator;
    }
};

logging::logger migrators::_logger("lsa-migrator-sanitizer");

#else

class migrators : public enable_lw_shared_from_this<migrators> {
    std::vector<const migrate_fn_type*> _migrators;
    std::deque<uint32_t> _unused_ids;
public:
    static constexpr uint32_t maximum_id_value = std::numeric_limits<uint32_t>::max() / 2;

    uint32_t add(const migrate_fn_type* m) {
        if (!_unused_ids.empty()) {
            auto idx = _unused_ids.front();
            _unused_ids.pop_front();
            _migrators[idx] = m;
            return idx;
        }
        _migrators.push_back(m);
        return _migrators.size() - 1;
    }
    void remove(uint32_t idx) {
        _unused_ids.push_back(idx);
    }
    const migrate_fn_type*& operator[](uint32_t idx) {
        return _migrators[idx];
    }
};

#endif

static
migrators&
static_migrators() {
    static thread_local lw_shared_ptr<migrators> obj = make_lw_shared<migrators>();
    return *obj;
}

}

namespace debug {

thread_local migrators* static_migrators = &::static_migrators();

}


uint32_t
migrate_fn_type::register_migrator(migrate_fn_type* m) {
    auto& migrators = *debug::static_migrators;
    auto idx = migrators.add(m);
    m->_migrators = migrators.shared_from_this();
    return idx;
}

void
migrate_fn_type::unregister_migrator(uint32_t index) {
    static_migrators().remove(index);
}

namespace logalloc {

#ifdef DEBUG_LSA_SANITIZER

class region_sanitizer {
    struct allocation {
        size_t size;
        saved_backtrace backtrace;
    };
private:
    static logging::logger logger;

    bool _broken = false;
    std::unordered_map<const void*, allocation> _allocations;
private:
    template<typename Function>
    void run_and_handle_errors(Function&& fn) noexcept {
        memory::disable_failure_guard dfg;
        if (_broken) {
            return;
        }
        try {
            fn();
        } catch (...) {
            logger.error("Internal error, disabling the sanitizer: {}", std::current_exception());
            _broken = true;
            _allocations.clear();
        }
    }
private:
    void on_error() { abort(); }
public:
    void on_region_destruction() noexcept {
        run_and_handle_errors([&] {
            if (_allocations.empty()) {
                return;
            }
            for (auto [ptr, alloc] : _allocations) {
                logger.error("Leaked {} byte object at {} allocated from:\n{}",
                             alloc.size, ptr, alloc.backtrace);
            }
            on_error();
        });
    }
    void on_allocation(const void* ptr, size_t size) noexcept {
        run_and_handle_errors([&] {
            auto [ it, success ] = _allocations.emplace(ptr, allocation { size, current_backtrace() });
            if (!success) {
                logger.error("Attempting to allocate an {} byte object at an already occupied address {}:\n{}\n"
                             "Previous allocation of {} bytes:\n{}",
                             ptr, size, current_backtrace(), it->second.size, it->second.backtrace);
                on_error();
            }
        });
    }
    void on_free(const void* ptr, size_t size) noexcept {
        run_and_handle_errors([&] {
            auto it = _allocations.find(ptr);
            if (it == _allocations.end()) {
                logger.error("Attempting to free an object at {} (size: {}) that does not exist\n{}",
                             ptr, size, current_backtrace());
                on_error();
            }
            if (it->second.size != size) {
                logger.error("Mismatch between allocation and deallocation size of object at {}: {} vs. {}:\n{}\n"
                             "Allocated at:\n{}",
                             ptr, it->second.size, size, current_backtrace(), it->second.backtrace);
                on_error();
            }
            _allocations.erase(it);
        });
    }
    void on_migrate(const void* src, size_t size, const void* dst) noexcept {
        run_and_handle_errors([&] {
            auto it_src = _allocations.find(src);
            if (it_src == _allocations.end()) {
                logger.error("Attempting to migrate an object at {} (size: {}) that does not exist",
                             src, size);
                on_error();
            }
            if (it_src->second.size != size) {
                logger.error("Mismatch between allocation and migration size of object at {}: {} vs. {}\n"
                             "Allocated at:\n{}",
                             src, it_src->second.size, size, it_src->second.backtrace);
                on_error();
            }
            auto [ it_dst, success ] = _allocations.emplace(dst, std::move(it_src->second));
            if (!success) {
                logger.error("Attempting to migrate an {} byte object to an already occupied address {}:\n"
                             "Migrated object allocated from:\n{}\n"
                             "Previous allocation of {} bytes at the destination:\n{}",
                             size, dst, it_src->second.backtrace, it_dst->second.size, it_dst->second.backtrace);
                on_error();
            }
            _allocations.erase(it_src);
        });
    }
    void merge(region_sanitizer& other) noexcept {
        run_and_handle_errors([&] {
            _broken = other._broken;
            if (_broken) {
                _allocations.clear();
            } else {
                _allocations.merge(other._allocations);
                if (!other._allocations.empty()) {
                    for (auto [ptr, o_alloc] : other._allocations) {
                        auto& alloc = _allocations.at(ptr);
                        logger.error("Conflicting allocations at address {} in merged regions\n"
                                     "{} bytes allocated from:\n{}\n"
                                     "{} bytes allocated from:\n{}",
                                     ptr, alloc.size, alloc.backtrace, o_alloc.size, o_alloc.backtrace);
                    }
                    on_error();
                }
            }
        });
    }
};

logging::logger region_sanitizer::logger("lsa-sanitizer");

#else

struct region_sanitizer {
    void on_region_destruction() noexcept { }
    void on_allocation(const void*, size_t) noexcept { }
    void on_free(const void* ptr, size_t size) noexcept { }
    void on_migrate(const void*, size_t, const void*) noexcept { }
    void merge(region_sanitizer&) noexcept { }
};

#endif

struct segment;

static logging::logger llogger("lsa");
static logging::logger timing_logger("lsa-timing");
static thread_local tracker tracker_instance;

using clock = std::chrono::steady_clock;

class tracker::impl {
    std::vector<region::impl*> _regions;
    seastar::metrics::metric_groups _metrics;
    bool _reclaiming_enabled = true;
    size_t _reclamation_step = 1;
    bool _abort_on_bad_alloc = false;
private:
    // Prevents tracker's reclaimer from running while live. Reclaimer may be
    // invoked synchronously with allocator. This guard ensures that this
    // object is not re-entered while inside one of the tracker's methods.
    struct reclaiming_lock {
        impl& _ref;
        bool _prev;
        reclaiming_lock(impl& ref)
            : _ref(ref)
            , _prev(ref._reclaiming_enabled)
        {
            _ref._reclaiming_enabled = false;
        }
        ~reclaiming_lock() {
            _ref._reclaiming_enabled = _prev;
        }
    };
    friend class tracker_reclaimer_lock;
public:
    impl();
    ~impl();
    void register_region(region::impl*);
    void unregister_region(region::impl*) noexcept;
    size_t reclaim(size_t bytes);
    reactor::idle_cpu_handler_result compact_on_idle(reactor::work_waiting_on_reactor check_for_work);
    size_t compact_and_evict(size_t bytes);
    size_t compact_and_evict_locked(size_t bytes);
    void full_compaction();
    void reclaim_all_free_segments();
    occupancy_stats region_occupancy();
    occupancy_stats occupancy();
    size_t non_lsa_used_space();
    void set_reclamation_step(size_t step_in_segments) { _reclamation_step = step_in_segments; }
    size_t reclamation_step() const { return _reclamation_step; }
    void enable_abort_on_bad_alloc() { _abort_on_bad_alloc = true; }
    bool should_abort_on_bad_alloc() const { return _abort_on_bad_alloc; }
};

class tracker_reclaimer_lock {
    tracker::impl::reclaiming_lock _lock;
public:
    tracker_reclaimer_lock() : _lock(shard_tracker().get_impl()) { }
};

tracker::tracker()
    : _impl(std::make_unique<impl>())
    , _reclaimer([this] { return reclaim(); }, memory::reclaimer_scope::sync)
{ }

tracker::~tracker() {
}

size_t tracker::reclaim(size_t bytes) {
    return _impl->reclaim(bytes);
}

reactor::idle_cpu_handler_result tracker::compact_on_idle(reactor::work_waiting_on_reactor check_for_work) {
    return _impl->compact_on_idle(check_for_work);
}

occupancy_stats tracker::region_occupancy() {
    return _impl->region_occupancy();
}

occupancy_stats tracker::occupancy() {
    return _impl->occupancy();
}

size_t tracker::non_lsa_used_space() const {
    return _impl->non_lsa_used_space();
}

void tracker::full_compaction() {
    return _impl->full_compaction();
}

void tracker::reclaim_all_free_segments() {
    return _impl->reclaim_all_free_segments();
}

tracker& shard_tracker() {
    return tracker_instance;
}

struct segment {
    static constexpr int size_shift = segment_size_shift;
    using size_type = std::conditional_t<(size_shift < 16), uint16_t, uint32_t>;
    static constexpr size_t size = segment_size;

    uint8_t data[size];

    segment() noexcept { }

    template<typename T = void>
    const T* at(size_t offset) const {
        return reinterpret_cast<const T*>(data + offset);
    }

    template<typename T = void>
    T* at(size_t offset) {
        return reinterpret_cast<T*>(data + offset);
    }

    bool is_empty() const;
    void record_alloc(size_type size);
    void record_free(size_type size);
    occupancy_stats occupancy() const;

#ifndef SEASTAR_DEFAULT_ALLOCATOR
    static void* operator new(size_t size) = delete;
    static void* operator new(size_t, void* ptr) noexcept { return ptr; }
    static void operator delete(void* ptr) = delete;
#endif
};

static constexpr size_t max_managed_object_size = segment_size * 0.1;
static constexpr auto max_used_space_ratio_for_compaction = 0.85;
static constexpr size_t max_used_space_for_compaction = segment_size * max_used_space_ratio_for_compaction;
static constexpr size_t min_free_space_for_compaction = segment_size - max_used_space_for_compaction;

static_assert(min_free_space_for_compaction >= max_managed_object_size,
    "Segments which cannot fit max_managed_object_size must not be considered compactible for the sake of forward progress of compaction");

// Since we only compact if there's >= min_free_space_for_compaction of free space,
// we use min_free_space_for_compaction as the histogram's minimum size and put
// everything below that value in the same bucket.
extern constexpr log_heap_options segment_descriptor_hist_options(min_free_space_for_compaction, 3, segment_size);

struct segment_descriptor : public log_heap_hook<segment_descriptor_hist_options> {
    segment::size_type _free_space;
    region::impl* _region;

    segment_descriptor()
        : _region(nullptr)
    { }

    bool is_empty() const {
        return _free_space == segment::size;
    }

    occupancy_stats occupancy() const {
        return { _free_space, segment::size };
    }

    void record_alloc(segment::size_type size) {
        _free_space -= size;
    }

    void record_free(segment::size_type size) {
        _free_space += size;
    }
};

using segment_descriptor_hist = log_heap<segment_descriptor, segment_descriptor_hist_options>;

#ifndef SEASTAR_DEFAULT_ALLOCATOR

// Segment pool implementation for the seastar allocator.
// Stores segment descriptors in a vector which is indexed using most significant
// bits of segment address.
//
// We prefer using high-address segments, and returning low-address segments to the seastar
// allocator in order to segregate lsa and non-lsa memory, to reduce fragmentation.
class segment_pool {
    memory::memory_layout _layout;
    uintptr_t _segments_base; // The address of the first segment
    std::vector<segment_descriptor> _segments;
    size_t _segments_in_use{};
    utils::dynamic_bitset _lsa_owned_segments_bitmap; // owned by this
    utils::dynamic_bitset _lsa_free_segments_bitmap;  // owned by this, but not in use
    size_t _free_segments = 0;
    size_t _current_emergency_reserve_goal = 1;
    size_t _emergency_reserve_max = 30;
    bool _allocation_failure_flag = false;
    size_t _non_lsa_memory_in_use = 0;
    size_t _non_lsa_reserve = 0;
    // Invariants - a segment is in one of the following states:
    //   In use by some region
    //     - set in _lsa_owned_segments_bitmap
    //     - clear in _lsa_free_segments_bitmap
    //     - counted in _segments_in_use
    //   Free:
    //     - set in _lsa_owned_segments_bitmap
    //     - set in _lsa_free_segments_bitmap
    //     - counted in _unreserved_free_segments
    //   Non-lsa:
    //     - clear everywhere
private:
    segment* allocate_segment(size_t reserve);
    // reclamation_step is in segment units
    segment* allocate_segment(size_t reserve, size_t reclamation_step);
    void deallocate_segment(segment* seg);
    friend void* segment::operator new(size_t);
    friend void segment::operator delete(void*);

    segment* allocate_or_fallback_to_reserve();
    void free_or_restore_to_reserve(segment* seg) noexcept;
    segment* segment_from_idx(size_t idx) const {
        return reinterpret_cast<segment*>(_segments_base) + idx;
    }
    size_t idx_from_segment(segment* seg) const {
        return seg - reinterpret_cast<segment*>(_segments_base);
    }
    size_t max_segments() const {
        return (_layout.end - _segments_base) / segment::size;
    }
    bool can_allocate_more_memory(size_t size) {
        return memory::stats().free_memory() >= _non_lsa_reserve + size;
    }
public:
    segment_pool();
    void prime(size_t available_memory, size_t min_free_memory);
    segment* new_segment(region::impl* r);
    segment_descriptor& descriptor(const segment*);
    // Returns segment containing given object or nullptr.
    segment* containing_segment(const void* obj) const;
    segment* segment_from(const segment_descriptor& desc);
    void free_segment(segment*) noexcept;
    void free_segment(segment*, segment_descriptor&) noexcept;
    size_t segments_in_use() const;
    size_t current_emergency_reserve_goal() const { return _current_emergency_reserve_goal; }
    void set_emergency_reserve_max(size_t new_size) { _emergency_reserve_max = new_size; }
    size_t emergency_reserve_max() { return _emergency_reserve_max; }
    void set_current_emergency_reserve_goal(size_t goal) { _current_emergency_reserve_goal = goal; }
    void clear_allocation_failure_flag() { _allocation_failure_flag = false; }
    bool allocation_failure_flag() { return _allocation_failure_flag; }
    void refill_emergency_reserve();
    void update_non_lsa_memory_in_use(ssize_t n) {
        _non_lsa_memory_in_use += n;
    }
    size_t non_lsa_memory_in_use() const {
        return _non_lsa_memory_in_use;
    }
    size_t total_memory_in_use() const {
        return _non_lsa_memory_in_use + _segments_in_use * segment::size;
    }
    struct reservation_goal;
    void set_region(const segment* seg, region::impl* r) {
        set_region(descriptor(seg), r);
    }
    void set_region(segment_descriptor& desc, region::impl* r) {
        desc._region = r;
    }
    bool migrate_segment(segment* src, segment* dst);
    size_t reclaim_segments(size_t target);
    void reclaim_all_free_segments() {
        reclaim_segments(std::numeric_limits<size_t>::max());
    }

    struct stats {
        size_t segments_migrated;
        size_t segments_compacted;
        uint64_t memory_allocated;
        uint64_t memory_compacted;
    };
private:
    stats _stats{};
public:
    const stats& statistics() const { return _stats; }
    void on_segment_migration() { _stats.segments_migrated++; }
    void on_segment_compaction(size_t used_size);
    void on_memory_allocation(size_t size);
    size_t unreserved_free_segments() const { return _free_segments - std::min(_free_segments, _emergency_reserve_max); }
    size_t free_segments() const { return _free_segments; }
};

size_t segment_pool::reclaim_segments(size_t target) {
    // Reclaimer tries to release segments occupying lower parts of the address
    // space.

    llogger.debug("Trying to reclaim {} segments", target);

    // Reclamation. Migrate segments to higher addresses and shrink segment pool.
    size_t reclaimed_segments = 0;

    // We may fail to reclaim because a region has reclaim disabled (usually because
    // it is in an allocating_section. Failed reclaims can cause high CPU usage
    // if all of the lower addresses happen to be in a reclaim-disabled region (this
    // is somewhat mitigated by the fact that checking for reclaim disabled is very
    // cheap), but worse, failing a segment reclaim can lead to reclaimed memory
    // being fragmented.  This results in the original allocation continuing to fail.
    //
    // To combat that, we limit the number of failed reclaims. If we reach the limit,
    // we fail the reclaim.  The surrounding allocating_section will release the
    // reclaim_lock, and increase reserves, which will result in reclaim being
    // retried with all regions being reclaimable, and succeed in allocating
    // contiguous memory.
    size_t failed_reclaims_allowance = 10;

    for (size_t src_idx = _lsa_owned_segments_bitmap.find_first_set();
            reclaimed_segments != target && src_idx != utils::dynamic_bitset::npos
                    && _free_segments > _current_emergency_reserve_goal;
            src_idx = _lsa_owned_segments_bitmap.find_next_set(src_idx)) {
        auto src = segment_from_idx(src_idx);
        if (!_lsa_free_segments_bitmap.test(src_idx)) {
            auto dst_idx = _lsa_free_segments_bitmap.find_last_set();
            if (dst_idx == utils::dynamic_bitset::npos || dst_idx <= src_idx) {
                break;
            }
            assert(_lsa_owned_segments_bitmap.test(dst_idx));
            auto could_migrate = migrate_segment(src, segment_from_idx(dst_idx));
            if (!could_migrate) {
                if (--failed_reclaims_allowance == 0) {
                    break;
                }
                continue;
            }
        }
        _lsa_free_segments_bitmap.clear(src_idx);
        _lsa_owned_segments_bitmap.clear(src_idx);
        src->~segment();
        ::free(src);
        ++reclaimed_segments;
        --_free_segments;
    }

    llogger.debug("Reclaimed {} segments (requested {})", reclaimed_segments, target);
    return reclaimed_segments;
}

segment* segment_pool::allocate_segment(size_t reserve) {
    return allocate_segment(reserve, shard_tracker().reclamation_step());
}

segment* segment_pool::allocate_segment(size_t reserve, size_t reclamation_step)
{
    //
    // When allocating a segment we want to avoid:
    //  - LSA and general-purpose allocator shouldn't constantly fight each
    //    other for every last bit of memory
    //
    // allocate_segment() always works with LSA reclaimer disabled.
    // 1. Firstly, the algorithm tries to allocate an lsa-owned but free segment
    // 2. If no free segmented is available, a new segment is allocated from the
    //    system allocator. However, if the free memory is below set threshold
    //    this step is skipped.
    // 3. Finally, the algorithm ties to compact and evict data stored in LSA
    //    memory in order to reclaim enough segments.
    //
    do {
        tracker_reclaimer_lock rl;
        if (_free_segments > reserve) {
            auto free_idx = _lsa_free_segments_bitmap.find_last_set();
            _lsa_free_segments_bitmap.clear(free_idx);
            auto seg = segment_from_idx(free_idx);
            --_free_segments;
            return seg;
        }
        if (can_allocate_more_memory(segment::size)) {
            auto p = aligned_alloc(segment::size, segment::size);
            if (!p) {
                continue;
            }
            auto seg = new (p) segment;
            auto idx = idx_from_segment(seg);
            _lsa_owned_segments_bitmap.set(idx);
            return seg;
        }
    } while (shard_tracker().get_impl().compact_and_evict(reclamation_step * segment::size));
    if (shard_tracker().should_abort_on_bad_alloc()) {
        llogger.error("Aborting due to segment allocation failure");
        abort();
    }
    return nullptr;
}

void segment_pool::deallocate_segment(segment* seg)
{
    assert(_lsa_owned_segments_bitmap.test(idx_from_segment(seg)));
    _lsa_free_segments_bitmap.set(idx_from_segment(seg));
    _free_segments++;
}

void segment_pool::refill_emergency_reserve() {
    while (_free_segments < _emergency_reserve_max) {
        auto seg = allocate_segment(_emergency_reserve_max, _emergency_reserve_max - _free_segments);
        if (!seg) {
            throw std::bad_alloc();
        }
        ++_segments_in_use;
        free_segment(seg);
    }
}

segment_descriptor&
segment_pool::descriptor(const segment* seg) {
    uintptr_t seg_addr = reinterpret_cast<uintptr_t>(seg);
    uintptr_t index = (seg_addr - _segments_base) >> segment::size_shift;
    return _segments[index];
}

segment*
segment_pool::containing_segment(const void* obj) const {
    auto addr = reinterpret_cast<uintptr_t>(obj);
    auto offset = addr & (segment::size - 1);
    auto index = (addr - _segments_base) >> segment::size_shift;
    auto& desc = _segments[index];
    if (desc._region) {
        return reinterpret_cast<segment*>(addr - offset);
    } else {
        return nullptr;
    }
}

segment*
segment_pool::segment_from(const segment_descriptor& desc) {
    assert(desc._region);
    auto index = &desc - &_segments[0];
    return reinterpret_cast<segment*>(_segments_base + (index << segment::size_shift));
}

segment*
segment_pool::allocate_or_fallback_to_reserve() {
    auto seg = allocate_segment(_current_emergency_reserve_goal);
    if (!seg) {
        _allocation_failure_flag = true;
        throw std::bad_alloc();
    }
    return seg;
}

segment*
segment_pool::new_segment(region::impl* r) {
    auto seg = allocate_or_fallback_to_reserve();
    ++_segments_in_use;
    segment_descriptor& desc = descriptor(seg);
    desc._free_space = segment::size;
    desc._region = r;
    return seg;
}

void segment_pool::free_segment(segment* seg) noexcept {
    free_segment(seg, descriptor(seg));
}

void segment_pool::free_segment(segment* seg, segment_descriptor& desc) noexcept {
    llogger.trace("Releasing segment {}", seg);
    desc._region = nullptr;
    deallocate_segment(seg);
    --_segments_in_use;
}

segment_pool::segment_pool()
    : _layout(memory::get_memory_layout())
    , _segments_base(align_down(_layout.start, (uintptr_t)segment::size))
    , _segments(max_segments())
    , _lsa_owned_segments_bitmap(max_segments())
    , _lsa_free_segments_bitmap(max_segments())
{
}

void segment_pool::prime(size_t available_memory, size_t min_free_memory) {
    auto old_emergency_reserve = std::exchange(_emergency_reserve_max, std::numeric_limits<size_t>::max());
    try {
        // Allocate all of memory so that we occupy the top part. Afterwards, we'll start
        // freeing from the bottom.
        _non_lsa_reserve = 0;
        refill_emergency_reserve();
    } catch (std::bad_alloc&) {
        _emergency_reserve_max = old_emergency_reserve;
    }
    // We want to leave more free memory than just min_free_memory() in order to reduce
    // the frequency of expensive segment-migrating reclaim() called by the seastar allocator.
    size_t min_gap = 1 * 1024 * 1024;
    size_t max_gap = 64 * 1024 * 1024;
    size_t gap = std::min(max_gap, std::max(available_memory / 16, min_gap));
    _non_lsa_reserve = min_free_memory + gap;
    // Since the reclaimer is not yet in place, free some low memory for general use
    reclaim_segments(_non_lsa_reserve / segment::size);
}

#else

// Segment pool version for the standard allocator. Slightly less efficient
// than the version for seastar's allocator.
class segment_pool {
    class segment_deleter {
        segment_pool* _pool;
    public:
        explicit segment_deleter(segment_pool* pool) : _pool(pool) {}
        void operator()(segment* seg) const noexcept {
            if (seg) {
                ::free(seg);
                _pool->_std_memory_available += segment::size;
            }
        }
    };
    std::unordered_map<const segment*, segment_descriptor> _segments;
    std::unordered_map<const segment_descriptor*, segment*> _segment_descs;
    std::stack<std::unique_ptr<segment, segment_deleter>> _free_segments;
    size_t _segments_in_use{};
    size_t _non_lsa_memory_in_use = 0;
    size_t _std_memory_available = size_t(1) << 30; // emulate 1GB per shard
    friend segment_deleter;
public:
    void prime(size_t available_memory, size_t min_free_memory) {}
    segment* new_segment(region::impl* r) {
        if (_free_segments.empty()) {
            if (_std_memory_available < segment::size) {
                throw std::bad_alloc();
            }
            std::unique_ptr<segment, segment_deleter> seg{new (with_alignment(segment::size)) segment, segment_deleter(this)};
            _std_memory_available -= segment::size;
            _free_segments.push(std::move(seg));
        }
        ++_segments_in_use;
        auto seg = _free_segments.top().release();
        _free_segments.pop();
        assert((reinterpret_cast<uintptr_t>(seg) & (sizeof(segment) - 1)) == 0);
        segment_descriptor& desc = _segments[seg];
        desc._free_space = segment::size;
        desc._region = r;
        _segment_descs[&desc] = seg;
        return seg;
    }
    segment_descriptor& descriptor(const segment* seg) {
        auto i = _segments.find(seg);
        if (i != _segments.end()) {
            return i->second;
        } else {
            segment_descriptor& desc = _segments[seg];
            desc._region = nullptr;
            return desc;
        }
    }
    segment* segment_from(segment_descriptor& desc) {
        auto i = _segment_descs.find(&desc);
        assert(i != _segment_descs.end());
        return i->second;
    }
    void free_segment(segment* seg, segment_descriptor& desc) {
        free_segment(seg);
    }
    void free_segment(segment* seg) {
        --_segments_in_use;
        auto i = _segments.find(seg);
        assert(i != _segments.end());
        _segment_descs.erase(&i->second);
        _segments.erase(i);
        std::unique_ptr<segment, segment_deleter> useg{seg, segment_deleter(this)};
        _free_segments.push(std::move(useg));
    }
    segment* containing_segment(const void* obj) const {
        uintptr_t addr = reinterpret_cast<uintptr_t>(obj);
        auto seg = reinterpret_cast<segment*>(align_down(addr, static_cast<uintptr_t>(segment::size)));
        auto i = _segments.find(seg);
        if (i == _segments.end()) {
            return nullptr;
        }
        return seg;
    }
    size_t segments_in_use() const;
    size_t current_emergency_reserve_goal() const { return 0; }
    void set_current_emergency_reserve_goal(size_t goal) { }
    void set_emergency_reserve_max(size_t new_size) { }
    size_t emergency_reserve_max() { return 0; }
    void clear_allocation_failure_flag() { }
    bool allocation_failure_flag() { return false; }
    void refill_emergency_reserve() {}
    void update_non_lsa_memory_in_use(ssize_t n) {
        _non_lsa_memory_in_use += n;
    }
    size_t non_lsa_memory_in_use() const {
        return _non_lsa_memory_in_use;
    }
    size_t total_memory_in_use() const {
        return _non_lsa_memory_in_use + _segments_in_use * segment::size;
    }
    size_t unreserved_free_segments() const {
        return 0;
    }
    void set_region(const segment* seg, region::impl* r) {
        set_region(descriptor(seg), r);
    }
    void set_region(segment_descriptor& desc, region::impl* r) {
        desc._region = r;
    }
    size_t reclaim_segments(size_t target) {
        size_t reclaimed = 0;
        while (reclaimed < target && !_free_segments.empty()) {
            _free_segments.pop();
            ++reclaimed;
        }
        return reclaimed;
    }
    void reclaim_all_free_segments() {
        reclaim_segments(std::numeric_limits<size_t>::max());
    }

    struct stats {
        size_t segments_migrated;
        size_t segments_compacted;
        uint64_t memory_allocated;
        uint64_t memory_compacted;
    };
private:
    stats _stats{};
public:
    const stats& statistics() const { return _stats; }
    void on_segment_migration() { _stats.segments_migrated++; }
    void on_segment_compaction(size_t used_space);
    void on_memory_allocation(size_t size);
    size_t free_segments() const { return 0; }
public:
    class reservation_goal;
};

#endif

void segment_pool::on_segment_compaction(size_t used_size) {
    _stats.segments_compacted++;
    _stats.memory_compacted += used_size;
}

void segment_pool::on_memory_allocation(size_t size) {
    _stats.memory_allocated += size;
}

// RAII wrapper to maintain segment_pool::current_emergency_reserve_goal()
class segment_pool::reservation_goal {
    segment_pool& _sp;
    size_t _old_goal;
public:
    reservation_goal(segment_pool& sp, size_t goal)
            : _sp(sp), _old_goal(_sp.current_emergency_reserve_goal()) {
        _sp.set_current_emergency_reserve_goal(goal);
    }
    ~reservation_goal() {
        _sp.set_current_emergency_reserve_goal(_old_goal);
    }
};

size_t segment_pool::segments_in_use() const {
    return _segments_in_use;
}

static thread_local segment_pool shard_segment_pool;

void segment::record_alloc(segment::size_type size) {
    shard_segment_pool.descriptor(this).record_alloc(size);
}

void segment::record_free(segment::size_type size) {
    shard_segment_pool.descriptor(this).record_free(size);
}

bool segment::is_empty() const {
    return shard_segment_pool.descriptor(this).is_empty();
}

occupancy_stats
segment::occupancy() const {
    return { shard_segment_pool.descriptor(this)._free_space, segment::size };
}

//
// For interface documentation see logalloc::region and allocation_strategy.
//
// Allocation dynamics.
//
// Objects are allocated inside fixed-size segments. Objects don't cross
// segment boundary. Active allocations are served from a single segment using
// bump-the-pointer method. That segment is called the active segment. When
// active segment fills up, it is closed. Closed segments are kept in a heap
// which orders them by occupancy. As objects are freed, the segment become
// sparser and are eventually released. Objects which are too large are
// allocated using standard allocator.
//
// Segment layout.
//
// Objects in a segment are laid out sequentially. Each object is preceded by
// a descriptor (see object_descriptor). Object alignment is respected, so if
// there is a gap between the end of current object and the next object's
// descriptor, a trunk of the object descriptor is left right after the
// current object with the flags byte indicating the amount of padding.
//
// Per-segment metadata is kept in a separate array, managed by segment_pool
// object.
//
class region_impl final : public basic_region_impl {
    // Serialized object descriptor format:
    //  byte0 byte1 ... byte[n-1]
    //  bit0-bit5: ULEB64 significand
    //  bit6: 1 iff first byte
    //  bit7: 1 iff last byte
    // This format allows decoding both forwards and backwards (by scanning for bit7/bit6 respectively);
    // backward decoding is needed to recover the descriptor from the object pointer when freeing.
    //
    // Significand interpretation (value = n):
    //     even:  dead object, size n/2 (including descriptor)
    //     odd:   migrate_fn_type at index n/2, from static_migrators()
    class object_descriptor {
    private:
        uint32_t _n;
    private:
        explicit object_descriptor(uint32_t n) : _n(n) {}
    public:
        static_assert(migrators::maximum_id_value <= std::numeric_limits<uint32_t>::max()
            && uint64_t(migrators::maximum_id_value) * 2 + 1 <= std::numeric_limits<uint32_t>::max());

        object_descriptor(allocation_strategy::migrate_fn migrator)
                : _n(migrator->index() * 2 + 1)
        { }

        static object_descriptor make_dead(size_t size) {
            return object_descriptor(size * 2);
        }

        allocation_strategy::migrate_fn migrator() const {
            return static_migrators()[_n / 2];
        }

        uint8_t alignment() const {
            return migrator()->align();
        }

        // excluding descriptor
        segment::size_type live_size(const void* obj) const {
            return migrator()->size(obj);
        }

        // including descriptor
        segment::size_type dead_size() const {
            return _n / 2;
        }

        bool is_live() const {
            return (_n & 1) == 1;
        }

        segment::size_type encoded_size() const {
            return log2floor(_n) / 6 + 1; // 0 is illegal
        }

        void encode(char*& pos) const {
            uint64_t b = 64;
            auto n = _n;
            do {
                b |= n & 63;
                n >>= 6;
                if (!n) {
                    b |= 128;
                }
                *pos++ = b;
                b = 0;
            } while (n);
        }

        // non-canonical encoding to allow padding (for alignment); encoded_size must be
        // sufficient (greater than this->encoded_size())
        void encode(char*& pos, size_t encoded_size) const {
            uint64_t b = 64;
            auto n = _n;
            do {
                b |= n & 63;
                n >>= 6;
                if (!--encoded_size) {
                    b |= 128;
                }
                *pos++ = b;
                b = 0;
            } while (encoded_size);
        }

        static object_descriptor decode_forwards(const char*& pos) {
            unsigned n = 0;
            unsigned shift = 0;
            auto p = pos; // avoid aliasing; p++ doesn't touch memory
            uint8_t b;
            do {
                b = *p++;
                if (shift < 32) {
                    // non-canonical encoding can cause large shift; undefined in C++
                    n |= uint32_t(b & 63) << shift;
                }
                shift += 6;
            } while ((b & 128) == 0);
            pos = p;
            return object_descriptor(n);
        }

        static object_descriptor decode_backwards(const char*& pos) {
            unsigned n = 0;
            uint8_t b;
            auto p = pos; // avoid aliasing; --p doesn't touch memory
            do {
                b = *--p;
                n = (n << 6) | (b & 63);
            } while ((b & 64) == 0);
            pos = p;
            return object_descriptor(n);
        }

        friend std::ostream& operator<<(std::ostream& out, const object_descriptor& desc) {
            if (!desc.is_live()) {
                return out << sprint("{free %d}", desc.dead_size());
            } else {
                auto m = desc.migrator();
                auto x = reinterpret_cast<uintptr_t>(&desc) + sizeof(desc);
                x = align_up(x, m->align());
                auto obj = reinterpret_cast<const void*>(x);
                return out << sprint("{migrator=%p, alignment=%d, size=%d}",
                                      (void*)m, m->align(), m->size(obj));
            }
        }
    };
private:
    region* _region = nullptr;
    region_group* _group = nullptr;
    segment* _active = nullptr;
    size_t _active_offset;
    segment_descriptor_hist _segment_descs; // Contains only closed segments
    occupancy_stats _closed_occupancy;
    occupancy_stats _non_lsa_occupancy;
    // This helps us keeping track of the region_group* heap. That's because we call update before
    // we have a chance to update the occupancy stats - mainly because at this point we don't know
    // what will we do with the new segment. Also, because we are not ever interested in the
    // fraction used, we'll keep it as a scalar and convert when we need to present it as an
    // occupancy. We could actually just present this as a scalar as well and never use occupancies,
    // but consistency is good.
    size_t _evictable_space = 0;
    // This is a mask applied to _evictable_space with bitwise-and before it's returned from evictable_space().
    // Used for forcing the result to zero without using conditionals.
    size_t _evictable_space_mask = std::numeric_limits<size_t>::max();
    bool _evictable = false;
    region_sanitizer _sanitizer;
    uint64_t _id;
    eviction_fn _eviction_fn;

    region_group::region_heap::handle_type _heap_handle;
private:
    struct compaction_lock {
        region_impl& _region;
        bool _prev;
        compaction_lock(region_impl& r)
            : _region(r)
            , _prev(r._reclaiming_enabled)
        {
            _region._reclaiming_enabled = false;
        }
        ~compaction_lock() {
            _region._reclaiming_enabled = _prev;
        }
    };

    void* alloc_small(allocation_strategy::migrate_fn migrator, segment::size_type size, size_t alignment) {
        if (!_active) {
            _active = new_segment();
            _active_offset = 0;
        }

        auto desc = object_descriptor(migrator);
        auto desc_encoded_size = desc.encoded_size();

        size_t obj_offset = align_up(_active_offset + desc_encoded_size, alignment);
        if (obj_offset + size > segment::size) {
            close_and_open();
            return alloc_small(migrator, size, alignment);
        }

        auto old_active_offset = _active_offset;
        auto pos = _active->at<char>(_active_offset);
        // Use non-canonical encoding to allow for alignment pad
        desc.encode(pos, obj_offset - _active_offset);
        _active_offset = obj_offset + size;
        _active->record_alloc(_active_offset - old_active_offset);
        return pos;
    }

    template<typename Func>
    void for_each_live(segment* seg, Func&& func) {
        // scylla-gdb.py:scylla_lsa_segment is coupled with this implementation.

        static_assert(std::is_same<void, std::result_of_t<Func(const object_descriptor*, void*)>>::value, "bad Func signature");

        auto pos = seg->at<const char>(0);
        while (pos < seg->at<const char>(segment::size)) {
            auto old_pos = pos;
            const auto desc = object_descriptor::decode_forwards(pos);
            if (desc.is_live()) {
                auto size = desc.live_size(pos);
                func(&desc, const_cast<char*>(pos));
                pos += size;
            } else {
                pos = old_pos + desc.dead_size();
            }
        }
    }

    void close_active() {
        if (!_active) {
            return;
        }
        if (_active_offset < segment::size) {
            auto desc = object_descriptor::make_dead(segment::size - _active_offset);
            auto pos =_active->at<char>(_active_offset);
            desc.encode(pos);
        }
        llogger.trace("Closing segment {}, used={}, waste={} [B]", _active, _active->occupancy(), segment::size - _active_offset);
        _closed_occupancy += _active->occupancy();

        _segment_descs.push(shard_segment_pool.descriptor(_active));
        _active = nullptr;
    }

    void free_segment(segment_descriptor& desc) noexcept {
        free_segment(shard_segment_pool.segment_from(desc), desc);
    }

    void free_segment(segment* seg) noexcept {
        free_segment(seg, shard_segment_pool.descriptor(seg));
    }

    void free_segment(segment* seg, segment_descriptor& desc) noexcept {
        shard_segment_pool.free_segment(seg, desc);
        if (_group) {
            _evictable_space -= segment_size;
            _group->decrease_usage(_heap_handle, -segment::size);
        }
    }

    segment* new_segment() {
        segment* seg = shard_segment_pool.new_segment(this);
        if (_group) {
            _evictable_space += segment_size;
            _group->increase_usage(_heap_handle, segment::size);
        }
        return seg;
    }

    void compact(segment* seg, segment_descriptor& desc) {
        ++_invalidate_counter;

        for_each_live(seg, [this] (const object_descriptor* desc, void* obj) {
            auto size = desc->live_size(obj);
            auto dst = alloc_small(desc->migrator(), size, desc->alignment());
            _sanitizer.on_migrate(obj, size, dst);
            desc->migrator()->migrate(obj, dst, size);
        });

        free_segment(seg, desc);
    }

    void close_and_open() {
        segment* new_active = new_segment();
        close_active();
        _active = new_active;
        _active_offset = 0;
    }

    static uint64_t next_id() {
        static std::atomic<uint64_t> id{0};
        return id.fetch_add(1);
    }
    struct degroup_temporarily {
        region_impl* impl;
        region_group* group;
        explicit degroup_temporarily(region_impl* impl)
                : impl(impl), group(impl->_group) {
            if (group) {
                group->del(impl);
            }
        }
        ~degroup_temporarily() {
            if (group) {
                group->add(impl);
            }
        }
    };

public:
    explicit region_impl(region* region, region_group* group = nullptr)
        : _region(region), _group(group), _id(next_id())
    {
        _preferred_max_contiguous_allocation = max_managed_object_size;
        tracker_instance._impl->register_region(this);
        try {
            if (group) {
                group->add(this);
            }
        } catch (...) {
            tracker_instance._impl->unregister_region(this);
            throw;
        }
    }

    virtual ~region_impl() {
        _sanitizer.on_region_destruction();

        tracker_instance._impl->unregister_region(this);

        while (!_segment_descs.empty()) {
            auto& desc = _segment_descs.one_of_largest();
            _segment_descs.pop_one_of_largest();
            assert(desc.is_empty());
            free_segment(desc);
        }
        _closed_occupancy = {};
        if (_active) {
            assert(_active->is_empty());
            free_segment(_active);
            _active = nullptr;
        }
        if (_group) {
            _group->del(this);
        }
    }

    region_impl(region_impl&&) = delete;
    region_impl(const region_impl&) = delete;

    bool empty() const {
        return occupancy().used_space() == 0;
    }

    occupancy_stats occupancy() const {
        occupancy_stats total = _non_lsa_occupancy;
        total += _closed_occupancy;
        if (_active) {
            total += _active->occupancy();
        }
        return total;
    }

    region_group* group() {
        return _group;
    }

    occupancy_stats compactible_occupancy() const {
        return _closed_occupancy;
    }

    occupancy_stats evictable_occupancy() const {
        return occupancy_stats(0, _evictable_space & _evictable_space_mask);
    }

    void ground_evictable_occupancy() {
        _evictable_space_mask = 0;
        if (_group) {
            _group->decrease_evictable_usage(_heap_handle);
        }
    }

    //
    // Returns true if this region can be compacted and compact() will make forward progress,
    // so that this will eventually stop:
    //
    //    while (is_compactible()) { compact(); }
    //
    bool is_compactible() const {
        return _reclaiming_enabled
            && (_closed_occupancy.free_space() >= 2 * segment::size)
            && _segment_descs.contains_above_min();
    }

    bool is_idle_compactible() {
        return is_compactible();
    }

    virtual void* alloc(allocation_strategy::migrate_fn migrator, size_t size, size_t alignment) override {
        compaction_lock _(*this);
        memory::on_alloc_point();
        shard_segment_pool.on_memory_allocation(size);
        if (size > max_managed_object_size) {
            auto ptr = standard_allocator().alloc(migrator, size, alignment);
            // This isn't very acurrate, the correct free_space value would be
            // malloc_usable_size(ptr) - size, but there is no way to get
            // the exact object size at free.
            auto allocated_size = malloc_usable_size(ptr);
            _non_lsa_occupancy += occupancy_stats(0, allocated_size);
            if (_group) {
                 _evictable_space += allocated_size;
                _group->increase_usage(_heap_handle, allocated_size);
            }
            shard_segment_pool.update_non_lsa_memory_in_use(allocated_size);
            return ptr;
        } else {
            auto ptr = alloc_small(migrator, (segment::size_type) size, alignment);
            _sanitizer.on_allocation(ptr, size);
            return ptr;
        }
    }

private:
    void on_non_lsa_free(void* obj) noexcept {
        auto allocated_size = malloc_usable_size(obj);
        _non_lsa_occupancy -= occupancy_stats(0, allocated_size);
        if (_group) {
            _evictable_space -= allocated_size;
            _group->decrease_usage(_heap_handle, allocated_size);
        }
        shard_segment_pool.update_non_lsa_memory_in_use(-allocated_size);
    }
public:
    virtual void free(void* obj) noexcept override {
        compaction_lock _(*this);
        segment* seg = shard_segment_pool.containing_segment(obj);
        if (!seg) {
            on_non_lsa_free(obj);
            standard_allocator().free(obj);
            return;
        }

        auto pos = reinterpret_cast<const char*>(obj);
        auto desc = object_descriptor::decode_backwards(pos);
        free(obj, desc.live_size(obj));
    }

    virtual void free(void* obj, size_t size) noexcept override {
        compaction_lock _(*this);
        segment* seg = shard_segment_pool.containing_segment(obj);

        if (!seg) {
            on_non_lsa_free(obj);
            standard_allocator().free(obj, size);
            return;
        }

        _sanitizer.on_free(obj, size);

        segment_descriptor& seg_desc = shard_segment_pool.descriptor(seg);

        auto pos = reinterpret_cast<const char*>(obj);
        auto old_pos = pos;
        auto desc = object_descriptor::decode_backwards(pos);
        auto dead_size = size + (old_pos - pos);
        desc = object_descriptor::make_dead(dead_size);
        auto npos = const_cast<char*>(pos);
        desc.encode(npos);

        if (seg != _active) {
            _closed_occupancy -= seg->occupancy();
        }

        seg_desc.record_free(dead_size);

        if (seg != _active) {
            if (seg_desc.is_empty()) {
                _segment_descs.erase(seg_desc);
                free_segment(seg, seg_desc);
            } else {
                _segment_descs.adjust_up(seg_desc);
                _closed_occupancy += seg_desc.occupancy();
            }
        }
    }

    virtual size_t object_memory_size_in_allocator(const void* obj) const noexcept override {
        segment* seg = shard_segment_pool.containing_segment(obj);

        if (!seg) {
            return standard_allocator().object_memory_size_in_allocator(obj);
        } else {
            auto pos = reinterpret_cast<const char*>(obj);
            auto desc = object_descriptor::decode_backwards(pos);
            return desc.encoded_size() + desc.live_size(obj);
        }
    }

    // Merges another region into this region. The other region is made
    // to refer to this region.
    // Doesn't invalidate references to allocated objects.
    void merge(region_impl& other) noexcept {
        // degroup_temporarily allocates via binomial_heap::push(), which should not
        // fail, because we have a matching deallocation before that and we don't
        // allocate between them.
        memory::disable_failure_guard dfg;

        compaction_lock dct1(*this);
        compaction_lock dct2(other);
        degroup_temporarily dgt1(this);
        degroup_temporarily dgt2(&other);

        if (_active && _active->is_empty()) {
            shard_segment_pool.free_segment(_active);
            _active = nullptr;
        }
        if (!_active) {
            _active = other._active;
            other._active = nullptr;
            _active_offset = other._active_offset;
            if (_active) {
                shard_segment_pool.set_region(_active, this);
            }
        } else {
            other.close_active();
        }

        for (auto& desc : other._segment_descs) {
            shard_segment_pool.set_region(desc, this);
        }
        _segment_descs.merge(other._segment_descs);

        _closed_occupancy += other._closed_occupancy;
        _non_lsa_occupancy += other._non_lsa_occupancy;
        other._closed_occupancy = {};
        other._non_lsa_occupancy = {};

        // Make sure both regions will notice a future increment
        // to the reclaim counter
        _invalidate_counter = std::max(_invalidate_counter, other._invalidate_counter);

        _sanitizer.merge(other._sanitizer);
        other._sanitizer = { };
    }

    // Returns occupancy of the sparsest compactible segment.
    occupancy_stats min_occupancy() const {
        if (_segment_descs.empty()) {
            return {};
        }
        return _segment_descs.one_of_largest().occupancy();
    }

    void compact_single_segment_locked() {
        auto& desc = _segment_descs.one_of_largest();
        _segment_descs.pop_one_of_largest();
        _closed_occupancy -= desc.occupancy();
        segment* seg = shard_segment_pool.segment_from(desc);
        auto seg_occupancy = desc.occupancy();
        llogger.debug("Compacting segment {} from region {}, {}", seg, id(), seg_occupancy);
        compact(seg, desc);
        shard_segment_pool.on_segment_compaction(seg_occupancy.used_space());
    }

    // Compacts a single segment
    void compact() {
        compaction_lock _(*this);
        compact_single_segment_locked();
    }

    void migrate_segment(segment* src, segment_descriptor& src_desc, segment* dst, segment_descriptor& dst_desc) {
        ++_invalidate_counter;
        size_t segment_size;
        if (src != _active) {
            _segment_descs.erase(src_desc);
            _segment_descs.push(dst_desc);
            segment_size = segment::size;
        } else {
            _active = dst;
            segment_size = _active_offset;
        }

        size_t offset = 0;
        while (offset < segment_size) {
            auto pos = src->at<const char>(offset);
            auto dpos = dst->at<char>(offset);
            auto old_pos = pos;
            auto desc = object_descriptor::decode_forwards(pos);
            // Keep same size as before to maintain alignment
            desc.encode(dpos, pos - old_pos);
            if (desc.is_live()) {
                offset += pos - old_pos;
                auto size = desc.live_size(pos);
                offset += size;
                _sanitizer.on_migrate(pos, size, dpos);
                desc.migrator()->migrate(const_cast<char*>(pos), dpos, size);
            } else {
                offset += desc.dead_size();
            }
        }
        shard_segment_pool.on_segment_migration();
    }

    // Compacts everything. Mainly for testing.
    // Invalidates references to allocated objects.
    void full_compaction() {
        compaction_lock _(*this);
        llogger.debug("Full compaction, {}", occupancy());
        close_and_open();
        segment_descriptor_hist all;
        std::swap(all, _segment_descs);
        _closed_occupancy = {};
        while (!all.empty()) {
            auto& desc = all.one_of_largest();
            all.pop_one_of_largest();
            compact(shard_segment_pool.segment_from(desc), desc);
        }
        llogger.debug("Done, {}", occupancy());
    }

    allocation_strategy& allocator() {
        return *this;
    }

    uint64_t id() const {
        return _id;
    }

    // Returns true if this pool is evictable, so that evict_some() can be called.
    bool is_evictable() const {
        return _evictable && _reclaiming_enabled;
    }

    memory::reclaiming_result evict_some() {
        ++_invalidate_counter;
        return _eviction_fn();
    }

    void make_not_evictable() {
        _evictable = false;
        _eviction_fn = {};
    }

    void make_evictable(eviction_fn fn) {
        _evictable = true;
        _eviction_fn = std::move(fn);
    }

    const eviction_fn& evictor() const {
        return _eviction_fn;
    }

    friend class region;
    friend class region_group;
    friend class region_group::region_evictable_occupancy_ascending_less_comparator;
};

inline void
region_group_binomial_group_sanity_check(const region_group::region_heap& bh) {
#ifdef SEASTAR_DEBUG
    bool failed = false;
    size_t last =  std::numeric_limits<size_t>::max();
    for (auto b = bh.ordered_begin(); b != bh.ordered_end(); b++) {
        auto t = (*b)->evictable_occupancy().total_space();
        if (!(t <= last)) {
            failed = true;
            break;
        }
        last = t;
    }
    if (!failed) {
        return;
    }

    printf("Sanity checking FAILED, size %ld\n", bh.size());
    for (auto b = bh.ordered_begin(); b != bh.ordered_end(); b++) {
        auto r = (*b);
        auto t = r->evictable_occupancy().total_space();
        printf(" r = %p (id=%ld), occupancy = %ld\n",r, r->id(), t);
    }
    assert(0);
#endif
}

void tracker::set_reclamation_step(size_t step_in_segments) {
    _impl->set_reclamation_step(step_in_segments);
}

size_t tracker::reclamation_step() const {
    return _impl->reclamation_step();
}

void tracker::enable_abort_on_bad_alloc() {
    return _impl->enable_abort_on_bad_alloc();
}

bool tracker::should_abort_on_bad_alloc() {
    return _impl->should_abort_on_bad_alloc();
}

memory::reclaiming_result tracker::reclaim() {
    return reclaim(_impl->reclamation_step() * segment::size)
           ? memory::reclaiming_result::reclaimed_something
           : memory::reclaiming_result::reclaimed_nothing;
}

bool
region_group::region_evictable_occupancy_ascending_less_comparator::operator()(region_impl* r1, region_impl* r2) const {
    return r1->evictable_occupancy().total_space() < r2->evictable_occupancy().total_space();
}

region::region()
    : _impl(make_shared<impl>(this))
{ }

region::region(region_group& group)
        : _impl(make_shared<impl>(this, &group)) {
}

region_impl& region::get_impl() {
    return *static_cast<region_impl*>(_impl.get());
}
const region_impl& region::get_impl() const {
    return *static_cast<const region_impl*>(_impl.get());
}

region::region(region&& other) {
    this->_impl = std::move(other._impl);
    get_impl()._region = this;
}

region& region::operator=(region&& other) {
    this->_impl = std::move(other._impl);
    get_impl()._region = this;
    return *this;
}

region::~region() {
}

occupancy_stats region::occupancy() const {
    return get_impl().occupancy();
}

region_group* region::group() {
    return get_impl().group();
}

void region::merge(region& other) noexcept {
    if (_impl != other._impl) {
        get_impl().merge(other.get_impl());
        other._impl = _impl;
    }
}

void region::full_compaction() {
    get_impl().full_compaction();
}

memory::reclaiming_result region::evict_some() {
    if (get_impl().is_evictable()) {
        return get_impl().evict_some();
    }
    return memory::reclaiming_result::reclaimed_nothing;
}

void region::make_evictable(eviction_fn fn) {
    get_impl().make_evictable(std::move(fn));
}

void region::ground_evictable_occupancy() {
    get_impl().ground_evictable_occupancy();
}

const eviction_fn& region::evictor() const {
    return get_impl().evictor();
}

std::ostream& operator<<(std::ostream& out, const occupancy_stats& stats) {
    return out << sprint("%.2f%%, %d / %d [B]",
        stats.used_fraction() * 100, stats.used_space(), stats.total_space());
}

occupancy_stats tracker::impl::region_occupancy() {
    reclaiming_lock _(*this);
    occupancy_stats total{};
    for (auto&& r: _regions) {
        total += r->occupancy();
    }
    return total;
}

occupancy_stats tracker::impl::occupancy() {
    reclaiming_lock _(*this);
    auto occ = region_occupancy();
    {
        auto s = shard_segment_pool.free_segments() * segment::size;
        occ += occupancy_stats(s, s);
    }
    return occ;
}

size_t tracker::impl::non_lsa_used_space() {
    auto free_space_in_lsa = shard_segment_pool.free_segments() * segment_size;
    return memory::stats().allocated_memory() - region_occupancy().total_space() - free_space_in_lsa;
}

void tracker::impl::reclaim_all_free_segments()
{
    llogger.debug("Reclaiming all free segments");
    shard_segment_pool.reclaim_all_free_segments();
    llogger.debug("Reclamation done");
}

void tracker::impl::full_compaction() {
    reclaiming_lock _(*this);

    llogger.debug("Full compaction on all regions, {}", region_occupancy());

    for (region_impl* r : _regions) {
        if (r->reclaiming_enabled()) {
            r->full_compaction();
        }
    }

    llogger.debug("Compaction done, {}", region_occupancy());
}

static void reclaim_from_evictable(region::impl& r, size_t target_mem_in_use) {
    while (true) {
        auto deficit = shard_segment_pool.total_memory_in_use() - target_mem_in_use;
        auto occupancy = r.occupancy();
        auto used = occupancy.used_space();
        if (used == 0) {
            break;
        }
        // Before attempting segment compaction, try to evict at least deficit and one segment more so that
        // for workloads in which eviction order matches allocation order we will reclaim full segments
        // without needing to perform expensive compaction.
        auto used_target = used - std::min(used, deficit + segment::size);
        llogger.debug("Evicting {} bytes from region {}, occupancy={}", used - used_target, r.id(), r.occupancy());
        while (r.occupancy().used_space() > used_target || !r.is_compactible()) {
            if (r.evict_some() == memory::reclaiming_result::reclaimed_nothing) {
                if (r.is_compactible()) { // Need to make forward progress in case there is nothing to evict.
                    break;
                }
                llogger.debug("Unable to evict more, evicted {} bytes", used - r.occupancy().used_space());
                return;
            }
            if (shard_segment_pool.total_memory_in_use() <= target_mem_in_use) {
                llogger.debug("Target met after evicting {} bytes", used - r.occupancy().used_space());
                return;
            }
            if (r.empty()) {
                return;
            }
        }
        llogger.debug("Compacting after evicting {} bytes", used - r.occupancy().used_space());
        r.compact();
    }
}

struct reclaim_timer {
    clock::time_point start;
    bool enabled;
    reclaim_timer() {
        if (timing_logger.is_enabled(logging::log_level::debug)) {
            start = clock::now();
            enabled = true;
        } else {
            enabled = false;
        }
    }
    ~reclaim_timer() {
        if (enabled) {
            auto duration = clock::now() - start;
            timing_logger.debug("Reclamation cycle took {} us.",
                std::chrono::duration_cast<std::chrono::duration<double, std::micro>>(duration).count());
        }
    }
    void stop(size_t released) {
        if (enabled) {
            enabled = false;
            auto duration = clock::now() - start;
            auto bytes_per_second = static_cast<float>(released) / std::chrono::duration_cast<std::chrono::duration<float>>(duration).count();
            timing_logger.debug("Reclamation cycle took {} us. Reclamation rate = {} MiB/s",
                                std::chrono::duration_cast<std::chrono::duration<double, std::micro>>(duration).count(),
                                sprint("%.3f", bytes_per_second / (1024*1024)));
        }
    }
};

reactor::idle_cpu_handler_result tracker::impl::compact_on_idle(reactor::work_waiting_on_reactor check_for_work) {
    if (!_reclaiming_enabled) {
        return reactor::idle_cpu_handler_result::no_more_work;
    }
    reclaiming_lock rl(*this);
    if (_regions.empty()) {
        return reactor::idle_cpu_handler_result::no_more_work;
    }
    segment_pool::reservation_goal open_emergency_pool(shard_segment_pool, 0);

    auto cmp = [] (region::impl* c1, region::impl* c2) {
        if (c1->is_idle_compactible() != c2->is_idle_compactible()) {
            return !c1->is_idle_compactible();
        }
        return c2->min_occupancy() < c1->min_occupancy();
    };

    boost::range::make_heap(_regions, cmp);

    while (!check_for_work()) {
        boost::range::pop_heap(_regions, cmp);
        region::impl* r = _regions.back();

        if (!r->is_idle_compactible()) {
            return reactor::idle_cpu_handler_result::no_more_work;
        }

        r->compact();

        boost::range::push_heap(_regions, cmp);
    }
    return reactor::idle_cpu_handler_result::interrupted_by_higher_priority_task;
}

size_t tracker::impl::reclaim(size_t memory_to_release) {
    // Reclamation steps:
    // 1. Try to release free segments from segment pool and emergency reserve.
    // 2. Compact used segments and/or evict data.

    if (!_reclaiming_enabled) {
        return 0;
    }
    reclaiming_lock rl(*this);
    reclaim_timer timing_guard;

    size_t mem_released;
    {
        reclaiming_lock rl(*this);
        constexpr auto max_bytes = std::numeric_limits<size_t>::max() - segment::size;
        auto segments_to_release = align_up(std::min(max_bytes, memory_to_release), segment::size) >> segment::size_shift;
        auto nr_released = shard_segment_pool.reclaim_segments(segments_to_release);
        mem_released = nr_released * segment::size;
        if (mem_released > memory_to_release) {
            return memory_to_release;
        }
    }
    auto compacted = compact_and_evict_locked(memory_to_release - mem_released);

    // compact_and_evict_locked() will not return segments to the standard allocator,
    // so do it here:
    auto nr_released = shard_segment_pool.reclaim_segments(compacted / segment::size);

    return mem_released + nr_released * segment::size;
}

size_t tracker::impl::compact_and_evict(size_t memory_to_release) {
    if (!_reclaiming_enabled) {
        return 0;
    }
    reclaiming_lock rl(*this);
    reclaim_timer timing_guard;
    return compact_and_evict_locked(memory_to_release);
}

size_t tracker::impl::compact_and_evict_locked(size_t memory_to_release) {
    //
    // Algorithm outline.
    //
    // Regions are kept in a max-heap ordered so that regions with
    // sparser segments are picked first. Non-compactible regions will be
    // picked last. In each iteration we try to release one whole segment from
    // the region which has the sparsest segment. We do it until we released
    // enough segments or there are no more regions we can compact.
    //
    // When compaction is not sufficient to reclaim space, we evict data from
    // evictable regions.
    //

    // This may run synchronously with allocation, so we should not allocate
    // memory, otherwise we may get std::bad_alloc. Currently we only allocate
    // in the logger when debug level is enabled. It's disabled during normal
    // operation. Having it is still valuable during testing and in most cases
    // should work just fine even if allocates.

    size_t mem_released = 0;

    size_t mem_in_use = shard_segment_pool.total_memory_in_use();
    auto target_mem = mem_in_use - std::min(mem_in_use, memory_to_release - mem_released);

    llogger.debug("Compacting, requested {} bytes, {} bytes in use, target is {}",
        memory_to_release, mem_in_use, target_mem);

    // Allow dipping into reserves while compacting
    segment_pool::reservation_goal open_emergency_pool(shard_segment_pool, 0);

    auto cmp = [] (region::impl* c1, region::impl* c2) {
        if (c1->is_compactible() != c2->is_compactible()) {
            return !c1->is_compactible();
        }
        return c2->min_occupancy() < c1->min_occupancy();
    };

    boost::range::make_heap(_regions, cmp);

    if (llogger.is_enabled(logging::log_level::debug)) {
        llogger.debug("Occupancy of regions:");
        for (region::impl* r : _regions) {
            llogger.debug(" - {}: min={}, avg={}", r->id(), r->min_occupancy(), r->compactible_occupancy());
        }
    }

    while (shard_segment_pool.total_memory_in_use() > target_mem) {
        boost::range::pop_heap(_regions, cmp);
        region::impl* r = _regions.back();

        if (!r->is_compactible()) {
            llogger.trace("Unable to release segments, no compactible pools.");
            break;
        }

        // Prefer evicting if average occupancy ratio is above the compaction threshold to avoid
        // overhead of compaction in workloads where allocation order matches eviction order, where
        // we can reclaim memory by eviction only. In some cases the cost of compaction on allocation
        // would be higher than the cost of repopulating the region with evicted items.
        if (r->is_evictable() && r->occupancy().used_space() >= max_used_space_ratio_for_compaction * r->occupancy().total_space()) {
            reclaim_from_evictable(*r, target_mem);
        } else {
            r->compact();
        }

        boost::range::push_heap(_regions, cmp);
    }

    auto released_during_compaction = mem_in_use - shard_segment_pool.total_memory_in_use();

    if (shard_segment_pool.total_memory_in_use() > target_mem) {
        llogger.debug("Considering evictable regions.");
        // FIXME: Fair eviction
        for (region::impl* r : _regions) {
            if (r->is_evictable()) {
                reclaim_from_evictable(*r, target_mem);
                if (shard_segment_pool.total_memory_in_use() <= target_mem) {
                    break;
                }
            }
        }
    }

    mem_released += mem_in_use - shard_segment_pool.total_memory_in_use();

    llogger.debug("Released {} bytes (wanted {}), {} during compaction",
        mem_released, memory_to_release, released_during_compaction);

    return mem_released;
}

#ifndef SEASTAR_DEFAULT_ALLOCATOR

bool segment_pool::migrate_segment(segment* src, segment* dst)
{
    auto& src_desc = descriptor(src);
    auto& dst_desc = descriptor(dst);

    llogger.debug("Migrating segment {} to {} (region @{})",
        src, dst, src_desc._region);

    {
        if (!src_desc._region->reclaiming_enabled()) {
            llogger.trace("Cannot move segment {}", src);
            return false;
        }
        assert(!dst_desc._region);
        dst_desc._free_space = src_desc._free_space;
        src_desc._region->migrate_segment(src, src_desc, dst, dst_desc);
        assert(_lsa_owned_segments_bitmap.test(idx_from_segment(src)));
    }
    _lsa_free_segments_bitmap.set(idx_from_segment(src));
    _lsa_free_segments_bitmap.clear(idx_from_segment(dst));
    dst_desc._region = src_desc._region;
    src_desc._region = nullptr;
    return true;
}

#endif

void tracker::impl::register_region(region::impl* r) {
    reclaiming_lock _(*this);
    _regions.push_back(r);
    llogger.debug("Registered region @{} with id={}", r, r->id());
}

void tracker::impl::unregister_region(region::impl* r) noexcept {
    reclaiming_lock _(*this);
    llogger.debug("Unregistering region, id={}", r->id());
    _regions.erase(std::remove(_regions.begin(), _regions.end(), r), _regions.end());
}

tracker::impl::impl() {
    namespace sm = seastar::metrics;

    _metrics.add_group("lsa", {
        sm::make_gauge("total_space_bytes", [this] { return region_occupancy().total_space(); },
                       sm::description("Holds a current size of allocated memory in bytes.")),

        sm::make_gauge("used_space_bytes", [this] { return region_occupancy().used_space(); },
                       sm::description("Holds a current amount of used memory in bytes.")),

        sm::make_gauge("small_objects_total_space_bytes", [this] { return region_occupancy().total_space() - shard_segment_pool.non_lsa_memory_in_use(); },
                       sm::description("Holds a current size of \"small objects\" memory region in bytes.")),

        sm::make_gauge("small_objects_used_space_bytes", [this] { return region_occupancy().used_space() - shard_segment_pool.non_lsa_memory_in_use(); },
                       sm::description("Holds a current amount of used \"small objects\" memory in bytes.")),

        sm::make_gauge("large_objects_total_space_bytes", [this] { return shard_segment_pool.non_lsa_memory_in_use(); },
                       sm::description("Holds a current size of allocated non-LSA memory.")),

        sm::make_gauge("non_lsa_used_space_bytes", [this] { return non_lsa_used_space(); },
                       sm::description("Holds a current amount of used non-LSA memory.")),

        sm::make_gauge("free_space", [this] { return shard_segment_pool.unreserved_free_segments() * segment_size; },
                       sm::description("Holds a current amount of free memory that is under lsa control.")),

        sm::make_gauge("occupancy", [this] { return region_occupancy().used_fraction() * 100; },
                       sm::description("Holds a current portion (in percents) of the used memory.")),

        sm::make_derive("segments_migrated", [this] { return shard_segment_pool.statistics().segments_migrated; },
                        sm::description("Counts a number of migrated segments.")),

        sm::make_derive("segments_compacted", [this] { return shard_segment_pool.statistics().segments_compacted; },
                        sm::description("Counts a number of compacted segments.")),

        sm::make_derive("memory_compacted", [this] { return shard_segment_pool.statistics().memory_compacted; },
                        sm::description("Counts number of bytes which were copied as part of segment compaction.")),

        sm::make_derive("memory_allocated", [this] { return shard_segment_pool.statistics().memory_allocated; },
                        sm::description("Counts number of bytes which were requested from LSA allocator.")),
    });
}

tracker::impl::~impl() {
    if (!_regions.empty()) {
        for (auto&& r : _regions) {
            llogger.error("Region with id={} not unregistered!", r->id());
        }
        abort();
    }
}

region_group_reclaimer region_group::no_reclaimer;

uint64_t region_group::top_region_evictable_space() const {
    return _regions.empty() ? 0 : _regions.top()->evictable_occupancy().total_space();
}

region* region_group::get_largest_region() {
    if (!_maximal_rg || _maximal_rg->_regions.empty()) {
        return nullptr;
    }
    return _maximal_rg->_regions.top()->_region;
}

void
region_group::add(region_group* child) {
    child->_subgroup_heap_handle = _subgroups.push(child);
    update(child->_total_memory);
}

void
region_group::del(region_group* child) {
    _subgroups.erase(child->_subgroup_heap_handle);
    update(-child->_total_memory);
}

void
region_group::add(region_impl* child) {
    child->_heap_handle = _regions.push(child);
    region_group_binomial_group_sanity_check(_regions);
    update(child->occupancy().total_space());
}

void
region_group::del(region_impl* child) {
    _regions.erase(child->_heap_handle);
    region_group_binomial_group_sanity_check(_regions);
    update(-child->occupancy().total_space());
}

bool
region_group::execution_permitted() noexcept {
    return do_for_each_parent(this, [] (auto rg) {
        return rg->under_pressure() ? stop_iteration::yes : stop_iteration::no;
    }) == nullptr;
}

future<>
region_group::start_releaser(scheduling_group deferred_work_sg) {
    return with_scheduling_group(deferred_work_sg, [this] {
        return later().then([this] {
            return repeat([this] () noexcept {
                if (_shutdown_requested) {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }

                if (!_blocked_requests.empty() && execution_permitted()) {
                    auto req = std::move(_blocked_requests.front());
                    _blocked_requests.pop_front();
                    req->allocate();
                    return make_ready_future<stop_iteration>(stop_iteration::no);
                } else {
                    // Block reclaiming to prevent signal() from being called by reclaimer inside wait()
                    // FIXME: handle allocation failures (not very likely) like allocating_section does
                    tracker_reclaimer_lock rl;
                    return _relief.wait().then([] {
                        return stop_iteration::no;
                    });
                }
            });
        });
    });
}

region_group::region_group(region_group *parent, region_group_reclaimer& reclaimer,
        scheduling_group deferred_work_sg)
    : _parent(parent)
    , _reclaimer(reclaimer)
    , _releaser(reclaimer_can_block() ? start_releaser(deferred_work_sg) : make_ready_future<>())
{
    if (_parent) {
        _parent->add(this);
    }
}

bool region_group::reclaimer_can_block() const {
    return _reclaimer.throttle_threshold() != std::numeric_limits<size_t>::max();
}

void region_group::notify_relief() {
    _relief.signal();
    for (region_group* child : _subgroups) {
        child->notify_relief();
    }
}

void region_group::update(ssize_t delta) {
    // Most-enclosing group which was relieved.
    region_group* top_relief = nullptr;

    do_for_each_parent(this, [&top_relief, delta] (region_group* rg) mutable {
        rg->update_maximal_rg();
        rg->_total_memory += delta;

        if (rg->_total_memory >= rg->_reclaimer.soft_limit_threshold()) {
            rg->_reclaimer.notify_soft_pressure();
        } else {
            rg->_reclaimer.notify_soft_relief();
        }

        if (rg->_total_memory > rg->_reclaimer.throttle_threshold()) {
            rg->_reclaimer.notify_pressure();
        } else if (rg->_reclaimer.under_pressure()) {
            rg->_reclaimer.notify_relief();
            top_relief = rg;
        }

        return stop_iteration::no;
    });

    if (top_relief) {
        top_relief->notify_relief();
    }
}

allocating_section::guard::guard()
    : _prev(shard_segment_pool.emergency_reserve_max())
{ }

allocating_section::guard::~guard() {
    shard_segment_pool.set_emergency_reserve_max(_prev);
}

#ifndef SEASTAR_DEFAULT_ALLOCATOR

void allocating_section::reserve() {
    shard_segment_pool.set_emergency_reserve_max(std::max(_lsa_reserve, _minimum_lsa_emergency_reserve));
    shard_segment_pool.refill_emergency_reserve();

    while (true) {
        size_t free = memory::stats().free_memory();
        if (free >= _std_reserve) {
            break;
        }
        if (!tracker_instance.reclaim(_std_reserve - free)) {
            throw std::bad_alloc();
        }
    }

    shard_segment_pool.clear_allocation_failure_flag();
}

void allocating_section::on_alloc_failure(logalloc::region& r) {
    r.allocator().invalidate_references();
    if (shard_segment_pool.allocation_failure_flag()) {
        _lsa_reserve *= 2; // FIXME: decay?
        llogger.debug("LSA allocation failure, increasing reserve in section {} to {} segments", this, _lsa_reserve);
    } else {
        _std_reserve *= 2; // FIXME: decay?
        llogger.debug("Standard allocator failure, increasing head-room in section {} to {} [B]", this, _std_reserve);
    }
    reserve();
}

#else

void allocating_section::reserve() {
}

void allocating_section::on_alloc_failure(logalloc::region&) {
    throw std::bad_alloc();
}

#endif

void allocating_section::set_lsa_reserve(size_t reserve) {
    _lsa_reserve = reserve;
}

void allocating_section::set_std_reserve(size_t reserve) {
    _std_reserve = reserve;
}

void region_group::on_request_expiry::operator()(std::unique_ptr<allocating_function>& func) noexcept {
    func->fail(std::make_exception_ptr(timed_out_error()));
}

future<> prime_segment_pool(size_t available_memory, size_t min_free_memory) {
    return smp::invoke_on_all([=] {
        shard_segment_pool.prime(available_memory, min_free_memory);
    });
}

uint64_t memory_allocated() {
    return shard_segment_pool.statistics().memory_allocated;
}

uint64_t memory_compacted() {
    return shard_segment_pool.statistics().memory_compacted;
}

}

// Orders segments by free space, assuming all segments have the same size.
// This avoids using the occupancy, which entails extra division operations.
template<>
size_t hist_key<logalloc::segment_descriptor>(const logalloc::segment_descriptor& desc) {
    return desc._free_space;
}
