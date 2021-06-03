/*
 * Copyright (C) 2015-present ScyllaDB
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
#include <seastar/core/reactor.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/backtrace.hh>

#include "utils/logalloc.hh"
#include "log.hh"
#include "utils/dynamic_bitset.hh"
#include "utils/log_heap.hh"
#include "utils/preempt.hh"

#include <random>
#include <chrono>

using namespace std::chrono_literals;

#ifdef SEASTAR_ASAN_ENABLED
#include "sanitizer/asan_interface.h"
// For each aligned 8 byte segment, the algorithm used by address
// sanitizer can represent any addressable prefix followd by a
// poisoned suffix. The details are at:
// https://github.com/google/sanitizers/wiki/AddressSanitizerAlgorithm
// For us this means that:
// * The descriptor must be 8 byte aligned. If it was not, making the
//   descriptor addressable would also make the end of the previous
//   value addressable.
// * Each value must be at least 8 byte aligned. If it was not, making
//   the value addressable would also make the end of the descriptor
//   addressable.
namespace debug {
constexpr size_t logalloc_alignment = 8;
}
template<typename T>
[[nodiscard]] static T align_up_for_asan(T val) {
    return align_up(val, size_t(8));
}
template<typename T>
void poison(const T* addr, size_t size) {
    // Both values and descriptors must be aligned.
    assert(uintptr_t(addr) % 8 == 0);
    // This can be followed by
    // * 8 byte aligned descriptor (this is a value)
    // * 8 byte aligned value
    // * dead value
    // * end of segment
    // In all cases, we can align up the size to guarantee that asan
    // is able to poison this.
    ASAN_POISON_MEMORY_REGION(addr, align_up_for_asan(size));
}
void unpoison(const char *addr, size_t size) {
    ASAN_UNPOISON_MEMORY_REGION(addr, size);
}
#else
namespace debug {
constexpr size_t logalloc_alignment = 1;
}
template<typename T>
[[nodiscard]] static T align_up_for_asan(T val) { return val; }
template<typename T>
void poison(const T* addr, size_t size) { }
void unpoison(const char *addr, size_t size) { }
#endif

namespace bi = boost::intrusive;

standard_allocation_strategy standard_allocation_strategy_instance;

namespace {

class migrators_base {
protected:
    std::vector<const migrate_fn_type*> _migrators;
};

#ifdef DEBUG_LSA_SANITIZER

class migrators : public migrators_base, public enable_lw_shared_from_this<migrators> {
private:
    struct backtrace_entry {
        saved_backtrace _registration;
        saved_backtrace _deregistration;
    };
    std::vector<std::unique_ptr<backtrace_entry>> _backtraces;

    static logging::logger _logger;
private:
    void on_error() { abort(); }
public:
    uint32_t add(const migrate_fn_type* m) {
        _migrators.push_back(m);
        _backtraces.push_back(std::make_unique<backtrace_entry>(backtrace_entry{current_backtrace(), {}}));
        return _migrators.size() - 1;
    }
    void remove(uint32_t idx) {
        if (idx >= _migrators.size()) {
            _logger.error("Attempting to deregister migrator id {} which was never registered:\n{}",
                          idx, current_backtrace());
            on_error();
        }
        if (!_migrators[idx]) {
            _logger.error("Attempting to double deregister migrator id {}:\n{}\n"
                          "Previously deregistered at:\n{}\nRegistered at:\n{}",
                          idx, current_backtrace(), _backtraces[idx]->_deregistration,
                          _backtraces[idx]->_registration);
            on_error();
        }
        _migrators[idx] = nullptr;
        _backtraces[idx]->_deregistration = current_backtrace();
    }
    const migrate_fn_type*& operator[](uint32_t idx) {
        if (idx >= _migrators.size()) {
            _logger.error("Attempting to use migrator id {} that was never registered:\n{}",
                          idx, current_backtrace());
            on_error();
        }
        if (!_migrators[idx]) {
            _logger.error("Attempting to use deregistered migrator id {}:\n{}\n"
                          "Deregistered at:\n{}\nRegistered at:\n{}",
                          idx, current_backtrace(), _backtraces[idx]->_deregistration,
                          _backtraces[idx]->_registration);
            on_error();
        }
        return _migrators[idx];
    }
};

logging::logger migrators::_logger("lsa-migrator-sanitizer");

#else

class migrators : public migrators_base, public enable_lw_shared_from_this<migrators> {
    std::vector<uint32_t> _unused_ids;

public:
    uint32_t add(const migrate_fn_type* m) {
        if (!_unused_ids.empty()) {
            uint32_t idx = _unused_ids.back();
            _unused_ids.pop_back();
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
static_migrators() noexcept {
    memory::scoped_critical_alloc_section dfg;
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
        memory::scoped_critical_alloc_section dfg;
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

static tracker& get_tracker_instance() noexcept {
    memory::scoped_critical_alloc_section dfg;
    static thread_local tracker obj;
    return obj;
}

static thread_local tracker& tracker_instance = get_tracker_instance();

using clock = std::chrono::steady_clock;

class background_reclaimer {
    scheduling_group _sg;
    noncopyable_function<void (size_t target)> _reclaim;
    timer<lowres_clock> _adjust_shares_timer;
    // If engaged, main loop is not running, set_value() to wake it.
    promise<>* _main_loop_wait = nullptr;
    future<> _done;
    bool _stopping = false;
    static constexpr size_t free_memory_threshold = 60'000'000;
private:
    bool have_work() const {
#ifndef SEASTAR_DEFAULT_ALLOCATOR
        return memory::stats().free_memory() < free_memory_threshold;
#else
        return false;
#endif
    }
    void main_loop_wake() {
        llogger.debug("background_reclaimer::main_loop_wake: waking {}", bool(_main_loop_wait));
        if (_main_loop_wait) {
            _main_loop_wait->set_value();
            _main_loop_wait = nullptr;
        }
    }
    future<> main_loop() {
        llogger.debug("background_reclaimer::main_loop: entry");
        while (true) {
            while (!_stopping && !have_work()) {
                promise<> wait;
                _main_loop_wait = &wait;
                llogger.trace("background_reclaimer::main_loop: sleep");
                co_await wait.get_future();
                llogger.trace("background_reclaimer::main_loop: awakened");
                _main_loop_wait = nullptr;
            }
            if (_stopping) {
                break;
            }
            _reclaim(free_memory_threshold - memory::stats().free_memory());
            co_await make_ready_future<>();
        }
        llogger.debug("background_reclaimer::main_loop: exit");
    }
    void adjust_shares() {
        if (have_work()) {
            auto shares = 1 + (1000 * (free_memory_threshold - memory::stats().free_memory())) / free_memory_threshold;
            _sg.set_shares(shares);
            llogger.trace("background_reclaimer::adjust_shares: {}", shares);
            if (_main_loop_wait) {
                main_loop_wake();
            }
        }
    }
public:
    explicit background_reclaimer(scheduling_group sg, noncopyable_function<void (size_t target)> reclaim)
            : _sg(sg)
            , _reclaim(std::move(reclaim))
            , _adjust_shares_timer(default_scheduling_group(), [this] { adjust_shares(); })
            , _done(with_scheduling_group(_sg, [this] { return main_loop(); })) {
        if (sg != default_scheduling_group()) {
            _adjust_shares_timer.arm_periodic(50ms);
        }
    }
    future<> stop() {
        _stopping = true;
        main_loop_wake();
        return std::move(_done);
    }
};

class tracker::impl {
    std::optional<background_reclaimer> _background_reclaimer;
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
    future<> stop() {
        if (_background_reclaimer) {
            return _background_reclaimer->stop();
        } else {
            return make_ready_future<>();
        }
    }
    void register_region(region::impl*);
    void unregister_region(region::impl*) noexcept;
    size_t reclaim(size_t bytes, is_preemptible p);
    // Compacts one segment at a time from sparsest segment to least sparse until work_waiting_on_reactor returns true
    // or there are no more segments to compact.
    idle_cpu_handler_result compact_on_idle(work_waiting_on_reactor check_for_work);
    // Releases whole segments back to the segment pool.
    // After the call, if there is enough evictable memory, the amount of free segments in the pool
    // will be at least reserve_segments + div_ceil(bytes, segment::size).
    // Returns the amount by which segment_pool.total_memory_in_use() has decreased.
    size_t compact_and_evict(size_t reserve_segments, size_t bytes, is_preemptible p);
    void full_compaction();
    void reclaim_all_free_segments();
    occupancy_stats region_occupancy();
    occupancy_stats occupancy();
    size_t non_lsa_used_space();
    // Set the minimum number of segments reclaimed during single reclamation cycle.
    void set_reclamation_step(size_t step_in_segments) { _reclamation_step = step_in_segments; }
    size_t reclamation_step() const { return _reclamation_step; }
    // Abort on allocation failure from LSA
    void enable_abort_on_bad_alloc() { _abort_on_bad_alloc = true; }
    bool should_abort_on_bad_alloc() const { return _abort_on_bad_alloc; }
    void setup_background_reclaim(scheduling_group sg) {
        assert(!_background_reclaimer);
        _background_reclaimer.emplace(sg, [this] (size_t target) {
            reclaim(target, is_preemptible::yes);
        });
    }
private:
    // Like compact_and_evict() but assumes that reclaim_lock is held around the operation.
    size_t compact_and_evict_locked(size_t reserve_segments, size_t bytes, is_preemptible preempt);
};

class tracker_reclaimer_lock {
    tracker::impl::reclaiming_lock _lock;
public:
    tracker_reclaimer_lock() : _lock(shard_tracker().get_impl()) { }
};

tracker::tracker()
    : _impl(std::make_unique<impl>())
    , _reclaimer([this] (seastar::memory::reclaimer::request r) { return reclaim(r); }, memory::reclaimer_scope::sync)
{ }

tracker::~tracker() {
}

future<>
tracker::stop() {
    return _impl->stop();
}

size_t tracker::reclaim(size_t bytes) {
    return _impl->reclaim(bytes, is_preemptible::no);
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

struct alignas(segment_size) segment {
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

    bool is_empty();
    void record_alloc(size_type size);
    void record_free(size_type size);
    occupancy_stats occupancy();

    static void* operator new(size_t size) = delete;
    static void* operator new(size_t, void* ptr) noexcept { return ptr; }
    static void operator delete(void* ptr) = delete;
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
class segment_store {
    memory::memory_layout _layout;
    uintptr_t _segments_base; // The address of the first segment

public:
    size_t non_lsa_reserve = 0;
    segment_store()
        : _layout(memory::get_memory_layout())
        , _segments_base(align_down(_layout.start, (uintptr_t)segment::size)) {
    }
    segment* segment_from_idx(size_t idx) const {
        return reinterpret_cast<segment*>(_segments_base) + idx;
    }
    size_t idx_from_segment(segment* seg) const {
        return seg - reinterpret_cast<segment*>(_segments_base);
    }
    size_t new_idx_for_segment(segment* seg) {
        return idx_from_segment(seg);
    }
    void free_segment(segment *seg) { }
    size_t max_segments() const {
        return (_layout.end - _segments_base) / segment::size;
    }
    bool can_allocate_more_segments() {
        return memory::stats().free_memory() >= non_lsa_reserve + segment::size;
    }
};
#else
class segment_store {
    std::vector<segment*> _segments;
    std::unordered_map<segment*, size_t> _segment_indexes;
    static constexpr size_t _std_memory_available = size_t(1) << 30; // emulate 1GB per shard
    std::vector<segment*>::iterator find_empty() {
        // segment 0 is a marker for no segment
        return std::find(_segments.begin() + 1, _segments.end(), nullptr);
    }

public:
    size_t non_lsa_reserve = 0;
    segment_store() : _segments(max_segments()) {
        _segment_indexes.reserve(max_segments());
    }
    segment* segment_from_idx(size_t idx) const {
        assert(idx < _segments.size());
        return _segments[idx];
    }
    size_t idx_from_segment(segment* seg) {
        // segment 0 is a marker for no segment
        auto i = _segment_indexes.find(seg);
        if (i == _segment_indexes.end()) {
            return 0;
        }
        return i->second;
    }
    size_t new_idx_for_segment(segment* seg) {
        auto i = find_empty();
        assert(i != _segments.end());
        *i = seg;
        size_t ret = i - _segments.begin();
        _segment_indexes[seg] = ret;
        return ret;
    }
    void free_segment(segment *seg) {
        size_t i = idx_from_segment(seg);
        assert(i != 0);
        _segment_indexes.erase(seg);
        _segments[i] = nullptr;
    }
    ~segment_store() {
        for (segment *seg : _segments) {
            if (seg) {
                seg->~segment();
                free(seg);
            }
        }
    }
    size_t max_segments() const {
        return _std_memory_available / segment::size;
    }
    bool can_allocate_more_segments() {
        auto i = find_empty();
        return i != _segments.end();
    }
};
#endif

// Segment pool implementation for the seastar allocator.
// Stores segment descriptors in a vector which is indexed using most significant
// bits of segment address.
//
// We prefer using high-address segments, and returning low-address segments to the seastar
// allocator in order to segregate lsa and non-lsa memory, to reduce fragmentation.
class segment_pool {
    segment_store _store;
    std::vector<segment_descriptor> _segments;
    size_t _segments_in_use{};
    utils::dynamic_bitset _lsa_owned_segments_bitmap; // owned by this
    utils::dynamic_bitset _lsa_free_segments_bitmap;  // owned by this, but not in use
    size_t _free_segments = 0;
    size_t _current_emergency_reserve_goal = 1;
    size_t _emergency_reserve_max = 30;
    bool _allocation_failure_flag = false;
    bool _allocation_enabled = true;

    struct allocation_lock {
        segment_pool& _pool;
        bool _prev;
        allocation_lock(segment_pool& p)
            : _pool(p)
            , _prev(p._allocation_enabled)
        {
            _pool._allocation_enabled = false;
        }
        ~allocation_lock() {
            _pool._allocation_enabled = _prev;
        }
    };

    size_t _non_lsa_memory_in_use = 0;
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
    void deallocate_segment(segment* seg);
    friend void* segment::operator new(size_t);
    friend void segment::operator delete(void*);

    segment* allocate_or_fallback_to_reserve();
    void free_or_restore_to_reserve(segment* seg) noexcept;
    segment* segment_from_idx(size_t idx) const {
        return _store.segment_from_idx(idx);
    }
    size_t idx_from_segment(segment* seg) {
        return _store.idx_from_segment(seg);
    }
    size_t max_segments() const {
        return _store.max_segments();
    }
    bool can_allocate_more_segments() {
        return _allocation_enabled && _store.can_allocate_more_segments();
    }
    bool compact_segment(segment* seg);
public:
    segment_pool();
    void prime(size_t available_memory, size_t min_free_memory);
    segment* new_segment(region::impl* r);
    segment_descriptor& descriptor(segment*);
    // Returns segment containing given object or nullptr.
    segment* containing_segment(const void* obj);
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
    size_t total_free_memory() const {
        return _free_segments * segment::size;
    }
    struct reservation_goal;
    void set_region(segment* seg, region::impl* r) {
        set_region(descriptor(seg), r);
    }
    void set_region(segment_descriptor& desc, region::impl* r) {
        desc._region = r;
    }
    size_t reclaim_segments(size_t target, is_preemptible preempt);
    void reclaim_all_free_segments() {
        reclaim_segments(std::numeric_limits<size_t>::max(), is_preemptible::no);
    }

    struct stats {
        size_t segments_compacted;
        uint64_t memory_allocated;
        uint64_t memory_compacted;
    };
private:
    stats _stats{};
public:
    const stats& statistics() const { return _stats; }
    void on_segment_compaction(size_t used_size);
    void on_memory_allocation(size_t size);
    size_t unreserved_free_segments() const { return _free_segments - std::min(_free_segments, _emergency_reserve_max); }
    size_t free_segments() const { return _free_segments; }
};

size_t segment_pool::reclaim_segments(size_t target, is_preemptible preempt) {
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
            if (!compact_segment(src)) {
                if (--failed_reclaims_allowance == 0) {
                    break;
                }
                continue;
            }
        }
        _lsa_free_segments_bitmap.clear(src_idx);
        _lsa_owned_segments_bitmap.clear(src_idx);
        _store.free_segment(src);
        src->~segment();
        ::free(src);
        ++reclaimed_segments;
        --_free_segments;
        if (preempt && need_preempt()) {
            break;
        }
    }

    llogger.debug("Reclaimed {} segments (requested {})", reclaimed_segments, target);
    return reclaimed_segments;
}

segment* segment_pool::allocate_segment(size_t reserve)
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
        if (can_allocate_more_segments()) {
            memory::disable_abort_on_alloc_failure_temporarily dfg;
            auto p = aligned_alloc(segment::size, segment::size);
            if (!p) {
                continue;
            }
            auto seg = new (p) segment;
            poison(seg, sizeof(segment));
            auto idx = _store.new_idx_for_segment(seg);
            _lsa_owned_segments_bitmap.set(idx);
            return seg;
        }
    } while (shard_tracker().get_impl().compact_and_evict(reserve, shard_tracker().reclamation_step() * segment::size, is_preemptible::no));
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
        auto seg = allocate_segment(_emergency_reserve_max);
        if (!seg) {
            throw std::bad_alloc();
        }
        ++_segments_in_use;
        free_segment(seg);
    }
}

segment_descriptor&
segment_pool::descriptor(segment* seg) {
    uintptr_t index = idx_from_segment(seg);
    return _segments[index];
}

segment*
segment_pool::containing_segment(const void* obj) {
    auto addr = reinterpret_cast<uintptr_t>(obj);
    auto offset = addr & (segment::size - 1);
    auto seg = reinterpret_cast<segment*>(addr - offset);
    auto index = idx_from_segment(seg);
    auto& desc = _segments[index];
    if (desc._region) {
        return seg;
    } else {
        return nullptr;
    }
}

segment*
segment_pool::segment_from(const segment_descriptor& desc) {
    assert(desc._region);
    auto index = &desc - &_segments[0];
    return segment_from_idx(index);
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
    llogger.trace("Releasing segment {}", fmt::ptr(seg));
    desc._region = nullptr;
    deallocate_segment(seg);
    --_segments_in_use;
}

segment_pool::segment_pool()
    : _segments(max_segments())
    , _lsa_owned_segments_bitmap(max_segments())
    , _lsa_free_segments_bitmap(max_segments())
{
}

void segment_pool::prime(size_t available_memory, size_t min_free_memory) {
    auto old_emergency_reserve = std::exchange(_emergency_reserve_max, std::numeric_limits<size_t>::max());
    try {
        // Allocate all of memory so that we occupy the top part. Afterwards, we'll start
        // freeing from the bottom.
        _store.non_lsa_reserve = 0;
        refill_emergency_reserve();
    } catch (std::bad_alloc&) {
        _emergency_reserve_max = old_emergency_reserve;
    }
    // We want to leave more free memory than just min_free_memory() in order to reduce
    // the frequency of expensive segment-migrating reclaim() called by the seastar allocator.
    size_t min_gap = 1 * 1024 * 1024;
    size_t max_gap = 32 * 1024 * 1024;
    size_t gap = std::min(max_gap, std::max(available_memory / 16, min_gap));
    _store.non_lsa_reserve = min_free_memory + gap;
    // Since the reclaimer is not yet in place, free some low memory for general use
    reclaim_segments(_store.non_lsa_reserve / segment::size, is_preemptible::no);
}

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

static segment_pool& get_shard_segment_pool() noexcept {
    memory::scoped_critical_alloc_section dfg;
    static thread_local segment_pool obj;
    return obj;
}

static thread_local segment_pool& shard_segment_pool = get_shard_segment_pool();

void segment::record_alloc(segment::size_type size) {
    shard_segment_pool.descriptor(this).record_alloc(size);
}

void segment::record_free(segment::size_type size) {
    shard_segment_pool.descriptor(this).record_free(size);
}

bool segment::is_empty() {
    return shard_segment_pool.descriptor(this).is_empty();
}

occupancy_stats
segment::occupancy() {
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
            auto start = pos;
            do {
                b |= n & 63;
                n >>= 6;
                if (!n) {
                    b |= 128;
                }
                unpoison(pos, 1);
                *pos++ = b;
                b = 0;
            } while (n);
            poison(start, pos - start);
        }

        // non-canonical encoding to allow padding (for alignment); encoded_size must be
        // sufficient (greater than this->encoded_size())
        void encode(char*& pos, size_t encoded_size) const {
            uint64_t b = 64;
            auto start = pos;
            unpoison(start, encoded_size);
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
            poison(start, pos - start);
        }

        static object_descriptor decode_forwards(const char*& pos) {
            unsigned n = 0;
            unsigned shift = 0;
            auto p = pos; // avoid aliasing; p++ doesn't touch memory
            uint8_t b;
            do {
                unpoison(p, 1);
                b = *p++;
                if (shift < 32) {
                    // non-canonical encoding can cause large shift; undefined in C++
                    n |= uint32_t(b & 63) << shift;
                }
                shift += 6;
            } while ((b & 128) == 0);
            poison(pos, p - pos);
            pos = p;
            return object_descriptor(n);
        }

        static object_descriptor decode_backwards(const char*& pos) {
            unsigned n = 0;
            uint8_t b;
            auto p = pos; // avoid aliasing; --p doesn't touch memory
            do {
                --p;
                unpoison(p, 1);
                b = *p;
                n = (n << 6) | (b & 63);
            } while ((b & 64) == 0);
            poison(p, pos - p);
            pos = p;
            return object_descriptor(n);
        }

        friend std::ostream& operator<<(std::ostream& out, const object_descriptor& desc) {
            if (!desc.is_live()) {
                return out << format("{{free {:d}}}", desc.dead_size());
            } else {
                auto m = desc.migrator();
                auto x = reinterpret_cast<uintptr_t>(&desc) + sizeof(desc);
                x = align_up(x, m->align());
                auto obj = reinterpret_cast<const void*>(x);
                return out << format("{{migrator={:p}, alignment={:d}, size={:d}}}",
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

    void* alloc_small(const object_descriptor& desc, segment::size_type size, size_t alignment) {
        if (!_active) {
            _active = new_segment();
            _active_offset = 0;
        }

        auto desc_encoded_size = desc.encoded_size();

        size_t obj_offset = align_up_for_asan(align_up(_active_offset + desc_encoded_size, alignment));
        if (obj_offset + size > segment::size) {
            close_and_open();
            return alloc_small(desc, size, alignment);
        }

        auto old_active_offset = _active_offset;
        auto pos = _active->at<char>(_active_offset);
        // Use non-canonical encoding to allow for alignment pad
        desc.encode(pos, obj_offset - _active_offset);
        unpoison(pos, size);
        _active_offset = obj_offset + size;

        // Align the end of the value so that the next descriptor is aligned
        _active_offset = align_up_for_asan(_active_offset);
        _active->record_alloc(_active_offset - old_active_offset);
        return pos;
    }

    template<typename Func>
    void for_each_live(segment* seg, Func&& func) {
        // scylla-gdb.py:scylla_lsa_segment is coupled with this implementation.

        static_assert(std::is_same<void, std::result_of_t<Func(const object_descriptor*, void*, size_t)>>::value, "bad Func signature");

        auto pos = align_up_for_asan(seg->at<const char>(0));
        while (pos < seg->at<const char>(segment::size)) {
            auto old_pos = pos;
            const auto desc = object_descriptor::decode_forwards(pos);
            if (desc.is_live()) {
                auto size = desc.live_size(pos);
                func(&desc, const_cast<char*>(pos), size);
                pos += size;
            } else {
                pos = old_pos + desc.dead_size();
            }
            pos = align_up_for_asan(pos);
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
        llogger.trace("Closing segment {}, used={}, waste={} [B]", fmt::ptr(_active), _active->occupancy(), segment::size - _active_offset);
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

    void compact_segment_locked(segment* seg, segment_descriptor& desc) {
        auto seg_occupancy = desc.occupancy();
        llogger.debug("Compacting segment {} from region {}, {}", fmt::ptr(seg), id(), seg_occupancy);

        ++_invalidate_counter;

        for_each_live(seg, [this] (const object_descriptor* desc, void* obj, size_t size) {
            auto dst = alloc_small(*desc, size, desc->alignment());
            _sanitizer.on_migrate(obj, size, dst);
            desc->migrator()->migrate(obj, dst, size);
        });

        free_segment(seg, desc);
        shard_segment_pool.on_segment_compaction(seg_occupancy.used_space());
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
            auto ptr = alloc_small(object_descriptor(migrator), (segment::size_type) size, alignment);
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
        auto dead_size = align_up_for_asan(size + (old_pos - pos));
        desc = object_descriptor::make_dead(dead_size);
        auto npos = const_cast<char*>(pos);
        desc.encode(npos);
        poison(pos, dead_size);

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
        memory::scoped_critical_alloc_section dfg;

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

    // Compacts a single segment, most appropriate for it
    void compact() {
        compaction_lock _(*this);
        auto& desc = _segment_descs.one_of_largest();
        _segment_descs.pop_one_of_largest();
        _closed_occupancy -= desc.occupancy();
        segment* seg = shard_segment_pool.segment_from(desc);
        compact_segment_locked(seg, desc);
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
            compact_segment_locked(shard_segment_pool.segment_from(desc), desc);
        }
        llogger.debug("Done, {}", occupancy());
    }

    void compact_segment(segment* seg, segment_descriptor& desc) {
        compaction_lock _(*this);
        if (_active == seg) {
            close_active();
        }
        _segment_descs.erase(desc);
        _closed_occupancy -= desc.occupancy();
        compact_segment_locked(seg, desc);
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

size_t tracker::reclamation_step() const {
    return _impl->reclamation_step();
}

bool tracker::should_abort_on_bad_alloc() {
    return _impl->should_abort_on_bad_alloc();
}

void tracker::configure(const config& cfg) {
    if (cfg.defragment_on_idle) {
        engine().set_idle_cpu_handler([this] (reactor::work_waiting_on_reactor check_for_work) {
            return _impl->compact_on_idle(check_for_work);
        });
    }

    _impl->set_reclamation_step(cfg.lsa_reclamation_step);
    if (cfg.abort_on_lsa_bad_alloc) {
        _impl->enable_abort_on_bad_alloc();
    }
    _impl->setup_background_reclaim(cfg.background_reclaim_sched_group);
}

memory::reclaiming_result tracker::reclaim(seastar::memory::reclaimer::request r) {
    return reclaim(std::max(r.bytes_to_reclaim, _impl->reclamation_step() * segment::size))
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

occupancy_stats region::evictable_occupancy() {
    return get_impl().evictable_occupancy();
}

const eviction_fn& region::evictor() const {
    return get_impl().evictor();
}

std::ostream& operator<<(std::ostream& out, const occupancy_stats& stats) {
    return out << format("{:.2f}%, {:d} / {:d} [B]",
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
#ifdef SEASTAR_DEFAULT_ALLOCATOR
    return 0;
#else
    auto free_space_in_lsa = shard_segment_pool.free_segments() * segment_size;
    return memory::stats().allocated_memory() - region_occupancy().total_space() - free_space_in_lsa;
#endif
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

static void reclaim_from_evictable(region::impl& r, size_t target_mem_in_use, is_preemptible preempt) {
    llogger.debug("reclaim_from_evictable: total_memory_in_use={} target={}", shard_segment_pool.total_memory_in_use(), target_mem_in_use);

    // Before attempting segment compaction, try to evict at least deficit and one segment more so that
    // for workloads in which eviction order matches allocation order we will reclaim full segments
    // without needing to perform expensive compaction.
    auto deficit = shard_segment_pool.total_memory_in_use() - target_mem_in_use;
    auto used = r.occupancy().used_space();
    auto used_target = used - std::min(used, deficit + segment::size);

    while (shard_segment_pool.total_memory_in_use() > target_mem_in_use) {
        used = r.occupancy().used_space();
        if (used > used_target) {
            llogger.debug("Evicting {} bytes from region {}, occupancy={} in advance",
                    used - used_target, r.id(), r.occupancy());
        } else {
            llogger.debug("Evicting from region {}, occupancy={} until it's compactible", r.id(), r.occupancy());
        }
        while (r.occupancy().used_space() > used_target || !r.is_compactible()) {
            if (r.evict_some() == memory::reclaiming_result::reclaimed_nothing) {
                if (r.is_compactible()) { // Need to make forward progress in case there is nothing to evict.
                    break;
                }
                llogger.debug("Unable to evict more, evicted {} bytes", used - r.occupancy().used_space());
                return;
            }
            if (preempt && need_preempt()) {
                llogger.debug("reclaim_from_evictable preempted");
                return;
            }
        }
        // If there are many compactible segments, we will keep compacting without
        // entering the eviction loop above. So the preemption check there is not
        // sufficient and we also need to check here.
        //
        // Note that a preemptible reclaim_from_evictable may not do any real progress,
        // but it doesn't need to. Preemptible (background) reclaim is an optimization.
        // If the system is overwhelmed, and reclaim_from_evictable keeps getting
        // preempted without doing any useful work, then eventually memory will be
        // exhausted and reclaim will be called synchronously, without preemption.
        if (preempt && need_preempt()) {
            llogger.debug("reclaim_from_evictable preempted");
            return;
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
                                format("{:.3f}", bytes_per_second / (1024*1024)));
        }
    }
};

idle_cpu_handler_result tracker::impl::compact_on_idle(work_waiting_on_reactor check_for_work) {
    if (!_reclaiming_enabled) {
        return idle_cpu_handler_result::no_more_work;
    }
    reclaiming_lock rl(*this);
    if (_regions.empty()) {
        return idle_cpu_handler_result::no_more_work;
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
            return idle_cpu_handler_result::no_more_work;
        }

        r->compact();

        boost::range::push_heap(_regions, cmp);
    }
    return idle_cpu_handler_result::interrupted_by_higher_priority_task;
}

size_t tracker::impl::reclaim(size_t memory_to_release, is_preemptible preempt) {
    // Reclamation steps:
    // 1. Try to release free segments from segment pool and emergency reserve.
    // 2. Compact used segments and/or evict data.

    if (!_reclaiming_enabled) {
        return 0;
    }
    reclaiming_lock rl(*this);
    reclaim_timer timing_guard;

    constexpr auto max_bytes = std::numeric_limits<size_t>::max() - segment::size;
    auto segments_to_release = align_up(std::min(max_bytes, memory_to_release), segment::size) >> segment::size_shift;
    auto nr_released = shard_segment_pool.reclaim_segments(segments_to_release, preempt);
    size_t mem_released = nr_released * segment::size;
    if (mem_released >= memory_to_release) {
        return memory_to_release;
    }
    if (preempt && need_preempt()) {
        return mem_released;
    }

    auto compacted = compact_and_evict_locked(shard_segment_pool.current_emergency_reserve_goal(), memory_to_release - mem_released, preempt);

    if (compacted == 0) {
        return mem_released;
    }

    // compact_and_evict_locked() will not return segments to the standard allocator,
    // so do it here:
    nr_released = shard_segment_pool.reclaim_segments(compacted / segment::size, preempt);

    return mem_released + nr_released * segment::size;
}

size_t tracker::impl::compact_and_evict(size_t reserve_segments, size_t memory_to_release, is_preemptible preempt) {
    if (!_reclaiming_enabled) {
        return 0;
    }
    reclaiming_lock rl(*this);
    reclaim_timer timing_guard;
    size_t released = compact_and_evict_locked(reserve_segments, memory_to_release, preempt);
    timing_guard.stop(released);
    return released;
}

size_t tracker::impl::compact_and_evict_locked(size_t reserve_segments, size_t memory_to_release, is_preemptible preempt) {
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
    memory_to_release += (reserve_segments - std::min(reserve_segments, shard_segment_pool.free_segments())) * segment::size;
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
            reclaim_from_evictable(*r, target_mem, preempt);
        } else {
            r->compact();
        }

        boost::range::push_heap(_regions, cmp);

        if (preempt && need_preempt()) {
            break;
        }
    }

    auto released_during_compaction = mem_in_use - shard_segment_pool.total_memory_in_use();

    if (shard_segment_pool.total_memory_in_use() > target_mem) {
        llogger.debug("Considering evictable regions.");
        // FIXME: Fair eviction
        for (region::impl* r : _regions) {
            if (preempt && need_preempt()) {
                break;
            }
            if (r->is_evictable()) {
                reclaim_from_evictable(*r, target_mem, preempt);
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

void tracker::impl::register_region(region::impl* r) {
    // If needed, increase capacity of regions before taking the reclaim lock,
    // to avoid failing an allocation when push_back() tries to increase
    // capacity.
    //
    // The capacity increase is atomic (wrt _regions) so it cannot be
    // observed
    if (_regions.size() == _regions.capacity()) {
        auto copy = _regions;
        copy.reserve(copy.capacity() * 2);
        _regions = std::move(copy);
    }
    reclaiming_lock _(*this);
    _regions.push_back(r);
    llogger.debug("Registered region @{} with id={}", fmt::ptr(r), r->id());
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

bool segment_pool::compact_segment(segment* seg) {
    auto& desc = descriptor(seg);
    if (!desc._region->reclaiming_enabled()) {
        return false;
    }

    // Called with emergency reserve, open one for
    // region::alloc_small not to throw if it needs
    // one more segment
    reservation_goal open_emergency_pool(*this, 0);
    allocation_lock no_alloc(*this);
    tracker_reclaimer_lock no_reclaim;

    desc._region->compact_segment(seg, desc);
    return true;
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

region_group::region_group(sstring name, region_group *parent,
        region_group_reclaimer& reclaimer, scheduling_group deferred_work_sg)
    : _parent(parent)
    , _reclaimer(reclaimer)
    , _blocked_requests(on_request_expiry{std::move(name)})
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

void allocating_section::maybe_decay_reserve() {
    // The decay rate is inversely proportional to the reserve
    // (every (s_segments_per_decay/_lsa_reserve) allocations).
    //
    // If the reserve is high, it is expensive since we may need to
    // evict a lot of memory to satisfy the reserve. Hence, we are
    // willing to risk a more frequent bad_alloc in order to decay it.
    // The cost of a bad_alloc is also lower compared to maintaining
    // the reserve.
    //
    // If the reserve is low, it is not expensive to maintain, so we
    // decay it at a lower rate.

    _remaining_lsa_segments_until_decay -= _lsa_reserve;
    if (_remaining_lsa_segments_until_decay < 0) {
        _remaining_lsa_segments_until_decay = s_segments_per_decay;
        _lsa_reserve = std::max(s_min_lsa_reserve, _lsa_reserve / 2);
        llogger.debug("Decaying LSA reserve in section {} to {} segments", static_cast<void*>(this), _lsa_reserve);
    }

    _remaining_std_bytes_until_decay -= _std_reserve;
    if (_remaining_std_bytes_until_decay < 0) {
        _remaining_std_bytes_until_decay = s_bytes_per_decay;
        _std_reserve = std::max(s_min_std_reserve, _std_reserve / 2);
        llogger.debug("Decaying standard allocator head-room in section {} to {} [B]", static_cast<void*>(this), _std_reserve);
    }
}

void allocating_section::reserve() {
  try {
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
  } catch (const std::bad_alloc&) {
        if (shard_tracker().should_abort_on_bad_alloc()) {
            llogger.error("Aborting due to allocation failure");
            abort();
        }
        throw;
  }
}

void allocating_section::on_alloc_failure(logalloc::region& r) {
    r.allocator().invalidate_references();
    if (shard_segment_pool.allocation_failure_flag()) {
        _lsa_reserve *= 2;
        llogger.debug("LSA allocation failure, increasing reserve in section {} to {} segments", fmt::ptr(this), _lsa_reserve);
    } else {
        _std_reserve *= 2;
        llogger.debug("Standard allocator failure, increasing head-room in section {} to {} [B]", fmt::ptr(this), _std_reserve);
    }
    reserve();
}

void allocating_section::set_lsa_reserve(size_t reserve) {
    _lsa_reserve = reserve;
}

void allocating_section::set_std_reserve(size_t reserve) {
    _std_reserve = reserve;
}

void region_group::on_request_expiry::operator()(std::unique_ptr<allocating_function>& func) noexcept {
    func->fail(std::make_exception_ptr(blocked_requests_timed_out_error{_name}));
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

occupancy_stats lsa_global_occupancy_stats() {
    return occupancy_stats(shard_segment_pool.total_free_memory(), shard_segment_pool.total_memory_in_use());
}

}

// Orders segments by free space, assuming all segments have the same size.
// This avoids using the occupancy, which entails extra division operations.
template<>
size_t hist_key<logalloc::segment_descriptor>(const logalloc::segment_descriptor& desc) {
    return desc._free_space;
}
