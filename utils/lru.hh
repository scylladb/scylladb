/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "utils/assert.hh"
#include "utils/count_min_sketch.hh"
#include <boost/intrusive/list.hpp>
#include <seastar/core/memory.hh>
#include <algorithm>
#include <cmath>

// Identifies which W-TinyLFU segment an evictable belongs to.
enum class lru_segment : uint8_t {
    none = 0,
    window = 1,
    probation = 2,
    protected_ = 3,
};

class evictable {
    friend class lru;
    // For bookkeeping, we want the unlinking of evictables to be explicit.
    // E.g. if the cache's internal data structure consists of multiple lists, we would
    // like to know which list is an element being removed from.
    // Therefore, we are using auto_unlink only to be able to call unlink() in the move constructor
    // and we do NOT rely on automatic unlinking in _lru_link's destructor.
    // It's the programmer's responsibility. to call lru::remove on the evictable before its destruction.
    // Failure to do so is a bug, and it will trigger an assertion in the destructor.
protected:
    using link_base = boost::intrusive::list_member_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>;
    struct lru_link_type : link_base {
        lru_link_type() noexcept = default;
        lru_link_type(lru_link_type&& o) noexcept {
            swap_nodes(o);
        }
    };
    static_assert(std::is_nothrow_constructible_v<lru_link_type, lru_link_type&&>);
private:
    lru_link_type _lru_link;
    lru_segment _segment = lru_segment::none;
    uint64_t _sketch_key = 0;
protected:
    // Prevent destruction via evictable pointer. LRU is not aware of allocation strategy.
    // Prevent destruction of a linked evictable. While we could unlink the evictable here
    // in the destructor, we can't perform proper accounting for that without access to the
    // head of the containing list.
    ~evictable() {
        SCYLLA_ASSERT(!_lru_link.is_linked());
    }
    evictable() = default;
    evictable(evictable&&) noexcept = default;
public:
    virtual void on_evicted() noexcept = 0;

    // Used for testing to avoid cascading eviction of the containing object.
    virtual void on_evicted_shallow() noexcept { on_evicted(); }

    bool is_linked() const {
        return _lru_link.is_linked();
    }

    void swap(evictable& o) noexcept {
        _lru_link.swap_nodes(o._lru_link);
        std::swap(_segment, o._segment);
        std::swap(_sketch_key, o._sketch_key);
    }

    virtual bool is_index() const noexcept {
        return false;
    }

    void set_sketch_key(uint64_t key) noexcept { _sketch_key = key; }
    uint64_t sketch_key() const noexcept { return _sketch_key; }
};

// Sstable index cache shares memory with the data cache.
// To prevent index entries from depriving the data cache of memory,
// there is a limit (index_cache_fraction) on the total fraction of cache usable
// by index entries.
//
// To maintain this limit, index entries might have to be evicted outside of the regular LRU order.
// Therefore they are linked both in the common LRU list and in a separate LRU list for index entries.
class index_evictable : public evictable {
    friend class lru;
    evictable::lru_link_type _index_lru_link;
    bool is_index() const noexcept override {
        return true;
    }
};

// Implements W-TinyLFU cache replacement for row cache and sstable index cache.
//
// W-TinyLFU uses a small admission window backed by an LRU and a main cache
// organized as a Segmented LRU (SLRU) with probation and protected segments.
// Admission to the main cache is controlled by a TinyLFU frequency filter
// implemented via a Count-Min Sketch.
//
// New entries enter the window. When eviction is needed, the window victim
// competes with the probation victim: the entry with higher estimated
// frequency survives in probation while the other is evicted.
// Touching an entry in probation promotes it to the protected segment.
// When the protected segment exceeds its target size, the least-recently-used
// protected entry is demoted back to probation.
class lru {
private:
    using lru_type = boost::intrusive::list<evictable,
        boost::intrusive::member_hook<evictable, evictable::lru_link_type, &evictable::_lru_link>,
        boost::intrusive::constant_time_size<false>>; // we need this to have bi::auto_unlink on hooks.
    lru_type _window;
    lru_type _probation;
    lru_type _protected;

    // See the comment to index_evictable.
    using index_lru_type = boost::intrusive::list<index_evictable,
        boost::intrusive::member_hook<index_evictable, index_evictable::lru_link_type, &index_evictable::_index_lru_link>,
        boost::intrusive::constant_time_size<false>>; // we need this to have bi::auto_unlink on hooks.
    index_lru_type _index_list;

    using reclaiming_result = seastar::memory::reclaiming_result;

    // Default sketch width log2.  Results in 65536 counters per row (128 KiB total).
    static constexpr size_t default_sketch_width_log2 = 16;
    // Minimum / maximum sketch width log2, clamped during resize.
    static constexpr size_t min_sketch_width_log2 = 10; // 1 K counters per row
    static constexpr size_t max_sketch_width_log2 = 24; // 16 M counters per row
    // Window segment target as a percentage of total cache entries (default 1%).
    // Configurable at runtime via set_window_percent().
    size_t _window_percent = 1;
    // Protected segment target: (100 - window_percent - 1)% approximated as 80%.
    static constexpr size_t default_protected_percent = 80;
    utils::count_min_sketch _sketch{default_sketch_width_log2};
    size_t _window_size = 0;
    size_t _probation_size = 0;
    size_t _protected_size = 0;
    size_t _sample_count = 0;
    // Aging threshold: reset sketch every max(min_sample_threshold, total_entries * 10)
    // accesses.  Matches Caffeine's sampleSize = 10 * maximumEntries.
    static constexpr size_t min_sample_threshold = 1000;
    size_t _sample_threshold = min_sample_threshold;

    // Hash-DoS jitter: minimum frequency for randomized admission (matches Caffeine).
    static constexpr uint8_t admit_hashdos_threshold = 6;
    // Simple LCG for jitter (avoids #include <random>; one per lru instance).
    uint32_t _jitter_state = 0x12345678;

public:
    struct stats {
        // Admission gate
        uint64_t tinylfu_admissions = 0;
        uint64_t tinylfu_rejections = 0;
        uint64_t tinylfu_jitter_admissions = 0;

        // Path 2: evictions bypassing the admission gate
        uint64_t direct_evictions = 0;

        // Segment flow
        uint64_t protected_promotions = 0;
        uint64_t protected_demotions = 0;
        uint64_t window_to_probation = 0;

        // Sketch lifecycle
        uint64_t sketch_resets = 0;

        // Sampled avg frequency per segment (updated once per sketch reset)
        double sampled_avg_freq_window = 0;
        double sampled_avg_freq_probation = 0;
        double sampled_avg_freq_protected = 0;

        // Frequency histogram for entries at admission time
        uint64_t admission_freq_bucket_0_1 = 0;
        uint64_t admission_freq_bucket_2_3 = 0;
        uint64_t admission_freq_bucket_4_7 = 0;
        uint64_t admission_freq_bucket_8_15 = 0;

        // LSA eviction trigger tracking
        uint64_t eviction_calls = 0;
        uint64_t eviction_calls_empty = 0;
    };

private:
    stats _stats{};

    void record_freq_bucket(uint8_t freq) noexcept {
        if (freq <= 1) ++_stats.admission_freq_bucket_0_1;
        else if (freq <= 3) ++_stats.admission_freq_bucket_2_3;
        else if (freq <= 7) ++_stats.admission_freq_bucket_4_7;
        else ++_stats.admission_freq_bucket_8_15;
    }

    void sample_segment_frequencies() noexcept {
        auto avg_freq = [this](const lru_type& list, size_t count) -> double {
            if (count == 0) return 0.0;
            uint64_t sum = 0;
            size_t sampled = 0;
            constexpr size_t max_sample = 1000;
            for (const auto& e : list) {
                sum += _sketch.estimate(entry_key(e));
                if (++sampled >= max_sample) break;
            }
            return sampled > 0 ? static_cast<double>(sum) / sampled : 0.0;
        };
        _stats.sampled_avg_freq_window = avg_freq(_window, _window_size);
        _stats.sampled_avg_freq_probation = avg_freq(_probation, _probation_size);
        _stats.sampled_avg_freq_protected = avg_freq(_protected, _protected_size);
    }

    size_t total_size() const noexcept {
        return _window_size + _probation_size + _protected_size;
    }

    size_t max_window_size() const noexcept {
        return std::max(size_t(1), total_size() * _window_percent / 100);
    }

    size_t max_protected_size() const noexcept {
        return total_size() * default_protected_percent / 100;
    }

    static uint64_t entry_key(const evictable& e) noexcept {
        // Fall back to address if no sketch key set yet (safe standalone
        // until callers are updated to set logical keys).
        return e._sketch_key ? e._sketch_key
                             : static_cast<uint64_t>(reinterpret_cast<uintptr_t>(&e));
    }

    // xorshift32 — fast, no extra includes, sufficient for jitter.
    uint32_t jitter_next() noexcept {
        _jitter_state ^= _jitter_state << 13;
        _jitter_state ^= _jitter_state >> 17;
        _jitter_state ^= _jitter_state << 5;
        return _jitter_state;
    }

    void record_access(const evictable& e) noexcept {
        _sketch.increment(entry_key(e));
        if (++_sample_count >= _sample_threshold) {
            _sketch.reset();
            _sample_count = 0;
            _sample_threshold = std::max(min_sample_threshold, total_size() * 10);
            ++_stats.sketch_resets;
            sample_segment_frequencies();
        }
    }

    lru_type& segment_list(lru_segment seg) noexcept {
        switch (seg) {
            case lru_segment::window: return _window;
            case lru_segment::probation: return _probation;
            case lru_segment::protected_: return _protected;
            default: {
                SCYLLA_ASSERT(false && "invalid segment");
                __builtin_unreachable();
            }
        }
    }

    void increment_size(lru_segment seg) noexcept {
        switch (seg) {
            case lru_segment::window: ++_window_size; break;
            case lru_segment::probation: ++_probation_size; break;
            case lru_segment::protected_: ++_protected_size; break;
            default: break;
        }
    }

    void decrement_size(lru_segment seg) noexcept {
        switch (seg) {
            case lru_segment::window: --_window_size; break;
            case lru_segment::probation: --_probation_size; break;
            case lru_segment::protected_: --_protected_size; break;
            default: break;
        }
    }

    void remove_from_segment(evictable& e) noexcept {
        auto& list = segment_list(e._segment);
        list.erase(list.iterator_to(e));
        decrement_size(e._segment);
        e._segment = lru_segment::none;
    }

    void add_to_segment(evictable& e, lru_segment seg) noexcept {
        e._segment = seg;
        segment_list(seg).push_back(e);
        increment_size(seg);
    }

    // Move excess protected entries to probation.
    void rebalance_protected() noexcept {
        size_t max_prot = max_protected_size();
        while (_protected_size > max_prot && !_protected.empty()) {
            ++_stats.protected_demotions;
            evictable& victim = _protected.front();
            remove_from_segment(victim);
            add_to_segment(victim, lru_segment::probation);
        }
    }

    // Drain excess entries from the window segment using TinyLFU admission.
    // Returns true if any entries were evicted.
    //
    // Each excess window entry competes against the probation LRU victim:
    // if the window entry has higher frequency it replaces the victim in
    // probation; otherwise the window entry is evicted.  This runs to
    // completion so the window converges to its target size.
    template <bool Shallow = false>
    bool drain_window() noexcept {
        bool drained_any = false;
        while (_window_size > max_window_size() && !_window.empty()) {
            evictable& w_victim = _window.front();

            if (!_probation.empty()) {
                evictable& p_victim = _probation.front();
                uint8_t w_freq = _sketch.estimate(entry_key(w_victim));
                uint8_t p_freq = _sketch.estimate(entry_key(p_victim));

                bool admit_candidate;
                if (w_freq > p_freq) {
                    admit_candidate = true;
                } else if (w_freq >= admit_hashdos_threshold) {
                    admit_candidate = (jitter_next() & 127) == 0;
                    if (admit_candidate) ++_stats.tinylfu_jitter_admissions;
                } else {
                    admit_candidate = false;
                }

                if (admit_candidate) {
                    ++_stats.tinylfu_admissions;
                    ++_stats.window_to_probation;
                    record_freq_bucket(w_freq);
                    remove_from_segment(w_victim);
                    add_to_segment(w_victim, lru_segment::probation);
                    remove(p_victim);
                    if constexpr (!Shallow) {
                        p_victim.on_evicted();
                    } else {
                        p_victim.on_evicted_shallow();
                    }
                } else {
                    ++_stats.tinylfu_rejections;
                    record_freq_bucket(w_freq);
                    remove(w_victim);
                    if constexpr (!Shallow) {
                        w_victim.on_evicted();
                    } else {
                        w_victim.on_evicted_shallow();
                    }
                }
                drained_any = true;
                continue;
            }

            // Probation is empty: move window victim to probation and retry.
            ++_stats.window_to_probation;
            remove_from_segment(w_victim);
            add_to_segment(w_victim, lru_segment::probation);
        }
        return drained_any;
    }

    // Evicts a single element using W-TinyLFU policy.
    template <bool Shallow = false>
    reclaiming_result do_evict(bool should_evict_index) noexcept {
        // Index eviction path: evict the least recently used index entry.
        if (should_evict_index && !_index_list.empty()) {
            evictable& e = _index_list.front();
            remove(e);
            if constexpr (!Shallow) {
                e.on_evicted();
            } else {
                e.on_evicted_shallow();
            }
            return reclaiming_result::reclaimed_something;
        }

        if (_window.empty() && _probation.empty() && _protected.empty()) {
            return reclaiming_result::reclaimed_nothing;
        }

        rebalance_protected();

        if (drain_window<Shallow>()) {
            return reclaiming_result::reclaimed_something;
        }

        // Window is within target. Evict from probation, then window, then protected.
        ++_stats.direct_evictions;
        evictable* victim = nullptr;
        if (!_probation.empty()) {
            victim = &_probation.front();
        } else if (!_window.empty()) {
            victim = &_window.front();
        } else if (!_protected.empty()) {
            victim = &_protected.front();
        } else {
            return reclaiming_result::reclaimed_nothing;
        }
        remove(*victim);
        if constexpr (!Shallow) {
            victim->on_evicted();
        } else {
            victim->on_evicted_shallow();
        }
        return reclaiming_result::reclaimed_something;
    }

public:
    ~lru() {
        auto drain = [this](lru_type& list) {
            while (!list.empty()) {
                evictable& e = list.front();
                remove(e);
                e.on_evicted();
            }
        };
        drain(_window);
        drain(_probation);
        drain(_protected);
    }

    void remove(evictable& e) noexcept {
        auto& list = segment_list(e._segment);
        list.erase(list.iterator_to(e));
        decrement_size(e._segment);
        e._segment = lru_segment::none;
        if (e.is_index()) {
            _index_list.erase(_index_list.iterator_to(static_cast<index_evictable&>(e)));
        }
    }

    void add(evictable& e) noexcept {
        record_access(e);
        add_to_segment(e, lru_segment::window);
        if (e.is_index()) {
            _index_list.push_back(static_cast<index_evictable&>(e));
        }
        // Move excess window entries to probation when the window grows
        // significantly past its target.  This doesn't evict anything
        // (eviction only happens from LSA's evict() path), but it keeps
        // entries flowing into the SLRU segments so the admission filter
        // and probation->protected promotion can function.
        //
        // Without this, the window bloats between LSA evict() calls
        // and the SLRU segments starve.
        size_t max_win = max_window_size();
        if (_window_size > max_win + std::max(max_win, size_t(64))) {
            while (_window_size > max_win && !_window.empty()) {
                ++_stats.window_to_probation;
                evictable& victim = _window.front();
                remove_from_segment(victim);
                add_to_segment(victim, lru_segment::probation);
            }
        }
    }

    // Like add(e) but makes sure that e is evicted right before "more_recent" in the absence of later touches.
    void add_before(evictable& more_recent, evictable& e) noexcept {
        record_access(e);
        lru_segment seg = more_recent._segment;
        auto& list = segment_list(seg);
        list.insert(list.iterator_to(more_recent), e);
        e._segment = seg;
        increment_size(seg);
    }

    // Handles access to an entry:
    //  - In window: moves to back of window.
    //  - In probation: promotes to protected.
    //  - In protected: moves to back of protected.
    //  - Not linked: adds to window.
    void touch(evictable& e) noexcept {
        record_access(e);

        switch (e._segment) {
            case lru_segment::none:
                add_to_segment(e, lru_segment::window);
                break;
            case lru_segment::window:
                _window.erase(_window.iterator_to(e));
                _window.push_back(e);
                break;
            case lru_segment::probation:
                ++_stats.protected_promotions;
                _probation.erase(_probation.iterator_to(e));
                --_probation_size;
                e._segment = lru_segment::protected_;
                _protected.push_back(e);
                ++_protected_size;
                break;
            case lru_segment::protected_:
                _protected.erase(_protected.iterator_to(e));
                _protected.push_back(e);
                break;
        }
    }

    // Evicts a single element using the W-TinyLFU policy.
    reclaiming_result evict(bool should_evict_index = false) noexcept {
        ++_stats.eviction_calls;
        auto result = do_evict<false>(should_evict_index);
        if (result == reclaiming_result::reclaimed_nothing) {
            ++_stats.eviction_calls_empty;
        }
        return result;
    }

    // Evicts a single element using the W-TinyLFU policy.
    // Will call on_evicted_shallow() instead of on_evicted().
    reclaiming_result evict_shallow() noexcept {
        ++_stats.eviction_calls;
        auto result = do_evict<true>(false);
        if (result == reclaiming_result::reclaimed_nothing) {
            ++_stats.eviction_calls_empty;
        }
        return result;
    }

    // Evicts all elements.
    // May stall the reactor, use only in tests.
    void evict_all() {
        while (evict() == reclaiming_result::reclaimed_something) {}
    }

    /// Resize the Count-Min Sketch to match the given width_log2.
    ///
    /// Existing counter values are scaled in-place (see count_min_sketch::resize).
    /// _sample_count is reset to 0 because the counter magnitudes changed.
    /// This is safe to call at any time; on-going record_access() calls will
    /// use the new threshold on the next invocation.
    void resize_sketch(size_t new_width_log2) noexcept {
        new_width_log2 = std::clamp(new_width_log2, min_sketch_width_log2, max_sketch_width_log2);
        _sketch.resize(new_width_log2);
        _sample_count = 0;
        // _sample_threshold stays entry-based; recomputed on next reset cycle.
    }

    /// Expose sketch frequency estimate for testing/debugging.
    uint8_t sketch_estimate(uint64_t key) const noexcept {
        return _sketch.estimate(key);
    }

    size_t current_max_window_size() const noexcept { return max_window_size(); }
    size_t current_max_protected_size() const noexcept { return max_protected_size(); }

    /// Fully reset the Count-Min Sketch (zero all counters) and clear the
    /// sample counter.  Call after evict_all() when a completely clean slate
    /// is needed (e.g. cache_tracker::clear(), test teardown).
    void reset_sketch() noexcept {
        _sketch.clear();
        _sample_count = 0;
    }

    /// Compute an appropriate sketch width_log2 given the cache size in bytes
    /// and a desired number of sketch entries per MB of cache.
    ///
    /// Formula:  estimated_entries = cache_bytes / 1MB * entries_per_mb
    ///           width_log2 = ceil(log2(estimated_entries)), clamped to
    ///           [min_sketch_width_log2, max_sketch_width_log2].
    ///
    /// With the default entries_per_mb = 1024 (i.e. 1 entry per KB) and a
    /// 100 MB per-shard cache, this gives:
    ///   100 * 1024 = 102400 entries → width_log2 = ceil(log2(102400)) = 17
    /// Set the window segment percentage (clamped to [1, 99]).
    void set_window_percent(double percent) noexcept {
        size_t p = static_cast<size_t>(std::clamp(percent, 1.0, 99.0));
        _window_percent = p;
    }

    size_t window_percent() const noexcept { return _window_percent; }

    stats& get_stats() noexcept { return _stats; }
    const stats& get_stats() const noexcept { return _stats; }

    size_t window_size() const noexcept { return _window_size; }
    size_t probation_size() const noexcept { return _probation_size; }
    size_t protected_size() const noexcept { return _protected_size; }
    size_t sample_count() const noexcept { return _sample_count; }
    size_t sample_threshold() const noexcept { return _sample_threshold; }

    static size_t compute_sketch_width_log2(size_t cache_bytes, double entries_per_mb) noexcept {
        constexpr double bytes_per_mb = 1024.0 * 1024.0;
        double estimated_entries = (static_cast<double>(cache_bytes) / bytes_per_mb) * entries_per_mb;
        if (estimated_entries < 1.0) {
            return min_sketch_width_log2;
        }
        size_t log2 = static_cast<size_t>(std::ceil(std::log2(estimated_entries)));
        return std::clamp(log2, min_sketch_width_log2, max_sketch_width_log2);
    }
};
