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
#include <seastar/core/preempt.hh>
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

    // Packed layout:
    //   bits [1:0]  — lru_segment tag (none=0, window=1, probation=2, protected=3)
    //   bit  [2]    — has_sketch_key flag
    //   bit  [3]    — directly_inserted flag (entry is linked in protected and
    //                 counted in _protected_direct_size; cleared on unlink)
    //   bit  [4]    — routes_to_protected flag (sticky routing hint: this entry
    //                 always (re-)enters the LRU in the protected segment;
    //                 survives unlinking)
    //   bits [63:5] — sketch key value (59 bits of token hash)
    uint64_t _packed = 0;

    static constexpr uint64_t segment_mask  = 0x3;
    static constexpr uint64_t has_key_mask  = uint64_t(1) << 2;
    static constexpr uint64_t direct_mask   = uint64_t(1) << 3;
    static constexpr uint64_t routing_mask  = uint64_t(1) << 4;
    static constexpr uint64_t key_shift     = 5;
    static constexpr uint64_t meta_mask     = segment_mask | has_key_mask | direct_mask | routing_mask;

    lru_segment get_segment() const noexcept {
        return static_cast<lru_segment>(_packed & segment_mask);
    }
    void set_segment(lru_segment seg) noexcept {
        _packed = (_packed & ~segment_mask) | static_cast<uint64_t>(seg);
    }
protected:
    ~evictable() {
        SCYLLA_ASSERT(!_lru_link.is_linked());
    }
    evictable() = default;
    evictable(evictable&&) noexcept = default;
public:
    virtual void on_evicted() noexcept = 0;
    virtual void on_evicted_shallow() noexcept { on_evicted(); }

    bool is_linked() const {
        return _lru_link.is_linked();
    }

    void swap(evictable& o) noexcept {
        _lru_link.swap_nodes(o._lru_link);
        std::swap(_packed, o._packed);
    }

    void set_sketch_key(uint64_t key) noexcept {
        _packed = (key << key_shift) | has_key_mask | (_packed & (segment_mask | direct_mask | routing_mask));
    }
    uint64_t sketch_key() const noexcept {
        return _packed >> key_shift;
    }
    bool has_sketch_key() const noexcept {
        return _packed & has_key_mask;
    }
    bool is_directly_inserted() const noexcept {
        return _packed & direct_mask;
    }
    void set_directly_inserted(bool v) noexcept {
        if (v) {
            _packed |= direct_mask;
        } else {
            _packed &= ~direct_mask;
        }
    }
    // Sticky routing hint for entries of multi-row (clustering key) partitions:
    // they always (re-)enter the LRU in the protected segment and are never
    // demoted. Unlike directly_inserted, this survives unlinking, so evicted
    // entries which get re-linked (e.g. a partition's last dummy touched after
    // eviction) return to protected rather than the admission window.
    bool routes_to_protected() const noexcept {
        return _packed & routing_mask;
    }
    void set_routes_to_protected(bool v) noexcept {
        if (v) {
            _packed |= routing_mask;
        } else {
            _packed &= ~routing_mask;
        }
    }
};

// Backwards-compatibility alias. Index entries now participate in the
// regular LRU without a separate list or hard capacity cap.
using index_evictable = evictable;

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
        boost::intrusive::constant_time_size<false>>;
    lru_type _window;
    lru_type _probation;
    lru_type _protected;

    using reclaiming_result = seastar::memory::reclaiming_result;

    static constexpr size_t default_sketch_width_log2 = 16;
    static constexpr size_t min_sketch_width_log2 = 10;
    static constexpr size_t max_sketch_width_log2 = 24;
    double _window_fraction = 0.01;
    static constexpr size_t default_protected_percent = 80;
    utils::count_min_sketch _sketch{default_sketch_width_log2};
    size_t _window_size = 0;
    size_t _probation_size = 0;
    size_t _protected_size = 0;
    size_t _protected_direct_size = 0;
    size_t _sample_count = 0;
    static constexpr size_t min_sample_threshold = 1000;
    size_t _sample_threshold = min_sample_threshold;
    static constexpr uint8_t admit_hashdos_threshold = 6;
    uint32_t _jitter_state = 0x12345678;

public:
    struct stats {
        uint64_t tinylfu_admissions = 0;
        uint64_t tinylfu_rejections = 0;
        uint64_t tinylfu_jitter_admissions = 0;
        uint64_t direct_evictions = 0;
        uint64_t protected_promotions = 0;
        uint64_t protected_demotions = 0;
        uint64_t window_to_probation = 0;
        uint64_t sketch_resets = 0;
        double sampled_avg_freq_window = 0;
        double sampled_avg_freq_probation = 0;
        double sampled_avg_freq_protected = 0;
        uint64_t admission_freq_bucket_0_1 = 0;
        uint64_t admission_freq_bucket_2_3 = 0;
        uint64_t admission_freq_bucket_4_7 = 0;
        uint64_t admission_freq_bucket_8_15 = 0;
        uint64_t eviction_calls = 0;
        uint64_t eviction_calls_empty = 0;
    };

private:
    stats _stats{};

    void record_freq_bucket(uint8_t freq) noexcept {
        static constexpr size_t bucket_map[16] = {
            0, 0, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3
        };
        uint64_t* buckets[] = {
            &_stats.admission_freq_bucket_0_1,
            &_stats.admission_freq_bucket_2_3,
            &_stats.admission_freq_bucket_4_7,
            &_stats.admission_freq_bucket_8_15,
        };
        ++(*buckets[bucket_map[freq & 0xf]]);
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
        return std::max(size_t(1), static_cast<size_t>(total_size() * _window_fraction));
    }

    size_t max_protected_size() const noexcept {
        size_t main_size = total_size() - std::min(max_window_size(), total_size());
        return main_size * default_protected_percent / 100;
    }

    static uint64_t entry_key(const evictable& e) noexcept {
        if (e.has_sketch_key()) {
            return e.sketch_key();
        }
        return static_cast<uint64_t>(reinterpret_cast<uintptr_t>(&e));
    }

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
        lru_type* lists[] = { nullptr, &_window, &_probation, &_protected };
        auto idx = static_cast<unsigned>(seg);
        SCYLLA_ASSERT(idx >= 1 && idx <= 3);
        return *lists[idx];
    }

    size_t* segment_size_ptr(lru_segment seg) noexcept {
        size_t* sizes[] = { nullptr, &_window_size, &_probation_size, &_protected_size };
        return sizes[static_cast<unsigned>(seg)];
    }

    void increment_size(lru_segment seg) noexcept {
        if (auto* p = segment_size_ptr(seg)) { ++(*p); }
    }

    void decrement_size(lru_segment seg) noexcept {
        if (auto* p = segment_size_ptr(seg)) { --(*p); }
    }

    void remove_from_segment(evictable& e) noexcept {
        auto seg = e.get_segment();
        if (seg == lru_segment::protected_ && e.is_directly_inserted()) {
            --_protected_direct_size;
            e.set_directly_inserted(false);
        }
        auto& list = segment_list(seg);
        list.erase(list.iterator_to(e));
        decrement_size(seg);
        e.set_segment(lru_segment::none);
    }

    void add_to_segment(evictable& e, lru_segment seg) noexcept {
        e.set_segment(seg);
        segment_list(seg).push_back(e);
        increment_size(seg);
    }

    // Move a linked-out entry to the back of protected. Sticky entries keep
    // their direct (never-demoted) status; others become regular promoted
    // entries subject to rebalance_protected().
    void promote_to_protected(evictable& e) noexcept {
        if (e.routes_to_protected()) {
            e.set_directly_inserted(true);
            ++_protected_direct_size;
        }
        e.set_segment(lru_segment::protected_);
        _protected.push_back(e);
        ++_protected_size;
    }

    // Move excess promoted entries from protected to probation.
    // Directly-inserted entries (multi-row schemas) are skipped in place —
    // they must remain in protected and their LRU ordering must not be disturbed.
    void rebalance_protected() noexcept {
        size_t promoted_count = _protected_size - _protected_direct_size;
        size_t max_prot = max_protected_size();
        auto it = _protected.begin();
        while (promoted_count > max_prot && it != _protected.end()) {
            evictable& victim = *it;
            ++it; // advance before remove invalidates the iterator
            if (victim.is_directly_inserted()) {
                continue;
            }
            ++_stats.protected_demotions;
            remove_from_segment(victim);
            add_to_segment(victim, lru_segment::probation);
            --promoted_count;
            if (seastar::need_preempt()) {
                break;
            }
        }
    }

    // Drain excess window entries using TinyLFU admission gate.
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
                if (seastar::need_preempt()) {
                    break;
                }
                continue;
            }

            ++_stats.window_to_probation;
            remove_from_segment(w_victim);
            add_to_segment(w_victim, lru_segment::probation);
        }
        return drained_any;
    }

    // Standard W-TinyLFU eviction: rebalance protected, drain window,
    // then evict from probation/window/protected.
    template <bool Shallow = false>
    reclaiming_result do_evict() noexcept {
        if (_window.empty() && _probation.empty() && _protected.empty()) {
            return reclaiming_result::reclaimed_nothing;
        }

        rebalance_protected();

        if (drain_window<Shallow>()) {
            return reclaiming_result::reclaimed_something;
        }

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
        if constexpr (!Shallow) { victim->on_evicted(); } else { victim->on_evicted_shallow(); }
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
        auto seg = e.get_segment();
        if (seg == lru_segment::protected_ && e.is_directly_inserted()) {
            --_protected_direct_size;
            e.set_directly_inserted(false);
        }
        auto& list = segment_list(seg);
        list.erase(list.iterator_to(e));
        decrement_size(seg);
        e.set_segment(lru_segment::none);
    }

    void add(evictable& e) noexcept {
        if (e.routes_to_protected()) {
            add_to_protected(e);
            return;
        }
        record_access(e);
        add_to_segment(e, lru_segment::window);
        // No drain here. The window grows unbounded; drain_window()
        // inside do_evict() handles the overflow with the admission
        // gate. This ensures every eviction goes through the frequency
        // comparison, providing scan resistance.
    }

    // Insert directly into the protected segment, bypassing the window.
    // Used for multi-row schemas where window admission creates MVCC/fairness issues.
    void add_to_protected(evictable& e) noexcept {
        record_access(e);
        e.set_routes_to_protected(true);
        e.set_directly_inserted(true);
        add_to_segment(e, lru_segment::protected_);
        ++_protected_direct_size;
    }

    void add_before(evictable& more_recent, evictable& e) noexcept {
        record_access(e);
        auto seg = more_recent.get_segment();
        if (more_recent.routes_to_protected()) {
            e.set_routes_to_protected(true);
        }
        if (seg == lru_segment::protected_ && more_recent.is_directly_inserted()) {
            e.set_directly_inserted(true);
            ++_protected_direct_size;
        }
        auto& list = segment_list(seg);
        list.insert(list.iterator_to(more_recent), e);
        e.set_segment(seg);
        increment_size(seg);
    }

    // Protected entries skip the sketch increment — they are past the
    // admission gate and don't need frequency tracking.
    void touch(evictable& e) noexcept {
        switch (e.get_segment()) {
            case lru_segment::none:
                // Re-linking an unlinked entry is an insertion, not a promotion:
                // route sticky (multi-row partition) entries back to protected.
                add(e);
                break;
            case lru_segment::window:
                record_access(e);
                // Promote re-accessed window entries directly to protected.
                // With unbounded window, entries stay here until drain_window()
                // runs during do_evict(). Promoting on touch() ensures hot
                // entries reach protected, while cold/scan entries remain in
                // the window and get evicted by the admission gate.
                ++_stats.protected_promotions;
                _window.erase(_window.iterator_to(e));
                --_window_size;
                promote_to_protected(e);
                break;
            case lru_segment::probation:
                record_access(e);
                ++_stats.protected_promotions;
                _probation.erase(_probation.iterator_to(e));
                --_probation_size;
                promote_to_protected(e);
                break;
            case lru_segment::protected_:
                _protected.erase(_protected.iterator_to(e));
                _protected.push_back(e);
                break;
        }
    }

    reclaiming_result evict() noexcept {
        ++_stats.eviction_calls;
        auto result = do_evict<false>();
        if (result == reclaiming_result::reclaimed_nothing) {
            ++_stats.eviction_calls_empty;
        }
        return result;
    }

    reclaiming_result evict_shallow() noexcept {
        ++_stats.eviction_calls;
        auto result = do_evict<true>();
        if (result == reclaiming_result::reclaimed_nothing) {
            ++_stats.eviction_calls_empty;
        }
        return result;
    }

    void evict_all() {
        while (evict() == reclaiming_result::reclaimed_something) {}
    }

    void resize_sketch(size_t new_width_log2) {
        new_width_log2 = std::clamp(new_width_log2, min_sketch_width_log2, max_sketch_width_log2);
        _sketch.resize(new_width_log2);
        _sample_count = 0;
    }

    uint8_t sketch_estimate(uint64_t key) const noexcept {
        return _sketch.estimate(key);
    }

    size_t current_max_window_size() const noexcept { return max_window_size(); }
    size_t current_max_protected_size() const noexcept { return max_protected_size(); }

    void reset_sketch() noexcept {
        _sketch.clear();
        _sample_count = 0;
    }

    void set_window_fraction(double fraction) noexcept {
        _window_fraction = std::clamp(fraction, 0.01, 0.99);
    }

    double window_fraction() const noexcept { return _window_fraction; }

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
