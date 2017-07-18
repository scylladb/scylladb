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

#include <chrono>
#include <unordered_map>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set.hpp>

#include <seastar/core/timer.hh>
#include <seastar/core/gate.hh>

#include "exceptions/exceptions.hh"
#include "utils/loading_shared_values.hh"
#include "log.hh"

namespace bi = boost::intrusive;

namespace utils {

using loading_cache_clock_type = seastar::lowres_clock;
using auto_unlink_list_hook = bi::list_base_hook<bi::link_mode<bi::auto_unlink>>;

template<typename Tp, typename Key, typename Hash, typename EqualPred, typename LoadingSharedValuesStats>
class timestamped_val {
public:
    using value_type = Tp;
    using loading_values_type = typename utils::loading_shared_values<Key, timestamped_val, Hash, EqualPred, LoadingSharedValuesStats, 256>;
    class lru_entry;

private:
    value_type _value;
    loading_cache_clock_type::time_point _loaded;
    loading_cache_clock_type::time_point _last_read;
    lru_entry* _lru_entry_ptr = nullptr; /// MRU item is at the front, LRU - at the back

public:
    timestamped_val(value_type val)
        : _value(std::move(val))
        , _loaded(loading_cache_clock_type::now())
        , _last_read(_loaded)
    {}
    timestamped_val(timestamped_val&&) = default;

    timestamped_val& operator=(value_type new_val) {
        assert(_lru_entry_ptr);

        _value = std::move(new_val);
        _loaded = loading_cache_clock_type::now();
        return *this;
    }

    value_type& value() noexcept {
        touch();
        return _value;
    }

    loading_cache_clock_type::time_point last_read() const noexcept {
        return _last_read;
    }

    loading_cache_clock_type::time_point loaded() const noexcept {
        return _loaded;
    }

    bool ready() const noexcept {
        return _lru_entry_ptr;
    }

private:
    void touch() noexcept {
        assert(_lru_entry_ptr);
        _last_read = loading_cache_clock_type::now();
        _lru_entry_ptr->touch();
    }

    void set_anchor_back_reference(lru_entry* lru_entry_ptr) noexcept {
        _lru_entry_ptr = lru_entry_ptr;
    }
};

/// \brief This is and LRU list entry which is also an anchor for a loading_cache value.
template<typename Tp, typename Key, typename Hash, typename EqualPred, typename LoadingSharedValuesStats>
class timestamped_val<Tp, Key, Hash, EqualPred, LoadingSharedValuesStats>::lru_entry : public auto_unlink_list_hook {
private:
    using ts_value_type = timestamped_val<Tp, Key, Hash, EqualPred, LoadingSharedValuesStats>;
    using loading_values_type = typename ts_value_type::loading_values_type;

public:
    using lru_list_type = bi::list<lru_entry, bi::constant_time_size<false>>;
    using timestamped_val_ptr = typename loading_values_type::entry_ptr;

private:
    timestamped_val_ptr _ts_val_ptr;
    lru_list_type& _lru_list;

public:
    lru_entry(timestamped_val_ptr ts_val, lru_list_type& lru_list)
        : _ts_val_ptr(std::move(ts_val))
        , _lru_list(lru_list)
    {
        _ts_val_ptr->set_anchor_back_reference(this);
    }

    ~lru_entry() {
        _ts_val_ptr->set_anchor_back_reference(nullptr);
    }

    /// Set this item as the most recently used item.
    /// The MRU item is going to be at the front of the _lru_list, the LRU item - at the back.
    void touch() noexcept {
        auto_unlink_list_hook::unlink();
        _lru_list.push_front(*this);
    }

    const Key& key() const noexcept {
        return loading_values_type::to_key(_ts_val_ptr);
    }

    timestamped_val& timestamped_value() noexcept { return *_ts_val_ptr; }
    const timestamped_val& timestamped_value() const noexcept { return *_ts_val_ptr; }
    timestamped_val_ptr timestamped_value_ptr() noexcept { return _ts_val_ptr; }
};

enum class loading_cache_reload_enabled { no, yes };

template<typename Key,
         typename Tp,
         typename Hash = std::hash<Key>,
         typename EqualPred = std::equal_to<Key>,
         typename LoadingSharedValuesStats = utils::do_nothing_loading_shared_values_stats,
         typename Alloc = std::allocator<typename timestamped_val<Tp, Key, Hash, EqualPred, LoadingSharedValuesStats>::lru_entry>>
class loading_cache {
private:
    using ts_value_type = timestamped_val<Tp, Key, Hash, EqualPred, LoadingSharedValuesStats>;
    using loading_values_type = typename ts_value_type::loading_values_type;
    using timestamped_val_ptr = typename loading_values_type::entry_ptr;
    using ts_value_lru_entry = typename ts_value_type::lru_entry;
    using set_iterator = typename loading_values_type::iterator;
    using lru_list_type = typename ts_value_lru_entry::lru_list_type;

public:
    using value_type = Tp;
    using key_type = Key;


    template<typename Func>
    loading_cache(size_t max_size, std::chrono::milliseconds expiry, std::chrono::milliseconds refresh, logging::logger& logger, Func&& load)
                : _max_size(max_size)
                , _expiry(expiry)
                , _refresh(refresh)
                , _logger(logger)
                , _load(std::forward<Func>(load)) {

        // If expiration period is zero - caching is disabled
        if (!caching_enabled()) {
            return;
        }

        // Sanity check: if expiration period is given then non-zero refresh period and maximal size are required
        if (_refresh == std::chrono::milliseconds(0) || _max_size == 0) {
            throw exceptions::configuration_exception("loading_cache: caching is enabled but refresh period and/or max_size are zero");
        }

        _timer_period = std::min(_expiry, _refresh);
        _timer.set_callback([this] { on_timer(); });
        _timer.arm(_timer_period);
    }

    ~loading_cache() {
        _lru_list.erase_and_dispose(_lru_list.begin(), _lru_list.end(), [] (ts_value_lru_entry* ptr) { loading_cache::destroy_ts_value(ptr); });
    }

    future<Tp> get(const Key& k) {
        // If caching is disabled - always load in the foreground
        if (!caching_enabled()) {
            return _load(k);
        }

        return _loading_values.get_or_load(k, [this] (const Key& k) {
            return _load(k).then([this] (value_type val) {
                return ts_value_type(std::move(val));
            });
        }).then([this, k] (timestamped_val_ptr ts_val_ptr) {
            // check again since it could have already been inserted and initialized
            if (!ts_val_ptr->ready()) {
                _logger.trace("{}: storing the value for the first time", k);

                ts_value_lru_entry* new_lru_entry = Alloc().allocate(1);
                new(new_lru_entry) ts_value_lru_entry(std::move(ts_val_ptr), _lru_list);

                // This will "touch" the entry and add it to the LRU list.
                return make_ready_future<Tp>(new_lru_entry->timestamped_value_ptr()->value());
            }

            return make_ready_future<Tp>(ts_val_ptr->value());
        });
    }

    future<> stop() {
        return _timer_reads_gate.close().finally([this] { _timer.cancel(); });
    }

private:
    set_iterator set_find(const Key& k) noexcept {
        set_iterator it = _loading_values.find(k);
        set_iterator end_it = set_end();

        if (it == end_it || !it->ready()) {
            return end_it;
        }
        return it;
    }

    set_iterator set_end() noexcept {
        return _loading_values.end();
    }

    set_iterator set_begin() noexcept {
        return _loading_values.begin();
    }

    bool caching_enabled() const {
        return _expiry != std::chrono::milliseconds(0);
    }

    static void destroy_ts_value(ts_value_lru_entry* val) {
        val->~ts_value_lru_entry();
        Alloc().deallocate(val, 1);
    }

    future<> reload(ts_value_lru_entry& lru_entry) {
        return _load(lru_entry.key()).then_wrapped([this, key = lru_entry.key()] (auto&& f) mutable {
            // if the entry has been evicted by now - simply end here
            set_iterator it = set_find(key);
            if (it == set_end()) {
                _logger.trace("{}: entry was dropped during the reload", key);
                return make_ready_future<>();
            }

            // The exceptions are related to the load operation itself.
            // We should ignore them for the background reads - if
            // they persist the value will age and will be reloaded in
            // the forground. If the foreground READ fails the error
            // will be propagated up to the user and will fail the
            // corresponding query.
            try {
                *it = f.get0();
            } catch (std::exception& e) {
                _logger.debug("{}: reload failed: {}", key, e.what());
            } catch (...) {
                _logger.debug("{}: reload failed: unknown error", key);
            }

            return make_ready_future<>();
        });
    }

    void drop_expired() {
        auto now = loading_cache_clock_type::now();
        _lru_list.remove_and_dispose_if([now, this] (const ts_value_lru_entry& lru_entry) {
            using namespace std::chrono;
            // An entry should be discarded if it hasn't been reloaded for too long or nobody cares about it anymore
            const ts_value_type& v = lru_entry.timestamped_value();
            auto since_last_read = now - v.last_read();
            auto since_loaded = now - v.loaded();
            if (_expiry < since_last_read || _expiry < since_loaded) {
                _logger.trace("drop_expired(): {}: dropping the entry: _expiry {},  ms passed since: loaded {} last_read {}", lru_entry.key(), _expiry.count(), duration_cast<milliseconds>(since_loaded).count(), duration_cast<milliseconds>(since_last_read).count());
                return true;
            }
            return false;
        }, [this] (ts_value_lru_entry* p) {
            loading_cache::destroy_ts_value(p);
        });
    }

    // Shrink the cache to the _max_size discarding the least recently used items
    void shrink() {
        if (_loading_values.size() > _max_size) {
            auto num_items_to_erase = _loading_values.size() - _max_size;
            for (size_t i = 0; i < num_items_to_erase; ++i) {
                using namespace std::chrono;
                auto it = _lru_list.rbegin();
                // This may happen if there are pending insertions into the loading_shared_values that hasn't been yet finalized.
                // In this case the number of elements in the _lru_list will be less than the _loading_values.size().
                if (_lru_list.rbegin() == _lru_list.rend()) {
                    return;
                }
                ts_value_lru_entry& lru_entry = *it;
                _logger.trace("shrink(): {}: dropping the entry: ms since last_read {}", lru_entry.key(), duration_cast<milliseconds>(loading_cache_clock_type::now() - lru_entry.timestamped_value().last_read()).count());
                loading_cache::destroy_ts_value(&lru_entry);
            }
        }
    }

    // Try to bring the load factors of the _loading_values into a known range.
    void periodic_rehash() noexcept {
        try {
            _loading_values.rehash();
        } catch (...) {
            // if rehashing fails - continue with the current buckets array
        }
    }

    void on_timer() {
        _logger.trace("on_timer(): start");

        // Clean up items that were not touched for the whole _expiry period.
        drop_expired();

        // Remove the least recently used items if map is too big.
        shrink();

        // check if rehashing is needed and do it if it is.
        periodic_rehash();

        // Reload all those which vlaue needs to be reloaded.
        with_gate(_timer_reads_gate, [this] {
            return parallel_for_each(_lru_list.begin(), _lru_list.end(), [this] (ts_value_lru_entry& lru_entry) {
                _logger.trace("on_timer(): {}: checking the value age", lru_entry.key());
                if (lru_entry.timestamped_value().loaded() + _refresh < loading_cache_clock_type::now()) {
                    _logger.trace("on_timer(): {}: reloading the value", lru_entry.key());
                    return this->reload(lru_entry);
                }
                return now();
            }).finally([this] {
                _logger.trace("on_timer(): rearming");
                _timer.arm(loading_cache_clock_type::now() + _timer_period);
            });
        });
    }

    loading_values_type _loading_values;
    lru_list_type _lru_list;
    size_t _max_size = 0;
    std::chrono::milliseconds _expiry;
    std::chrono::milliseconds _refresh;
    loading_cache_clock_type::duration _timer_period;
    logging::logger& _logger;
    std::function<future<Tp>(const Key&)> _load;
    timer<loading_cache_clock_type> _timer;
    seastar::gate _timer_reads_gate;
};

}

