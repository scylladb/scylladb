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

#include <seastar/core/timer.hh>

#include "utils/exceptions.hh"

namespace utils {
// Simple variant of the "LoadingCache" used for permissions in origin.

typedef lowres_clock loading_cache_clock_type;

template<typename _Tp>
class timestamped_val {
private:
    std::experimental::optional<_Tp> _opt_value;
    loading_cache_clock_type::time_point _loaded;
    loading_cache_clock_type::time_point _last_read;

public:
    timestamped_val()
        : _loaded(loading_cache_clock_type::now())
        , _last_read(_loaded) {}

    timestamped_val(const timestamped_val&) = default;
    timestamped_val(timestamped_val&&) = default;

    // Make sure copy/move-assignments don't go through the template below
    timestamped_val& operator=(const timestamped_val&) = default;
    timestamped_val& operator=(timestamped_val&) = default;
    timestamped_val& operator=(timestamped_val&&) = default;

    template <typename U>
    timestamped_val& operator=(U&& new_val) {
        _opt_value = std::forward<U>(new_val);
        _loaded = loading_cache_clock_type::now();
        return *this;
    }

    const _Tp& value() {
        _last_read = loading_cache_clock_type::now();
        return _opt_value.value();
    }

    explicit operator bool() const noexcept {
        return bool(_opt_value);
    }

    loading_cache_clock_type::time_point last_read() const noexcept {
        return _last_read;
    }

    loading_cache_clock_type::time_point loaded() const noexcept {
        return _loaded;
    }
};

class shared_mutex {
private:
    lw_shared_ptr<semaphore> _mutex_ptr;

public:
    shared_mutex() : _mutex_ptr(make_lw_shared<semaphore>(1)) {}
    semaphore& get() const noexcept {
        return *_mutex_ptr;
    }
};

template<typename _Key,
         typename _Tp,
         typename _Hash = std::hash<_Key>,
         typename _Pred = std::equal_to<_Key>,
         typename _Alloc = std::allocator<std::pair<const _Key, timestamped_val<_Tp>>>,
         typename SharedMutexMapAlloc = std::allocator<std::pair<const _Key, shared_mutex>>>
class loading_cache {
private:
    typedef timestamped_val<_Tp> ts_value_type;
    typedef std::unordered_map<_Key, ts_value_type, _Hash, _Pred, _Alloc> map_type;
    typedef std::unordered_map<_Key, shared_mutex, _Hash, _Pred, SharedMutexMapAlloc> write_mutex_map_type;
    typedef loading_cache<_Key, _Tp, _Hash, _Pred, _Alloc> _MyType;

public:
    typedef _Tp value_type;
    typedef typename map_type::key_type key_type;
    typedef typename map_type::allocator_type allocator_type;
    typedef typename map_type::hasher hasher;
    typedef typename map_type::key_equal key_equal;
    typedef typename map_type::iterator iterator;

    template<typename Func>
    loading_cache(size_t max_size, std::chrono::milliseconds expiry, std::chrono::milliseconds refresh, logging::logger& logger, Func&& load, const hasher& hf = hasher(), const key_equal& eql = key_equal(), const allocator_type& a = allocator_type())
                : _map(10, hf, eql, a)
                , _max_size(max_size)
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

        _timer.set_callback([this] { on_timer(); });
        _timer.arm(_refresh);
    }

    future<_Tp> get(const _Key& k) {
        // If caching is disabled - always load in the foreground
        if (!caching_enabled()) {
            return _load(k);
        }

        // If the key is not in the cache yet, then _map[k] is going to create a
        // new uninitialized value in the map. If the value is already in the
        // cache (the fast path) simply return the value. Otherwise, take the
        // mutex and try to load the value (the slow path).
        ts_value_type& ts_val = _map[k];
        if (ts_val) {
            return make_ready_future<_Tp>(ts_val.value());
        } else {
            return slow_load(k);
        }
    }

private:
    bool caching_enabled() const {
        return _expiry != std::chrono::milliseconds(0);
    }

    future<_Tp> slow_load(const _Key& k) {
        // If the key is not in the cache yet, then _write_mutex_map[k] is going
        // to create a new value with the initialized mutex. In this case a
        // mutex is going to serialize the producers and only the first one is
        // going to actually issue a load operation and initialize
        // the value with the received result. The rest are going to see (and
        // read) the initialized value when they enter the critical section.
        shared_mutex sm = _write_mutex_map[k];
        return with_semaphore(sm.get(), 1, [this, k] {
            ts_value_type& ts_val = _map[k];
            if (ts_val) {
                return make_ready_future<_Tp>(ts_val.value());
            }
            _logger.trace("{}: storing the value for the first time", k);
            return _load(k).then([this, k] (_Tp t) {
                // we have to "re-read" the _map here because the value may have been evicted by now
                ts_value_type& ts_val = _map[k];
                ts_val = std::move(t);
                return make_ready_future<_Tp>(ts_val.value());
            });
        }).finally([sm] {});
    }

    future<> reload(const _Key& k, ts_value_type& ts_val) {
        return _load(k).then_wrapped([this, &ts_val, &k] (auto&& f) {
            // The exceptions are related to the load operation itself.
            // We should ignore them for the background reads - if
            // they persist the value will age and will be reloaded in
            // the forground. If the foreground READ fails the error
            // will be propagated up to the user and will fail the
            // corresponding query.
            try {
                ts_val = f.get0();
            } catch (std::exception& e) {
                _logger.debug("{}: reload failed: {}", k, e.what());
            } catch (...) {
                _logger.debug("{}: reload failed: unknown error", k);
            }
        });
    }

    // We really miss the std::erase_if()... :(
    void drop_expired() {
        auto now = loading_cache_clock_type::now();
        auto i = _map.begin();
        auto e = _map.end();

        while (i != e) {
            // An entry should be discarded if it hasn't been reloaded for too long or nobody cares about it anymore
            auto since_last_read = now - i->second.last_read();
            auto since_loaded = now - i->second.loaded();
            if (_expiry < since_last_read || _expiry < since_loaded) {
                using namespace std::chrono;
                _logger.trace("drop_expired(): {}: dropping the entry: _expiry {},  ms passed since: loaded {} last_read {}", i->first, _expiry.count(), duration_cast<milliseconds>(since_loaded).count(), duration_cast<milliseconds>(since_last_read).count());
                i = _map.erase(i);
                continue;
            }
            ++i;
        }
    }

    // Shrink the cache to the _max_size discarding the least recently used items
    void shrink() {
        if (_max_size != 0 && _map.size() > _max_size) {
            std::vector<iterator> tmp;
            tmp.reserve(_map.size());

            iterator i = _map.begin();
            while (i != _map.end()) {
                tmp.emplace_back(i++);
            }

            std::sort(tmp.begin(), tmp.end(), [] (iterator i1, iterator i2) {
               return i1->second.last_read() < i2->second.last_read();
            });

            tmp.resize(_map.size() - _max_size);
            std::for_each(tmp.begin(), tmp.end(), [this] (auto& k) {
                using namespace std::chrono;
                _logger.trace("shrink(): {}: dropping the entry: ms since last_read {}", k->first, duration_cast<milliseconds>(loading_cache_clock_type::now() - k->second.last_read()).count());
                _map.erase(k);
            });
        }
    }

    void on_timer() {
        _logger.trace("on_timer(): start");

        auto timer_start_tp = loading_cache_clock_type::now();

        // Clear all cached mutexes
        _write_mutex_map.clear();

        // Clean up items that were not touched for the whole _expiry period.
        drop_expired();

        // Remove the least recently used items if map is too big.
        shrink();

        // Reload all those which vlaue needs to be reloaded.
        parallel_for_each(_map.begin(), _map.end(), [this, curr_time = timer_start_tp] (auto& i) {
            _logger.trace("on_timer(): {}: checking the value age", i.first);
            if (i.second && i.second.loaded() + _refresh < curr_time) {
                _logger.trace("on_timer(): {}: reloading the value", i.first);
                return this->reload(i.first, i.second);
            }
            return now();
        }).finally([this, timer_start_tp] {
            _logger.trace("on_timer(): rearming");
            _timer.arm(timer_start_tp + _refresh);
        });
    }

    map_type _map;
    write_mutex_map_type _write_mutex_map;
    size_t _max_size;
    std::chrono::milliseconds _expiry;
    std::chrono::milliseconds _refresh;
    logging::logger& _logger;
    std::function<future<_Tp>(const _Key&)> _load;
    timer<lowres_clock> _timer;
};

}

