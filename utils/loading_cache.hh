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

namespace utils {
// Simple variant of the "LoadingCache" used for permissions in origin.

typedef lowres_clock loading_cache_clock_type;

template<typename _Tp>
struct timestamped_val {
    _Tp value;
    loading_cache_clock_type::time_point loaded;
    loading_cache_clock_type::time_point last_read;

    timestamped_val(_Tp v, loading_cache_clock_type::time_point tp)
        : value(std::move(v))
        , loaded(tp)
        , last_read(tp) {}
};

template<typename _Key, typename _Tp, typename _Hash = std::hash<_Key>,
                typename _Pred = std::equal_to<_Key>,
                typename _Alloc = std::allocator<std::pair<const _Key, timestamped_val<_Tp>> > >
class loading_cache {
private:
    typedef timestamped_val<_Tp> ts_value_type;
    typedef std::unordered_map<_Key, ts_value_type, _Hash, _Pred, _Alloc> map_type;
    typedef loading_cache<_Key, _Tp, _Hash, _Pred, _Alloc> _MyType;

public:
    typedef _Tp value_type;
    typedef typename map_type::key_type key_type;
    typedef typename map_type::allocator_type allocator_type;
    typedef typename map_type::hasher hasher;
    typedef typename map_type::key_equal key_equal;
    typedef typename map_type::iterator iterator;

    template<typename Func>
    loading_cache(size_t max_size, std::chrono::milliseconds expiry,
                    std::chrono::milliseconds refresh, logging::logger& logger, Func && load,
                    const hasher& hf = hasher(), const key_equal& eql =
                                    key_equal(), const allocator_type& a =
                                    allocator_type())
                    : _map(10, hf, eql, a), _max_size(max_size), _expiry(
                                    expiry), _refresh(refresh), _logger(logger), _load(
                                    std::forward<Func>(load)) {

        _period = _expiry;

        if (expiry == std::chrono::milliseconds() && _max_size != 0) {
            _period = std::chrono::milliseconds(5000);
        }
        if (_period != std::chrono::milliseconds()) {
            _timer.set_callback([this] { on_timer(); });
            _timer.arm(_period);
        }
    }

    future<_Tp> get(const _Key & k) {
        auto i = _map.find(k);
        if (i == _map.end()) {
            _logger.trace("{}: key not found - loading in the forground", k);
            return load(k);
        }
        i->second.last_read = loading_cache_clock_type::now();
        return make_ready_future<_Tp>(i->second.value);
    }

private:
    future<_Tp> load(const _Key& k) {
        return _load(k).then([this, k] (_Tp t) {
            auto i = _map.emplace(k, ts_value_type(std::move(t), loading_cache_clock_type::now()));
            // It may happen that here we tried to emplace the already existing
            // key because a few queries are trying to insert it.
            // We may safely ignore this.
            return make_ready_future<_Tp>(i.first->second.value);
        });
    }

    future<> reload(const _Key& k) {
        return _load(k).then_wrapped([this, k] (auto&& f) {
            auto i = _map.find(k);
            if (i == _map.end()) {
                throw std::logic_error("Calling reload() on a non-existing key");
            }
            // The exceptions in _load() may be related to the READ mutation
            // itself. We should ignore them for the background reads - if they
            // persist the value will age and will be reloaded in the forground.
            // If the foreground READ fails the error will be propagated up to
            // the user and will fail the corresponding query.
            try {
                i->second.value = f.get0();
                i->second.loaded = loading_cache_clock_type::now();
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
            auto since_last_read = now - i->second.last_read;
            auto since_loaded = now - i->second.loaded;
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
               return i1->second.last_read < i2->second.last_read;
            });

            tmp.resize(_map.size() - _max_size);
            std::for_each(tmp.begin(), tmp.end(), [this] (auto& k) {
                using namespace std::chrono;
                _logger.trace("shrink(): {}: dropping the entry: ms since last_read {}", k->first, duration_cast<milliseconds>(loading_cache_clock_type::now() - k->second.last_read).count());
                _map.erase(k);
            });
        }
    }

    void on_timer() {
        _logger.trace("on_timer(): start");

        auto timer_start_tp = loading_cache_clock_type::now();

        // Clean up items that were not touched for the whole _expiry period.
        drop_expired();

        // Remove the least recently used items if map is too big.
        shrink();

        // Reload all those which vlaue needs to be reloaded.
        //
        // The code below uses the fact that _map iterators are not passed to
        // the asynch part of the function. Otherwise there could be a race
        // with the get() which may insert a new element into the map, which may
        // invalidate the previously created iterators.
        parallel_for_each(_map.begin(), _map.end(), [this] (auto& i) {
            _logger.trace("on_timer(): {}: checking the key", i.first);
            if (i.second.loaded + _refresh < loading_cache_clock_type::now()) {
                _logger.trace("on_timer(): {}: reloading the key", i.first);
                return this->reload(i.first);
            }
            return now();
        }).finally([this, timer_start_tp] {
            _logger.trace("on_timer(): rearming");
            _timer.arm(timer_start_tp + _period);
        });
    }

    map_type _map;
    size_t _max_size;
    std::chrono::milliseconds _expiry;
    std::chrono::milliseconds _refresh;
    std::chrono::milliseconds _period;
    logging::logger& _logger;
    std::function<future<_Tp>(_Key)> _load;
    timer<lowres_clock> _timer;
};

}

