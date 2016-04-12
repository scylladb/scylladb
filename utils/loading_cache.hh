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

typedef steady_clock_type loading_cache_clock_type;

template<typename _Tp>
struct timestamped_val {
    _Tp value;
    typename loading_cache_clock_type::time_point loaded;
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

    template<typename Func>
    loading_cache(size_t max_size, std::chrono::milliseconds expiry,
                    std::chrono::milliseconds refresh, Func && load,
                    const hasher& hf = hasher(), const key_equal& eql =
                                    key_equal(), const allocator_type& a =
                                    allocator_type())
                    : _map(10, hf, eql, a), _max_size(max_size), _expiry(
                                    expiry), _refresh(refresh), _load(
                                    std::forward<Func>(load)) {

        auto period = _expiry;

        if (expiry == std::chrono::milliseconds() && _max_size != 0) {
            period = std::chrono::milliseconds(5000);
        }
        if (period != std::chrono::milliseconds()) {
            _timer.set_callback(std::bind(&_MyType::on_timer, this));
            _timer.arm_periodic(period);
        }
    }

    future<_Tp> get(const _Key & k) {
        auto i = _map.find(k);

        if (i == _map.end()
                        || (i->second.loaded + _refresh)
                                        < loading_cache_clock_type::now()) {
            return _load(k).then([this, k](_Tp t) {
                _map[k] = ts_value_type{t, loading_cache_clock_type::now()};
                return make_ready_future<_Tp>(std::move(t));
            });
        }
        return make_ready_future<_Tp>(i->second.value);
    }

private:
    void on_timer() {
        auto i = _map.begin();
        auto e = _map.end();

        auto now = loading_cache_clock_type::now();

        while (i != e) {
            if ((i->second.loaded + _expiry) < now) {
                i = _map.erase(i);
                continue;
            }
            ++i;
        }

        if (_max_size != 0 && _map.size() > _max_size) {
            typedef typename map_type::iterator iterator;
            std::vector<iterator> tmp;
            for (i = _map.begin(); i != e; ++i) {
                tmp.emplace_back(i);
            }
            std::sort(tmp.begin(), tmp.end(), [](iterator i1, iterator i2) {
               return i1->second.loaded < i2->second.loaded;
            });

            for (auto& e : tmp) {
                _map.erase(e);
                if (_map.size() < _max_size) {
                    break;
                }
            }
        }
    }

    std::unordered_map<_Key, ts_value_type, _Hash, _Pred, _Alloc> _map;
    size_t _max_size;
    std::chrono::milliseconds _expiry;
    std::chrono::milliseconds _refresh;
    std::function<future<_Tp>(_Key)> _load;
    timer<> _timer;
};

}

