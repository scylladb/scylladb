/*
 * Copyright (C) 2011 Clearspring Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2018-present ScyllaDB
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

/*
 Based on the following implementation ([2]) for the Space-Saving algorithm from [1].

 [1] Metwally, A., Agrawal, D., & El Abbadi, A. (2005, January).
     Efficient computation of frequent and top-k elements in data streams.
     In International Conference on Database Theory (pp. 398-412). Springer, Berlin, Heidelberg.
     http://www.cse.ust.hk/~raywong/comp5331/References/EfficientComputationOfFrequentAndTop-kElementsInDataStreams.pdf

 [2] https://github.com/addthis/stream-lib/blob/master/src/main/java/com/clearspring/analytics/stream/StreamSummary.java

 The algorithm keeps a map between keys seen and their counts, keeping a bound on the number of tracked keys.
 Replacement policy evicts the key with the lowest count while inheriting its count, and recording an estimation
 of the error which results from that.
 This error estimation can be later used to prove if the distribution we arrived at corresponds to the real top-K,
 which we can display alongside the results.
 Accuracy depends on the number of tracked keys.

*/

#include <cstdio>
#include <list>
#include <unordered_map>
#include <memory>
#include <tuple>
#include <assert.h>

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include "utils/chunked_vector.hh"

namespace utils {

using namespace seastar;

template <class T, class Hash = std::hash<T>, class KeyEqual = std::equal_to<T>>
class space_saving_top_k {
private:
    struct bucket;
    using buckets_iterator = typename std::list<bucket>::iterator;

    struct counter {
        buckets_iterator bucket_it;
        T item;
        unsigned count = 0;
        unsigned error = 0;

        counter(T item, unsigned count = 0, unsigned error = 0) : item(item), count(count), error(error) {}
    };

    using counter_ptr = lw_shared_ptr<counter>;

    using counters = std::list<counter_ptr>;
    using counters_iterator = typename counters::iterator;

    using counters_map = std::unordered_map<T, counters_iterator, Hash, KeyEqual>;
    using counters_map_iterator = typename counters_map::iterator;

    struct bucket {
        std::list<counter_ptr> counters;
        unsigned count;

        bucket(counter_ptr ctr) {
            count = ctr->count;
            counters.push_back(ctr);
        }

        bucket(T item, unsigned count, unsigned error) {
            counters.push_back(make_lw_shared<counter>(item, count, error));
            this->count = count;
        }
    };

    using buckets = std::list<bucket>;

    size_t _capacity;
    counters_map _counters_map;
    buckets _buckets; // buckets list in ascending order
    bool _valid = true;

public:
    /// capacity: maximum number of elements to be tracked
    space_saving_top_k(size_t capacity = 256) : _capacity(capacity) {}

    size_t capacity() const { return _capacity; }

    size_t size() const {
        if (!_valid) {
            throw std::runtime_error("space_saving_top_k state is invalid");
        }
        return _counters_map.size();
    }

    bool valid() const { return _valid; }

    // returns true if item is a new one
    bool append(T item, unsigned inc = 1, unsigned err = 0) {
        return std::get<0>(append_return_all(std::move(item), inc, err));
    }

    // returns optionally dropped item (due to capacity overflow)
    std::optional<T> append_return_dropped(T item, unsigned inc = 1, unsigned err = 0) {
        return std::get<1>(append_return_all(std::move(item), inc, err));
    }

    // returns whether an element is new and an optionally dropped item (due to capacity overflow)
    std::tuple<bool, std::optional<T>> append_return_all(T item, unsigned inc = 1, unsigned err = 0) {
        if (!_valid) {
            return {false, std::optional<T>()};
        }
        try {
            counters_map_iterator cmap_it = _counters_map.find(item);
            bool is_new_item = cmap_it == _counters_map.end();
            std::optional<T> dropped_item;
            counters_iterator counter_it;
            if (is_new_item) {
                if (size() < _capacity) {
                    _buckets.emplace_front(bucket(std::move(item), 0, err)); // inc added later via increment_counter
                    buckets_iterator new_bucket_it = _buckets.begin();
                    counter_it = new_bucket_it->counters.begin();
                    (*counter_it)->bucket_it = new_bucket_it;
                } else {
                    buckets_iterator min_bucket = _buckets.begin();
                    assert(min_bucket != _buckets.end());
                    counter_it = min_bucket->counters.begin();
                    assert(counter_it != min_bucket->counters.end());
                    counter_ptr ctr = *counter_it;
                    _counters_map.erase(ctr->item);
                    dropped_item = std::exchange(ctr->item, std::move(item));
                    ctr->error = min_bucket->count + err;
                }
                _counters_map[item] = std::move(counter_it);
            } else {
                counter_it = cmap_it->second;
            }

            increment_counter(counter_it, inc);

            return {is_new_item, std::move(dropped_item)};
        } catch (...) {
            _valid = false;
            std::rethrow_exception(std::current_exception());
        }
    }

private:
    void increment_counter(counters_iterator counter_it, unsigned inc) {
        counter_ptr ctr = *counter_it;

        buckets_iterator old_bucket_it = ctr->bucket_it;
        auto& old_buck = *old_bucket_it;
        old_buck.counters.erase(counter_it);

        ctr->count += inc;

        buckets_iterator bi_prev = old_bucket_it;
        buckets_iterator bi_next = std::next(old_bucket_it);
        while (bi_next != _buckets.end()) {
            bucket& buck = *bi_next;
            if (ctr->count == buck.count) {
                buck.counters.push_back(ctr);
                counter_it = std::prev(buck.counters.end());
                break;
            } else if (ctr->count > buck.count) {
                bi_prev = bi_next;
                bi_next = std::next(bi_prev);
            } else {
                bi_next = _buckets.end(); // create new bucket
            }
        }

        if (bi_next == _buckets.end()) {
            bucket buck{ctr};
            counter_it = buck.counters.begin();
            bi_next = _buckets.insert(std::next(bi_prev), std::move(buck));
        }
        ctr->bucket_it = bi_next;
        _counters_map[ctr->item] = std::move(counter_it);

        if (old_buck.counters.empty()) {
            _buckets.erase(old_bucket_it);
        }
    }

    //-----------------------------------------------------------------------------------------
    // Results
public:
    struct result {
        T item;
        unsigned count;
        unsigned error;
    };

    using results = chunked_vector<result>;

    results top(unsigned k) const
    {
        if (!_valid) {
            throw std::runtime_error("space_saving_top_k state is invalid");
        }

        results list;
        // _buckets are in ascending order
        for (auto b_it = _buckets.rbegin(); b_it != _buckets.rend(); ++b_it) {
            auto& b = *b_it;
            for (auto& c: b.counters) {
                if (list.size() == k) {
                    return list;
                }
                list.emplace_back(result{c->item, c->count, c->error});
            }
        }
        return list;
    }

    void append(const results& res) {
        for (auto& r: res) {
            append(r.item, r.count, r.error);
        }
    }

    //-----------------------------------------------------------------------------------------
    // Diagnostics
public:
    template <class TT>
    friend std::ostream& operator<<(std::ostream& out, const typename space_saving_top_k<TT>::counter& c);
    template <class TT>
    friend std::ostream& operator<<(std::ostream& out, const typename space_saving_top_k<TT>::counters_map& counters_map);
    template <class TT>
    friend std::ostream& operator<<(std::ostream& out, const typename space_saving_top_k<TT>::buckets& buckets);
    template <class TT>
    friend std::ostream& operator<<(std::ostream& out, const space_saving_top_k<TT>& top_k);
};

//---------------------------------------------------------------------------------------------

template <class T>
std::ostream& operator<<(std::ostream& out, const typename space_saving_top_k<T>::counter& c) {
    out << c.item << " " << c.count << "/" << c.error << " " << &*c.bucket_it;
    return out;
}

template <class T>
std::ostream& operator<<(std::ostream& out, const typename space_saving_top_k<T>::counters_map& counters_map) {
    out << "{\n";
    for (auto const& [item, counter_i]: counters_map) {
        out << item << " => " << **counter_i << "\n";
    }
    out << "}\n";
    return out;
}

template <class T>
std::ostream& operator<<(std::ostream& out, const typename space_saving_top_k<T>::buckets& buckets) {
    for (auto& b: buckets) {
        out << &b << " " << b.count << " [";
        for (auto& c: b.counters) {
            out << *c << " ";
        }
        out << "]\n";
    }
    return out;
}

template <class T>
std::ostream& operator<<(std::ostream& out, const space_saving_top_k<T>& top_k) {
    out << top_k._buckets;
    out << top_k._counters_map;
    out << "---\n";
    return out;
}

} // namespace utils
