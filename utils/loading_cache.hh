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
#include <boost/intrusive/parent_from_member.hpp>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/transformed.hpp>

#include <seastar/core/reactor.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/gate.hh>

#include "exceptions/exceptions.hh"
#include "utils/loading_shared_values.hh"
#include "utils/chunked_vector.hh"
#include "log.hh"

namespace bi = boost::intrusive;

namespace utils {

using loading_cache_clock_type = seastar::lowres_clock;
using safe_link_list_hook = bi::list_base_hook<bi::link_mode<bi::safe_link>>;

template<typename Tp, typename Key, typename EntrySize , typename Hash, typename EqualPred, typename LoadingSharedValuesStats>
class timestamped_val {
public:
    using value_type = Tp;
    using loading_values_type = typename utils::loading_shared_values<Key, timestamped_val, Hash, EqualPred, LoadingSharedValuesStats, 256>;
    class lru_entry;
    class value_ptr;

private:
    value_type _value;
    loading_cache_clock_type::time_point _loaded;
    loading_cache_clock_type::time_point _last_read;
    lru_entry* _lru_entry_ptr = nullptr; /// MRU item is at the front, LRU - at the back
    size_t _size = 0;

public:
    timestamped_val(value_type val)
        : _value(std::move(val))
        , _loaded(loading_cache_clock_type::now())
        , _last_read(_loaded)
        , _size(EntrySize()(_value))
    {}
    timestamped_val(timestamped_val&&) = default;

    timestamped_val& operator=(value_type new_val) {
        assert(_lru_entry_ptr);

        _value = std::move(new_val);
        _loaded = loading_cache_clock_type::now();
        _lru_entry_ptr->cache_size() -= _size;
        _size = EntrySize()(_value);
        _lru_entry_ptr->cache_size() += _size;
        return *this;
    }

    value_type& value() noexcept { return _value; }
    const value_type& value() const noexcept { return _value; }

    static const timestamped_val& container_of(const value_type& value) {
        return *bi::get_parent_from_member(&value, &timestamped_val::_value);
    }

    loading_cache_clock_type::time_point last_read() const noexcept {
        return _last_read;
    }

    loading_cache_clock_type::time_point loaded() const noexcept {
        return _loaded;
    }

    size_t size() const {
        return _size;
    }

    bool ready() const noexcept {
        return _lru_entry_ptr;
    }

    lru_entry* lru_entry_ptr() const noexcept {
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

template <typename Tp>
struct simple_entry_size {
    size_t operator()(const Tp& val) {
        return 1;
    }
};

template<typename Tp, typename Key, typename EntrySize , typename Hash, typename EqualPred, typename LoadingSharedValuesStats>
class timestamped_val<Tp, Key, EntrySize, Hash, EqualPred, LoadingSharedValuesStats>::value_ptr {
private:
    using ts_value_type = timestamped_val<Tp, Key, EntrySize, Hash, EqualPred, LoadingSharedValuesStats>;
    using loading_values_type = typename ts_value_type::loading_values_type;

public:
    using timestamped_val_ptr = typename loading_values_type::entry_ptr;
    using value_type = Tp;

private:
    timestamped_val_ptr _ts_val_ptr;

public:
    value_ptr(timestamped_val_ptr ts_val_ptr) : _ts_val_ptr(std::move(ts_val_ptr)) { _ts_val_ptr->touch(); }
    explicit operator bool() const noexcept { return bool(_ts_val_ptr); }
    value_type& operator*() const noexcept { return _ts_val_ptr->value(); }
    value_type* operator->() const noexcept { return &_ts_val_ptr->value(); }
};

/// \brief This is and LRU list entry which is also an anchor for a loading_cache value.
template<typename Tp, typename Key, typename EntrySize , typename Hash, typename EqualPred, typename LoadingSharedValuesStats>
class timestamped_val<Tp, Key, EntrySize, Hash, EqualPred, LoadingSharedValuesStats>::lru_entry : public safe_link_list_hook {
private:
    using ts_value_type = timestamped_val<Tp, Key, EntrySize, Hash, EqualPred, LoadingSharedValuesStats>;
    using loading_values_type = typename ts_value_type::loading_values_type;

public:
    using lru_list_type = bi::list<lru_entry>;
    using timestamped_val_ptr = typename loading_values_type::entry_ptr;

private:
    timestamped_val_ptr _ts_val_ptr;
    lru_list_type& _lru_list;
    size_t& _cache_size;

public:
    lru_entry(timestamped_val_ptr ts_val, lru_list_type& lru_list, size_t& cache_size)
        : _ts_val_ptr(std::move(ts_val))
        , _lru_list(lru_list)
        , _cache_size(cache_size)
    {
        _ts_val_ptr->set_anchor_back_reference(this);
        _cache_size += _ts_val_ptr->size();
    }

    ~lru_entry() {
        if (safe_link_list_hook::is_linked()) {
            _lru_list.erase(_lru_list.iterator_to(*this));
        }
        _cache_size -= _ts_val_ptr->size();
        _ts_val_ptr->set_anchor_back_reference(nullptr);
    }

    size_t& cache_size() noexcept {
        return _cache_size;
    }

    /// Set this item as the most recently used item.
    /// The MRU item is going to be at the front of the _lru_list, the LRU item - at the back.
    void touch() noexcept {
        if (safe_link_list_hook::is_linked()) {
            _lru_list.erase(_lru_list.iterator_to(*this));
        }
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

/// \brief Loading cache is a cache that loads the value into the cache using the given asynchronous callback.
///
/// Each cached value if reloading is enabled (\tparam ReloadEnabled == loading_cache_reload_enabled::yes) is reloaded after
/// the "refresh" time period since it was loaded for the last time.
///
/// The values are going to be evicted from the cache if they are not accessed during the "expiration" period or haven't
/// been reloaded even once during the same period.
///
/// If "expiration" is set to zero - the caching is going to be disabled and get_XXX(...) is going to call the "loader" callback
/// every time in order to get the requested value.
///
/// \note In order to avoid the eviction of cached entries due to "aging" of the contained value the user has to choose
/// the "expiration" to be at least ("refresh" + "max load latency"). This way the value is going to stay in the cache and is going to be
/// read in a non-blocking way as long as it's frequently accessed. Note however that since reloading is an asynchronous
/// procedure it may get delayed by other running task. Therefore choosing the "expiration" too close to the ("refresh" + "max load latency")
/// value one risks to have his/her cache values evicted when the system is heavily loaded.
///
/// The cache is also limited in size and if adding the next value is going
/// to exceed the cache size limit the least recently used value(s) is(are) going to be evicted until the size of the cache
/// becomes such that adding the new value is not going to break the size limit. If the new entry's size is greater than
/// the cache size then the get_XXX(...) method is going to return a future with the loading_cache::entry_is_too_big exception.
///
/// The size of the cache is defined as a sum of sizes of all cached entries.
/// The size of each entry is defined by the value returned by the \tparam EntrySize predicate applied on it.
///
/// The get(key) or get_ptr(key) methods ensures that the "loader" callback is called only once for each cached entry regardless of how many
/// callers are calling for the get_XXX(key) for the same "key" at the same time. Only after the value is evicted from the cache
/// it's going to be "loaded" in the context of get_XXX(key). As long as the value is cached get_XXX(key) is going to return the
/// cached value immediately and reload it in the background every "refresh" time period as described above.
///
/// \tparam Key type of the cache key
/// \tparam Tp type of the cached value
/// \tparam ReloadEnabled if loading_cache_reload_enabled::yes allow reloading the values otherwise don't reload
/// \tparam EntrySize predicate to calculate the entry size
/// \tparam Hash hash function
/// \tparam EqualPred equality predicate
/// \tparam LoadingSharedValuesStats statistics incrementing class (see utils::loading_shared_values)
/// \tparam Alloc elements allocator
template<typename Key,
         typename Tp,
         loading_cache_reload_enabled ReloadEnabled = loading_cache_reload_enabled::no,
         typename EntrySize = simple_entry_size<Tp>,
         typename Hash = std::hash<Key>,
         typename EqualPred = std::equal_to<Key>,
         typename LoadingSharedValuesStats = utils::do_nothing_loading_shared_values_stats,
         typename Alloc = std::allocator<typename timestamped_val<Tp, Key, EntrySize, Hash, EqualPred, LoadingSharedValuesStats>::lru_entry>>
class loading_cache {
private:
    using ts_value_type = timestamped_val<Tp, Key, EntrySize, Hash, EqualPred, LoadingSharedValuesStats>;
    using loading_values_type = typename ts_value_type::loading_values_type;
    using timestamped_val_ptr = typename loading_values_type::entry_ptr;
    using ts_value_lru_entry = typename ts_value_type::lru_entry;
    using set_iterator = typename loading_values_type::iterator;
    using lru_list_type = typename ts_value_lru_entry::lru_list_type;
    using list_iterator = typename lru_list_type::iterator;
    struct value_extractor_fn {
        Tp& operator()(ts_value_lru_entry& le) const {
            return le.timestamped_value().value();
        }
    };

public:
    using value_type = Tp;
    using key_type = Key;
    using value_ptr = typename ts_value_type::value_ptr;

    class entry_is_too_big : public std::exception {};
    using iterator = boost::transform_iterator<value_extractor_fn, list_iterator>;

private:
    loading_cache(size_t max_size, std::chrono::milliseconds expiry, std::chrono::milliseconds refresh, logging::logger& logger)
        : _max_size(max_size)
        , _expiry(expiry)
        , _refresh(refresh)
        , _logger(logger)
        , _timer([this] { on_timer(); })
    {
        // Sanity check: if expiration period is given then non-zero refresh period and maximal size are required
        if (caching_enabled() && (_refresh == std::chrono::milliseconds(0) || _max_size == 0)) {
            throw exceptions::configuration_exception("loading_cache: caching is enabled but refresh period and/or max_size are zero");
        }
    }

public:
    template<typename Func>
    loading_cache(size_t max_size, std::chrono::milliseconds expiry, std::chrono::milliseconds refresh, logging::logger& logger, Func&& load)
        : loading_cache(max_size, expiry, refresh, logger)
    {
        static_assert(ReloadEnabled == loading_cache_reload_enabled::yes, "This constructor should only be invoked when ReloadEnabled == loading_cache_reload_enabled::yes");
        static_assert(std::is_same<future<value_type>, std::result_of_t<Func(const key_type&)>>::value, "Bad Func signature");

        _load = std::forward<Func>(load);

        // If expiration period is zero - caching is disabled
        if (!caching_enabled()) {
            return;
        }

        _timer_period = std::min(_expiry, _refresh);
        _timer.arm(_timer_period);
    }

    loading_cache(size_t max_size, std::chrono::milliseconds expiry, logging::logger& logger)
        : loading_cache(max_size, expiry, loading_cache_clock_type::time_point::max().time_since_epoch(), logger)
    {
        static_assert(ReloadEnabled == loading_cache_reload_enabled::no, "This constructor should only be invoked when ReloadEnabled == loading_cache_reload_enabled::no");

        // If expiration period is zero - caching is disabled
        if (!caching_enabled()) {
            return;
        }

        _timer_period = _expiry;
        _timer.arm(_timer_period);
    }

    ~loading_cache() {
        _lru_list.erase_and_dispose(_lru_list.begin(), _lru_list.end(), [] (ts_value_lru_entry* ptr) { loading_cache::destroy_ts_value(ptr); });
    }

    template <typename LoadFunc>
    future<value_ptr> get_ptr(const Key& k, LoadFunc&& load) {
        static_assert(std::is_same<future<value_type>, std::result_of_t<LoadFunc(const key_type&)>>::value, "Bad LoadFunc signature");
        // We shouldn't be here if caching is disabled
        assert(caching_enabled());

        return _loading_values.get_or_load(k, [this, load = std::forward<LoadFunc>(load)] (const Key& k) mutable {
            return load(k).then([this] (value_type val) {
                return ts_value_type(std::move(val));
            });
        }).then([this, k] (timestamped_val_ptr ts_val_ptr) {
            // check again since it could have already been inserted and initialized
            if (!ts_val_ptr->ready()) {
                _logger.trace("{}: storing the value for the first time", k);

                if (ts_val_ptr->size() > _max_size) {
                    return make_exception_future<value_ptr>(entry_is_too_big());
                }

                ts_value_lru_entry* new_lru_entry = Alloc().allocate(1);
                new(new_lru_entry) ts_value_lru_entry(std::move(ts_val_ptr), _lru_list, _current_size);

                // This will "touch" the entry and add it to the LRU list - we must do this before the shrink() call.
                value_ptr vp(new_lru_entry->timestamped_value_ptr());

                // Remove the least recently used items if map is too big.
                shrink();

                return make_ready_future<value_ptr>(std::move(vp));
            }

            return make_ready_future<value_ptr>(std::move(ts_val_ptr));
        });
    }

    future<value_ptr> get_ptr(const Key& k) {
        static_assert(ReloadEnabled == loading_cache_reload_enabled::yes, "");
        return get_ptr(k, _load);
    }

    future<Tp> get(const Key& k) {
        static_assert(ReloadEnabled == loading_cache_reload_enabled::yes, "");

        // If caching is disabled - always load in the foreground
        if (!caching_enabled()) {
            return _load(k);
        }

        return get_ptr(k).then([] (value_ptr v_ptr) {
            return make_ready_future<Tp>(*v_ptr);
        });
    }

    future<> stop() {
        return _timer_reads_gate.close().finally([this] { _timer.cancel(); });
    }

    template<typename KeyType, typename KeyHasher, typename KeyEqual>
    iterator find(const KeyType& key, KeyHasher key_hasher_func, KeyEqual key_equal_func) noexcept {
        return boost::make_transform_iterator(to_list_iterator(set_find(key, std::move(key_hasher_func), std::move(key_equal_func))), _value_extractor_fn);
    };

    iterator find(const Key& k) noexcept {
        return boost::make_transform_iterator(to_list_iterator(set_find(k)), _value_extractor_fn);
    }

    iterator end() {
        return boost::make_transform_iterator(list_end(), _value_extractor_fn);
    }

    iterator begin() {
        return boost::make_transform_iterator(list_begin(), _value_extractor_fn);
    }

    template <typename Pred>
    void remove_if(Pred&& pred) {
        static_assert(std::is_same<bool, std::result_of_t<Pred(const value_type&)>>::value, "Bad Pred signature");

        _lru_list.remove_and_dispose_if([this, &pred] (const ts_value_lru_entry& v) {
            return pred(v.timestamped_value().value());
        }, [this] (ts_value_lru_entry* p) {
            loading_cache::destroy_ts_value(p);
        });
    }

    void remove(const Key& k) {
        auto it = set_find(k);
        if (it == set_end()) {
            return;
        }

        _lru_list.erase_and_dispose(_lru_list.iterator_to(*it->lru_entry_ptr()), [this] (ts_value_lru_entry* p) { loading_cache::destroy_ts_value(p); });
    }

    void remove(iterator it) {
        if (it == end()) {
            return;
        }

        const ts_value_type& val = ts_value_type::container_of(*it);
        _lru_list.erase_and_dispose(_lru_list.iterator_to(*val.lru_entry_ptr()), [this] (ts_value_lru_entry* p) { loading_cache::destroy_ts_value(p); });
    }

    size_t size() const {
        return _lru_list.size();
    }

    /// \brief returns the memory size the currently cached entries occupy according to the EntrySize predicate.
    size_t memory_footprint() const {
        return _current_size;
    }

private:
    /// Should only be called on values for which the following holds: set_it == set_end() || set_it->ready()
    /// For instance this always holds for iterators returned by set_find(...).
    list_iterator to_list_iterator(set_iterator set_it) {
        if (set_it != set_end()) {
            return _lru_list.iterator_to(*set_it->lru_entry_ptr());
        }
        return list_end();
    }

    set_iterator ready_entry_iterator(set_iterator it) {
        set_iterator end_it = set_end();

        if (it == end_it || !it->ready()) {
            return end_it;
        }
        return it;
    }

    template<typename KeyType, typename KeyHasher, typename KeyEqual>
    set_iterator set_find(const KeyType& key, KeyHasher key_hasher_func, KeyEqual key_equal_func) noexcept {
        return ready_entry_iterator(_loading_values.find(key, std::move(key_hasher_func), std::move(key_equal_func)));
    }

    // keep the default non-templated overloads to ease on the compiler for specifications
    // that do not require the templated find().
    set_iterator set_find(const Key& key) noexcept {
        return ready_entry_iterator(_loading_values.find(key));
    }

    set_iterator set_end() noexcept {
        return _loading_values.end();
    }

    set_iterator set_begin() noexcept {
        return _loading_values.begin();
    }

    list_iterator list_end() noexcept {
        return _lru_list.end();
    }

    list_iterator list_begin() noexcept {
        return _lru_list.begin();
    }

    bool caching_enabled() const {
        return _expiry != std::chrono::milliseconds(0);
    }

    static void destroy_ts_value(ts_value_lru_entry* val) {
        val->~ts_value_lru_entry();
        Alloc().deallocate(val, 1);
    }

    future<> reload(timestamped_val_ptr ts_value_ptr) {
        const Key& key = loading_values_type::to_key(ts_value_ptr);

        // Do nothing if the entry has been dropped before we got here (e.g. by the _load() call on another key that is
        // also being reloaded).
        if (!ts_value_ptr->lru_entry_ptr()) {
            _logger.trace("{}: entry was dropped before the reload", key);
            return make_ready_future<>();
        }

        return _load(key).then_wrapped([this, ts_value_ptr = std::move(ts_value_ptr), &key] (auto&& f) mutable {
            // if the entry has been evicted by now - simply end here
            if (!ts_value_ptr->lru_entry_ptr()) {
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
                *ts_value_ptr = f.get0();
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
            if (_expiry < since_last_read || (ReloadEnabled == loading_cache_reload_enabled::yes && _expiry < since_loaded)) {
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
        while (_current_size > _max_size) {
            using namespace std::chrono;
            ts_value_lru_entry& lru_entry = *_lru_list.rbegin();
            _logger.trace("shrink(): {}: dropping the entry: ms since last_read {}", lru_entry.key(), duration_cast<milliseconds>(loading_cache_clock_type::now() - lru_entry.timestamped_value().last_read()).count());
            loading_cache::destroy_ts_value(&lru_entry);
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

        // check if rehashing is needed and do it if it is.
        periodic_rehash();

        if constexpr (ReloadEnabled == loading_cache_reload_enabled::no) {
            _logger.trace("on_timer(): rearming");
            _timer.arm(loading_cache_clock_type::now() + _timer_period);
            return;
        }

        // Reload all those which value needs to be reloaded.
        with_gate(_timer_reads_gate, [this] {
            auto to_reload = boost::copy_range<utils::chunked_vector<timestamped_val_ptr>>(_lru_list
                    | boost::adaptors::filtered([this] (ts_value_lru_entry& lru_entry) {
                        return lru_entry.timestamped_value().loaded() + _refresh < loading_cache_clock_type::now();
                    })
                    | boost::adaptors::transformed([] (ts_value_lru_entry& lru_entry) {
                        return lru_entry.timestamped_value_ptr();
                    }));

            return parallel_for_each(std::move(to_reload), [this] (timestamped_val_ptr ts_value_ptr) {
                _logger.trace("on_timer(): {}: reloading the value", loading_values_type::to_key(ts_value_ptr));
                return this->reload(std::move(ts_value_ptr));
            }).finally([this] {
                _logger.trace("on_timer(): rearming");
                _timer.arm(loading_cache_clock_type::now() + _timer_period);
            });
        });
    }

    loading_values_type _loading_values;
    lru_list_type _lru_list;
    size_t _current_size = 0;
    size_t _max_size = 0;
    std::chrono::milliseconds _expiry;
    std::chrono::milliseconds _refresh;
    loading_cache_clock_type::duration _timer_period;
    logging::logger& _logger;
    std::function<future<Tp>(const Key&)> _load;
    timer<loading_cache_clock_type> _timer;
    seastar::gate _timer_reads_gate;
    value_extractor_fn _value_extractor_fn;
};

}

