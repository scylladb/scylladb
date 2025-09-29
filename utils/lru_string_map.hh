/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <boost/intrusive/list.hpp>
#include <absl/container/node_hash_map.h>
#include <optional>
#include <string>
#include <string_view>
#include "seastar/core/loop.hh"
#include "seastarx.hh"
#include <seastar/core/future.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/coroutine.hh>
#include "seastar/coroutine/maybe_yield.hh"
#include "utils/log.hh"

// A simple LRU cache, mapping strings to values of type Value.
// The cache keeps track of the order of usage of the entries, and allows
// to evict the least recently used entries.
// This implementation is optimized for string keys - it avoids unnecessary
// string allocations on lookups by using transparent lookup in the underlying map.
// The preexisting lru.hh implementation is not helpful for this use case - it has
// only LRU ordering with two lists, while we integrate with a map and even reuse
// of `evictable` base class for single entries wouldn't make anything easier.
template<typename Value>
class lru_string_map {
protected:
    struct key {
        mutable boost::intrusive::list_member_hook<> list_hook;
        std::string str;
        explicit key(std::string_view s) : str(s) {
        }
        explicit key(std::string s) : str(std::move(s)) {
        }
        // We link keys in intrusive::list, so make sure main container provides stable address for keys (e.g in rehashing).
        key() = delete;
        key(const key& k) = delete;
        key(const key&& k) = delete;
        key& operator=(const key& k) = delete;
        key& operator=(key&& k) = delete;
    };
    struct key_hash {
        using is_transparent = void;
        size_t operator()(const key& k) const noexcept {
            return std::hash<std::string_view>{}(k.str);
        }
        size_t operator()(std::string_view sv) const noexcept {
            return std::hash<std::string_view>{}(sv);
        }
    };
    struct key_equal {
        using is_transparent = void;
        bool operator()(const key& k, std::string_view sv) const noexcept {
            return k.str == sv;
        }
        bool operator()(std::string_view sv, const key& k) const noexcept {
            return sv == k.str;
        }
        bool operator()(const key& a, const key& b) const noexcept {
            return a.str == b.str;
        }
    };

    // The underlying map. We use absl::node_hash_map because it provides stable addresses for keys,
    // which is required by boost::intrusive::list. We confirm it by deleting key constructors.
    // It also provides transparent lookup, which is required to avoid unnecessary string allocations on lookups,
    // and guarantees O(1) erase complexity.
    // Could we use std::unordered_map? Regarding stable addresses for keys it should be ok (it would compile).
    // But until C++26 it doesn't fully support transparent lookup.
    using string_map = absl::node_hash_map<key, Value, key_hash, key_equal>;
    string_map _map;
    using lru_key_list_hook = boost::intrusive::member_hook<key, boost::intrusive::list_member_hook<>,&key::list_hook>;
    using lru_key_list = boost::intrusive::list<key, lru_key_list_hook>;
    lru_key_list _lru_key_list;

    // Move the already existing key to the back of the LRU list
    void touch_lru(string_map::iterator it) {
        _lru_key_list.erase(_lru_key_list.iterator_to(it->first));
        push_back_lru(it);
    }
    // Add the key to the back of the LRU list
    void push_back_lru(string_map::iterator it) {
        // key is const, but it has mutable list_hook, so we remove constness
        // to allow moving it in the list.
        // This is safe because we only use this for moving keys in the LRU list,
        // and we never modify the key itself.
        key& key_ref = const_cast<key&>(it->first);
        _lru_key_list.push_back(key_ref);
    }

    bool sanity_check() const {
        return (_lru_key_list.size() == _map.size());
    }

public:
    size_t size() const { 
        return _map.size();
    }
    void reserve(size_t n) {
        return _map.reserve(n);
    }

    // Clears all entries from cache
    // If `release` is true, the memory used by the cache is released.
    // Returns the number of entries removed.
    // WARNING: May stall the reactor when the size is too big.
    // Consider limiting the size before calling this function.
    size_t clear(bool release=false) {
        size_t ret = size();
        _lru_key_list.clear();
        if (release) {
            _map = string_map();
        } else {
            _map.clear();
        }
        return ret;
    }

    // Clears all entries from cache gently - yielding after every `evictions_per_yield` iterations.
    // Cache remains valid and usable during yields, however if anything is added, it will
    // get removed eventually.
    // Returns the number of entries removed.
    future<size_t> clear_gently(size_t evictions_per_yield=100) {
        size_t ret = 0;
        evictions_per_yield = evictions_per_yield > 0 ? evictions_per_yield : 1;
        while (!_map.empty()) {
            if (!pop()) [[unlikely]] {
                // try to recover from sanity check failure
                _map.erase(_map.begin());
            }
            if (++ret % evictions_per_yield == 0) {
                co_await coroutine::maybe_yield();
            }
        }
        co_return ret;
    }

    // Lookup a value by a key
    // If found, the value is moved to the back of the LRU list (most recently used).
    // Returns std::nullopt if the key is not found.
    template<typename KeyType>
    std::optional<std::reference_wrapper<Value>> find(KeyType&& key) {
        auto it = _map.find(std::forward<KeyType>(key));
        if (it == _map.end()) {
            return std::nullopt;
        }
        touch_lru(it);
        return std::ref(it->second);
    }

    // Removes the least recently used item from the cache
    // Returns false if the cache was empty, true otherwise.
    bool pop() {
        if (_lru_key_list.empty()) [[unlikely]] {
            return false;
        }
        key& key_ref = _lru_key_list.front();
        _lru_key_list.pop_front();
        _map.erase(key_ref);
        return true;
    }

    // Update the key if it already exists, or insert it to the map and LRU list if it doesn't.
    template<typename KeyType>
    void insert_or_assign(KeyType&& key, Value&& value) {
        auto ret = _map.insert_or_assign(std::forward<KeyType>(key), std::move(value));
        if (ret.second) [[unlikely]] { // newly inserted
            push_back_lru(ret.first);
        } else {
            touch_lru(ret.first);
        }
    }
};

// A sized LRU cache, with a maximum number of entries.
// This class provides functions to manage cache size, especially evictions
// and resizing.
template<typename Value>
class sized_lru_string_map : protected lru_string_map<Value> {
    using _base = lru_string_map<Value>;
    logging::logger& _logger;
    uint64_t& _evictions;
public:
    sized_lru_string_map(logging::logger& logger, uint64_t& evictions)
     : _logger(logger), _evictions(evictions) {
    }
    template<typename KeyType>
    std::optional<std::reference_wrapper<Value>> find(KeyType&& key) {
        return _base::find(std::forward<KeyType>(key));
    }
    template<typename KeyType>
    void insert(KeyType&& key, Value&& value) {
        if (make_space_for_one()) {
            _base::insert_or_assign(std::forward<KeyType>(key), std::move(value));
        }
    }

private:
    size_t _max_cache_entries = 0;
public:
    size_t get_max_size() const {
        return _max_cache_entries;
    }
    bool disabled() const {
        return _max_cache_entries == 0;
    }

    void set_max_size(size_t max_cache_entries) {
        if (max_cache_entries == _resize_target) {
            return;
        }
        _resize_target = max_cache_entries;
        resize();
    }

private:
    future<> _background_resizing = make_ready_future<>();
    size_t _max_loop = 3000; // max number of evictions before yielding to other tasks
                             // if 0 then no yielding is done
public:
    // Stop the background resizing task, if running.
    future<> stop() {
        set_max_size(0);
        return std::move(_background_resizing);
    }

    void set_max_loop(size_t max_loop) {
        _max_loop = max_loop > 0 ? max_loop : 1;
    }

private:
    size_t _resize_target = 0;
    bool _resize_flush = false;

    bool resize(bool user_request = true) {
        if (!user_request) { // Sanity checks failed
            _max_cache_entries = 0; // immediately block new entries
            _resize_flush = true;   // let the background task know it should flush
        }
        if (!_background_resizing.available()) { 
            // Background task is already running
            return false;
        }
        if (user_request && partial_resize() == stop_iteration::yes) {
            // Small change - resizing done synchronously
            return true;
        }
        _background_resizing.ignore_ready_future();
        _background_resizing = repeat([this] -> future<stop_iteration> {
            if (_resize_flush) {
                // We omit any sanity checks, as they already failed if we are here.
                _evictions += co_await _base::clear_gently(_max_loop);
            }
            co_return partial_resize();
        });
        return false;
    }

    stop_iteration partial_resize() {
        if (_base::size() > _resize_target) { // we need to evict some entries
            // at once we do up to `_max_loop` evictions
            size_t evict_synchronously = std::min(_max_loop, _base::size() - _resize_target);
            while (evict_synchronously--) {
                if (!pop()) [[unlikely]] {
                    resize(false);
                    return stop_iteration::no;
                }
            }
            // if there are still some entries to evict, do it in next iteration
            if (_base::size() > _resize_target) {
                _max_cache_entries = _base::size(); // let user use cache during yielding
                return stop_iteration::no;
            }
        }
        if (_base::size() == 0) {
            // We can clear the map to release memory
            _base::clear(true);
        }
        // Reserve space for the new size
        _base::reserve(_resize_target);
        _max_cache_entries = _resize_target;
        _logger.trace("Max entries changed to {}", _max_cache_entries);
        return stop_iteration::yes;
    }

    void on_invalid_state() {
        on_internal_error(_logger, ("Detected cache inconsistency, purging cache."));
    }

    bool pop() {
        if (!_base::pop() || !_base::sanity_check()) [[unlikely]] {
            on_invalid_state();
            return false;
        }
        _evictions++;
        return true;
    }

public:
    bool sanity_check() {
        if (!_base::sanity_check()) [[unlikely]] {
            on_invalid_state();
            return resize(false);
        }
        return true;
    }

    bool make_space_for_one() {
        if (disabled()) {
            return false;
        }
        if (_base::size() >= _max_cache_entries) {
            if (!pop()) [[unlikely]] {
                // Flush.
                return resize(false);
            } else if (_base::size() >= _max_cache_entries) [[unlikely]] {
                on_internal_error(_logger, "Cache size exceeded maximum, resizing.");
                // Is there some unfinished resizing? Restart it.
                return resize(false);
            }
        }
        return true;
    }
};
