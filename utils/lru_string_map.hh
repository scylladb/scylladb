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
