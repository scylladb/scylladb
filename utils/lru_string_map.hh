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
// Value must be moveable and is not copied internally.
// The cache keeps track of the order of usage of the entries, and allows
// to evict the least recently used entries.
template<typename Value>
class lru_string_map {
    struct key {
        mutable boost::intrusive::list_member_hook<> list_hook;
        std::string str;
        explicit key(std::string_view s) : str(s) {}
        // We link keys in intrusive::list, so make sure main container provides stable address for keys (e.g in rehashing).
        key() = delete;
        key(const key& k) = delete;
        key(const key&& k) = delete;
    };
    struct key_hash {
        using is_transparent = void;
        size_t operator()(const key& k) const noexcept { return std::hash<std::string_view>{}(k.str); }
        size_t operator()(std::string_view sv) const noexcept { return std::hash<std::string_view>{}(sv); }
    };
    struct key_equal {
        using is_transparent = void;
        bool operator()(const key& k, std::string_view sv) const noexcept { return k.str == sv; }
        bool operator()(std::string_view sv, const key& k) const noexcept { return sv == k.str; }
        bool operator()(const key& a, const key& b) const noexcept { return a.str == b.str; }
    };

    using string_map = absl::node_hash_map<key, Value, key_hash, key_equal>;
    string_map map;
    using lru_key_list_hook = boost::intrusive::member_hook<key, boost::intrusive::list_member_hook<>,&key::list_hook>;
    using lru_key_list = boost::intrusive::list<key, lru_key_list_hook>;
    lru_key_list lru_list;

    // Move the accessed key to the back of the LRU list
    template<bool IsNew=false> 
    void touch(string_map::iterator it) {
        // key is const, but it has mutable list_hook, so we remove constness
        // to allow moving it in the list.
        // This is safe because we only use this for moving keys in the LRU list,
        // and we never modify the key itself.
        key& key_ref = const_cast<key&>(it->first);
        if constexpr (!IsNew) {
            lru_list.erase(lru_list.iterator_to(key_ref));
        }
        lru_list.push_back(key_ref);
    }
public:
    enum class sanity_check { none, basic, detailed };
    // Returns the size of the cache.
    // By default no sanity checks are performed.
    // Optionally the level of sanity checks can be increased:
    // - basic: checks that the size of the LRU list matches the size of the map
    // - detailed: checks that all keys in the LRU list are present in the map
    // Throws std::logic_error if the state is inconsistent.
    template<sanity_check check=sanity_check::none>
    size_t size() const { 
        if constexpr (check == sanity_check::basic || check == sanity_check::detailed) {
            if (lru_list.size() != map.size()) {
                throw std::logic_error("Inconsistent state - size mismatch.");
            }
        }
        if constexpr (check == sanity_check::detailed) {
            for (const auto& k : lru_list) {
                if (map.find(k) == map.end()) {
                    throw std::logic_error("Inconsistent state - key not found.");
                }
            }
        }
        return map.size();
    }
    void reserve(size_t n) { return map.reserve(n); }

    // Clears all entries from cache
    // If `release` is true, the memory used by the cache is released.
    // Returns the number of entries removed.
    size_t clear(bool release=false) {
        lru_list.clear();
        auto ret = map.size();
        if (release) {
            map = string_map();
        } else {
            map.clear();
        }
        return ret;
    }

    // Lookup a value by a key
    // If found, the value is moved to the back of the LRU list (most recently used).
    // Returns std::nullopt if the key is not found.
    std::optional<std::reference_wrapper<Value>> find(std::string_view key) {
        auto it = map.find(key);
        if (it == map.end()) {
            return std::nullopt;
        }
        touch(it);
        return std::ref(it->second);
    }

    // Removes the least recently used item from the cache
    // Returns false if the cache was empty, true otherwise.
    bool pop() {
        if (lru_list.empty()) {
            return false;
        }
        key& key_ref = lru_list.front();
        lru_list.pop_front();
        map.erase(key_ref);
        return true;
    }

    // Insert or update the value for the given key.
    // The key is copied into the cache.
    void push(std::string_view key, Value&& value) {
        auto ret = map.insert_or_assign(key, std::move(value));
        (ret.second ? touch<true>(ret.first) : touch<false>(ret.first));
    }

    // Insert or update the value for the given key.
    // The key is moved into the cache.
    void push(std::string&& key, Value&& value) {
        auto ret = map.insert_or_assign(std::move(key), std::move(value));
        (ret.second ? touch<true>(ret.first) : touch<false>(ret.first));
    }

    // Evict items until the size is at most max_size.
    // Returns the number of evicted items.
    // Optionally it can use sanity checks (see size() for details).
    template<sanity_check check=sanity_check::none>
    size_t evict_until_size(size_t max_size) {
        size_t ss, is;
        ss = is = size<check>();
        while (ss > max_size && pop()) {
            ss = size<check>();
        }
        return is - ss;
    }
};
