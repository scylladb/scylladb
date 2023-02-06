/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include <seastar/core/lowres_clock.hh>

#include <unordered_map>
#include <list>

namespace utils {

// A map with the track of least recent visited entries.
template <class Key, class Val, class Clock = seastar::lowres_clock>
class recent_entries_map {
    struct recent_entry {
        Key _key;
        Val _value;
        typename Clock::time_point _last_visit;

        template<typename... Args>
        recent_entry(const Key& k, Args&&... args)
            : _key(k)
            , _value(std::forward<Args>(args)...)
            , _last_visit(Clock::now())
        {}

        recent_entry(const recent_entry&) = delete;
        recent_entry& operator=(const recent_entry&) = delete;
    };

    using recent_entries = std::list<recent_entry>;
    using entries_map = std::unordered_map<Key, typename recent_entries::iterator>;

private:
    recent_entries _recent_entries;
    entries_map _entries_map;

private:
    void touch(typename recent_entries::const_iterator it) {
        _recent_entries.splice(_recent_entries.begin(), _recent_entries, it);
        recent_entry& entry = *_recent_entries.begin();
        entry._last_visit = Clock::now();
    }

public:
    // Returns the size of the map.
    size_t size() const {
        return _entries_map.size();
    }

    // Returns the recent_entry with the given key, or constructs new one
    // if the corresponding key is not present in the map.
    template<typename... Args>
    Val& try_get_recent_entry(const Key& k, Args&&... args) {
        if (auto it = _entries_map.find(k); it != _entries_map.end()) {
            touch(it->second);

            recent_entry& entry = *it->second;
            return entry._value;
        }
        
        _recent_entries.emplace_front(k, std::forward<Args>(args)...);
        auto it = _recent_entries.begin();
        _entries_map.emplace(k, it);
        return it->_value;
    }

    // Removes the recent_entry with the given key.
    void remove_recent_entry(const Key& k) {
        if (auto it = _entries_map.find(k); it != _entries_map.end()) {
            _recent_entries.erase(it->second);
            _entries_map.erase(it);
        }
    }

    // Removes recent_entries not younger than the specified interval.
    void remove_least_recent_entries(std::chrono::milliseconds interval) {
        const auto now = Clock::now();

        auto it = std::find_if(_recent_entries.begin(), _recent_entries.end(), [now, interval](const recent_entry& entry) {
            return now - entry._last_visit >= interval;
        });

        for (; it != _recent_entries.end(); ) {
            _entries_map.erase(it->_key);
            it = _recent_entries.erase(it);
        }
    }
};

} // end of namespace utils