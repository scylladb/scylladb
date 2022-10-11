/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include "gms/inet_address.hh"
#include "gms/inet_address_serializer.hh"
#include "raft/raft.hh"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/util/log.hh>

#include <boost/intrusive/list.hpp>

#include <chrono>

namespace bi = boost::intrusive;

namespace service {

extern seastar::logger rslog;

using raft_ticker_type = seastar::timer<lowres_clock>;
// TODO: should be configurable.
static constexpr raft_ticker_type::duration raft_tick_interval = std::chrono::milliseconds(100);

// This class provides an abstraction of expirable server address mappings
// used by the raft rpc module to store connection info for servers in a raft group.
template <typename Clock = seastar::lowres_clock>
class raft_address_map {

    // Expiring mappings stay in the cache for 1 hour (if not accessed during this time period)
    static constexpr std::chrono::hours default_expiry_period{1};
    static constexpr size_t initial_buckets_count = 16;
    using clock_duration = typename Clock::duration;
    using clock_time_point = typename Clock::time_point;

    class expiring_entry_ptr;

    // An `inet_address` optionally equipped with a pointer to an entry
    // in LRU list of 'expiring entries'. If the pointer is set, it means that this
    // `timestamped_entry` is expiring; the corresponding LRU list entry contains
    // the last access time and we periodically delete elements from the LRU list
    // when they become too old.
    struct timestamped_entry {
        gms::inet_address _addr;
        expiring_entry_ptr* _lru_entry;

        explicit timestamped_entry(gms::inet_address addr)
            : _addr(std::move(addr)), _lru_entry(nullptr)
        {
        }

        ~timestamped_entry() {
            if (_lru_entry) {
                delete _lru_entry; // Deletes itself from LRU list
            }
        }

        void set_lru_back_pointer(expiring_entry_ptr* ptr) {
            _lru_entry = ptr;
        }
        expiring_entry_ptr* lru_entry_ptr() {
            return _lru_entry;
        }

        bool expiring() const {
            return _lru_entry != nullptr;
        }
    };

    using lru_list_hook = bi::list_base_hook<>;

    class expiring_entry_ptr : public lru_list_hook {
    public:
        // Base type for LRU list of expiring entries.
        //
        // When an entry is created with state, an
        // entry in this list is created, holding a pointer to the base entry
        // which contains the data.
        //
        // The LRU list is maintained in such a way that MRU (most recently used)
        // entries are at the beginning of the list while LRU entries move to the
        // end.
        using list_type = bi::list<expiring_entry_ptr>;

        explicit expiring_entry_ptr(list_type& l, const raft::server_id& entry_id, timestamped_entry& entry)
            : _expiring_list(l), _last_accessed(Clock::now()), _entry_id(entry_id), _ptr(entry)
        {
            _ptr.set_lru_back_pointer(this);
        }

        ~expiring_entry_ptr() {
            if (lru_list_hook::is_linked()) {
                _expiring_list.erase(_expiring_list.iterator_to(*this));
            }
            _ptr.set_lru_back_pointer(nullptr);
        }

        // Update last access timestamp and move ourselves to the front of LRU list.
        void touch() {
            _last_accessed = Clock::now();
            if (lru_list_hook::is_linked()) {
                _expiring_list.erase(_expiring_list.iterator_to(*this));
            }
            _expiring_list.push_front(*this);
        }
        // Test whether the entry has expired or not given a base time point and
        // an expiration period (the time period since the last access lies within
        // the given expiration period time frame).
        bool expired(clock_duration expiry_period) const {
            auto last_access_delta = Clock::now() - _last_accessed;
            return expiry_period < last_access_delta;
        }

        const raft::server_id& entry_id() {
            return _entry_id;
        }

    private:
        list_type& _expiring_list;
        clock_time_point _last_accessed;
        const raft::server_id& _entry_id;
        struct timestamped_entry& _ptr;
    };

    using map_type = std::unordered_map<raft::server_id, timestamped_entry>;
    using map_iterator = typename map_type::iterator;

    using expiring_list_type = typename expiring_entry_ptr::list_type;
    using expiring_list_iterator = typename expiring_list_type::iterator;

    // Container to hold address mappings (both permanent and expiring).
    //
    // Marked as `mutable` since the `find` function, which should naturally
    // be `const`, updates the entry's timestamp and thus requires
    // non-const access.
    mutable map_type _map;

    expiring_list_iterator to_list_iterator(timestamped_entry& e) const {
        return _expiring_list.iterator_to(*e.lru_entry_ptr());
    }

    // LRU list to hold expiring entries. Also declared as `mutable` for the
    // same reasons as `_map`.
    mutable expiring_list_type _expiring_list;

    // Timer that executes the cleanup procedure to erase expired
    // entries from the mappings container.
    //
    // Rearmed automatically in the following cases:
    // * A new expiring entry is created
    // * If there are still some expiring entries left in the LRU list after
    //   the cleanup is finished.
    seastar::timer<Clock> _timer;
    clock_duration _expiry_period;

    void drop_expired_entries() {
        auto list_it = _expiring_list.rbegin();
        while (list_it != _expiring_list.rend() && list_it->expired(_expiry_period)) {
            // Remove from both LRU list and base storage
            auto map_it = _map.find(list_it->entry_id());
            if (map_it != _map.end()) {
                _map.erase(map_it);
            }
            // Point at the oldest entry again
            list_it = _expiring_list.rbegin();
        }
        if (!_expiring_list.empty()) {
            // Rearm the timer in case there are still some expiring entries
            _timer.arm(_expiry_period);
        }
    }

    // Remove an entry pointer from LRU list, thus converting entry to regular state
    void unlink_and_dispose(expiring_list_iterator it) {
        if (it == _expiring_list.end()) {
            return;
        }
        _expiring_list.erase_and_dispose(it, [] (expiring_entry_ptr* ptr) { delete ptr; });
    }

    void add_expiring_entry(const raft::server_id& entry_id, timestamped_entry& entry) {
        auto exp_entry_ptr = new expiring_entry_ptr(_expiring_list, entry_id, entry);
        _expiring_list.push_front(*exp_entry_ptr);
        if (!_timer.armed()) {
            _timer.arm(_expiry_period);
        }
    }

public:
    raft_address_map()
        : _map(initial_buckets_count),
        _timer([this] { drop_expired_entries(); }),
        _expiry_period(default_expiry_period)
    {}

    // Find a mapping with a given id.
    //
    // If a mapping is expiring, the last access timestamp is updated automatically.
    std::optional<gms::inet_address> find(raft::server_id id) const {
        auto it = _map.find(id);
        if (it == _map.end()) {
            return std::nullopt;
        }
        auto& entry = it->second;
        if (entry.expiring()) {
            // Touch the entry to update it's access timestamp and move it to the front of LRU list
            to_list_iterator(entry)->touch();
        }
        return entry._addr;
    }
    // Linear search for id based on inet address. Used when
    // removing a node which id is unknown. Do not return self
    // - we need to remove id of the node self is replacing.
    std::optional<raft::server_id> find_replace_id(gms::inet_address addr, raft::server_id self) const {
        for (auto& [id, entry] : _map) {
            if (entry._addr == addr && id != self) {
                return id;
            }
        }
        return {};
    }
    // Inserts a new mapping or updates the existing one.
    // An entry can be changed from expiring to non expiring one, but not the other way.
    // The function verifies that if the mapping exists, then its inet_address
    // and the provided one match.
    //
    // This means that we cannot remap the entry's actual inet_address but
    // nonetheless the function can be used to promote the entry from
    // expiring to permanent or vice versa.
    void set(raft::server_id id, gms::inet_address addr, bool expiring) {
        auto [it, emplaced] = _map.try_emplace(std::move(id), std::move(addr));
        auto& entry = it->second;
        if (emplaced) {
            if (expiring) {
                add_expiring_entry(it->first, entry);
            }
            return;
        }

        // Don't allow to remap to a different address
        if (entry._addr != addr) {
            on_internal_error(rslog, format("raft_address_map: expected to get inet_address {} for raft server id {} (got {})",
                entry._addr, id, addr));
        }

        if (entry.expiring()) {
            if (!expiring) {
                // Change the mapping from expiring to regular
                unlink_and_dispose(to_list_iterator(entry));
            } else {
                // Update timestamp of expiring entry
                to_list_iterator(entry)->touch(); // Re-insert in the front of _expiring_list
            }
        }
        // No action needed when a regular entry is updated
    }

    // Convert a non-expiring entry to an expiring one
    std::optional<gms::inet_address> set_expiring_flag(raft::server_id id) {
        auto it = _map.find(id);
        if (it == _map.end()) {
            return std::nullopt;
        }
        auto& entry = it->second;
        if (entry.expiring()) {
            // Update timestamp of expiring entry
            to_list_iterator(entry)->touch(); // Re-insert in the front of _expiring_list
        } else {
            add_expiring_entry(it->first, entry);
        }
        return entry._addr;
    }

    // A shortcut to setting a new permanent address
    void set(raft::server_address addr) {
        return set(addr.id,
            ser::deserialize_from_buffer(addr.info, boost::type<gms::inet_address>{}),
            false);
    }

    // Map raft server_id to inet_address to be consumed by `messaging_service`
    gms::inet_address get_inet_address(raft::server_id id) const {
        auto it = find(id);
        if (!it) {
            on_internal_error(rslog, format("Destination raft server not found with id {}", id));
        }
        return *it;
    }
    raft::server_address get_server_address(raft::server_id id) const {
        return raft::server_address{id, ser::serialize_to_buffer<bytes>(get_inet_address(id))};
    }
};

} // end of namespace service
