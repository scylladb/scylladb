/*
 * Copyright (C) 2021-present ScyllaDB
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

#include "gms/inet_address.hh"
#include "raft/raft.hh"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/util/log.hh>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set.hpp>

#include <chrono>

namespace bi = boost::intrusive;

extern seastar::logger rslog;

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

    using set_base_hook = bi::unordered_set_base_hook<>;

    // Basically `inet_address` optionally equipped with a timestamp of the last
    // access time.
    // If timestamp is set an entry is considered to be expiring and
    // in such case it also contains a corresponding entry in LRU list
    // of expiring entries.
    struct timestamped_entry : public set_base_hook {
        // Base storage type to hold entries
        using set_type = bi::unordered_set<timestamped_entry>;

        set_type& _set;
        raft::server_id _id;
        gms::inet_address _addr;
        std::optional<clock_time_point> _last_accessed;
        expiring_entry_ptr* _lru_entry;

        explicit timestamped_entry(set_type& set, raft::server_id id, gms::inet_address addr, bool expiring)
            : _set(set), _id(id), _addr(std::move(addr)), _lru_entry(nullptr)
        {
            if (expiring) {
                _last_accessed = Clock::now();
            }
        }

        ~timestamped_entry() {
            if (_lru_entry) {
                delete _lru_entry; // Deletes itself from LRU list
            }
            if (set_base_hook::is_linked()) {
                _set.erase(_set.iterator_to(*this));
            }
        }

        void set_lru_back_pointer(expiring_entry_ptr* ptr) {
            _lru_entry = ptr;
        }
        expiring_entry_ptr* lru_entry_ptr() {
            return _lru_entry;
        }

        bool expiring() const {
            return static_cast<bool>(_last_accessed);
        }

        friend bool operator==(const timestamped_entry& a, const timestamped_entry& b){
            return a._id == b._id;
        }

        friend std::size_t hash_value(const timestamped_entry& v) {
            return std::hash<raft::server_id>()(v._id);
        }
    };

    using lru_list_hook = bi::list_base_hook<>;

    class expiring_entry_ptr : public lru_list_hook {
    public:
        // Base type for LRU list of expiring entries.
        //
        // When an entry is created with or promoted to expiring state, an
        // entry in this list is created, holding a pointer to the base entry
        // which contains the data.
        //
        // The LRU list is maintained in such a way that MRU (most recently used)
        // entries are at the beginning of the list while LRU entries move to the
        // end.
        using list_type = bi::list<expiring_entry_ptr>;

        explicit expiring_entry_ptr(list_type& l, timestamped_entry* e)
            : _expiring_list(l), _ptr(e)
        {
            _ptr->_last_accessed = Clock::now();
            _ptr->set_lru_back_pointer(this);
        }

        ~expiring_entry_ptr() {
            if (lru_list_hook::is_linked()) {
                _expiring_list.erase(_expiring_list.iterator_to(*this));
            }
            _ptr->_last_accessed = std::nullopt;
            _ptr->set_lru_back_pointer(nullptr);
        }

        // Update last access timestamp and move ourselves to the front of LRU list.
        void touch() {
            _ptr->_last_accessed = Clock::now();
            if (lru_list_hook::is_linked()) {
                _expiring_list.erase(_expiring_list.iterator_to(*this));
            }
            _expiring_list.push_front(*this);
        }
        // Test whether the entry has expired or not given a base time point and
        // an expiration period (the time period since the last access lies within
        // the given expiration period time frame).
        bool expired(clock_duration expiry_period) const {
            auto last_access_delta = Clock::now() - *_ptr->_last_accessed;
            return expiry_period < last_access_delta;
        }

        timestamped_entry* timestamped_entry_ptr() {
            return _ptr;
        }

    private:
        list_type& _expiring_list;
        timestamped_entry* _ptr;
    };

    struct id_compare {
        bool operator()(const raft::server_id& id, const timestamped_entry& e) const {
            return id == e._id;
        }
    };

    using set_type = typename timestamped_entry::set_type;
    using set_bucket_traits = typename set_type::bucket_traits;
    using set_iterator = typename set_type::iterator;

    using expiring_list_type = typename expiring_entry_ptr::list_type;
    using expiring_list_iterator = typename expiring_list_type::iterator;

    std::vector<typename set_type::bucket_type> _buckets;
    // Container to hold address mappings (both permanent and expiring).
    //
    // Marked as `mutable` since the `find` function, which should naturally
    // be `const`, updates the entry's timestamp and thus requires
    // non-const access.
    mutable set_type _set;

    expiring_list_iterator to_list_iterator(set_iterator it) const {
        if (it != _set.end()) {
            return _expiring_list.iterator_to(*it->lru_entry_ptr());
        }
        return _expiring_list.end();
    }

    set_iterator to_set_iterator(expiring_list_iterator it) const {
        if (it != _expiring_list.end()) {
            return _set.iterator_to(*it->timestamped_entry_ptr());
        }
        return _set.end();
    }

    // LRU list to hold expiring entries. Also declared as `mutable` for the
    // same reasons as `_set`.
    mutable expiring_list_type _expiring_list;

    // Timer that executes the cleanup procedure to erase expired
    // entries from the mappings container.
    //
    // Rearmed automatically in the following cases:
    // * A new expiring entry is created
    // * Regular entry is promoted to expiring
    // * If there are still some expiring entries left in the LRU list after
    //   the cleanup is finished.
    seastar::timer<Clock> _timer;
    clock_duration _expiry_period;

    void drop_expired_entries() {
        auto list_it = _expiring_list.rbegin();
        if (list_it == _expiring_list.rend()) {
            return;
        }
        while (list_it->expired(_expiry_period)) {
            auto base_list_it = list_it.base();
            // When converting from rbegin() reverse_iterator to base iterator,
            // we need to decrement it explicitly, because otherwise it will point
            // to end().
            --base_list_it;
            // Remove from both LRU list and base storage
            unlink_and_dispose(to_set_iterator(base_list_it));
            // Point at the oldest entry again
            list_it = _expiring_list.rbegin();
            if (list_it == _expiring_list.rend()) {
                break;
            }
        }
        if (!_expiring_list.empty()) {
            // Rearm the timer in case there are still some expiring entries
            _timer.arm(_expiry_period);
        }
    }

    // Remove an entry from both the base storage and LRU list
    void unlink_and_dispose(set_iterator it) {
        if (it == _set.end()) {
            return;
        }
        _set.erase_and_dispose(it, [] (timestamped_entry* ptr) { delete ptr; });
    }
    // Remove an entry pointer from LRU list, thus converting entry to regular state
    void unlink_and_dispose(expiring_list_iterator it) {
        if (it == _expiring_list.end()) {
            return;
        }
        _expiring_list.erase_and_dispose(it, [] (expiring_entry_ptr* ptr) { delete ptr; });
    }

public:
    raft_address_map()
        : _buckets(initial_buckets_count),
        _set(set_bucket_traits(_buckets.data(), _buckets.size())),
        _timer([this] { drop_expired_entries(); }),
        _expiry_period(default_expiry_period)
    {}

    ~raft_address_map() {
        _set.clear_and_dispose([] (timestamped_entry* ptr) { delete ptr; });
    }

    // Find a mapping with a given id.
    //
    // If a mapping is expiring, the last access timestamp is updated automatically.
    std::optional<gms::inet_address> find(raft::server_id id) const {
        auto set_it = _set.find(id, std::hash<raft::server_id>(), id_compare());
        if (set_it == _set.end()) {
            return std::nullopt;
        }
        if (set_it->expiring()) {
            // Touch the entry to update it's access timestamp and move it to the front of LRU list
            to_list_iterator(set_it)->touch();
        }
        return set_it->_addr;
    }
    // Inserts a new mapping or updates the existing one.
    // The function verifies that if the mapping exists, then its inet_address
    // and the provided one match.
    //
    // This means that we cannot remap the entry's actual inet_address but
    // nonetheless the function can be used to promote the entry from
    // expiring to permanent or vice versa.
    void set(raft::server_id id, gms::inet_address addr, bool expiring) {
        auto set_it = _set.find(id, std::hash<raft::server_id>(), id_compare());
        if (set_it == _set.end()) {
            auto entry = new timestamped_entry(_set, std::move(id), std::move(addr), expiring);
            _set.insert(*entry);
            if (expiring) {
                auto ts_ptr = new expiring_entry_ptr(_expiring_list, entry);
                _expiring_list.push_front(*ts_ptr);
                if (!_timer.armed()) {
                    _timer.arm(_expiry_period);
                }
            }
            return;
        }

        // Don't allow to remap to a different address
        if (set_it->_addr != addr) {
            on_internal_error(rslog, format("raft_address_map: expected to get inet_address {} for raft server id {} (got {})",
                set_it->_addr, id, addr));
        }

        if (set_it->expiring() && !expiring) {
            // Change the mapping from expiring to regular
            unlink_and_dispose(to_list_iterator(set_it));
        } else if (!set_it->expiring() && expiring) {
            // Promote regular mapping to expiring
            //
            // Insert a pointer to the entry into lru list of expiring entries
            auto ts_ptr = new expiring_entry_ptr(_expiring_list, &*set_it);
            // Update last access timestamp and add to LRU list
            ts_ptr->touch();
            // Rearm the timer since we are inserting an expiring entry
            if (!_timer.armed()) {
                _timer.arm(_expiry_period);
            }
        } else if (set_it->expiring() && expiring) {
            // Update timestamp of expiring entry
            to_list_iterator(set_it)->touch(); // Re-insert in the front of _expiring_list
        }
        // No action needed when a regular entry is updated
    }
    // Erase an entry from the server address map.
    // Does nothing if an element with a given id does not exist.
    void erase(raft::server_id id) {
        auto set_it = _set.find(id, std::hash<raft::server_id>(), id_compare());
        if (set_it == _set.end()) {
            return;
        }
        // Erase both from LRU list and base storage
        unlink_and_dispose(set_it);
    }
};