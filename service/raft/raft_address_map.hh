/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include "utils/assert.hh"
#include "gms/inet_address.hh"
#include "gms/generation-number.hh"
#include "raft/raft.hh"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/util/log.hh>

#include <boost/intrusive/list.hpp>

#include <chrono>
#include <source_location>

namespace bi = boost::intrusive;

namespace db {
class system_keyspace;
}

namespace service {

extern seastar::logger rslog;

using raft_ticker_type = seastar::timer<lowres_clock>;
// TODO: should be configurable.
static constexpr raft_ticker_type::duration raft_tick_interval = std::chrono::milliseconds(100);

// This class provides an abstraction of expirable server address mappings
// used by the raft rpc module to store connection info for servers in a raft group.
template <typename Clock>
class raft_address_map_t : public peering_sharded_service<raft_address_map_t<Clock>> {
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
        std::optional<gms::inet_address> _addr;
        // The address map's source of IP addresses is gossip,
        // which can reorder events it delivers. It is therefore
        // possible that we get an outdated IP address after
        // the map has been updated with a new one, and revert
        // the mapping to an incorrect one (see #14274). To
        // protect against outdated information we mark each
        // entry with its generation number, when available,
        // and drop updates with outdated generations. 0 means
        // there is no generation available - e.g. it's set when
        // we load the persisted map state from system.peers at
        // boot.
        gms::generation_type _generation_number;
        std::unique_ptr<expiring_entry_ptr> _lru_entry;

        explicit timestamped_entry(gms::generation_type generation_number,
            std::optional<gms::inet_address> addr)
            : _addr(std::move(addr)), _generation_number(generation_number), _lru_entry(nullptr)
        {
        }

        bool expiring() const {
            return _lru_entry != nullptr;
        }
    };

    class expiring_entry_ptr : public bi::list_base_hook<> {
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

        explicit expiring_entry_ptr(list_type& l, const raft::server_id& entry_id)
            : _expiring_list(l), _last_accessed(Clock::now()), _entry_id(entry_id)
        {
            _expiring_list.push_front(*this);
        }

        ~expiring_entry_ptr() {
            _expiring_list.erase(_expiring_list.iterator_to(*this));
        }

        // Update last access timestamp and move ourselves to the front of LRU list.
        void touch() {
            _last_accessed = Clock::now();
            _expiring_list.erase(_expiring_list.iterator_to(*this));
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
    };

    using map_type = std::unordered_map<raft::server_id, timestamped_entry>;
    using map_iterator = typename map_type::iterator;

    using expiring_list_type = typename expiring_entry_ptr::list_type;
    using expiring_list_iterator = typename expiring_list_type::iterator;

    // LRU list to hold expiring entries.
    //
    // Marked as `mutable` since the `find` function, which should naturally
    // be `const`, updates the entry's timestamp and thus requires
    // non-const access.
    mutable expiring_list_type _expiring_list;

    // Container to hold address mappings (both permanent and expiring).
    // Declared as `mutable` for the same reasons as `_expiring_list`.
    //
    // It's important that _map is declared after _expiring_list, so it's
    // destroyed first: when we destroy _map, the LRU entries corresponding
    // to expiring entries are also destroyed, which unlinks them from the list,
    // so the list must still exist.
    mutable map_type _map;

    // Timer that executes the cleanup procedure to erase expired
    // entries from the mappings container.
    //
    // Rearmed automatically in the following cases:
    // * A new expiring entry is created
    // * If there are still some expiring entries left in the LRU list after
    //   the cleanup is finished.
    seastar::timer<Clock> _timer;
    clock_duration _expiry_period;

    std::optional<future<>> _replication_fiber{make_ready_future<>()};

    void drop_expired_entries(bool force = false) {
        auto list_it = _expiring_list.rbegin();
        while (list_it != _expiring_list.rend() && (list_it->expired(_expiry_period) || force)) {
            // Remove from both LRU list and base storage
            auto map_it = _map.find(list_it->entry_id());
            if (map_it == _map.end()) {
                on_internal_error(rslog, format(
                    "raft_address_map::drop_expired_entries: missing entry with id {}", list_it->entry_id()));
            }
            _map.erase(map_it);
            // Point at the oldest entry again
            list_it = _expiring_list.rbegin();
        }
        if (!_expiring_list.empty()) {
            // Rearm the timer in case there are still some expiring entries
            _timer.arm(_expiry_period);
        }
    }

    void add_expiring_entry(const raft::server_id& entry_id, timestamped_entry& entry) {
        entry._lru_entry = std::make_unique<expiring_entry_ptr>(_expiring_list, entry_id);
        if (!_timer.armed()) {
            _timer.arm(_expiry_period);
        }
    }

    template <std::invocable<raft_address_map_t&> F>
    void replicate(F f, seastar::compat::source_location l = seastar::compat::source_location::current()) {
        if (this_shard_id() != 0) {
            auto msg = format("raft_address_map::{}() called on shard {} != 0",
                l.function_name(), this_shard_id());
            on_internal_error(rslog, msg);
        }
        if (!_replication_fiber) {
            return;
        }

        _replication_fiber = _replication_fiber->then([this, f = std::move(f), l] () -> future<> {
            try {
                co_await this->container().invoke_on_others([f] (raft_address_map_t& self) {
                    f(self);
                });
            } catch (...) {
                rslog.error("raft_address_map_t::replicate (called from {}) failed: {}",
                            l.function_name(), std::current_exception());
            }
        });
    }

    void replicate_set_nonexpiring(const raft::server_id& id) {
        replicate([id] (raft_address_map_t& self) {
            self.handle_set_nonexpiring(id);
        });
    }

    void replicate_set_expiring(const raft::server_id& id) {
        replicate([id] (raft_address_map_t& self) {
            self.handle_set_expiring(id);
        });
    }

    void replicate_add_or_update_entry(const raft::server_id& id,
            gms::generation_type generation_number, const gms::inet_address& ip_addr,
            bool update_if_exists) {
        replicate([id, generation_number, ip_addr, update_if_exists] (raft_address_map_t& self) {
            self.handle_add_or_update_entry(id, generation_number, ip_addr, update_if_exists);
        });
    }

    void handle_set_nonexpiring(const raft::server_id& id) {
        auto [it, _] = _map.try_emplace(id, timestamped_entry{gms::generation_type{}, std::nullopt});
        auto& entry = it->second;

        if (entry.expiring()) {
            entry._lru_entry = nullptr;
        }
    }

    void handle_set_expiring(const raft::server_id& id) {
        auto it = _map.find(id);
        if (it == _map.end()) {
            return;
        }
        auto& entry = it->second;
        if (entry.expiring()) {
            return;
        }
        add_expiring_entry(it->first, entry);
    }

    void handle_add_or_update_entry(const raft::server_id& id,
            gms::generation_type generation_number, const gms::inet_address& ip_addr,
            bool update_if_exists) {
        auto [it, emplaced] = _map.try_emplace(id, timestamped_entry{generation_number, ip_addr});
        auto& entry = it->second;
        if (emplaced) {
            add_expiring_entry(it->first, entry);
        } else if ((update_if_exists && generation_number >= entry._generation_number) || !entry._addr) {
            entry._addr = ip_addr;
            entry._generation_number = generation_number;
            if (entry.expiring()) {
                entry._lru_entry->touch(); // Re-insert in the front of _expiring_list
            }
        }
    }


public:
    raft_address_map_t()
        : _map(initial_buckets_count),
        _timer([this] { drop_expired_entries(); }),
        _expiry_period(default_expiry_period)
    {}

    future<> stop() {
        SCYLLA_ASSERT(_replication_fiber);
        co_await *std::exchange(_replication_fiber, std::nullopt);
    }

    ~raft_address_map_t() {
        SCYLLA_ASSERT(!_replication_fiber);
    }

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
            entry._lru_entry->touch();
        }
        return entry._addr;
    }

    // Find an id with a given mapping.
    //
    // If a mapping is expiring, the last access timestamp is updated automatically.
    //
    // The only purpose of this function is to allow passing IPs with the
    // --ignore-dead-nodes parameter in raft_removenode and raft_replace. As this
    // feature is deprecated, we should also remove this function when the
    // deprecation period ends.
    std::optional<raft::server_id> find_by_addr(gms::inet_address addr) const {
        rslog.warn("Finding Raft nodes by IP addresses is deprecated. Please use Host IDs instead.");
        if (addr == gms::inet_address{}) {
            on_internal_error(rslog, "raft_address_map::find_by_addr: called with an empty address");
        }
        auto it = std::find_if(_map.begin(), _map.end(), [&](auto&& mapping) { return mapping.second._addr == addr; });
        if (it == _map.end()) {
            return std::nullopt;
        }
        auto& entry = it->second;
        if (entry.expiring()) {
            entry._lru_entry->touch();
        }
        return it->first;
    }

    // Convert an expiring entry to a non-expiring one, or
    // insert a new non-expiring entry if the entry is missing.
    // Called on Raft configuration changes to mark the new
    // member of Raft configuration as a non-temporary member
    // of the address map. The configuration member may be
    // lacking an IP address but it will be added later.
    // Can only be called on shard 0.
    // The expiring state is replicated to other shards.
    void set_nonexpiring(raft::server_id id) {
        handle_set_nonexpiring(id);
        replicate_set_nonexpiring(id);
    }

    // Convert a non-expiring entry to an expiring one,
    // eventually erasing it from the mapping. Never inserts an
    // entry if it doesn't exist.
    // Can be called only on shard 0.
    // The expiring state is replicated to other shards.
    void set_expiring(raft::server_id id) {
        handle_set_expiring(id);
        replicate_set_expiring(id);
    }
    // Insert a new mapping with an IP address if it doesn't
    // exist yet. Creates a mapping only on the current shard. Doesn't
    // update the mapping if it already exists.
    // The purpose of this function is to cache an IP address
    // on a local shard while gossiper messages are still
    // arriving.
    // Used primarily from Raft RPC to speed up Raft at boot.
    void opt_add_entry(raft::server_id id, gms::inet_address addr) {
        if (addr == gms::inet_address{}) {
            on_internal_error(rslog, format("IP address missing for {}", id));
        }
        handle_add_or_update_entry(id, gms::generation_type{}, addr, false);
    }
    // Insert or update entry with a new IP address on all shards.
    // Used when we get a gossip notification about a node IP
    // address. Overrides the current IP address if present,
    // as long as the generation of the new entry is greater.
    // If no entry is present, creates an expiring entry - there
    // must be a separate Raft configuration change event (@sa
    // set_nonexpiring()) to mark the entry as non expiring.
    void add_or_update_entry(raft::server_id id, gms::inet_address addr,
            gms::generation_type generation_number = {}) {
        if (addr == gms::inet_address{}) {
            on_internal_error(rslog, format("IP address missing for {}", id));
        }
        handle_add_or_update_entry(id, generation_number, addr, true);
        replicate_add_or_update_entry(id, generation_number, addr, true);
    }

    // Drop all expiring entries immediately, without waiting for expiry.
    // Used for testing
    void force_drop_expiring_entries() {
        drop_expired_entries(true);
    }
};

using raft_address_map = raft_address_map_t<seastar::lowres_clock>;

} // end of namespace service
