/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include "utils/assert.hh"
#include "utils/replicator.hh"
#include "gms/inet_address.hh"
#include "gms/generation-number.hh"

#include <seastar/core/on_internal_error.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/log.hh>

#include <boost/intrusive/list.hpp>

#include <chrono>
#include "locator/host_id.hh"

namespace bi = boost::intrusive;

namespace db {
class system_keyspace;
}

namespace service {

extern seastar::logger rslog;

// This class provides an abstraction of expirable server address mappings
// used by the messaging service and raft rpc module to store host id to ip mapping
template <typename Clock>
class address_map_t : public peering_sharded_service<address_map_t<Clock>> {
    // Expiring mappings stay in the cache for 1 hour (if not accessed during this time period)
    static constexpr std::chrono::hours default_expiry_period{1};
    static constexpr size_t initial_buckets_count = 16;
    using clock_duration = typename Clock::duration;
    using clock_time_point = typename Clock::time_point;

    class expiring_entry_ptr;

public:
    // Represents a change intent to a host mapping.
    // Associative, but not commutative.
    // Not all updates provide a generation, so we must merge updates in order.
    // Expiry changes are not resolved using generation number, they rely on order.
    struct entry_mutation {
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
        // The generation is only used to resolve addr changes.
        gms::generation_type generation;
        std::optional<gms::inet_address> addr;

        // If engaged, indicates that this mutation changes the "expiring" status of the entry.
        // If disengaged, the "expiring" status is not changed by the update.
        // Updates of "expiring" status are always applied in order, last update wins.
        std::optional<bool> expiring;

        explicit operator bool() const {
            return addr.has_value() || expiring.has_value();
        }

        // We rely on this to never throw for exception safety.
        void apply(const entry_mutation& m) noexcept {
            if (m.addr && (m.generation >= generation || !addr)) {
                generation = m.generation;
                addr = m.addr;
            }
            if (m.expiring) {
                expiring = m.expiring;
            }
        }
    };

    using host_mutation = std::pair<locator::host_id, entry_mutation>;
    using cluster_mutation = std::unordered_map<locator::host_id, entry_mutation>;

private:
    // An `inet_address` optionally equipped with a pointer to an entry
    // in LRU list of 'expiring entries'. If the pointer is set, it means that this
    // `timestamped_entry` is expiring; the corresponding LRU list entry contains
    // the last access time and we periodically delete elements from the LRU list
    // when they become too old.
    struct timestamped_entry {
        entry_mutation _m;
        std::unique_ptr<expiring_entry_ptr> _lru_entry;

        explicit timestamped_entry(entry_mutation m)
            : _m(std::move(m)), _lru_entry(nullptr)
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

        explicit expiring_entry_ptr(list_type& l, const locator::host_id& entry_id)
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

        const locator::host_id& entry_id() {
            return _entry_id;
        }

    private:
        list_type& _expiring_list;
        clock_time_point _last_accessed;
        const locator::host_id& _entry_id;
    };

    using map_type = std::unordered_map<locator::host_id, timestamped_entry>;
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

    struct replicator_impl;

    // Engaged on shard 0
    std::unique_ptr<replicator_impl> _replicator;

    void drop_expired_entries(bool force = false) {
        auto list_it = _expiring_list.rbegin();
        while (list_it != _expiring_list.rend() && (list_it->expired(_expiry_period) || force)) {
            // Remove from both LRU list and base storage
            auto map_it = _map.find(list_it->entry_id());
            if (map_it == _map.end()) {
                on_internal_error(rslog, format(
                    "address_map::drop_expired_entries: missing entry with id {}", list_it->entry_id()));
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

    void add_expiring_entry(const locator::host_id& entry_id, timestamped_entry& entry) {
        entry._lru_entry = std::make_unique<expiring_entry_ptr>(_expiring_list, entry_id);
        if (!_timer.armed()) {
            _timer.arm(_expiry_period);
        }
    }

    void apply_to_all(locator::host_id id, entry_mutation m, std::source_location l = std::source_location::current());

public: // Used by replicator
    static void prepare_apply(cluster_mutation& map, const host_mutation& mf) {
        map.try_emplace(mf.first, entry_mutation{});
    }

    // Applies mf to map.
    // Must not throw when called after prepare_apply(map, mf).
    static void apply(cluster_mutation& map, const host_mutation& mf) {
        auto [it, emplaced] = map.try_emplace(mf.first, mf.second);
        if (!emplaced) {
            it->second.apply(mf.second);
        }
    }

    static void apply(cluster_mutation& dst, const cluster_mutation& src) {
        for (auto&& e : src) {
            apply(dst, e);
        }
    }

    void on_replication_failed(std::exception_ptr e) {
        rslog.warn("address map replication failed: {}", e);
    }

    // Strong exception guarantees: no side effects on exception.
    void apply_locally(const host_mutation& mf, bool update_if_exists = true) {
        auto&& [id, m]  = mf;
        if (!m) {
            return;
        }

        auto [it, emplaced] = _map.try_emplace(id, timestamped_entry{m});
        auto& entry = it->second;

        if (emplaced) {
            if (!m.expiring || *m.expiring) {
                try {
                    add_expiring_entry(it->first, entry);
                } catch (...) {
                    _map.erase(it);
                    throw;
                }
            }
        } else if (update_if_exists) {
            if (m.expiring) {
                if (*m.expiring) {
                    // Do first, so that we don't leave side effects on exception
                    if (!entry.expiring()) {
                        add_expiring_entry(it->first, entry);
                    }
                } else {
                    entry._lru_entry = nullptr;
                }
            }
            entry._m.apply(m);
            if (entry.expiring()) {
                entry._lru_entry->touch();
            }
        }
    }

    // Weak exception guarantees: It's allowed to partially apply m.
    void apply_locally(const cluster_mutation& m) {
        for (auto&& e : m) {
            apply_locally(e);
        }
    }

public:
    address_map_t();
    future<> stop();

    // Resolves when all local updates replicate everywhere.
    // Call on shard 0 only.
    future<> barrier();

    // Find a mapping with a given id.
    //
    // If a mapping is expiring, the last access timestamp is updated automatically.
    std::optional<gms::inet_address> find(locator::host_id id) const {
        auto it = _map.find(id);
        if (it == _map.end()) {
            return std::nullopt;
        }
        auto& entry = it->second;
        if (entry.expiring()) {
            // Touch the entry to update it's access timestamp and move it to the front of LRU list
            entry._lru_entry->touch();
        }
        return entry._m.addr;
    }

    // Same as find() above but expects mapping to exist
    gms::inet_address get(locator::host_id id) const {
        try {
            return find(id).value();
        } catch (std::bad_optional_access& err) {
            on_internal_error(rslog, fmt::format("No ip address for {} when one is expected", id));
        }
    }

    // Find an id with a given mapping.
    //
    // If a mapping is expiring, the last access timestamp is updated automatically.
    //
    // The only purpose of this function is to allow passing IPs with the
    // --ignore-dead-nodes parameter in raft_removenode and raft_replace. As this
    // feature is deprecated, we should also remove this function when the
    // deprecation period ends.
    std::optional<locator::host_id> find_by_addr(gms::inet_address addr) const {
        rslog.warn("Finding Raft nodes by IP addresses is deprecated. Please use Host IDs instead.");
        if (addr == gms::inet_address{}) {
            on_internal_error(rslog, "address_map::find_by_addr: called with an empty address");
        }
        auto it = std::find_if(_map.begin(), _map.end(), [&](auto&& mapping) {
            return mapping.second._m.addr.value_or(gms::inet_address{}) == addr;
        });
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
    void set_nonexpiring(locator::host_id id) {
        apply_to_all(id, entry_mutation{ .expiring = false });
    }

    // Convert a non-expiring entry to an expiring one,
    // eventually erasing it from the mapping. Never inserts an
    // entry if it doesn't exist.
    // Can be called only on shard 0.
    // The expiring state is replicated to other shards.
    void set_expiring(locator::host_id id) {
        apply_to_all(id, entry_mutation{ .expiring = true });
    }
    // Insert a new mapping with an IP address if it doesn't
    // exist yet. Creates a mapping only on the current shard. Doesn't
    // update the mapping if it already exists.
    // The purpose of this function is to cache an IP address
    // on a local shard while gossiper messages are still
    // arriving.
    // Used primarily from Raft RPC to speed up Raft at boot.
    void opt_add_entry(locator::host_id id, gms::inet_address addr) {
        if (addr == gms::inet_address{}) {
            on_internal_error(rslog, format("IP address missing for {}", id));
        }
        apply_locally(std::make_pair(id, entry_mutation{ .addr = addr }), false);
    }
    // Insert or update entry with a new IP address on all shards.
    // Used when we get a gossip notification about a node IP
    // address. Overrides the current IP address if present,
    // as long as the generation of the new entry is greater.
    // If no entry is present, creates an expiring entry - there
    // must be a separate Raft configuration change event (@sa
    // set_nonexpiring()) to mark the entry as non expiring.
    void add_or_update_entry(locator::host_id id, gms::inet_address addr,
            gms::generation_type generation_number = {}) {
        if (addr == gms::inet_address{}) {
            on_internal_error(rslog, format("IP address missing for {}", id));
        }
        apply_to_all(id, entry_mutation{
            .generation = generation_number,
            .addr = addr,
        });
    }

    // Drop all expiring entries immediately, without waiting for expiry.
    // Used for testing
    void force_drop_expiring_entries() {
        drop_expired_entries(true);
    }
};

template <typename Clock>
struct address_map_t<Clock>::replicator_impl : public replicator<typename address_map_t<Clock>::cluster_mutation, address_map_t<Clock>> {
    using replicator<typename address_map_t<Clock>::cluster_mutation, address_map_t<Clock>>::replicator;
};

template <typename Clock>
address_map_t<Clock>::address_map_t()
    : _map(initial_buckets_count)
    , _timer([this] { drop_expired_entries(); })
    , _expiry_period(default_expiry_period)
{
    if (this_shard_id() == 0) {
        _replicator = std::make_unique<replicator_impl>(*this);
    }
}

template <typename Clock>
future<> address_map_t<Clock>::stop() {
    if (_replicator) {
        co_await _replicator->stop();
    }
}

template <typename Clock>
future<> address_map_t<Clock>::barrier() {
    if (this_shard_id() != 0) {
        on_internal_error(rslog, "barrier() must be called on shard 0");
    }
    return _replicator->barrier();
}

template <typename Clock>
void address_map_t<Clock>::apply_to_all(locator::host_id id, entry_mutation m, std::source_location l) {
    if (this_shard_id() != 0) {
        on_internal_error(rslog, format("address_map::{}() called on shard {} != 0", l.function_name(), this_shard_id()));
    }
    _replicator->apply_to_all(std::make_pair(id, m));
}

} // end of namespace service
