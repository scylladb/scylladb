/*
 * Copyright (C) 2015-present ScyllaDB
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

#include <memory>
#include <functional>
#include <unordered_map>
#include "gms/inet_address.hh"
#include "locator/snitch_base.hh"
#include "dht/i_partitioner.hh"
#include "token_metadata.hh"
#include "snitch_base.hh"
#include <seastar/util/bool_class.hh>
#include "utils/maybe_yield.hh"

// forward declaration since database.hh includes this file
class keyspace;

namespace locator {

using inet_address = gms::inet_address;
using token = dht::token;

enum class replication_strategy_type {
    simple,
    local,
    network_topology,
    everywhere_topology,
};

using can_yield = utils::can_yield;

class abstract_replication_strategy {
private:
    long _last_invalidated_ring_version = 0;
    std::unordered_map<token, inet_address_vector_replica_set> _cached_endpoints;
    uint64_t _cache_hits_count = 0;

    static logging::logger logger;

    std::unordered_map<token, inet_address_vector_replica_set>&
    get_cached_endpoints(const token_metadata& tm);
protected:
    sstring _ks_name;
    // TODO: Do we need this member at all?
    //keyspace* _keyspace = nullptr;
    std::map<sstring, sstring> _config_options;
    const shared_token_metadata& _shared_token_metadata;
    snitch_ptr& _snitch;
    replication_strategy_type _my_type;

    template <typename... Args>
    void err(const char* fmt, Args&&... args) const {
        logger.error(fmt, std::forward<Args>(args)...);
    }

    template <typename... Args>
    void warn(const char* fmt, Args&&... args) const {
        logger.warn(fmt, std::forward<Args>(args)...);
    }

    template <typename... Args>
    void debug(const char* fmt, Args&&... args) const {
        logger.debug(fmt, std::forward<Args>(args)...);
    }

    void validate_replication_factor(sstring rf) const;

public:
    abstract_replication_strategy(
        const sstring& keyspace_name,
        const shared_token_metadata& stm,
        snitch_ptr& snitch,
        const std::map<sstring, sstring>& config_options,
        replication_strategy_type my_type);

    // The returned vector has size O(number of normal token owners), which is O(number of nodes in the cluster).
    // Note: it is not guaranteed that the function will actually yield. If the complexity of a particular implementation
    // is small, that implementation may not yield since by itself it won't cause a reactor stall (assuming practical
    // cluster sizes and number of tokens per node). The caller is responsible for yielding if they call this function
    // in a loop.
    virtual inet_address_vector_replica_set calculate_natural_endpoints(const token& search_token, const token_metadata& tm, can_yield = can_yield::no) const = 0;

    virtual ~abstract_replication_strategy() {}
    static std::unique_ptr<abstract_replication_strategy> create_replication_strategy(const sstring& ks_name, const sstring& strategy_name, const shared_token_metadata& stm, const std::map<sstring, sstring>& config_options);
    static void validate_replication_strategy(const sstring& ks_name,
                                              const sstring& strategy_name,
                                              const shared_token_metadata& stm,
                                              const std::map<sstring, sstring>& config_options);
    inet_address_vector_replica_set get_natural_endpoints(const token& search_token, can_yield = can_yield::no);
    inet_address_vector_replica_set get_natural_endpoints_without_node_being_replaced(const token& search_token, can_yield = can_yield::no);
    virtual void validate_options() const = 0;
    virtual std::optional<std::set<sstring>> recognized_options() const = 0;
    virtual size_t get_replication_factor() const = 0;
    // Decide if the replication strategy allow removing the node being
    // replaced from the natural endpoints when a node is being replaced in the
    // cluster. LocalStrategy is the not allowed to do so because it always
    // returns the node itself as the natural_endpoints and the node will not
    // appear in the pending_endpoints.
    virtual bool allow_remove_node_being_replaced_from_natural_endpoints() const = 0;
    uint64_t get_cache_hits_count() const { return _cache_hits_count; }
    replication_strategy_type get_type() const { return _my_type; }

    // get_ranges() returns the list of ranges held by the given endpoint.
    // The list is sorted, and its elements are non overlapping and non wrap-around.
    // It the analogue of Origin's getAddressRanges().get(endpoint).
    // This function is not efficient, and not meant for the fast path.
    dht::token_range_vector get_ranges(inet_address ep, can_yield can_yield = can_yield::no) const {
        return do_get_ranges(ep, _shared_token_metadata.get(), can_yield);
    }

    // Use the token_metadata provided by the caller instead of _token_metadata
    // Caller must ensure that token_metadata will not change throughout the call if can_yield::yes
    dht::token_range_vector get_ranges(inet_address ep, const token_metadata_ptr tmptr, can_yield can_yield = can_yield::no) const {
        return do_get_ranges(ep, std::move(tmptr), can_yield);
    }
private:
    // Caller must ensure that token_metadata will not change throughout the call if can_yield::yes.
    dht::token_range_vector do_get_ranges(inet_address ep, const token_metadata_ptr tmptr, can_yield) const;
    virtual inet_address_vector_replica_set do_get_natural_endpoints(const token& search_token, const token_metadata& tm, can_yield);

public:
    // get_primary_ranges() returns the list of "primary ranges" for the given
    // endpoint. "Primary ranges" are the ranges that the node is responsible
    // for storing replica primarily, which means this is the first node
    // returned calculate_natural_endpoints().
    // This function is the analogue of Origin's
    // StorageService.getPrimaryRangesForEndpoint().
    dht::token_range_vector get_primary_ranges(inet_address ep, can_yield);
    // get_primary_ranges_within_dc() is similar to get_primary_ranges()
    // except it assigns a primary node for each range within each dc,
    // instead of one node globally.
    dht::token_range_vector get_primary_ranges_within_dc(inet_address ep, can_yield);

    std::unordered_multimap<inet_address, dht::token_range> get_address_ranges(const token_metadata& tm, can_yield) const;
    std::unordered_multimap<inet_address, dht::token_range> get_address_ranges(const token_metadata& tm, inet_address endpoint, can_yield) const;

    std::unordered_map<dht::token_range, inet_address_vector_replica_set> get_range_addresses(const token_metadata& tm, can_yield) const;

    dht::token_range_vector get_pending_address_ranges(const token_metadata_ptr tmptr, token pending_token, inet_address pending_address, can_yield) const;

    dht::token_range_vector get_pending_address_ranges(const token_metadata_ptr tmptr, std::unordered_set<token> pending_tokens, inet_address pending_address, can_yield) const;
};

}
