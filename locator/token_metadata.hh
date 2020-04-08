/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015 ScyllaDB
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

#include <map>
#include <unordered_set>
#include <unordered_map>
#include "gms/inet_address.hh"
#include "dht/i_partitioner.hh"
#include "utils/UUID.hh"
#include <optional>
#include <memory>
#include <boost/range/iterator_range.hpp>
#include <boost/icl/interval.hpp>
#include "range.hh"

// forward declaration since database.hh includes this file
class keyspace;

namespace locator {

class abstract_replication_strategy;

using inet_address = gms::inet_address;
using token = dht::token;

// Endpoint Data Center and Rack names
struct endpoint_dc_rack {
    sstring dc;
    sstring rack;
};

class topology {
public:
    topology() {}
    topology(const topology& other);

    void clear();

    /**
     * Stores current DC/rack assignment for ep
     */
    void add_endpoint(const inet_address& ep);

    /**
     * Removes current DC/rack assignment for ep
     */
    void remove_endpoint(inet_address ep);

    /**
     * Re-reads the DC/rack info for the given endpoint
     * @param ep endpoint in question
     */
    void update_endpoint(inet_address ep);

    /**
     * Returns true iff contains given endpoint
     */
    bool has_endpoint(inet_address) const;

    std::unordered_map<sstring,
                       std::unordered_set<inet_address>>&
    get_datacenter_endpoints() {
        return _dc_endpoints;
    }

    const std::unordered_map<sstring,
                           std::unordered_set<inet_address>>&
    get_datacenter_endpoints() const {
        return _dc_endpoints;
    }

    std::unordered_map<sstring,
                       std::unordered_map<sstring,
                                          std::unordered_set<inet_address>>>&
    get_datacenter_racks() {
        return _dc_racks;
    }

    const endpoint_dc_rack& get_location(const inet_address& ep) const;
private:
    /** multi-map: DC -> endpoints in that DC */
    std::unordered_map<sstring,
                       std::unordered_set<inet_address>>
        _dc_endpoints;

    /** map: DC -> (multi-map: rack -> endpoints in that rack) */
    std::unordered_map<sstring,
                       std::unordered_map<sstring,
                                          std::unordered_set<inet_address>>>
        _dc_racks;

    /** reverse-lookup map: endpoint -> current known dc/rack assignment */
    std::unordered_map<inet_address, endpoint_dc_rack> _current_locations;
};

class token_metadata_impl;
class tokens_iterator_impl;

class token_metadata final {
    std::unique_ptr<token_metadata_impl> _impl;
public:
    using UUID = utils::UUID;
    using inet_address = gms::inet_address;
private:
    class tokens_iterator : public std::iterator<std::input_iterator_tag, token> {
        using impl_type = tokens_iterator_impl;
        std::unique_ptr<impl_type> _impl;
    public:
        explicit tokens_iterator(tokens_iterator_impl);
        tokens_iterator(const tokens_iterator&);
        tokens_iterator(tokens_iterator&&) = default;
        tokens_iterator& operator=(const tokens_iterator&);
        tokens_iterator& operator=(tokens_iterator&&) = default;
        ~tokens_iterator();
        bool operator==(const tokens_iterator& it) const;
        bool operator!=(const tokens_iterator& it) const;
        const token& operator*();
        tokens_iterator& operator++();
    };
    token_metadata(std::unordered_map<token, inet_address> token_to_endpoint_map, std::unordered_map<inet_address, utils::UUID> endpoints_map, topology topology);
public:
    token_metadata();
    explicit token_metadata(std::unique_ptr<token_metadata_impl> impl);
    token_metadata(const token_metadata&);
    token_metadata(token_metadata&&) noexcept; // Can't use "= default;" - hits some static_assert in unique_ptr
    token_metadata& operator=(const token_metadata&);
    token_metadata& operator=(token_metadata&&) noexcept;
    ~token_metadata();
    const std::vector<token>& sorted_tokens() const;
    void update_normal_token(token token, inet_address endpoint);
    void update_normal_tokens(std::unordered_set<token> tokens, inet_address endpoint);
    void update_normal_tokens(const std::unordered_map<inet_address, std::unordered_set<token>>& endpoint_tokens);
    const token& first_token(const token& start) const;
    size_t first_token_index(const token& start) const;
    std::optional<inet_address> get_endpoint(const token& token) const;
    std::vector<token> get_tokens(const inet_address& addr) const;
    const std::unordered_map<token, inet_address>& get_token_to_endpoint() const;
    const std::unordered_set<inet_address>& get_leaving_endpoints() const;
    const std::unordered_map<token, inet_address>& get_bootstrap_tokens() const;
    void update_topology(inet_address ep);
    tokens_iterator tokens_end() const;
    /**
     * Creates an iterable range of the sorted tokens starting at the token next
     * after the given one.
     *
     * @param start A token that will define the beginning of the range
     *
     * @return The requested range (see the description above)
     */
    boost::iterator_range<tokens_iterator> ring_range(const token& start, bool include_min = false) const;
    boost::iterator_range<tokens_iterator> ring_range(
        const std::optional<dht::partition_range::bound>& start, bool include_min = false) const;

    topology& get_topology();
    const topology& get_topology() const;
    void debug_show();

    /**
     * Store an end-point to host ID mapping.  Each ID must be unique, and
     * cannot be changed after the fact.
     *
     * @param hostId
     * @param endpoint
     */
    void update_host_id(const UUID& host_id, inet_address endpoint);

    /** Return the unique host ID for an end-point. */
    UUID get_host_id(inet_address endpoint) const;

    /// Return the unique host ID for an end-point or nullopt if not found.
    std::optional<UUID> get_host_id_if_known(inet_address endpoint) const;

    /** Return the end-point for a unique host ID */
    std::optional<inet_address> get_endpoint_for_host_id(UUID host_id) const;

    /** @return a copy of the endpoint-to-id map for read-only operations */
    const std::unordered_map<inet_address, utils::UUID>& get_endpoint_to_host_id_map_for_reading() const;

    void add_bootstrap_token(token t, inet_address endpoint);

    void add_bootstrap_tokens(std::unordered_set<token> tokens, inet_address endpoint);

    void remove_bootstrap_tokens(std::unordered_set<token> tokens);

    void add_leaving_endpoint(inet_address endpoint);

    void remove_endpoint(inet_address endpoint);

    bool is_member(inet_address endpoint);

    bool is_leaving(inet_address endpoint);

    // Is this node being replaced by another node
    bool is_being_replaced(inet_address endpoint);

    // Is any node being replaced by another node
    bool is_any_node_being_replaced();

    void add_replacing_endpoint(inet_address existing_node, inet_address replacing_node);

    void del_replacing_endpoint(inet_address existing_node);

    /**
     * Create a copy of TokenMetadata with only tokenToEndpointMap. That is, pending ranges,
     * bootstrap tokens and leaving endpoints are not included in the copy.
     */
    token_metadata clone_only_token_map();
    /**
     * Create a copy of TokenMetadata with tokenToEndpointMap reflecting situation after all
     * current leave operations have finished.
     *
     * @return new token metadata
     */
    token_metadata clone_after_all_left();
    /**
     * Create a copy of TokenMetadata with tokenToEndpointMap reflecting situation after all
     * current leave, and move operations have finished.
     *
     * @return new token metadata
     */
    token_metadata clone_after_all_settled();
    dht::token_range_vector get_primary_ranges_for(std::unordered_set<token> tokens);

    dht::token_range_vector get_primary_ranges_for(token right);
    static boost::icl::interval<token>::interval_type range_to_interval(range<dht::token> r);
    static range<dht::token> interval_to_range(boost::icl::interval<token>::interval_type i);

    /** a mutable map may be returned but caller should not modify it */
    const std::unordered_map<range<token>, std::unordered_set<inet_address>>& get_pending_ranges(sstring keyspace_name);

    std::vector<range<token>> get_pending_ranges(sstring keyspace_name, inet_address endpoint);
     /**
     * Calculate pending ranges according to bootsrapping and leaving nodes. Reasoning is:
     *
     * (1) When in doubt, it is better to write too much to a node than too little. That is, if
     * there are multiple nodes moving, calculate the biggest ranges a node could have. Cleaning
     * up unneeded data afterwards is better than missing writes during movement.
     * (2) When a node leaves, ranges for other nodes can only grow (a node might get additional
     * ranges, but it will not lose any of its current ranges as a result of a leave). Therefore
     * we will first remove _all_ leaving tokens for the sake of calculation and then check what
     * ranges would go where if all nodes are to leave. This way we get the biggest possible
     * ranges with regard current leave operations, covering all subsets of possible final range
     * values.
     * (3) When a node bootstraps, ranges of other nodes can only get smaller. Without doing
     * complex calculations to see if multiple bootstraps overlap, we simply base calculations
     * on the same token ring used before (reflecting situation after all leave operations have
     * completed). Bootstrapping nodes will be added and removed one by one to that metadata and
     * checked what their ranges would be. This will give us the biggest possible ranges the
     * node could have. It might be that other bootstraps make our actual final ranges smaller,
     * but it does not matter as we can clean up the data afterwards.
     *
     * NOTE: This is heavy and ineffective operation. This will be done only once when a node
     * changes state in the cluster, so it should be manageable.
     */
    future<> calculate_pending_ranges(abstract_replication_strategy& strategy, const sstring& keyspace_name);
    future<> calculate_pending_ranges_for_leaving(
        abstract_replication_strategy& strategy,
        lw_shared_ptr<std::unordered_multimap<range<token>, inet_address>> new_pending_ranges,
        lw_shared_ptr<token_metadata> all_left_metadata);
    void calculate_pending_ranges_for_bootstrap(
        abstract_replication_strategy& strategy,
        lw_shared_ptr<std::unordered_multimap<range<token>, inet_address>> new_pending_ranges,
        lw_shared_ptr<token_metadata> all_left_metadata);
    future<> calculate_pending_ranges_for_replacing(
            abstract_replication_strategy& strategy,
            lw_shared_ptr<std::unordered_multimap<range<token>, inet_address>> new_pending_ranges);

    token get_predecessor(token t);

    std::vector<inet_address> get_all_endpoints() const;
    size_t get_all_endpoints_count() const;

    /* Returns the number of different endpoints that own tokens in the ring.
     * Bootstrapping tokens are not taken into account. */
    size_t count_normal_token_owners() const;


    sstring print_pending_ranges();
    std::vector<gms::inet_address> pending_endpoints_for(const token& token, const sstring& keyspace_name);

    /** @return an endpoint to token multimap representation of tokenToEndpointMap (a copy) */
    std::multimap<inet_address, token> get_endpoint_to_token_map_for_reading();
    /**
     * @return a (stable copy, won't be modified) Token to Endpoint map for all the normal and bootstrapping nodes
     *         in the cluster.
     */
    std::map<token, inet_address> get_normal_and_bootstrapping_token_to_endpoint_map();

    long get_ring_version() const;
    void invalidate_cached_rings();

    friend class token_metadata_impl;
};

}
