/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include <map>
#include <unordered_set>
#include <boost/bimap.hpp>
#include "gms/inet_address.hh"
#include "dht/i_partitioner.hh"
#include "utils/UUID.hh"
#include <experimental/optional>

namespace locator {

using inet_address = gms::inet_address;
using token = dht::token;

class topology {

};

class token_metadata final {
    /**
     * Maintains token to endpoint map of every node in the cluster.
     * Each Token is associated with exactly one Address, but each Address may have
     * multiple tokens.  Hence, the BiMultiValMap collection.
     */
    // FIXME: have to be BiMultiValMap
    std::map<token, inet_address> _token_to_endpoint_map;

    /** Maintains endpoint to host ID map of every node in the cluster */
    boost::bimap<inet_address, utils::UUID> _endpoint_to_host_id_map;

    std::vector<token> _sorted_tokens;

    topology _topology;

    std::vector<token> sort_tokens();

    token_metadata(std::map<token, inet_address> token_to_endpoint_map, boost::bimap<inet_address, utils::UUID> endpoints_map, topology topology);
public:
    token_metadata() {};
    const std::vector<token>& sorted_tokens() const;
    void update_normal_token(token token, inet_address endpoint);
    void update_normal_tokens(std::unordered_set<token> tokens, inet_address endpoint);
    void update_normal_tokens(std::unordered_map<inet_address, std::unordered_set<token>>& endpoint_tokens);
    const token& first_token(const token& start);
    size_t first_token_index(const token& start);
    std::experimental::optional<inet_address> get_endpoint(const token& token) const;
};

}
