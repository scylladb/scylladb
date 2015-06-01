/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "utils/UUID.hh"
#include "token_metadata.hh"
#include <experimental/optional>

namespace locator {

token_metadata::token_metadata(std::map<token, inet_address> token_to_endpoint_map, std::unordered_map<inet_address, utils::UUID> endpoints_map, topology topology) :
    _token_to_endpoint_map(token_to_endpoint_map), _endpoint_to_host_id_map(endpoints_map), _topology(topology) {
    _sorted_tokens = sorted_tokens();
}

std::vector<token> token_metadata::sort_tokens() {
    std::vector<token> sorted;
    sorted.reserve(_token_to_endpoint_map.size());

    for (auto&& i : _token_to_endpoint_map) {
        sorted.push_back(i.first);
    }

    return sorted;
}

const std::vector<token>& token_metadata::sorted_tokens() const {
    return _sorted_tokens;
}

std::vector<token> token_metadata::get_tokens(const inet_address& addr) const {
    std::vector<token> res;
    for (auto&& i : _token_to_endpoint_map) {
        if (i.second == addr) {
            res.push_back(i.first);
        }
    }
    return res;
}
/**
 * Update token map with a single token/endpoint pair in normal state.
 */
void token_metadata::update_normal_token(token t, inet_address endpoint)
{
    update_normal_tokens(std::unordered_set<token>({t}), endpoint);
}

void token_metadata::update_normal_tokens(std::unordered_set<token> tokens, inet_address endpoint) {
    std::unordered_map<inet_address, std::unordered_set<token>> endpoint_tokens ({{endpoint, tokens}});
    update_normal_tokens(endpoint_tokens);
}

/**
 * Update token map with a set of token/endpoint pairs in normal state.
 *
 * Prefer this whenever there are multiple pairs to update, as each update (whether a single or multiple)
 * is expensive (CASSANDRA-3831).
 *
 * @param endpointTokens
 */
void token_metadata::update_normal_tokens(std::unordered_map<inet_address, std::unordered_set<token>>& endpoint_tokens) {
    if (endpoint_tokens.empty()) {
        return;
    }

    bool should_sort_tokens = false;
    for (auto&& i : endpoint_tokens)
    {
        inet_address endpoint = i.first;
        std::unordered_set<token>& tokens = i.second;

        assert(!tokens.empty());

        for(auto it = _token_to_endpoint_map.begin(), ite = _token_to_endpoint_map.end(); it != ite;) {
            if(it->second == endpoint) {
                it = _token_to_endpoint_map.erase(it);
            } else {
                ++it;
            }
        }

#if 0
        bootstrapTokens.removeValue(endpoint);
        topology.addEndpoint(endpoint);
        leavingEndpoints.remove(endpoint);
        removeFromMoving(endpoint); // also removing this endpoint from moving
#endif
        for (const token& t : tokens)
        {
            auto prev = _token_to_endpoint_map.insert(std::pair<token, inet_address>(t, endpoint));
            should_sort_tokens |= prev.second; // new token inserted -> sort
            if (prev.first->second  != endpoint) {
                //logger.warn("Token {} changing ownership from {} to {}", token, prev.first->second, endpoint);
                prev.first->second = endpoint;
            }
        }
    }

    if (should_sort_tokens) {
        _sorted_tokens = sort_tokens();
    }
}

size_t token_metadata::first_token_index(const token& start) {
    assert(_sorted_tokens.size() > 0);
    auto it = std::lower_bound(_sorted_tokens.begin(), _sorted_tokens.end(), start);
    if (it == _sorted_tokens.end()) {
        return 0;
    } else {
        return std::distance(_sorted_tokens.begin(), it);
    }
}

const token& token_metadata::first_token(const token& start) {
    return _sorted_tokens[first_token_index(start)];
}

std::experimental::optional<inet_address> token_metadata::get_endpoint(const token& token) const {
    auto it = _token_to_endpoint_map.find(token);
    if (it == _token_to_endpoint_map.end()) {
        return std::experimental::nullopt;
    } else {
        return it->second;
    }
}

void token_metadata::debug_show() {
    auto reporter = std::make_shared<timer<lowres_clock>>();
    reporter->set_callback ([reporter, this] {
        print("Endpoint -> Token\n");
        for (auto x : _token_to_endpoint_map) {
            print("inet_address=%s, token=%s\n", x.second, x.first);
        }
        print("Endpoint -> UUID\n");
        for (auto x: _endpoint_to_host_id_map) {
            print("inet_address=%s, uuid=%s\n", x.first, x.second);
        }
    });
    reporter->arm_periodic(std::chrono::seconds(1));
}

void token_metadata::update_host_id(const UUID& host_id, inet_address endpoint) {
#if 0
    assert host_id != null;
    assert endpoint != null;

    InetAddress storedEp = _endpoint_to_host_id_map.inverse().get(host_id);
    if (storedEp != null) {
        if (!storedEp.equals(endpoint) && (FailureDetector.instance.isAlive(storedEp))) {
            throw new RuntimeException(String.format("Host ID collision between active endpoint %s and %s (id=%s)",
                                                     storedEp,
                                                     endpoint,
                                                     host_id));
        }
    }

    UUID storedId = _endpoint_to_host_id_map.get(endpoint);
    // if ((storedId != null) && (!storedId.equals(host_id)))
        logger.warn("Changing {}'s host ID from {} to {}", endpoint, storedId, host_id);
#endif
    _endpoint_to_host_id_map[endpoint] = host_id;
}

utils::UUID token_metadata::get_host_id(inet_address endpoint) {
    assert(_endpoint_to_host_id_map.count(endpoint));
    return _endpoint_to_host_id_map.at(endpoint);
}

gms::inet_address token_metadata::get_endpoint_for_host_id(UUID host_id) {
    auto beg = _endpoint_to_host_id_map.cbegin();
    auto end = _endpoint_to_host_id_map.cend();
    auto it = std::find_if(beg, end, [host_id] (auto x) {
        return x.second == host_id;
    });
    assert(it != end);
    return (*it).first;
}

const auto& token_metadata::get_endpoint_to_host_id_map_for_reading() {
    return _endpoint_to_host_id_map;
}



}
