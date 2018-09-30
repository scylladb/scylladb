/*
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

#include "utils/UUID.hh"
#include "token_metadata.hh"
#include <experimental/optional>
#include "locator/snitch_base.hh"
#include "locator/abstract_replication_strategy.hh"
#include "log.hh"
#include "stdx.hh"
#include "partition_range_compat.hh"
#include <unordered_map>
#include <algorithm>
#include <boost/icl/interval.hpp>
#include <boost/icl/interval_map.hpp>

namespace locator {

static logging::logger tlogger("token_metadata");

template <typename C, typename V>
static void remove_by_value(C& container, V value) {
    for (auto it = container.begin(); it != container.end();) {
        if (it->second == value) {
            it = container.erase(it);
        } else {
            it++;
        }
    }
}

token_metadata::token_metadata(std::unordered_map<token, inet_address> token_to_endpoint_map, std::unordered_map<inet_address, utils::UUID> endpoints_map, topology topology) :
    _token_to_endpoint_map(token_to_endpoint_map), _endpoint_to_host_id_map(endpoints_map), _topology(topology) {
    _sorted_tokens = sort_tokens();
}

std::vector<token> token_metadata::sort_tokens() {
    std::vector<token> sorted;
    sorted.reserve(_token_to_endpoint_map.size());

    for (auto&& i : _token_to_endpoint_map) {
        sorted.push_back(i.first);
    }

    std::sort(sorted.begin(), sorted.end());

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
    std::sort(res.begin(), res.end());
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
    if (tokens.empty()) {
        return;
    }
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
    for (auto&& i : endpoint_tokens) {
        inet_address endpoint = i.first;
        std::unordered_set<token>& tokens = i.second;

        if (tokens.empty()) {
            auto msg = sprint("tokens is empty in update_normal_tokens");
            tlogger.error("{}", msg);
            throw std::runtime_error(msg);
        }

        for(auto it = _token_to_endpoint_map.begin(), ite = _token_to_endpoint_map.end(); it != ite;) {
            if(it->second == endpoint) {
                it = _token_to_endpoint_map.erase(it);
            } else {
                ++it;
            }
        }

        _topology.add_endpoint(endpoint);
        remove_by_value(_bootstrap_tokens, endpoint);
        _leaving_endpoints.erase(endpoint);
        invalidate_cached_rings();
        for (const token& t : tokens)
        {
            auto prev = _token_to_endpoint_map.insert(std::pair<token, inet_address>(t, endpoint));
            should_sort_tokens |= prev.second; // new token inserted -> sort
            if (prev.first->second != endpoint) {
                tlogger.warn("Token {} changing ownership from {} to {}", t, prev.first->second, endpoint);
                prev.first->second = endpoint;
            }
        }
    }

    if (should_sort_tokens) {
        _sorted_tokens = sort_tokens();
    }
}

size_t token_metadata::first_token_index(const token& start) const {
    if (_sorted_tokens.empty()) {
        auto msg = sprint("sorted_tokens is empty in first_token_index!");
        tlogger.error("{}", msg);
        throw std::runtime_error(msg);
    }
    auto it = std::lower_bound(_sorted_tokens.begin(), _sorted_tokens.end(), start);
    if (it == _sorted_tokens.end()) {
        return 0;
    } else {
        return std::distance(_sorted_tokens.begin(), it);
    }
}

const token& token_metadata::first_token(const token& start) const {
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
        for (auto x : _endpoint_to_host_id_map) {
            print("inet_address=%s, uuid=%s\n", x.first, x.second);
        }
        print("Sorted Token\n");
        for (auto x : _sorted_tokens) {
            print("token=%s\n", x);
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
        tlogger.warn("Changing {}'s host ID from {} to {}", endpoint, storedId, host_id);
#endif
    _endpoint_to_host_id_map[endpoint] = host_id;
}

utils::UUID token_metadata::get_host_id(inet_address endpoint) const {
    if (!_endpoint_to_host_id_map.count(endpoint)) {
        throw std::runtime_error(sprint("host_id for endpoint %s is not found", endpoint));
    }
    return _endpoint_to_host_id_map.at(endpoint);
}

std::optional<utils::UUID> token_metadata::get_host_id_if_known(inet_address endpoint) const {
    auto it = _endpoint_to_host_id_map.find(endpoint);
    if (it == _endpoint_to_host_id_map.end()) {
        return { };
    }
    return it->second;
}

std::experimental::optional<inet_address> token_metadata::get_endpoint_for_host_id(UUID host_id) const {
    auto beg = _endpoint_to_host_id_map.cbegin();
    auto end = _endpoint_to_host_id_map.cend();
    auto it = std::find_if(beg, end, [host_id] (auto x) {
        return x.second == host_id;
    });
    if (it == end) {
        return {};
    } else {
        return (*it).first;
    }
}

const std::unordered_map<inet_address, utils::UUID>& token_metadata::get_endpoint_to_host_id_map_for_reading() const{
    return _endpoint_to_host_id_map;
}

bool token_metadata::is_member(inet_address endpoint) {
    return _topology.has_endpoint(endpoint);
}

void token_metadata::add_bootstrap_token(token t, inet_address endpoint) {
    std::unordered_set<token> tokens{t};
    add_bootstrap_tokens(tokens, endpoint);
}

boost::iterator_range<token_metadata::tokens_iterator>
token_metadata::ring_range(
    const std::experimental::optional<dht::partition_range::bound>& start,
    bool include_min) const
{
    auto r = ring_range(start ? start->value().token() : dht::minimum_token(), include_min);

    if (!r.empty()) {
        // We should skip the first token if it's excluded by the range.
        if (start
            && !start->is_inclusive()
            && !start->value().has_key()
            && start->value().token() == *r.begin())
        {
            r.pop_front();
        }
    }

    return r;
}

void token_metadata::add_bootstrap_tokens(std::unordered_set<token> tokens, inet_address endpoint) {
    for (auto t : tokens) {
        auto old_endpoint = _bootstrap_tokens.find(t);
        if (old_endpoint != _bootstrap_tokens.end() && (*old_endpoint).second != endpoint) {
            auto msg = sprint("Bootstrap Token collision between %s and %s (token %s", (*old_endpoint).second, endpoint, t);
            throw std::runtime_error(msg);
        }

        auto old_endpoint2 = _token_to_endpoint_map.find(t);
        if (old_endpoint2 != _token_to_endpoint_map.end() && (*old_endpoint2).second != endpoint) {
            auto msg = sprint("Bootstrap Token collision between %s and %s (token %s", (*old_endpoint2).second, endpoint, t);
            throw std::runtime_error(msg);
        }
    }

    // Unfortunately, std::remove_if does not work with std::map
    for (auto it = _bootstrap_tokens.begin(); it != _bootstrap_tokens.end();) {
        if ((*it).second == endpoint) {
            it = _bootstrap_tokens.erase(it);
        } else {
            it++;
        }
    }

    for (auto t : tokens) {
        _bootstrap_tokens[t] = endpoint;
    }
}

void token_metadata::remove_bootstrap_tokens(std::unordered_set<token> tokens) {
    if (tokens.empty()) {
        auto msg = sprint("tokens is empty in remove_bootstrap_tokens!");
        tlogger.error("{}", msg);
        throw std::runtime_error(msg);
    }
    for (auto t : tokens) {
        _bootstrap_tokens.erase(t);
    }
}

bool token_metadata::is_leaving(inet_address endpoint) {
    return _leaving_endpoints.count(endpoint);
}

void token_metadata::remove_endpoint(inet_address endpoint) {
    remove_by_value(_bootstrap_tokens, endpoint);
    remove_by_value(_token_to_endpoint_map, endpoint);
    _topology.remove_endpoint(endpoint);
    _leaving_endpoints.erase(endpoint);
    _endpoint_to_host_id_map.erase(endpoint);
    _sorted_tokens = sort_tokens();
    invalidate_cached_rings();
}

token token_metadata::get_predecessor(token t) {
    auto& tokens = sorted_tokens();
    auto it = std::lower_bound(tokens.begin(), tokens.end(), t);
    if (it == tokens.end() || *it != t) {
        auto msg = sprint("token error in get_predecessor!");
        tlogger.error("{}", msg);
        throw std::runtime_error(msg);
    }
    if (it == tokens.begin()) {
        // If the token is the first element, its preprocessor is the last element
        return tokens.back();
    } else {
        return *(--it);
    }
}

dht::token_range_vector token_metadata::get_primary_ranges_for(std::unordered_set<token> tokens) {
    dht::token_range_vector ranges;
    ranges.reserve(tokens.size() + 1); // one of the ranges will wrap
    for (auto right : tokens) {
        auto left = get_predecessor(right);
        ::compat::unwrap_into(
                wrapping_range<token>(range_bound<token>(left, false), range_bound<token>(right)),
                dht::token_comparator(),
                [&] (auto&& rng) { ranges.push_back(std::move(rng)); });
    }
    return ranges;
}

dht::token_range_vector token_metadata::get_primary_ranges_for(token right) {
    return get_primary_ranges_for(std::unordered_set<token>{right});
}

boost::icl::interval<token>::interval_type
token_metadata::range_to_interval(range<dht::token> r) {
    bool start_inclusive = false;
    bool end_inclusive = false;
    token start = dht::minimum_token();
    token end = dht::maximum_token();

    if (r.start()) {
        start = r.start()->value();
        start_inclusive = r.start()->is_inclusive();
    }

    if (r.end()) {
        end = r.end()->value();
        end_inclusive = r.end()->is_inclusive();
    }

    if (start_inclusive == false && end_inclusive == false) {
        return boost::icl::interval<token>::open(std::move(start), std::move(end));
    } else if (start_inclusive == false && end_inclusive == true) {
        return boost::icl::interval<token>::left_open(std::move(start), std::move(end));
    } else if (start_inclusive == true && end_inclusive == false) {
        return boost::icl::interval<token>::right_open(std::move(start), std::move(end));
    } else {
        return boost::icl::interval<token>::closed(std::move(start), std::move(end));
    }
}

range<dht::token>
token_metadata::interval_to_range(boost::icl::interval<token>::interval_type i) {
    bool start_inclusive;
    bool end_inclusive;
    auto bounds = i.bounds().bits();
    if (bounds == boost::icl::interval_bounds::static_open) {
        start_inclusive = false;
        end_inclusive = false;
    } else if (bounds == boost::icl::interval_bounds::static_left_open) {
        start_inclusive = false;
        end_inclusive = true;
    } else if (bounds == boost::icl::interval_bounds::static_right_open) {
        start_inclusive = true;
        end_inclusive = false;
    } else if (bounds == boost::icl::interval_bounds::static_closed) {
        start_inclusive = true;
        end_inclusive = true;
    } else {
        throw std::runtime_error("Invalid boost::icl::interval<token> bounds");
    }
    return range<dht::token>({{i.lower(), start_inclusive}}, {{i.upper(), end_inclusive}});
}

void token_metadata::set_pending_ranges(const sstring& keyspace_name,
        std::unordered_multimap<range<token>, inet_address> new_pending_ranges) {
    if (new_pending_ranges.empty()) {
        _pending_ranges.erase(keyspace_name);
        _pending_ranges_map.erase(keyspace_name);
        _pending_ranges_interval_map.erase(keyspace_name);
        return;
    }
    std::unordered_map<range<token>, std::unordered_set<inet_address>> map;
    for (const auto& x : new_pending_ranges) {
        map[x.first].emplace(x.second);
    }

    // construct a interval map to speed up the search
    _pending_ranges_interval_map[keyspace_name] = {};
    for (const auto& m : map) {
        _pending_ranges_interval_map[keyspace_name] +=
                std::make_pair(range_to_interval(m.first), m.second);
    }
    _pending_ranges[keyspace_name] = std::move(new_pending_ranges);
    _pending_ranges_map[keyspace_name] = std::move(map);
}

std::unordered_multimap<range<token>, inet_address>&
token_metadata::get_pending_ranges_mm(sstring keyspace_name) {
    return _pending_ranges[keyspace_name];
}

const std::unordered_map<range<token>, std::unordered_set<inet_address>>&
token_metadata::get_pending_ranges(sstring keyspace_name) {
    return _pending_ranges_map[keyspace_name];
}

std::vector<range<token>>
token_metadata::get_pending_ranges(sstring keyspace_name, inet_address endpoint) {
    std::vector<range<token>> ret;
    for (auto x : get_pending_ranges_mm(keyspace_name)) {
        auto& range_token = x.first;
        auto& ep = x.second;
        if (ep == endpoint) {
            ret.push_back(range_token);
        }
    }
    return ret;
}

future<> token_metadata::calculate_pending_ranges_for_leaving(
        abstract_replication_strategy& strategy,
        lw_shared_ptr<std::unordered_multimap<range<token>, inet_address>> new_pending_ranges,
        lw_shared_ptr<token_metadata> all_left_metadata) {
    std::unordered_multimap<inet_address, dht::token_range> address_ranges = strategy.get_address_ranges(*this);
    // get all ranges that will be affected by leaving nodes
    std::unordered_set<range<token>> affected_ranges;
    for (auto endpoint : _leaving_endpoints) {
        auto r = address_ranges.equal_range(endpoint);
        for (auto x = r.first; x != r.second; x++) {
            affected_ranges.emplace(x->second);
        }
    }
    // for each of those ranges, find what new nodes will be responsible for the range when
    // all leaving nodes are gone.
    auto metadata = clone_only_token_map(); // don't do this in the loop! #7758
    auto affected_ranges_size = affected_ranges.size();
    tlogger.debug("In calculate_pending_ranges: affected_ranges.size={} stars", affected_ranges_size);
    return do_with(std::move(metadata), std::move(affected_ranges), [&strategy, new_pending_ranges, all_left_metadata, affected_ranges_size] (auto& metadata, auto& affected_ranges) {
        return do_for_each(affected_ranges, [&metadata, &strategy, new_pending_ranges, all_left_metadata] (auto& r) {
            auto t = r.end() ? r.end()->value() : dht::maximum_token();
            auto current_endpoints = strategy.calculate_natural_endpoints(t, metadata);
            auto new_endpoints = strategy.calculate_natural_endpoints(t, *all_left_metadata);
            std::vector<inet_address> diff;
            std::sort(current_endpoints.begin(), current_endpoints.end());
            std::sort(new_endpoints.begin(), new_endpoints.end());
            std::set_difference(new_endpoints.begin(), new_endpoints.end(),
                current_endpoints.begin(), current_endpoints.end(), std::back_inserter(diff));
            for (auto& ep : diff) {
                new_pending_ranges->emplace(r, ep);
            }
        }).finally([affected_ranges_size] {
            tlogger.debug("In calculate_pending_ranges: affected_ranges.size={} ends", affected_ranges_size);
        });
    });
}

void token_metadata::calculate_pending_ranges_for_bootstrap(
        abstract_replication_strategy& strategy,
        lw_shared_ptr<std::unordered_multimap<range<token>, inet_address>> new_pending_ranges,
        lw_shared_ptr<token_metadata> all_left_metadata) {
    // For each of the bootstrapping nodes, simply add and remove them one by one to
    // allLeftMetadata and check in between what their ranges would be.
    std::unordered_multimap<inet_address, token> bootstrap_addresses;
    for (auto& x : _bootstrap_tokens) {
        bootstrap_addresses.emplace(x.second, x.first);
    }

    // TODO: share code with unordered_multimap_to_unordered_map
    std::unordered_map<inet_address, std::unordered_set<token>> tmp;
    for (auto& x : bootstrap_addresses) {
        auto& addr = x.first;
        auto& t = x.second;
        tmp[addr].insert(t);
    }
    for (auto& x : tmp) {
        auto& endpoint = x.first;
        auto& tokens = x.second;
        all_left_metadata->update_normal_tokens(tokens, endpoint);
        for (auto& x : strategy.get_address_ranges(*all_left_metadata)) {
            if (x.first == endpoint) {
                new_pending_ranges->emplace(x.second, endpoint);
            }
        }
        all_left_metadata->remove_endpoint(endpoint);
    }
}

future<> token_metadata::calculate_pending_ranges(abstract_replication_strategy& strategy, const sstring& keyspace_name) {
    auto new_pending_ranges = make_lw_shared<std::unordered_multimap<range<token>, inet_address>>();

    if (_bootstrap_tokens.empty() && _leaving_endpoints.empty()) {
        tlogger.debug("No bootstrapping, leaving nodes -> empty pending ranges for {}", keyspace_name);
        set_pending_ranges(keyspace_name, std::move(*new_pending_ranges));
        return make_ready_future<>();
    }

    // Copy of metadata reflecting the situation after all leave operations are finished.
    auto all_left_metadata = make_lw_shared<token_metadata>(clone_after_all_left());

    return calculate_pending_ranges_for_leaving(strategy, new_pending_ranges, all_left_metadata).then([this, keyspace_name, &strategy, new_pending_ranges, all_left_metadata] {
        // At this stage newPendingRanges has been updated according to leave operations. We can
        // now continue the calculation by checking bootstrapping nodes.
        calculate_pending_ranges_for_bootstrap(strategy, new_pending_ranges, all_left_metadata);

        // At this stage newPendingRanges has been updated according to leaving and bootstrapping nodes.
        set_pending_ranges(keyspace_name, std::move(*new_pending_ranges));

        if (tlogger.is_enabled(logging::log_level::debug)) {
            tlogger.debug("Pending ranges: {}", (_pending_ranges.empty() ? "<empty>" : print_pending_ranges()));
        }
        return make_ready_future<>();
    });

}

sstring token_metadata::print_pending_ranges() {
    std::stringstream ss;

    for (auto& x : _pending_ranges) {
        auto& keyspace_name = x.first;
        ss << "\nkeyspace_name = " << keyspace_name << " {\n";
        for (auto& m : x.second) {
            ss << m.second << " : " << m.first << "\n";
        }
        ss << "}\n";
    }

    return sstring(ss.str());
}

void token_metadata::add_leaving_endpoint(inet_address endpoint) {
     _leaving_endpoints.emplace(endpoint);
}

token_metadata token_metadata::clone_after_all_settled() {
    token_metadata metadata = clone_only_token_map();

    for (auto endpoint : _leaving_endpoints) {
        metadata.remove_endpoint(endpoint);
    }

    return metadata;
}

std::vector<gms::inet_address> token_metadata::pending_endpoints_for(const token& token, const sstring& keyspace_name) {
    // Fast path 0: no pending ranges at all
    if (_pending_ranges_interval_map.empty()) {
        return {};
    }

    // Fast path 1: no pending ranges for this keyspace_name
    if (_pending_ranges_interval_map[keyspace_name].empty()) {
        return {};
    }

    // Slow path: lookup pending ranges
    std::vector<gms::inet_address> endpoints;
    auto interval = range_to_interval(range<dht::token>(token));
    auto it = _pending_ranges_interval_map[keyspace_name].find(interval);
    if (it != _pending_ranges_interval_map[keyspace_name].end()) {
        // interval_map does not work with std::vector, convert to std::vector of ips
        endpoints = std::vector<gms::inet_address>(it->second.begin(), it->second.end());
    }
    return endpoints;
}

std::map<token, inet_address> token_metadata::get_normal_and_bootstrapping_token_to_endpoint_map() {
    std::map<token, inet_address> ret(_token_to_endpoint_map.begin(), _token_to_endpoint_map.end());
    ret.insert(_bootstrap_tokens.begin(), _bootstrap_tokens.end());
    return ret;
}

std::multimap<inet_address, token> token_metadata::get_endpoint_to_token_map_for_reading() {
    std::multimap<inet_address, token> cloned;
    for (const auto& x : _token_to_endpoint_map) {
        cloned.emplace(x.second, x.first);
    }
    return cloned;
}


/////////////////// class topology /////////////////////////////////////////////
inline void topology::clear() {
    _dc_endpoints.clear();
    _dc_racks.clear();
    _current_locations.clear();
}

topology::topology(const topology& other) {
    _dc_endpoints = other._dc_endpoints;
    _dc_racks = other._dc_racks;
    _current_locations = other._current_locations;
}

void topology::add_endpoint(const inet_address& ep)
{
    auto& snitch = i_endpoint_snitch::get_local_snitch_ptr();
    sstring dc = snitch->get_datacenter(ep);
    sstring rack = snitch->get_rack(ep);
    auto current = _current_locations.find(ep);

    if (current != _current_locations.end()) {
        if (current->second.dc == dc && current->second.rack == rack) {
            return;
        }

        _dc_racks[current->second.dc][current->second.rack].erase(ep);
        _dc_endpoints[current->second.dc].erase(ep);
    }

    _dc_endpoints[dc].insert(ep);
    _dc_racks[dc][rack].insert(ep);
    _current_locations[ep] = {dc, rack};
}

void topology::update_endpoint(inet_address ep) {
    if (!_current_locations.count(ep) || !locator::i_endpoint_snitch::snitch_instance().local_is_initialized()) {
        return;
    }

    add_endpoint(ep);
}

void topology::remove_endpoint(inet_address ep)
{
    auto cur_dc_rack = _current_locations.find(ep);

    if (cur_dc_rack == _current_locations.end()) {
        return;
    }

    _dc_endpoints[cur_dc_rack->second.dc].erase(ep);
    _dc_racks[cur_dc_rack->second.dc][cur_dc_rack->second.rack].erase(ep);
    _current_locations.erase(cur_dc_rack);
}

bool topology::has_endpoint(inet_address ep) const
{
    auto i = _current_locations.find(ep);
    return i != _current_locations.end();
}

/////////////////// class topology end /////////////////////////////////////////
} // namespace locator
