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

#include "locator/abstract_replication_strategy.hh"
#include "utils/class_registrator.hh"
#include "exceptions/exceptions.hh"
#include "stdx.hh"

namespace locator {

logging::logger abstract_replication_strategy::logger("replication_strategy");

abstract_replication_strategy::abstract_replication_strategy(
    const sstring& ks_name,
    token_metadata& token_metadata,
    snitch_ptr& snitch,
    const std::map<sstring, sstring>& config_options,
    replication_strategy_type my_type)
        : _ks_name(ks_name)
        , _config_options(config_options)
        , _token_metadata(token_metadata)
        , _snitch(snitch)
        , _my_type(my_type) {}

std::unique_ptr<abstract_replication_strategy> abstract_replication_strategy::create_replication_strategy(const sstring& ks_name, const sstring& strategy_name, token_metadata& tk_metadata, const std::map<sstring, sstring>& config_options) {
    assert(locator::i_endpoint_snitch::get_local_snitch_ptr());
    try {
        return create_object<abstract_replication_strategy,
                             const sstring&,
                             token_metadata&,
                             snitch_ptr&,
                             const std::map<sstring, sstring>&>
            (strategy_name, ks_name, tk_metadata,
             locator::i_endpoint_snitch::get_local_snitch_ptr(), config_options);
    } catch (const no_such_class& e) {
        throw exceptions::configuration_exception(e.what());
    }
}

void abstract_replication_strategy::validate_replication_strategy(const sstring& ks_name,
                                                                  const sstring& strategy_name,
                                                                  token_metadata& token_metadata,
                                                                  const std::map<sstring, sstring>& config_options)
{
    auto strategy = create_replication_strategy(ks_name, strategy_name, token_metadata, config_options);
    strategy->validate_options();
    auto expected = strategy->recognized_options();
    if (expected) {
        for (auto&& item : config_options) {
            sstring key = item.first;
            if (!expected->count(key)) {
                 throw exceptions::configuration_exception(sprint("Unrecognized strategy option {%s} passed to %s for keyspace %s", key, strategy_name, ks_name));
            }
        }
    }
}

std::vector<inet_address> abstract_replication_strategy::get_natural_endpoints(const token& search_token) {
    const token& key_token = _token_metadata.first_token(search_token);
    auto& cached_endpoints = get_cached_endpoints();
    auto res = cached_endpoints.find(key_token);

    if (res == cached_endpoints.end()) {
        auto endpoints = calculate_natural_endpoints(search_token, _token_metadata);
        cached_endpoints.emplace(key_token, endpoints);

        return std::move(endpoints);
    }

    ++_cache_hits_count;
    return res->second;
}

void abstract_replication_strategy::validate_replication_factor(sstring rf) const
{
    try {
        if (std::stol(rf) < 0) {
            throw exceptions::configuration_exception(
               sstring("Replication factor must be non-negative; found ") + rf);
        }
    } catch (...) {
        throw exceptions::configuration_exception(
            sstring("Replication factor must be numeric; found ") + rf);
    }
}

inline std::unordered_map<token, std::vector<inet_address>>&
abstract_replication_strategy::get_cached_endpoints() {
    if (_last_invalidated_ring_version != _token_metadata.get_ring_version()) {
        _cached_endpoints.clear();
        _last_invalidated_ring_version = _token_metadata.get_ring_version();
    }

    return _cached_endpoints;
}

static
void
insert_token_range_to_sorted_container_while_unwrapping(
        const dht::token& prev_tok,
        const dht::token& tok,
        dht::token_range_vector& ret) {
    if (prev_tok < tok) {
        auto pos = ret.end();
        if (!ret.empty() && !std::prev(pos)->end()) {
            // We inserted a wrapped range (a, b] previously as
            // (-inf, b], (a, +inf). So now we insert in the next-to-last
            // position to keep the last range (a, +inf) at the end.
            pos = std::prev(pos);
        }
        ret.insert(pos,
                dht::token_range{
                        dht::token_range::bound(prev_tok, false),
                        dht::token_range::bound(tok, true)});
    } else {
        ret.emplace_back(
                dht::token_range::bound(prev_tok, false),
                stdx::nullopt);
        // Insert in front to maintain sorded order
        ret.emplace(
                ret.begin(),
                stdx::nullopt,
                dht::token_range::bound(tok, true));
    }
}

dht::token_range_vector
abstract_replication_strategy::get_ranges(inet_address ep) const {
    dht::token_range_vector ret;
    auto prev_tok = _token_metadata.sorted_tokens().back();
    for (auto tok : _token_metadata.sorted_tokens()) {
        for (inet_address a : calculate_natural_endpoints(tok, _token_metadata)) {
            if (a == ep) {
                insert_token_range_to_sorted_container_while_unwrapping(prev_tok, tok, ret);
                break;
            }
        }
        prev_tok = tok;
    }
    return ret;
}

dht::token_range_vector
abstract_replication_strategy::get_primary_ranges(inet_address ep) {
    dht::token_range_vector ret;
    auto prev_tok = _token_metadata.sorted_tokens().back();
    for (auto tok : _token_metadata.sorted_tokens()) {
        auto&& eps = calculate_natural_endpoints(tok, _token_metadata);
        if (eps.size() > 0 && eps[0] == ep) {
            insert_token_range_to_sorted_container_while_unwrapping(prev_tok, tok, ret);
        }
        prev_tok = tok;
    }
    return ret;
}

dht::token_range_vector
abstract_replication_strategy::get_primary_ranges_within_dc(inet_address ep) {
    dht::token_range_vector ret;
    sstring local_dc = _snitch->get_datacenter(ep);
    std::unordered_set<inet_address> local_dc_nodes = _token_metadata.get_topology().get_datacenter_endpoints().at(local_dc);
    auto prev_tok = _token_metadata.sorted_tokens().back();
    for (auto tok : _token_metadata.sorted_tokens()) {
        auto&& eps = calculate_natural_endpoints(tok, _token_metadata);
        // Unlike get_primary_ranges() which checks if ep is the first
        // owner of this range, here we check if ep is the first just
        // among nodes which belong to the local dc of ep.
        for (auto& e : eps) {
            if (local_dc_nodes.count(e)) {
                if (e == ep) {
                    insert_token_range_to_sorted_container_while_unwrapping(prev_tok, tok, ret);
                }
                break;
            }
        }
        prev_tok = tok;
    }
    return ret;
}

std::unordered_multimap<inet_address, dht::token_range>
abstract_replication_strategy::get_address_ranges(token_metadata& tm) const {
    std::unordered_multimap<inet_address, dht::token_range> ret;
    for (auto& t : tm.sorted_tokens()) {
        dht::token_range_vector r = tm.get_primary_ranges_for(t);
        auto eps = calculate_natural_endpoints(t, tm);
        logger.debug("token={}, primary_range={}, address={}", t, r, eps);
        for (auto ep : eps) {
            for (auto&& rng : r) {
                ret.emplace(ep, rng);
            }
        }
    }
    return ret;
}

std::unordered_map<dht::token_range, std::vector<inet_address>>
abstract_replication_strategy::get_range_addresses(token_metadata& tm) const {
    std::unordered_map<dht::token_range, std::vector<inet_address>> ret;
    for (auto& t : tm.sorted_tokens()) {
        dht::token_range_vector ranges = tm.get_primary_ranges_for(t);
        auto eps = calculate_natural_endpoints(t, tm);
        for (auto& r : ranges) {
            ret.emplace(r, eps);
        }
    }
    return ret;
}

dht::token_range_vector
abstract_replication_strategy::get_pending_address_ranges(token_metadata& tm, token pending_token, inet_address pending_address) {
    return get_pending_address_ranges(tm, std::unordered_set<token>{pending_token}, pending_address);
}

dht::token_range_vector
abstract_replication_strategy::get_pending_address_ranges(token_metadata& tm, std::unordered_set<token> pending_tokens, inet_address pending_address) {
    dht::token_range_vector ret;
    auto temp = tm.clone_only_token_map();
    temp.update_normal_tokens(pending_tokens, pending_address);
    for (auto& x : get_address_ranges(temp)) {
        if (x.first == pending_address) {
            ret.push_back(x.second);
        }
    }
    return ret;
}

} // namespace locator
