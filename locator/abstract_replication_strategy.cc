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

#include "locator/abstract_replication_strategy.hh"
#include "utils/class_registrator.hh"
#include "exceptions/exceptions.hh"
#include <boost/range/algorithm/remove_if.hpp>
#include <seastar/core/coroutine.hh>

namespace locator {

logging::logger abstract_replication_strategy::logger("replication_strategy");

abstract_replication_strategy::abstract_replication_strategy(
    const sstring& ks_name,
    const shared_token_metadata& stm,
    snitch_ptr& snitch,
    const std::map<sstring, sstring>& config_options,
    replication_strategy_type my_type)
        : _ks_name(ks_name)
        , _config_options(config_options)
        , _shared_token_metadata(stm)
        , _snitch(snitch)
        , _my_type(my_type) {}

std::unique_ptr<abstract_replication_strategy> abstract_replication_strategy::create_replication_strategy(const sstring& ks_name, const sstring& strategy_name, const shared_token_metadata& stm, const std::map<sstring, sstring>& config_options) {
    assert(locator::i_endpoint_snitch::get_local_snitch_ptr());
    try {
        return create_object<abstract_replication_strategy,
                             const sstring&,
                             const shared_token_metadata&,
                             snitch_ptr&,
                             const std::map<sstring, sstring>&>
            (strategy_name, ks_name, stm,
             locator::i_endpoint_snitch::get_local_snitch_ptr(), config_options);
    } catch (const no_such_class& e) {
        throw exceptions::configuration_exception(e.what());
    }
}

void abstract_replication_strategy::validate_replication_strategy(const sstring& ks_name,
                                                                  const sstring& strategy_name,
                                                                  const shared_token_metadata& stm,
                                                                  const std::map<sstring, sstring>& config_options)
{
    auto strategy = create_replication_strategy(ks_name, strategy_name, stm, config_options);
    strategy->validate_options();
    auto expected = strategy->recognized_options();
    if (expected) {
        for (auto&& item : config_options) {
            sstring key = item.first;
            if (!expected->contains(key)) {
                 throw exceptions::configuration_exception(format("Unrecognized strategy option {{{}}} passed to {} for keyspace {}", key, strategy_name, ks_name));
            }
        }
    }
}

inet_address_vector_replica_set abstract_replication_strategy::get_natural_endpoints(const token& search_token, can_yield can_yield) {
    return do_get_natural_endpoints(search_token, *_shared_token_metadata.get(), can_yield);
}

inet_address_vector_replica_set abstract_replication_strategy::do_get_natural_endpoints(const token& search_token, const token_metadata& tm, can_yield can_yield) {
    const token& key_token = tm.first_token(search_token);
    auto& cached_endpoints = get_cached_endpoints(tm);
    auto res = cached_endpoints.find(key_token);

    if (res == cached_endpoints.end()) {
        auto endpoints = calculate_natural_endpoints(search_token, tm, can_yield);
        cached_endpoints.emplace(key_token, endpoints);

        return endpoints;
    }

    ++_cache_hits_count;
    return res->second;
}

inet_address_vector_replica_set abstract_replication_strategy::get_natural_endpoints_without_node_being_replaced(const token& search_token, can_yield can_yield) {
    token_metadata_ptr tmptr = _shared_token_metadata.get();
    inet_address_vector_replica_set natural_endpoints = do_get_natural_endpoints(search_token, *tmptr, can_yield);
    if (tmptr->is_any_node_being_replaced() &&
        allow_remove_node_being_replaced_from_natural_endpoints()) {
        // When a new node is started to replace an existing dead node, we want
        // to make the replacing node take writes but do not count it for
        // consistency level, because the replacing node can die and go away.
        // To do this, we filter out the existing node being replaced from
        // natural_endpoints and make the replacing node in the pending_endpoints.
        //
        // However, we can only apply the filter for the replication strategy
        // that allows it. For example, we can not apply the filter for
        // LocalStrategy because LocalStrategy always returns the node itself
        // as the natural_endpoints and the node will not appear in the
        // pending_endpoints.
        auto it = boost::range::remove_if(natural_endpoints, [tmptr = std::move(tmptr)] (gms::inet_address& p) {
            return tmptr->is_being_replaced(p);
        });
        natural_endpoints.erase(it, natural_endpoints.end());
    }
    return natural_endpoints;
}

void abstract_replication_strategy::validate_replication_factor(sstring rf) const
{
    if (rf.empty() || std::any_of(rf.begin(), rf.end(), [] (char c) {return !isdigit(c);})) {
        throw exceptions::configuration_exception(
                format("Replication factor must be numeric and non-negative, found '{}'", rf));
    }
    try {
        std::stol(rf);
    } catch (...) {
        throw exceptions::configuration_exception(
            sstring("Replication factor must be numeric; found ") + rf);
    }
}

inline std::unordered_map<token, inet_address_vector_replica_set>&
abstract_replication_strategy::get_cached_endpoints(const token_metadata& tm) {
    auto ring_version = tm.get_ring_version();
    if (_last_invalidated_ring_version != ring_version) {
        _cached_endpoints.clear();
        _last_invalidated_ring_version = ring_version;
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
                std::nullopt);
        // Insert in front to maintain sorded order
        ret.emplace(
                ret.begin(),
                std::nullopt,
                dht::token_range::bound(tok, true));
    }
}

// Caller must ensure that token_metadata will not change throughout the call if can_yield::yes.
dht::token_range_vector
abstract_replication_strategy::do_get_ranges(inet_address ep, const token_metadata_ptr tmptr, can_yield can_yield) const {
    dht::token_range_vector ret;
    const auto& tm = *tmptr;
    auto prev_tok = tm.sorted_tokens().back();
    for (auto tok : tm.sorted_tokens()) {
        for (inet_address a : calculate_natural_endpoints(tok, tm, can_yield)) {
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
abstract_replication_strategy::get_primary_ranges(inet_address ep, can_yield can_yield) {
    dht::token_range_vector ret;
    token_metadata_ptr tmptr = _shared_token_metadata.get();
    auto prev_tok = tmptr->sorted_tokens().back();
    for (auto tok : tmptr->sorted_tokens()) {
        auto&& eps = calculate_natural_endpoints(tok, *tmptr, can_yield);
        if (eps.size() > 0 && eps[0] == ep) {
            insert_token_range_to_sorted_container_while_unwrapping(prev_tok, tok, ret);
        }
        prev_tok = tok;
    }
    return ret;
}

dht::token_range_vector
abstract_replication_strategy::get_primary_ranges_within_dc(inet_address ep, can_yield can_yield) {
    dht::token_range_vector ret;
    sstring local_dc = _snitch->get_datacenter(ep);
    token_metadata_ptr tmptr = _shared_token_metadata.get();
    std::unordered_set<inet_address> local_dc_nodes = tmptr->get_topology().get_datacenter_endpoints().at(local_dc);
    auto prev_tok = tmptr->sorted_tokens().back();
    for (auto tok : tmptr->sorted_tokens()) {
        auto&& eps = calculate_natural_endpoints(tok, *tmptr, can_yield);
        // Unlike get_primary_ranges() which checks if ep is the first
        // owner of this range, here we check if ep is the first just
        // among nodes which belong to the local dc of ep.
        for (auto& e : eps) {
            if (local_dc_nodes.contains(e)) {
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
abstract_replication_strategy::get_address_ranges(const token_metadata& tm, can_yield can_yield) const {
    std::unordered_multimap<inet_address, dht::token_range> ret;
    for (auto& t : tm.sorted_tokens()) {
        dht::token_range_vector r = tm.get_primary_ranges_for(t);
        auto eps = calculate_natural_endpoints(t, tm, can_yield);
        logger.debug("token={}, primary_range={}, address={}", t, r, eps);
        for (auto ep : eps) {
            for (auto&& rng : r) {
                ret.emplace(ep, rng);
            }
        }

        if (can_yield) {
            seastar::thread::maybe_yield();
        }
    }
    return ret;
}

std::unordered_multimap<inet_address, dht::token_range>
abstract_replication_strategy::get_address_ranges(const token_metadata& tm, inet_address endpoint, can_yield can_yield) const {
    std::unordered_multimap<inet_address, dht::token_range> ret;
    for (auto& t : tm.sorted_tokens()) {
        auto eps = calculate_natural_endpoints(t, tm, can_yield);
        bool found = false;
        for (auto ep : eps) {
            if (ep != endpoint) {
                continue;
            }
            dht::token_range_vector r = tm.get_primary_ranges_for(t);
            logger.debug("token={} primary_range={} endpoint={}", t, r, endpoint);
            for (auto&& rng : r) {
                ret.emplace(ep, rng);
            }
            found = true;
            break;
        }
        if (!found) {
            logger.debug("token={} natural_endpoints={}: endpoint={} not found", t, eps, endpoint);
        }

        if (can_yield) {
            seastar::thread::maybe_yield();
        }
    }
    return ret;
}

std::unordered_map<dht::token_range, inet_address_vector_replica_set>
abstract_replication_strategy::get_range_addresses(const token_metadata& tm, can_yield can_yield) const {
    std::unordered_map<dht::token_range, inet_address_vector_replica_set> ret;
    for (auto& t : tm.sorted_tokens()) {
        dht::token_range_vector ranges = tm.get_primary_ranges_for(t);
        auto eps = calculate_natural_endpoints(t, tm, can_yield);
        for (auto& r : ranges) {
            ret.emplace(r, eps);
        }

        if (can_yield) {
            seastar::thread::maybe_yield();
        }
    }
    return ret;
}

dht::token_range_vector
abstract_replication_strategy::get_pending_address_ranges(const token_metadata_ptr tmptr, token pending_token, inet_address pending_address, can_yield can_yield) const {
    return get_pending_address_ranges(std::move(tmptr), std::unordered_set<token>{pending_token}, pending_address, can_yield);
}

dht::token_range_vector
abstract_replication_strategy::get_pending_address_ranges(const token_metadata_ptr tmptr, std::unordered_set<token> pending_tokens, inet_address pending_address, can_yield can_yield) const {
    dht::token_range_vector ret;
    token_metadata temp;
    if (can_yield) {
        temp = tmptr->clone_only_token_map().get0();
        temp.update_normal_tokens(pending_tokens, pending_address).get();
    } else {
        temp = tmptr->clone_only_token_map_sync();
        temp.update_normal_tokens_sync(pending_tokens, pending_address);
    }
    for (auto& x : get_address_ranges(temp, pending_address, can_yield)) {
            ret.push_back(x.second);
    }
    if (can_yield) {
        temp.clear_gently().get();
    }
    return ret;
}

} // namespace locator
