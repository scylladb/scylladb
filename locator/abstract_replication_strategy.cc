/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "locator/abstract_replication_strategy.hh"
#include "locator/tablet_replication_strategy.hh"
#include "utils/class_registrator.hh"
#include "exceptions/exceptions.hh"
#include <boost/range/algorithm/remove_if.hpp>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include "replica/database.hh"
#include "utils/stall_free.hh"

namespace locator {

logging::logger rslogger("replication_strategy");

abstract_replication_strategy::abstract_replication_strategy(
    const replication_strategy_config_options& config_options,
    replication_strategy_type my_type)
        : _config_options(config_options)
        , _my_type(my_type) {}

abstract_replication_strategy::ptr_type abstract_replication_strategy::create_replication_strategy(const sstring& strategy_name, const replication_strategy_config_options& config_options) {
    try {
        return create_object<abstract_replication_strategy,
                             const replication_strategy_config_options&>
            (strategy_name, config_options);
    } catch (const no_such_class& e) {
        throw exceptions::configuration_exception(e.what());
    }
}

void abstract_replication_strategy::validate_replication_strategy(const sstring& ks_name,
                                                                  const sstring& strategy_name,
                                                                  const replication_strategy_config_options& config_options,
                                                                  const gms::feature_service& fs,
                                                                  const topology& topology)
{
    auto strategy = create_replication_strategy(strategy_name, config_options);
    strategy->validate_options(fs);
    auto expected = strategy->recognized_options(topology);
    if (expected) {
        for (auto&& item : config_options) {
            sstring key = item.first;
            if (!expected->contains(key)) {
                 throw exceptions::configuration_exception(format("Unrecognized strategy option {{{}}} passed to {} for keyspace {}", key, strategy_name, ks_name));
            }
        }
    }
}

using strategy_class_registry = class_registry<
    locator::abstract_replication_strategy,
    const locator::replication_strategy_config_options&>;

sstring abstract_replication_strategy::to_qualified_class_name(std::string_view strategy_class_name) {
    return strategy_class_registry::to_qualified_class_name(strategy_class_name);
}

inet_address_vector_replica_set abstract_replication_strategy::get_natural_endpoints(const token& search_token, const vnode_effective_replication_map& erm) const {
    const token& key_token = erm.get_token_metadata_ptr()->first_token(search_token);
    auto res = erm.get_replication_map().find(key_token);
    return res->second;
}

stop_iteration abstract_replication_strategy::for_each_natural_endpoint_until(const token& search_token, const vnode_effective_replication_map& erm, const noncopyable_function<stop_iteration(const inet_address&)>& func) const {
    const token& key_token = erm.get_token_metadata_ptr()->first_token(search_token);
    auto res = erm.get_replication_map().find(key_token);
    for (const auto& ep : res->second) {
        if (func(ep) == stop_iteration::yes) {
            return stop_iteration::yes;
        }
    }
    return stop_iteration::no;
}

inet_address_vector_replica_set vnode_effective_replication_map::get_natural_endpoints_without_node_being_replaced(const token& search_token) const {
    inet_address_vector_replica_set natural_endpoints = get_natural_endpoints(search_token);
    maybe_remove_node_being_replaced(*_tmptr, *_rs, natural_endpoints);
    return natural_endpoints;
}

void maybe_remove_node_being_replaced(const token_metadata& tm,
                                      const abstract_replication_strategy& rs,
                                      inet_address_vector_replica_set& natural_endpoints) {
    if (tm.is_any_node_being_replaced() &&
        rs.allow_remove_node_being_replaced_from_natural_endpoints()) {
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
        auto it = boost::range::remove_if(natural_endpoints, [&] (gms::inet_address& p) {
            return tm.is_being_replaced(p);
        });
        natural_endpoints.erase(it, natural_endpoints.end());
    }
}

inet_address_vector_topology_change vnode_effective_replication_map::get_pending_endpoints(const token& search_token, const sstring& ks_name) const {
    return _tmptr->pending_endpoints_for(search_token, ks_name);
}

std::unique_ptr<token_range_splitter> vnode_effective_replication_map::make_splitter() const {
    return locator::make_splitter(_tmptr);
}

const per_table_replication_strategy* abstract_replication_strategy::maybe_as_per_table() const {
    if (!_per_table) {
        return nullptr;
    }
    return dynamic_cast<const per_table_replication_strategy*>(this);
}

const tablet_aware_replication_strategy* abstract_replication_strategy::maybe_as_tablet_aware() const {
    if (!_uses_tablets) {
        return nullptr;
    }
    return dynamic_cast<const tablet_aware_replication_strategy*>(this);
}

void abstract_replication_strategy::validate_replication_factor(sstring rf)
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

dht::token_range_vector
vnode_effective_replication_map::do_get_ranges(noncopyable_function<stop_iteration(bool&, const inet_address&)> consider_range_for_endpoint) const {
    dht::token_range_vector ret;
    const auto& tm = *_tmptr;
    const auto& sorted_tokens = tm.sorted_tokens();
    if (sorted_tokens.empty()) {
        on_internal_error(rslogger, "Token metadata is empty");
    }
    auto prev_tok = sorted_tokens.back();
    for (const auto& tok : sorted_tokens) {
        bool add_range = false;
        for_each_natural_endpoint_until(tok, [&] (const inet_address& ep) {
            return consider_range_for_endpoint(add_range, ep);
        });
        if (add_range) {
            insert_token_range_to_sorted_container_while_unwrapping(prev_tok, tok, ret);
        }
        prev_tok = tok;
    }
    return ret;
}

dht::token_range_vector
vnode_effective_replication_map::get_ranges(inet_address ep) const {
    // The callback function below is called for each endpoint
    // in each token natural endpoints.
    // Add the range if `ep` is found in the token's natural endpoints
    return do_get_ranges([ep] (bool& add_range, const inet_address& e) {
        if ((add_range = (e == ep))) {
            // stop iteration a match is found
            return stop_iteration::yes;
        }
        return stop_iteration::no;
    });
}

// Caller must ensure that token_metadata will not change throughout the call.
future<dht::token_range_vector>
abstract_replication_strategy::get_ranges(inet_address ep, token_metadata_ptr tmptr) const {
    co_return co_await get_ranges(ep, *tmptr);
}

// Caller must ensure that token_metadata will not change throughout the call.
future<dht::token_range_vector>
abstract_replication_strategy::get_ranges(inet_address ep, const token_metadata& tm) const {
    dht::token_range_vector ret;
    if (!tm.is_normal_token_owner(ep)) {
        co_return ret;
    }
    const auto& sorted_tokens = tm.sorted_tokens();
    if (sorted_tokens.empty()) {
        on_internal_error(rslogger, "Token metadata is empty");
    }
    auto prev_tok = sorted_tokens.back();
    for (auto tok : sorted_tokens) {
        bool should_add = false;
        if (get_type() == replication_strategy_type::everywhere_topology) {
            // everywhere_topology deserves this special fast path.
            // Using the common path would make the function quadratic in the number of endpoints.
            should_add = true;
        } else {
            auto eps = co_await calculate_natural_endpoints(tok, tm);
            should_add = eps.contains(ep);
        }
        if (should_add) {
            insert_token_range_to_sorted_container_while_unwrapping(prev_tok, tok, ret);
        }
        prev_tok = tok;
    }
    co_return ret;
}

dht::token_range_vector
vnode_effective_replication_map::get_primary_ranges(inet_address ep) const {
    // The callback function below is called for each endpoint
    // in each token natural endpoints.
    // Add the range if `ep` is the primary replica in the token's natural endpoints.
    // The primary replica is first in the natural endpoints list.
    return do_get_ranges([ep] (bool& add_range, const inet_address& e) {
        add_range = (e == ep);
        // stop the iteration once the first node was considered.
        return stop_iteration::yes;
    });
}

dht::token_range_vector
vnode_effective_replication_map::get_primary_ranges_within_dc(inet_address ep) const {
    const topology& topo = _tmptr->get_topology();
    sstring local_dc = topo.get_datacenter(ep);
    std::unordered_set<inet_address> local_dc_nodes = topo.get_datacenter_endpoints().at(local_dc);
    // The callback function below is called for each endpoint
    // in each token natural endpoints.
    // Add the range if `ep` is the datacenter primary replica in the token's natural endpoints.
    // The primary replica in each datacenter is determined by the natural endpoints list order.
    return do_get_ranges([ep, local_dc_nodes = std::move(local_dc_nodes)] (bool& add_range, const inet_address& e) {
        // Unlike get_primary_ranges() which checks if ep is the first
        // owner of this range, here we check if ep is the first just
        // among nodes which belong to the local dc of ep.
        if (!local_dc_nodes.contains(e)) {
            return stop_iteration::no;
        }
        add_range = (e == ep);
        // stop the iteration once the first node contained the local datacenter was considered.
        return stop_iteration::yes;
    });
}

future<std::unordered_map<dht::token_range, inet_address_vector_replica_set>>
vnode_effective_replication_map::get_range_addresses() const {
    const token_metadata& tm = *_tmptr;
    std::unordered_map<dht::token_range, inet_address_vector_replica_set> ret;
    for (const auto& [t, eps] : _replication_map) {
        dht::token_range_vector ranges = tm.get_primary_ranges_for(t);
        for (auto& r : ranges) {
            ret.emplace(r, eps);
        }
        co_await coroutine::maybe_yield();
    }
    co_return ret;
}

future<std::unordered_map<dht::token_range, inet_address_vector_replica_set>>
abstract_replication_strategy::get_range_addresses(const token_metadata& tm) const {
    std::unordered_map<dht::token_range, inet_address_vector_replica_set> ret;
    for (auto& t : tm.sorted_tokens()) {
        dht::token_range_vector ranges = tm.get_primary_ranges_for(t);
        auto eps = co_await calculate_natural_endpoints(t, tm);
        for (auto& r : ranges) {
            ret.emplace(r, eps.get_vector());
        }
    }
    co_return ret;
}

future<dht::token_range_vector>
abstract_replication_strategy::get_pending_address_ranges(const token_metadata_ptr tmptr, std::unordered_set<token> pending_tokens, inet_address pending_address, locator::endpoint_dc_rack dr) const {
    dht::token_range_vector ret;
    token_metadata temp = co_await tmptr->clone_only_token_map();
    temp.update_topology(pending_address, std::move(dr));
    co_await temp.update_normal_tokens(pending_tokens, pending_address);
    for (const auto& t : temp.sorted_tokens()) {
        auto eps = co_await calculate_natural_endpoints(t, temp);
        if (eps.contains(pending_address)) {
            dht::token_range_vector r = temp.get_primary_ranges_for(t);
            rslogger.debug("get_pending_address_ranges: token={} primary_range={} endpoint={}", t, r, pending_address);
            ret.insert(ret.end(), r.begin(), r.end());
        }
    }
    co_await temp.clear_gently();
    co_return ret;
}

future<mutable_vnode_effective_replication_map_ptr> calculate_effective_replication_map(replication_strategy_ptr rs, token_metadata_ptr tmptr) {
    replication_map replication_map;
    const auto& sorted_tokens = tmptr->sorted_tokens();

    if (!sorted_tokens.empty()) {
        replication_map.reserve(sorted_tokens.size());
        if (rs->natural_endpoints_depend_on_token()) {
            for (const auto &t : sorted_tokens) {
                auto eps = co_await rs->calculate_natural_endpoints(t, *tmptr);
                replication_map.emplace(t, eps.get_vector());
            }
        } else {
            auto eps = co_await rs->calculate_natural_endpoints(sorted_tokens.front(), *tmptr);
            for (const auto &t : sorted_tokens) {
                replication_map.emplace(t, eps.get_vector());
                co_await coroutine::maybe_yield();
            }
        }
    }

    auto rf = rs->get_replication_factor(*tmptr);
    co_return make_effective_replication_map(std::move(rs), std::move(tmptr), std::move(replication_map),
        ring_mapping{}, ring_mapping{}, rf);
}

future<replication_map> vnode_effective_replication_map::clone_endpoints_gently() const {
    replication_map cloned_endpoints;

    for (auto& i : _replication_map) {
        cloned_endpoints.emplace(i.first, i.second);
        co_await coroutine::maybe_yield();
    }

    co_return cloned_endpoints;
}

inet_address_vector_replica_set vnode_effective_replication_map::get_natural_endpoints(const token& search_token) const {
    return _rs->get_natural_endpoints(search_token, *this);
}

stop_iteration vnode_effective_replication_map::for_each_natural_endpoint_until(const token& search_token, const noncopyable_function<stop_iteration(const inet_address&)>& func) const {
    return _rs->for_each_natural_endpoint_until(search_token, *this, func);
}

future<> vnode_effective_replication_map::clear_gently() noexcept {
    co_await utils::clear_gently(_replication_map);
    co_await utils::clear_gently(_tmptr);
}

vnode_effective_replication_map::~vnode_effective_replication_map() {
    if (is_registered()) {
        _factory->erase_effective_replication_map(this);
        try {
            struct background_clear_holder {
                locator::replication_map replication_map;
                locator::token_metadata_ptr tmptr;
            };
            auto holder = make_lw_shared<background_clear_holder>({std::move(_replication_map), std::move(_tmptr)});
            auto fut = when_all(utils::clear_gently(holder->replication_map), utils::clear_gently(holder->tmptr)).discard_result().then([holder] {});
            _factory->submit_background_work(std::move(fut));
        } catch (...) {
            // ignore
        }
    }
}

effective_replication_map::effective_replication_map(replication_strategy_ptr rs,
                                                     token_metadata_ptr tmptr,
                                                     size_t replication_factor) noexcept
        : _rs(std::move(rs))
        , _tmptr(std::move(tmptr))
        , _replication_factor(replication_factor)
{ }

vnode_effective_replication_map::factory_key vnode_effective_replication_map::make_factory_key(const replication_strategy_ptr& rs, const token_metadata_ptr& tmptr) {
    return factory_key(rs->get_type(), rs->get_config_options(), tmptr->get_ring_version());
}

future<vnode_effective_replication_map_ptr> effective_replication_map_factory::create_effective_replication_map(replication_strategy_ptr rs, token_metadata_ptr tmptr) {
    // lookup key on local shard
    auto key = vnode_effective_replication_map::make_factory_key(rs, tmptr);
    auto erm = find_effective_replication_map(key);
    if (erm) {
        rslogger.debug("create_effective_replication_map: found {} [{}]", key, fmt::ptr(erm.get()));
        co_return erm;
    }

    // try to find a reference erm on shard 0
    // TODO:
    // - use hash of key to distribute the load
    // - instaintiate only on NUMA nodes
    auto ref_erm = co_await container().invoke_on(0, [key] (effective_replication_map_factory& ermf) -> future<foreign_ptr<vnode_effective_replication_map_ptr>> {
        auto erm = ermf.find_effective_replication_map(key);
        co_return make_foreign<vnode_effective_replication_map_ptr>(std::move(erm));
    });
    mutable_vnode_effective_replication_map_ptr new_erm;
    if (ref_erm) {
        auto rf = ref_erm->get_replication_factor();
        auto local_replication_map = co_await ref_erm->clone_endpoints_gently();
        new_erm = make_effective_replication_map(std::move(rs), std::move(tmptr), std::move(local_replication_map),
            ring_mapping{}, ring_mapping{}, rf);
    } else {
        new_erm = co_await calculate_effective_replication_map(std::move(rs), std::move(tmptr));
    }
    co_return insert_effective_replication_map(std::move(new_erm), std::move(key));
}

vnode_effective_replication_map_ptr effective_replication_map_factory::find_effective_replication_map(const vnode_effective_replication_map::factory_key& key) const {
    auto it = _effective_replication_maps.find(key);
    if (it != _effective_replication_maps.end()) {
        return it->second->shared_from_this();
    }
    return {};
}

vnode_effective_replication_map_ptr effective_replication_map_factory::insert_effective_replication_map(mutable_vnode_effective_replication_map_ptr erm, vnode_effective_replication_map::factory_key key) {
    auto [it, inserted] = _effective_replication_maps.insert({key, erm.get()});
    if (inserted) {
        rslogger.debug("insert_effective_replication_map: inserted {} [{}]", key, fmt::ptr(erm.get()));
        erm->set_factory(*this, std::move(key));
        return erm;
    }
    auto res = it->second->shared_from_this();
    rslogger.debug("insert_effective_replication_map: found {} [{}]", key, fmt::ptr(res.get()));
    return res;
}

bool effective_replication_map_factory::erase_effective_replication_map(vnode_effective_replication_map* erm) {
    const auto& key = erm->get_factory_key();
    auto it = _effective_replication_maps.find(key);
    if (it == _effective_replication_maps.end()) {
        rslogger.warn("Could not unregister effective_replication_map {} [{}]: key not found", key, fmt::ptr(erm));
        return false;
    }
    if (it->second != erm) {
        rslogger.warn("Could not unregister effective_replication_map {} [{}]: different instance [{}] is currently registered", key, fmt::ptr(erm), fmt::ptr(it->second));
        return false;
    }
    rslogger.debug("erase_effective_replication_map: erased {} [{}]", key, fmt::ptr(erm));
    _effective_replication_maps.erase(it);
    return true;
}

future<> effective_replication_map_factory::stop() noexcept {
    _stopped = true;
    if (!_effective_replication_maps.empty()) {
        for (auto it = _effective_replication_maps.begin(); it != _effective_replication_maps.end(); it = _effective_replication_maps.erase(it)) {
            auto& [key, erm] = *it;
            rslogger.debug("effective_replication_map_factory::stop found outstanding map {} [{}]", key, fmt::ptr(erm));
            // unregister outstanding effective_replication_maps
            // so they won't try to submit background work
            // to gently clear their contents when they are destroyed.
            erm->unregister();
        }

        // FIXME: reinstate the internal error
        // when https://github.com/scylladb/scylla/issues/8995
        // is fixed and shutdown order ensures that no outstanding maps
        // are expected here.
        // (see also https://github.com/scylladb/scylla/issues/9684)
        // on_internal_error_noexcept(rslogger, "effective_replication_map_factory stopped with outstanding maps");
    }

    return std::exchange(_background_work, make_ready_future<>());
}

void effective_replication_map_factory::submit_background_work(future<> fut) {
    if (fut.available() && !fut.failed()) {
        return;
    }
    if (_stopped) {
        on_internal_error(rslogger, "Cannot submit background work: registry already stopped");
    }
    _background_work = _background_work.then([fut = std::move(fut)] () mutable {
        return std::move(fut).handle_exception([] (std::exception_ptr ex) {
            // Ignore errors since we have nothing else to do about them.
            rslogger.warn("effective_replication_map_factory background task failed: {}. Ignored.", std::move(ex));
        });
    });
}

future<> global_vnode_effective_replication_map::get_keyspace_erms(sharded<replica::database>& sharded_db, std::string_view keyspace_name) {
    return sharded_db.invoke_on(0, [this, &sharded_db, keyspace_name] (replica::database& db) -> future<> {
        // To ensure we get the same effective_replication_map
        // on all shards, acquire the shared_token_metadata lock.
        //
        // As a sanity check compare the ring_version on each shard
        // to the reference version on shard 0.
        //
        // This invariant is achieved by storage_service::mutate_token_metadata
        // and storage_service::replicate_to_all_cores that first acquire the
        // shared_token_metadata lock, then prepare a mutated token metadata
        // that will have an incremented ring_version, use it to re-calculate
        // all e_r_m:s and clone both on all shards. including the ring version,
        // all under the lock.
        auto lk = co_await db.get_shared_token_metadata().get_lock();
        auto erm = db.find_keyspace(keyspace_name).get_effective_replication_map();
        auto ring_version = erm->get_token_metadata().get_ring_version();
        _erms[0] = make_foreign(std::move(erm));
        co_await coroutine::parallel_for_each(boost::irange(1u, smp::count), [this, &sharded_db, keyspace_name, ring_version] (unsigned shard) -> future<> {
            _erms[shard] = co_await sharded_db.invoke_on(shard, [keyspace_name, ring_version] (const replica::database& db) {
                const auto& ks = db.find_keyspace(keyspace_name);
                auto erm = ks.get_effective_replication_map();
                auto local_ring_version = erm->get_token_metadata().get_ring_version();
                if (local_ring_version != ring_version) {
                    on_internal_error(rslogger, format("Inconsistent effective_replication_map ring_verion {}, expected {}", local_ring_version, ring_version));
                }
                return make_foreign(std::move(erm));
            });
        });
    });
}

future<global_vnode_effective_replication_map> make_global_effective_replication_map(sharded<replica::database>& sharded_db, std::string_view keyspace_name) {
    global_vnode_effective_replication_map ret;
    co_await ret.get_keyspace_erms(sharded_db, keyspace_name);
    co_return ret;
}

} // namespace locator

std::ostream& operator<<(std::ostream& os, locator::replication_strategy_type t) {
    switch (t) {
    case locator::replication_strategy_type::simple:
        return os << "simple";
    case locator::replication_strategy_type::local:
        return os << "local";
    case locator::replication_strategy_type::network_topology:
        return os << "network_topology";
    case locator::replication_strategy_type::everywhere_topology:
        return os << "everywhere_topology";
    };
    std::abort();
}

std::ostream& operator<<(std::ostream& os, const locator::vnode_effective_replication_map::factory_key& key) {
    os << key.rs_type;
    os << '.' << key.ring_version;
    char sep = ':';
    for (const auto& [opt, val] : key.rs_config_options) {
        os << sep << opt << '=' << val;
        sep = ',';
    }
    return os;
}
