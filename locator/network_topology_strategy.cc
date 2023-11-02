/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <functional>

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "locator/network_topology_strategy.hh"
#include "locator/load_sketch.hh"
#include <boost/algorithm/string.hpp>
#include "utils/hash.hh"

namespace std {
template<>
struct hash<locator::endpoint_dc_rack> {
    size_t operator()(const locator::endpoint_dc_rack& v) const {
        return utils::tuple_hash()(std::tie(v.dc, v.rack));
    }
};
}

namespace locator {

network_topology_strategy::network_topology_strategy(
    const replication_strategy_config_options& config_options) :
        abstract_replication_strategy(config_options,
                                      replication_strategy_type::network_topology) {
    auto opts = config_options;
    process_tablet_options(*this, opts);

    for (auto& config_pair : opts) {
        auto& key = config_pair.first;
        auto& val = config_pair.second;

        //
        // FIXME!!!
        // The first option we get at the moment is a class name. Skip it!
        //
        if (boost::iequals(key, "class")) {
            continue;
        }

        if (boost::iequals(key, "replication_factor")) {
            throw exceptions::configuration_exception(
                "replication_factor is an option for SimpleStrategy, not "
                "NetworkTopologyStrategy");
        }

        validate_replication_factor(val);
        _dc_rep_factor.emplace(key, std::stol(val));
        _datacenteres.push_back(key);
    }

    _rep_factor = 0;

    for (auto& one_dc_rep_factor : _dc_rep_factor) {
        _rep_factor += one_dc_rep_factor.second;
    }

    if (rslogger.is_enabled(log_level::debug)) {
        sstring cfg;
        for (auto& p : _dc_rep_factor) {
            cfg += format(" {}:{}", p.first, p.second);
        }
        rslogger.debug("Configured datacenter replicas are: {}", cfg);
    }
}

using endpoint_dc_rack_set = std::unordered_set<endpoint_dc_rack>;

template <typename NodeId>
class natural_endpoints_tracker {
    /**
     * Endpoint adder applying the replication rules for a given DC.
     */
    struct data_center_endpoints {
        /** List accepted endpoints get pushed into. */
        set_type<NodeId>& _endpoints;

        /**
         * Racks encountered so far. Replicas are put into separate racks while possible.
         * For efficiency the set is shared between the instances, using the location pair (dc, rack) to make sure
         * clashing names aren't a problem.
         */
        endpoint_dc_rack_set& _racks;

        /** Number of replicas left to fill from this DC. */
        size_t _rf_left;
        ssize_t _acceptable_rack_repeats;

        data_center_endpoints(size_t rf, size_t rack_count, size_t node_count, set_type<NodeId>& endpoints, endpoint_dc_rack_set& racks)
            : _endpoints(endpoints)
            , _racks(racks)
            // If there aren't enough nodes in this DC to fill the RF, the number of nodes is the effective RF.
            , _rf_left(std::min(rf, node_count))
            // If there aren't enough racks in this DC to fill the RF, we'll still use at least one node from each rack,
            // and the difference is to be filled by the first encountered nodes.
            , _acceptable_rack_repeats(rf - rack_count)
        {}

        /**
         * Attempts to add an endpoint to the replicas for this datacenter, adding to the endpoints set if successful.
         * Returns true if the endpoint was added, and this datacenter does not require further replicas.
         */
        bool add_endpoint_and_check_if_done(const NodeId& ep, const endpoint_dc_rack& location) {
            if (done()) {
                return false;
            }

            if (_racks.emplace(location).second) {
                // New rack.
                --_rf_left;
                auto added = _endpoints.insert(ep).second;
                if (!added) {
                    throw std::runtime_error(fmt::format("Topology error: found {} in more than one rack", ep));
                }
                return done();
            }

            /**
             * Ensure we don't allow too many endpoints in the same rack, i.e. we have
             * minimum current rf_left + 1 distinct racks. See above, _acceptable_rack_repeats
             * is defined as RF - rack_count, i.e. how many nodes in a single rack we are ok
             * with.
             *
             * With RF = 3 and 2 Racks in DC,
             *
             * IP1, Rack1
             * IP2, Rack1
             * IP3, Rack1,    The line _acceptable_rack_repeats <= 0 will reject IP3.
             * IP4, Rack2
             *
             */
            if (_acceptable_rack_repeats <= 0) {
                // There must be rf_left distinct racks left, do not add any more rack repeats.
                return false;
            }

            if (!_endpoints.insert(ep).second) {
                // Cannot repeat a node.
                return false;
            }

            // Added a node that is from an already met rack to match RF when there aren't enough racks.
            --_acceptable_rack_repeats;
            --_rf_left;

            return done();
        }

        bool done() const {
            return _rf_left == 0;
        }
    };

    const generic_token_metadata<NodeId>& _tm;
    const topology& _tp;
    std::unordered_map<sstring, size_t> _dc_rep_factor;

    //
    // We want to preserve insertion order so that the first added endpoint
    // becomes primary.
    //
    set_type<NodeId> _replicas;
    // tracks the racks we have already placed replicas in
    endpoint_dc_rack_set _seen_racks;

    //
    // all endpoints in each DC, so we can check when we have exhausted all
    // the members of a DC
    //
    std::unordered_map<sstring, std::unordered_set<inet_address>> _all_endpoints;

    //
    // all racks in a DC so we can check when we have exhausted all racks in a
    // DC
    //
    std::unordered_map<sstring, std::unordered_map<sstring, std::unordered_set<inet_address>>> _racks;

    std::unordered_map<sstring_view, data_center_endpoints> _dcs;

    size_t _dcs_to_fill;

public:
    natural_endpoints_tracker(const generic_token_metadata<NodeId>& tm, const std::unordered_map<sstring, size_t>& dc_rep_factor)
        : _tm(tm)
        , _tp(_tm.get_topology())
        , _dc_rep_factor(dc_rep_factor)
        , _all_endpoints(_tp.get_datacenter_endpoints())
        , _racks(_tp.get_datacenter_racks())
    {
        // not aware of any cluster members
        assert(!_all_endpoints.empty() && !_racks.empty());

        auto size_for = [](auto& map, auto& k) {
            auto i = map.find(k);
            return i != map.end() ? i->second.size() : size_t(0);
        };

        // Create a data_center_endpoints object for each non-empty DC.
        for (auto& p : _dc_rep_factor) {
            auto& dc = p.first;
            auto rf = p.second;
            auto node_count = size_for(_all_endpoints, dc);

            if (rf == 0 || node_count == 0) {
                continue;
            }

            _dcs.emplace(dc, data_center_endpoints(rf, size_for(_racks, dc), node_count, _replicas, _seen_racks));
            _dcs_to_fill = _dcs.size();
        }
    }

    bool add_endpoint_and_check_if_done(NodeId ep) {
        auto& loc = _tp.get_location(ep);
        auto i = _dcs.find(loc.dc);
        if (i != _dcs.end() && i->second.add_endpoint_and_check_if_done(ep, loc)) {
            --_dcs_to_fill;
        }
        return done();
    }

    bool done() const noexcept {
        return _dcs_to_fill == 0;
    }

    set_type<NodeId>& replicas() noexcept {
        return _replicas;
    }
};

future<natural_ep_type>
network_topology_strategy::calculate_natural_endpoints(
    const token& search_token, const token_metadata& tm, bool use_host_id) const {

    return select_tm([&]<typename NodeId>(const generic_token_metadata<NodeId>& tm) -> future<natural_ep_type> {
        natural_endpoints_tracker<NodeId> tracker(tm, _dc_rep_factor);

        for (auto& next : tm.ring_range(search_token)) {
            co_await coroutine::maybe_yield();

            NodeId ep = *tm.get_endpoint(next);
            if (tracker.add_endpoint_and_check_if_done(ep)) {
                break;
            }
        }

        co_return std::move(tracker.replicas());
    }, tm, use_host_id);
}

void network_topology_strategy::validate_options(const gms::feature_service& fs) const {
    if(_config_options.empty()) {
        throw exceptions::configuration_exception("Configuration for at least one datacenter must be present");
    }
    validate_tablet_options(fs, _config_options);
    auto tablet_opts = recognized_tablet_options();
    for (auto& c : _config_options) {
        if (tablet_opts.contains(c.first)) {
            continue;
        }
        if (c.first == sstring("replication_factor")) {
            throw exceptions::configuration_exception(
                "replication_factor is an option for simple_strategy, not "
                "network_topology_strategy");
        }
        validate_replication_factor(c.second);
    }
}

std::optional<std::unordered_set<sstring>> network_topology_strategy::recognized_options(const topology& topology) const {
    // We only allow datacenter names as options
    auto opts = topology.get_datacenters();
    opts.merge(recognized_tablet_options());
    return opts;
}

effective_replication_map_ptr network_topology_strategy::make_replication_map(table_id table, token_metadata_ptr tm) const {
    if (!uses_tablets()) {
        on_internal_error(rslogger, format("make_replication_map() called for table {} but replication strategy not configured to use tablets", table));
    }
    return do_make_replication_map(table, shared_from_this(), std::move(tm), _rep_factor);
}

future<tablet_map> network_topology_strategy::allocate_tablets_for_new_table(schema_ptr s, token_metadata_ptr tm) const {
    auto tablet_count = get_initial_tablets();
    auto aligned_tablet_count = 1ul << log2ceil(tablet_count);
    if (tablet_count != aligned_tablet_count) {
        rslogger.info("Rounding up tablet count from {} to {} for table {}.{}", tablet_count, aligned_tablet_count, s->ks_name(), s->cf_name());
        tablet_count = aligned_tablet_count;
    }

    tablet_map tablets(tablet_count);
    load_sketch load(tm->get_new_strong());
    co_await load.populate();

    // FIXME: Don't use tokens to distribute nodes.
    // The following reuses the existing token-based algorithm used by NetworkTopologyStrategy.
    assert(!tm->get_new()->sorted_tokens().empty());
    auto token_range = tm->get_new()->ring_range(dht::token::get_random_token());

    for (tablet_id tb : tablets.tablet_ids()) {
        natural_endpoints_tracker<locator::host_id> tracker(*tm->get_new(), _dc_rep_factor);

        while (true) {
            co_await coroutine::maybe_yield();
            if (token_range.begin() == token_range.end()) {
                token_range = tm->get_new()->ring_range(dht::minimum_token());
            }
            locator::host_id ep = *tm->get_new()->get_endpoint(*token_range.begin());
            token_range.drop_front();
            if (tracker.add_endpoint_and_check_if_done(ep)) {
                break;
            }
        }

        tablet_replica_set replicas;
        for (auto&& ep : tracker.replicas()) {
            replicas.emplace_back(tablet_replica{ep, load.next_shard(ep)});
        }

        tablets.set_tablet(tb, tablet_info{std::move(replicas)});
    }

    tablet_logger.debug("Allocated tablets for {}.{} ({}): {}", s->ks_name(), s->cf_name(), s->id(), tablets);
    co_return tablets;
}

using registry = class_registrator<abstract_replication_strategy, network_topology_strategy, const replication_strategy_config_options&>;
static registry registrator("org.apache.cassandra.locator.NetworkTopologyStrategy");
static registry registrator_short_name("NetworkTopologyStrategy");
}
