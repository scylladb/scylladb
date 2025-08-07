/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <algorithm>
#include "local_strategy.hh"
#include "dht/token.hh"
#include "utils/class_registrator.hh"
#include "exceptions/exceptions.hh"


namespace locator {

local_strategy::local_strategy(replication_strategy_params params) :
        abstract_replication_strategy(params, replication_strategy_type::local) {
    _natural_endpoints_depend_on_token = false;
}

future<host_id_set> local_strategy::calculate_natural_endpoints(const token& t, const token_metadata& tm) const {
    return make_ready_future<host_id_set>(host_id_set{tm.get_topology().my_host_id()});
}

void local_strategy::validate_options(const gms::feature_service&, const locator::topology&) const {
    if (_uses_tablets) {
        throw exceptions::configuration_exception("LocalStrategy doesn't support tablet replication");
    }
}

size_t local_strategy::get_replication_factor(const token_metadata&) const {
    return 1;
}

sstring local_strategy::sanity_check_read_replicas(const effective_replication_map& erm, const host_id_vector_replica_set& read_replicas) const {
    if (read_replicas.size() > 1) {
        return seastar::format("local_strategy: the number of replicas for local_strategy is {}, cannot be higher than 1", read_replicas.size());
    }
    return {};
}

future<mutable_static_effective_replication_map_ptr> local_effective_replication_map::clone_gently(replication_strategy_ptr rs, token_metadata_ptr tmptr) const {
    return make_ready_future<mutable_static_effective_replication_map_ptr>(make_local_effective_replication_map_ptr(std::move(rs), std::move(tmptr)));
}

host_id_vector_replica_set local_effective_replication_map::get_natural_replicas(const token&) const {
    return _replica_set;
}

host_id_vector_topology_change local_effective_replication_map::get_pending_replicas(const token&) const {
    return host_id_vector_topology_change{};
}

host_id_vector_replica_set local_effective_replication_map::get_replicas_for_reading(const token& token) const {
    return _replica_set;
}

host_id_vector_replica_set local_effective_replication_map::get_replicas(const token&) const {
    return _replica_set;
}

std::optional<tablet_routing_info> local_effective_replication_map::check_locality(const token& token) const {
    return std::nullopt;
}

bool local_effective_replication_map::has_pending_ranges(locator::host_id endpoint) const {
    return false;
}

std::unique_ptr<token_range_splitter> local_effective_replication_map::make_splitter() const {
    class local_splitter : public token_range_splitter {
        std::optional<dht::token> _cur;
    public:
        local_splitter()
            : _cur(dht::minimum_token())
        { }

        void reset(dht::ring_position_view pos) override {
            _cur = pos.token();
        }

        std::optional<dht::token> next_token() override {
            if (auto cur = std::exchange(_cur, std::nullopt)) {
                return cur;
            }
            return std::nullopt;
        }
    };
    return std::make_unique<local_splitter>();
}

const dht::sharder& local_effective_replication_map::get_sharder(const schema& s) const {
    return s.get_sharder();
}

future<dht::token_range_vector> local_effective_replication_map::get_ranges(host_id ep) const {
    if (ep == _tmptr->get_topology().my_host_id()) {
        return make_ready_future<dht::token_range_vector>(_local_ranges);
    }
    return make_ready_future<dht::token_range_vector>();
}

using registry = class_registrator<abstract_replication_strategy, local_strategy, replication_strategy_params>;
static registry registrator("org.apache.cassandra.locator.LocalStrategy");
static registry registrator_short_name("LocalStrategy");

}
