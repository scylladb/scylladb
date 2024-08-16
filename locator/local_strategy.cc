/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <algorithm>
#include "local_strategy.hh"
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

using registry = class_registrator<abstract_replication_strategy, local_strategy, replication_strategy_params>;
static registry registrator("org.apache.cassandra.locator.LocalStrategy");
static registry registrator_short_name("LocalStrategy");

}
