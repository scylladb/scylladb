/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */


#include "locator/everywhere_replication_strategy.hh"
#include "utils/class_registrator.hh"
#include "locator/token_metadata.hh"
#include "exceptions/exceptions.hh"

namespace locator {

everywhere_replication_strategy::everywhere_replication_strategy(replication_strategy_params params) :
        abstract_replication_strategy(params, replication_strategy_type::everywhere_topology) {
    _natural_endpoints_depend_on_token = false;
}

future<host_id_set> everywhere_replication_strategy::calculate_natural_endpoints(const token& search_token, const token_metadata& tm) const {
    if (tm.sorted_tokens().empty()) {
        host_id_set result{host_id_vector_replica_set({tm.get_topology().my_host_id()})};
        return make_ready_future<host_id_set>(std::move(result));
    }
    const auto& all_endpoints = tm.get_normal_token_owners();
    return make_ready_future<host_id_set>(host_id_set(all_endpoints.begin(), all_endpoints.end()));
}

size_t everywhere_replication_strategy::get_replication_factor(const token_metadata& tm) const {
    return tm.sorted_tokens().empty() ? 1 : tm.count_normal_token_owners();
}

void everywhere_replication_strategy::validate_options(const gms::feature_service&) const {
    if (_uses_tablets) {
        throw exceptions::configuration_exception("EverywhereStrategy doesn't support tablet replication");
    }
}

sstring everywhere_replication_strategy::sanity_check_read_replicas(const effective_replication_map& erm, const host_id_vector_replica_set& read_replicas) const {
    const auto replication_factor = erm.get_replication_factor();
    if (read_replicas.size() > replication_factor) {
        return seastar::format("everywhere_replication_strategy: the number of replicas for everywhere_replication_strategy is {}, cannot be higher than replication factor {}", read_replicas.size(), replication_factor);
    }
    return {};
}


using registry = class_registrator<abstract_replication_strategy, everywhere_replication_strategy, replication_strategy_params>;
static registry registrator("org.apache.cassandra.locator.EverywhereStrategy");
static registry registrator_short_name("EverywhereStrategy");
}
