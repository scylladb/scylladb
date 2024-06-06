/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "raft_service_level_distributed_data_accessor.hh"
#include "cql3/query_processor.hh"
#include "db/consistency_level_type.hh"
#include <seastar/core/abort_source.hh>
#include "exceptions/exceptions.hh"
#include "service/raft/raft_group0_client.hh"
#include "db/system_keyspace.hh"
#include "types/types.hh"

namespace qos {

static logging::logger logger("raft_service_distributed_level_data_accessor");

static data_value timeout_to_data_value(const qos::service_level_options::timeout_type& tv) {
    return std::visit(overloaded_functor {
        [&] (const qos::service_level_options::unset_marker&) {
            return data_value::make_null(duration_type);
        },
        [&] (const qos::service_level_options::delete_marker&) {
            return data_value::make_null(duration_type);
        },
        [&] (const lowres_clock::duration& d) {
            return data_value(cql_duration(months_counter{0},
                    days_counter{0},
                    nanoseconds_counter{std::chrono::duration_cast<std::chrono::nanoseconds>(d).count()}));
        },
    }, tv);
}

raft_service_level_distributed_data_accessor::raft_service_level_distributed_data_accessor(cql3::query_processor& qp, service::raft_group0_client& group0_client)
    : _qp(qp)
    , _group0_client(group0_client) {}

future<qos::service_levels_info> raft_service_level_distributed_data_accessor::get_service_levels() const {
    return qos::get_service_levels(_qp, db::system_keyspace::NAME, db::system_keyspace::SERVICE_LEVELS_V2, db::consistency_level::LOCAL_ONE);
}

future<qos::service_levels_info> raft_service_level_distributed_data_accessor::get_service_level(sstring service_level_name) const {
    return qos::get_service_level(_qp, db::system_keyspace::NAME, db::system_keyspace::SERVICE_LEVELS_V2, std::move(service_level_name), db::consistency_level::LOCAL_ONE);
}

static void validate_state(const service::raft_group0_client& group0_client) {
    if (this_shard_id() != 0) {
        on_internal_error(logger, "raft_service_level_distributed_data_accessor: must be executed on shard 0");
    }
    if (group0_client.in_recovery()) {
        throw exceptions::invalid_request_exception("The cluster is in recovery mode. Changes to service levels are not allowed.");
    }
}

future<> raft_service_level_distributed_data_accessor::set_service_level(sstring service_level_name, qos::service_level_options slo, service::group0_batch& mc) const {
    validate_state(_group0_client);
    
    static sstring insert_query = format("INSERT INTO {}.{} (service_level, timeout, workload_type) VALUES (?, ?, ?);", db::system_keyspace::NAME, db::system_keyspace::SERVICE_LEVELS_V2);
    data_value workload = slo.workload == qos::service_level_options::workload_type::unspecified
            ? data_value::make_null(utf8_type)
            : data_value(qos::service_level_options::to_string(slo.workload));

    auto muts = co_await _qp.get_mutations_internal(insert_query, qos_query_state(), mc.write_timestamp(), {service_level_name, timeout_to_data_value(slo.timeout), workload});
    mc.add_mutations(std::move(muts), format("service levels internal statement: {}", insert_query));
}

future<> raft_service_level_distributed_data_accessor::drop_service_level(sstring service_level_name, service::group0_batch& mc) const {
    validate_state(_group0_client);

    static sstring delete_query = format("DELETE FROM {}.{} WHERE service_level= ?;", db::system_keyspace::NAME, db::system_keyspace::SERVICE_LEVELS_V2);

    auto muts = co_await _qp.get_mutations_internal(delete_query, qos_query_state(), mc.write_timestamp(), {service_level_name});
    mc.add_mutations(std::move(muts), format("service levels internal statement: {}", delete_query));
}

future<> raft_service_level_distributed_data_accessor::commit_mutations(service::group0_batch&& mc, abort_source& as) const {
    return std::move(mc).commit(_group0_client, as, ::service::raft_timeout{});
}

bool raft_service_level_distributed_data_accessor::is_v2() const {
    return true;
}

::shared_ptr<service_level_controller::service_level_distributed_data_accessor> raft_service_level_distributed_data_accessor::upgrade_to_v2(cql3::query_processor& qp, service::raft_group0_client& group0_client) const {
    return nullptr;
}

}

