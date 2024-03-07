/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "raft_service_level_distributed_data_accessor.hh"
#include "cql3/query_processor.hh"
#include "db/consistency_level_type.hh"
#include "seastar/core/abort_source.hh"
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

future<> raft_service_level_distributed_data_accessor::do_raft_command(service::group0_guard guard, abort_source& as, std::vector<mutation> mutations, std::string_view description) const {    
    service::write_mutations change {
        .mutations{mutations.begin(), mutations.end()},
    };

    auto group0_cmd = _group0_client.prepare_command(change, guard, description);
    co_await _group0_client.add_entry(std::move(group0_cmd), std::move(guard), &as);
}

future<> raft_service_level_distributed_data_accessor::set_service_level(sstring service_level_name, qos::service_level_options slo, std::optional<service::group0_guard> guard, abort_source& as) const {   
    if (this_shard_id() != 0) {
        on_internal_error(logger, "raft_service_level_distributed_data_accessor: must be executed on shard 0");
    }

    if (!guard) {
        on_internal_error(logger, "raft_service_level_distributed_data_accessor: guard must be present");
    }
    
    static sstring insert_query = format("INSERT INTO {}.{} (service_level, timeout, workload_type) VALUES (?, ?, ?);", db::system_keyspace::NAME, db::system_keyspace::SERVICE_LEVELS_V2);
    data_value workload = slo.workload == qos::service_level_options::workload_type::unspecified
            ? data_value::make_null(utf8_type)
            : data_value(qos::service_level_options::to_string(slo.workload));
    
    auto timestamp = guard->write_timestamp();
    auto muts = co_await _qp.get_mutations_internal(insert_query, qos_query_state(), timestamp, {service_level_name, timeout_to_data_value(slo.timeout), workload});

    co_await do_raft_command(std::move(*guard), as, std::move(muts), "set service level");
}

future<> raft_service_level_distributed_data_accessor::drop_service_level(sstring service_level_name, std::optional<service::group0_guard> guard, abort_source& as) const {
    //FIXME: remove this when `role_manager::remove_attribute()` will be done in one raft command
    if (!guard) {
        guard = co_await _group0_client.start_operation(&as);
    }

    if (this_shard_id() != 0) {
        on_internal_error(logger, "raft_service_level_distributed_data_accessor: must be executed on shard 0");
    }

    if (!guard) {
        on_internal_error(logger, "raft_service_level_distributed_data_accessor: guard must be present");
    }

    static sstring delete_query = format("DELETE FROM {}.{} WHERE service_level= ?;", db::system_keyspace::NAME, db::system_keyspace::SERVICE_LEVELS_V2);
    
    auto timestamp = guard->write_timestamp();
    auto muts = co_await _qp.get_mutations_internal(delete_query, qos_query_state(), timestamp, {service_level_name});

    co_await do_raft_command(std::move(*guard), as, std::move(muts), "drop service level");
}

bool raft_service_level_distributed_data_accessor::is_v2() const {
    return true;
}

::shared_ptr<service_level_controller::service_level_distributed_data_accessor> raft_service_level_distributed_data_accessor::upgrade_to_v2(cql3::query_processor& qp, service::raft_group0_client& group0_client) const {
    return nullptr;
}

}

