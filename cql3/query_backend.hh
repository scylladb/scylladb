/*
 * Copyright (C) 2023-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "data_dictionary/data_dictionary.hh"
#include "query-request.hh"
#include "locator/token_metadata.hh"
#include "exceptions/coordinator_result.hh"
#include "coordinator_query.hh"
#include "dht/token_range_endpoints.hh"

namespace service {
class forward_service;
class migration_manager;
class raft_group0_client;
class cas_request;
class storage_proxy;
}

namespace wasmtime {
class Engine;
}

namespace wasm {
class instance_cache;
class alien_thread_runner;
}

namespace cql_transport::messages {
class result_message;
}

namespace cql3 {

using computed_function_values = std::unordered_map<uint8_t, bytes_opt>;

// Abstract interface for the query backend, presented to the CQL3 code.
//
// It hides the details of how requests are executed.
class query_backend {
public:
    class impl;

    template<typename T = void>
    using result = exceptions::coordinator_result<T>;

private:
    shared_ptr<impl> _impl;

public:
    explicit query_backend(shared_ptr<impl>);
    ~query_backend();

public:
    data_dictionary::database db();
    service::forward_service& forwarder();
    service::migration_manager& get_migration_manager();
    service::raft_group0_client& get_group0_client();
    wasmtime::Engine& wasm_engine();
    wasm::instance_cache& wasm_instance_cache();
    wasm::alien_thread_runner& alien_runner();
    locator::token_metadata_ptr get_token_metadata_ptr();

    //FIXME: get rid of this, by providing abstract methods for all proxy methods currently used directly.
    service::storage_proxy& proxy();

    shared_ptr<cql_transport::messages::result_message> bounce_to_shard(unsigned shard, cql3::computed_function_values cached_fn_calls);
    future<> truncate_blocking(sstring keyspace, sstring cfname, std::optional<std::chrono::milliseconds> timeout_in_ms = std::nullopt);
    future<result<coordinator_query_result>> query_result(schema_ptr, lw_shared_ptr<query::read_command> cmd, dht::partition_range_vector&& partition_ranges,
            db::consistency_level cl, coordinator_query_options optional_params);
    future<coordinator_query_result> query(schema_ptr, lw_shared_ptr<query::read_command> cmd, dht::partition_range_vector&& partition_ranges,
            db::consistency_level cl, coordinator_query_options optional_params);
    future<result<>> mutate_with_triggers(std::vector<mutation> mutations, db::consistency_level cl, lowres_clock::time_point timeout, bool should_mutate_atomically,
            tracing::trace_state_ptr tr_state, service_permit permit, db::allow_per_partition_rate_limit allow_limit, bool raw_counters = false);
    future<bool> cas(schema_ptr schema, shared_ptr<service::cas_request> request, lw_shared_ptr<query::read_command> cmd, dht::partition_range_vector partition_ranges, coordinator_query_options query_options,
            db::consistency_level cl_for_paxos, db::consistency_level cl_for_learn, lowres_clock::time_point write_timeout, lowres_clock::time_point cas_timeout, bool write = true);
    future<std::vector<dht::token_range_endpoints>> describe_ring(const sstring& keyspace, bool include_only_local_dc = false) const;

    query::tombstone_limit get_tombstone_limit() const;
    query::max_result_size get_max_result_size(const query::partition_slice& slice) const;
};

// Make a backend, based on storage proxy.
query_backend make_storage_proxy_query_backend(
        service::storage_proxy& proxy,
        data_dictionary::database db,
        service::forward_service* fwd_service,
        service::migration_manager* mm,
        service::raft_group0_client* rgc,
        wasmtime::Engine* wasm_engine,
        wasm::instance_cache* wasm_ic,
        wasm::alien_thread_runner* wasm_atr);

} // namespace cql3
