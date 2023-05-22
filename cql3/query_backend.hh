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

namespace service {
class forward_service;
class migration_manager;
class raft_group0_client;
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

    //FIXME: get rid of this, by providing abstract methods for all proxy methods currently used directly.
    service::storage_proxy& proxy();

    shared_ptr<cql_transport::messages::result_message> bounce_to_shard(unsigned shard, cql3::computed_function_values cached_fn_calls);
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
