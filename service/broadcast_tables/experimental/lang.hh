/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include <optional>
#include <variant>

#include <seastar/core/future.hh>

#include "bytes.hh"
#include "utils/UUID.hh"
#include "exceptions/exceptions.hh"
#include "service/broadcast_tables/experimental/query_result.hh"


namespace service {

class raft_group0_client;
class group0_command;
class storage_proxy;

}

namespace service::broadcast_tables {

using namespace std::string_literals;

// Represents 'SELECT value WHERE key = {key} FROM system.broadcast_kv_store;'.
struct select_query {
    bytes key;
};

// Represents 'UPDATE system.broadcast_kv_store SET value = {new_value} WHERE key = {key} [IF value = {value_condition}];'.
// If value_condition is nullopt, the update is unconditional.
struct update_query {
    bytes key;
    bytes new_value;
    std::optional<bytes_opt> value_condition;
};

struct query {
    std::variant<select_query, update_query> q;
};

// Function checks if statement should be executed on broadcast table.
// For now it returns true if and only if target table is system.broadcast_kv_store.
bool is_broadcast_table_statement(const sstring& keyspace, const sstring& column_family);

future<query_result> execute(service::raft_group0_client& group0_client, const query& query);

future<query_result> execute_broadcast_table_query(service::storage_proxy& proxy, const query& query, utils::UUID cmd_id);

class unsupported_operation_error : public exceptions::invalid_request_exception {
public:
    unsupported_operation_error()
        : exceptions::invalid_request_exception{"currently unsupported operation on broadcast_kv_store"} {
    }

    template<typename S>
    unsupported_operation_error(const S& message)
        : exceptions::invalid_request_exception{"currently unsupported operation on broadcast_kv_store: "s + message} {
    }
};

} // namespace service::broadcast_tables
