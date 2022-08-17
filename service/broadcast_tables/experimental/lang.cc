/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "lang.hh"

#include <seastar/core/future.hh>

#include "data_dictionary/data_dictionary.hh"
#include "db/config.hh"
#include "db/system_keyspace.hh"
#include "exceptions/exceptions.hh"
#include "service/raft/raft_group0_client.hh"


namespace service::broadcast_tables {

bool is_broadcast_table_statement(const sstring& keyspace, const sstring& column_family) {
    return keyspace == db::system_keyspace::NAME && column_family == db::system_keyspace::BROADCAST_KV_STORE;
}

future<> execute(service::raft_group0_client& group0_client, const query& query) {
    throw exceptions::invalid_request_exception{"executing queries on broadcast_kv_store is currently not implemented"};
}

}