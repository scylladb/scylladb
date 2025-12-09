/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/chunked_vector.hh"
#include "mutation/mutation.hh"

namespace service {

class raft_group_registry;

class sc_storage_proxy: public peering_sharded_service<sc_storage_proxy> {
    raft_group_registry& _raft_groups;
    db::system_keyspace& _sys_ks;
public:
    sc_storage_proxy(raft_group_registry& raft_groups, db::system_keyspace& sys_ks);

    using mutatations_gen = noncopyable_function<utils::chunked_vector<mutation>(api::timestamp_type)>;
    future<> mutate(const schema& schema, const dht::token& token, mutatations_gen&& mutatations_gen);
};

}
