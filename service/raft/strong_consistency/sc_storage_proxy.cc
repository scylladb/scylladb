/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "sc_storage_proxy.hh"

namespace service {

sc_storage_proxy::sc_storage_proxy(raft_group_registry& raft_groups, db::system_keyspace& sys_ks)
    : _raft_groups(raft_groups)
    , _sys_ks(sys_ks)
{
}

future<> sc_storage_proxy::mutate(const schema& schema, const dht::token& token, mutatations_gen&& mutatations_gen) {
    (void)_raft_groups;
    (void)_sys_ks;
    return make_ready_future<>();
}
}
