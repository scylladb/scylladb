/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "rpc_fence.hh"
#include "db/system_keyspace.hh"
#include "seastar/core/print.hh"

namespace service {
stale_fencing_token::stale_fencing_token(const fencing_token& caller_token, const fencing_token& current_token)
    : runtime_error(format("stale fencing token, caller token {} < current token {}", caller_token, current_token))
{
}

rpc_fence::rpc_fence(fencing_token current_token)
    : current_token(std::move(current_token))
{
}

void rpc_fence::check(const fencing_token& caller_token) {
    if (caller_token < current_token) {
        throw stale_fencing_token(caller_token, current_token);
    }
}

// Increment with concurrent rpc-s is not handled yet.
// One possible solution is to wait for them:
// 1. track rpc start/finish here;
// 2. make increment multistep, at the first step broadcast the command "begin using new version for sending" to each node,
//    this must be persisted as 'topology_next_version';
// 3. then, wait for each node to stop using previous version;
// 4. and finally, broadcast command "begin using new version for checking" to each node; this command
//    updated persisted 'topology_version' and then drops 'topology_next_version'.
future<> rpc_fence::increment(fencing_token expected_token) {
    return container().invoke_on(0, [expected_token](rpc_fence& self) -> future<> {
        const auto new_version = self.current_token.topology_version + 1;
        if (new_version != expected_token.topology_version) {
            throw std::runtime_error(format("concurrent increment, expected {}, actual {}",
                                            expected_token.topology_version, new_version));
        }
        co_await db::system_keyspace::set_topology_version(new_version);
        co_await self.container().invoke_on_all([new_version](rpc_fence& local) {
            local.current_token.topology_version = new_version;
        });
    });
}
}

