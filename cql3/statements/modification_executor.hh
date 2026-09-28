/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "transport/messages/result_message_base.hh"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

namespace service {
class query_state;
}

namespace cql3 {

class query_processor;
class query_options;

namespace statements {

class modification_statement;

/*
 * How a modification reaches storage.
 *
 * A modification_statement knows what to write and deliberately not how to
 * commit it. That is this: chosen once, when the statement is prepared, from
 * the keyspace it addresses. One implementation writes through storage_proxy,
 * with the replication factor's eventual consistency; the other commits through
 * the Raft group that owns the partition.
 *
 * Implementations are stateless and shared.
 */
class modification_executor {
public:
    virtual ~modification_executor() = default;

    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    commit(const modification_statement& stmt, query_processor& qp,
            service::query_state& qs, const query_options& options) const = 0;

    // Whether this commits through Raft. The native protocol handler asks,
    // because a batch may not mix modifications that do with ones that do not.
    virtual bool is_strongly_consistent() const { return false; }
};

}

}
