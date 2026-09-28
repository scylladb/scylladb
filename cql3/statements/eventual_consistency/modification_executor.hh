/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include "cql3/statements/modification_executor.hh"

namespace cql3::statements::eventual_consistency {

/*
 * Commits a modification through storage_proxy, with the replication factor's
 * eventual consistency: a plain write, or a Paxos round when the modification
 * carries IF conditions.
 */
class modification_executor final : public cql3::statements::modification_executor {
public:
    future<::shared_ptr<cql_transport::messages::result_message>>
    commit(const modification_statement& stmt, query_processor& qp,
            service::query_state& qs, const query_options& options) const override;

    // Stateless, so one instance serves every statement that writes this way.
    static const modification_executor& instance();
};

}
