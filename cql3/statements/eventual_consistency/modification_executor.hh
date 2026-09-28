/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include "cql3/statements/modification_spec.hh"
#include "cql3/statements/modification_executor.hh"
#include "db/timeout_clock.hh"
#include "mutation/mutation.hh"
#include "utils/chunked_vector.hh"

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

/*
 * The mutations a modification produces, reading the old row through
 * storage_proxy first when it needs one - an IF condition, or an operation on a
 * list entry.
 *
 * Free, because a batch builds and merges the mutations of the modifications it
 * holds without ever committing any of them one at a time, and because the
 * query processor uses it to turn an internal statement into mutations it then
 * writes itself.
 *
 * @param local if true, any requests (for collections) performed should be done locally only.
 * @param now the current timestamp in microseconds to use if no timestamp is user provided.
 */
future<utils::chunked_vector<mutation>> get_mutations(const modification_statement& stmt,
        query_processor& qp, const query_options& options, db::timeout_clock::time_point timeout,
        bool local, int64_t now, service::query_state& qs, modification_spec&& spec);

}
