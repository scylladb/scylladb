/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/statements/modification_spec.hh"
#include "cql3/statements/modification_executor.hh"
#include "mutation/mutation.hh"
#include "mutation/timestamp.hh"

namespace cql3::statements::strong_consistency {

/*
 * Commits a modification through the Raft group which owns the partition it
 * addresses, rather than through storage_proxy.
 */
class modification_executor final : public cql3::statements::modification_executor {
public:
    future<::shared_ptr<cql_transport::messages::result_message>>
    commit(const modification_statement& stmt, query_processor& qp,
            service::query_state& qs, const query_options& options) const override;

    const cql3::statements::batch_executor& for_batch() const override;

    // Stateless, so one instance serves every statement that writes this way.
    static const modification_executor& instance();
};

// The single mutation a strongly consistent modification produces, for the given
// timestamp. Shared by single modifications and batches, which build one per
// modification and merge them.
mutation get_mutation(const modification_statement& stmt, const query_options& options,
        api::timestamp_type ts, const modification_spec& spec);

}
