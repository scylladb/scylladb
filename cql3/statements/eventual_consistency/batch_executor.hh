/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/statements/batch_executor.hh"

namespace cql3::statements::eventual_consistency {

/*
 * Commits a BATCH through storage_proxy: the merged mutations of what it holds,
 * written atomically when the batch is LOGGED, or a Paxos round when any of its
 * modifications carries IF conditions.
 */
class batch_executor final : public cql3::statements::batch_executor {
public:
    future<::shared_ptr<cql_transport::messages::result_message>>
    commit(const batch_statement& batch, query_processor& qp, service::query_state& qs,
            const query_options& options, bool local, api::timestamp_type now) const override;

    void validate(const batch_statement& batch) const override;

    // Stateless, so one instance serves every batch that writes this way.
    static const batch_executor& instance();
};

}
