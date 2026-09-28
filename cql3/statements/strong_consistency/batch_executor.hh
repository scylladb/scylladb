/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/statements/batch_executor.hh"

namespace cql3::statements::strong_consistency {

/*
 * Commits a BATCH through the Raft group which owns the partition it addresses.
 *
 * Every modification in the batch has to name the same partition of the same
 * table, because the group owns one partition and the batch commits one merged
 * mutation to it.
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
