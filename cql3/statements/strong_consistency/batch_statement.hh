/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/statements/batch_statement.hh"

namespace cql3::statements::strong_consistency {

/*
 * Turns a batch into a strongly consistent one, by merging the mutations of
 * its statements into a single mutation and committing it through Raft instead
 * of storage_proxy. Everything else - parsing state, mutation building, access
 * control - is inherited unchanged.
 *
 * Merging into one mutation is what makes the batch atomic, and is also why
 * every statement in it has to target the same partition.
 */
class batch_statement final : public cql3::statements::batch_statement {
public:
    batch_statement(int bound_terms, type type_, std::vector<single_statement> statements,
            std::unique_ptr<attributes> attrs, cql_stats& stats);

    batch_statement(type type_, std::vector<single_statement> statements,
            std::unique_ptr<attributes> attrs, cql_stats& stats);

protected:
    future<shared_ptr<cql_transport::messages::result_message>> do_execute(
            query_processor& qp, service::query_state& query_state, const query_options& options,
            bool local, api::timestamp_type now) const override;

private:
    // Rejects what the strongly consistent write path cannot honour. Runs from
    // the constructor rather than as an override of the base validate(), which
    // the base constructor calls before this class exists.
    void validate_strongly_consistent() const;
};

}
