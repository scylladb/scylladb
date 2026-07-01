/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/statements/schema_altering_statement.hh"
#include "cql3/statements/create_table_statement.hh"
#include "cql3/statements/select_statement.hh"

#include <seastar/core/shared_ptr.hh>

namespace cql3 {

class query_processor;

namespace statements {

/**
 * A prepared <code>CREATE TABLE <target> (PRIMARY KEY (...)) AS SELECT ... </code>.
 *
 * CTAS is implemented as an alias for a two-step process:
 *   1. CREATE TABLE <target> with columns inferred from the inner SELECT's
 *      result metadata (types) and the user-specified primary key.
 *   2. INSERT INTO <target> SELECT ...  (the existing distributed copy).
 *
 * It is a schema_altering_statement so the framework hands it a group0 guard for
 * the schema change; execute() performs the create and then runs the copy. Like
 * INSERT...SELECT, the copy is NOT atomic: if it fails midway, the (already
 * created) target retains the rows written so far.
 */
class create_table_select_statement : public schema_altering_statement {
    // The prepared CREATE TABLE for the target (built from the SELECT's columns).
    ::shared_ptr<create_table_statement> _create_stmt;
    // The prepared inner SELECT producing the rows to copy.
    ::shared_ptr<select_statement> _source;
    bool _if_not_exists;
    uint32_t _bound_terms;
    cql_stats& _stats;

public:
    create_table_select_statement(cf_name target,
            ::shared_ptr<create_table_statement> create_stmt,
            ::shared_ptr<select_statement> source,
            bool if_not_exists,
            uint32_t bound_terms,
            cql_stats& stats);

    virtual uint32_t get_bound_terms() const override { return _bound_terms; }

    virtual future<> check_access(query_processor& qp, const service::client_state& state) const override;

    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute(query_processor& qp, service::query_state& state, const query_options& options,
            std::optional<service::group0_guard> guard) const override;

    // This is the already-prepared statement; it is produced by
    // raw::create_table_select_statement::prepare() and never re-prepared.
    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats, const cql_config& cfg) override;
};

}

}
