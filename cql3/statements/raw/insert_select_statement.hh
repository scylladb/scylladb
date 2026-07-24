/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/statements/raw/modification_statement.hh"
#include "cql3/statements/raw/select_statement.hh"
#include "cql3/column_identifier.hh"
#include "data_dictionary/data_dictionary.hh"

#include <memory>
#include <vector>

namespace cql3 {

namespace statements {

namespace raw {

/**
 * A parsed <code>INSERT INTO <target> [(cols...)] SELECT ... FROM <source> ...</code>
 * statement.
 *
 * Unlike a plain INSERT, this statement does not target a single, fully
 * specified partition: it copies the (potentially unbounded) result set of an
 * inner SELECT into the target table. It is therefore prepared into a
 * dedicated, non-modification cql_statement that runs a distributed, paged
 * read->write job rather than a single mutation. See
 * cql3::statements::insert_select_statement.
 *
 * It derives from raw::modification_statement only so it can flow through the
 * grammar's insertStatement rule (which returns a modification_statement). The
 * modification-specific prepare path (prepare_internal) is intentionally
 * disabled so that the statement cannot be smuggled into a BATCH, where its
 * non-atomic, multi-partition semantics would be unsound.
 */
class insert_select_statement : public raw::modification_statement {
private:
    // Explicit target column list, in INSERT order. Empty means "all source
    // columns, matched positionally against the target's columns".
    const std::vector<::shared_ptr<column_identifier::raw>> _target_columns;
    // The inner SELECT producing the rows to insert.
    std::unique_ptr<raw::select_statement> _source;
public:
    insert_select_statement(cf_name target,
                  std::unique_ptr<attributes::raw> attrs,
                  std::vector<::shared_ptr<column_identifier::raw>> target_columns,
                  std::unique_ptr<raw::select_statement> source);

    // The grammar nests the source SELECT inside this statement, so the query
    // processor only resolves the *target* keyspace via prepare_keyspace().
    // Forward it to the inner SELECT as well, so an unqualified source table
    // (e.g. under USE <ks>) resolves against the connection keyspace instead of
    // hitting the has_keyspace() assertion at prepare time.
    virtual void prepare_keyspace(const service::client_state& state) override;

    // Top-level prepare: produces the distributed insert-select cql_statement.
    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats, const cql_config& cfg) override;

protected:
    // Disabled: INSERT ... SELECT cannot appear inside a BATCH and is never
    // prepared as a single-partition modification.
    virtual ::shared_ptr<cql3::statements::modification_statement> prepare_internal(data_dictionary::database db, schema_ptr schema,
                prepare_context& ctx, std::unique_ptr<attributes> attrs, cql_stats& stats) const override;
};

}

}

}
