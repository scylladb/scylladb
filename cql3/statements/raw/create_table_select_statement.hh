/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/statements/raw/cf_statement.hh"
#include "cql3/statements/raw/select_statement.hh"
#include "cql3/column_identifier.hh"
#include "data_dictionary/data_dictionary.hh"

#include <memory>
#include <vector>

namespace cql3 {

namespace statements {

namespace raw {

/**
 * A parsed <code>CREATE TABLE [IF NOT EXISTS] <target> (PRIMARY KEY (...)) AS
 * SELECT ... </code> statement.
 *
 * Prepared into a cql3::statements::create_table_select_statement, which creates
 * the target table (columns inferred from the inner SELECT's result metadata,
 * primary key as specified here) and then runs INSERT INTO <target> SELECT ...
 */
class create_table_select_statement : public raw::cf_statement {
private:
    bool _if_not_exists;
    // Primary-key columns, by name, referencing columns of the SELECT result.
    std::vector<::shared_ptr<column_identifier>> _partition_keys;
    std::vector<::shared_ptr<column_identifier>> _clustering_keys;
    // The inner SELECT producing the rows (and the column types) to materialize.
    std::unique_ptr<raw::select_statement> _source;
public:
    create_table_select_statement(cf_name target,
                  bool if_not_exists,
                  std::vector<::shared_ptr<column_identifier>> partition_keys,
                  std::vector<::shared_ptr<column_identifier>> clustering_keys,
                  std::unique_ptr<raw::select_statement> source);

    // The grammar nests the source SELECT inside this statement, so forward the
    // connection keyspace to it as well (see raw::insert_select_statement).
    virtual void prepare_keyspace(const service::client_state& state) override;

    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats, const cql_config& cfg) override;

protected:
    virtual audit::statement_category category() const override;
};

}

}

}
