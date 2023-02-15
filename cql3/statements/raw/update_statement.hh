/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/statements/raw/modification_statement.hh"
#include "cql3/column_identifier.hh"
#include "cql3/operation.hh"

#include "data_dictionary/data_dictionary.hh"

#include <vector>

namespace cql3 {

namespace statements {

class update_statement;
class modification_statement;

namespace raw {

class update_statement : public raw::modification_statement {
private:
    // Provided for an UPDATE
    std::vector<std::pair<::shared_ptr<column_identifier::raw>, std::unique_ptr<operation::raw_update>>> _updates;
    expr::expression _where_clause;
public:
    /**
     * Creates a new UpdateStatement from a column family name, columns map, consistency
     * level, and key expression.
     *
     * @param name column family being operated on
     * @param attrs additional attributes for statement (timestamp, timeToLive)
     * @param updates a map of column operations to perform
     * @param whereClause the where clause
     */
    update_statement(cf_name name,
        std::unique_ptr<attributes::raw> attrs,
        std::vector<std::pair<::shared_ptr<column_identifier::raw>, std::unique_ptr<operation::raw_update>>> updates,
        expr::expression where_clause,
        std::optional<expr::expression> conditions, bool if_exists);
protected:
    virtual ::shared_ptr<cql3::statements::modification_statement> prepare_internal(data_dictionary::database db, schema_ptr schema,
                prepare_context& ctx, std::unique_ptr<attributes> attrs, cql_stats& stats) const override;
};

}

}

}
