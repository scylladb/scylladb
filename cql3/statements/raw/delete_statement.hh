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
#include "cql3/attributes.hh"
#include "cql3/operation.hh"
#include "data_dictionary/data_dictionary.hh"

namespace cql3 {

class relation;

namespace statements {

class modification_statement;

namespace raw {

class delete_statement : public modification_statement {
private:
    std::vector<std::unique_ptr<operation::raw_deletion>> _deletions;
    expr::expression _where_clause;
public:
    delete_statement(cf_name name,
           std::unique_ptr<attributes::raw> attrs,
           std::vector<std::unique_ptr<operation::raw_deletion>> deletions,
           expr::expression where_clause,
           std::optional<expr::expression> conditions,
           bool if_exists);
protected:
    virtual ::shared_ptr<cql3::statements::modification_statement> prepare_internal(data_dictionary::database db, schema_ptr schema,
        prepare_context& ctx, std::unique_ptr<attributes> attrs, cql_stats& stats) const override;
};

}

}

}
