/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/statements/raw/cf_statement.hh"
#include "cql3/attributes.hh"
#include "cql3/cql_statement.hh"

#include <seastar/core/shared_ptr.hh>


#include <memory>

namespace cql3 {

namespace statements {

class modification_statement;

namespace raw {

class modification_statement : public cf_statement {
protected:
    const std::unique_ptr<attributes::raw> _attrs;
    const std::optional<expr::expression> _conditions;
private:
    const bool _if_not_exists;
    const bool _if_exists;
protected:
    modification_statement(cf_name name, std::unique_ptr<attributes::raw> attrs, std::optional<expr::expression> conditions = {}, bool if_not_exists = false, bool if_exists = false);

public:
    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;
    ::shared_ptr<cql_statement_opt_metadata> prepare_statement(data_dictionary::database db, prepare_context& ctx, cql_stats& stats);
    ::shared_ptr<cql3::statements::modification_statement> prepare(data_dictionary::database db, prepare_context& ctx, cql_stats& stats) const;
protected:
    virtual ::shared_ptr<cql3::statements::modification_statement> prepare_internal(data_dictionary::database db, schema_ptr schema,
        prepare_context& ctx, std::unique_ptr<attributes> attrs, cql_stats& stats) const = 0;

    // Helper function used by child classes to prepare conditions for a prepared statement.
    // Must be called before processing WHERE clause, because to perform sanity checks there
    // we need to know what kinds of conditions (static, regular) the statement has.
    void prepare_conditions(data_dictionary::database db, const schema& schema, prepare_context& ctx,
            cql3::statements::modification_statement& stmt) const;
};

}

}

}
