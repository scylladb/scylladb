/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "cql3/statements/raw/cf_statement.hh"
#include "cql3/attributes.hh"

namespace cql3 {

namespace statements {

namespace raw {

class rebuild_materialized_view_statement : public raw::cf_statement {
    expr::expression _where_clause;
    std::unique_ptr<attributes::raw> _attrs;

public:
    rebuild_materialized_view_statement(cf_name name, expr::expression where_clause, std::unique_ptr<attributes::raw> attrs);

    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;

protected:
    virtual audit::statement_category category() const override;
};

}

}

}
