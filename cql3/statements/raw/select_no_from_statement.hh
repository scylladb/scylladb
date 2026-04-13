/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "cql3/statements/raw/parsed_statement.hh"
#include "cql3/selection/raw_selector.hh"
#include <seastar/core/shared_ptr.hh>
#include <vector>

namespace cql3 {

namespace statements {

namespace raw {

/**
 * A SELECT statement without a FROM clause, e.g. SELECT 1, now(), toTimestamp(now()).
 * Only expressions that don't reference table columns are allowed.
 */
class select_no_from_statement : public parsed_statement {
    std::vector<::shared_ptr<cql3::selection::raw_selector>> _select_clause;
    sstring _keyspace;

public:
    select_no_from_statement(std::vector<::shared_ptr<cql3::selection::raw_selector>> select_clause);

    void set_keyspace(sstring ks) {
        _keyspace = std::move(ks);
    }

    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;

protected:
    virtual audit::statement_category category() const override;
    virtual audit::audit_info_ptr audit_info() const override;
};

}

}

}
