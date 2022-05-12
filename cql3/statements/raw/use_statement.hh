/*
 * Copyright (C) 2014-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/statements/raw/parsed_statement.hh"

#include <seastar/core/sstring.hh>

namespace cql3 {

namespace statements {

class prepared_statement;

namespace raw {

class use_statement : public parsed_statement {
private:
    const sstring _keyspace;

public:
    use_statement(sstring keyspace);

    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;
};

}

}

}
