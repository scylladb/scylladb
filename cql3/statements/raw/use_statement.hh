/*
 * Copyright (C) 2014-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
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
protected:
    virtual audit::statement_category category() const override;
    virtual audit::audit_info_ptr audit_info() const override {
        return audit::audit::create_audit_info(category(), _keyspace, sstring());
    }
};

}

}

}
