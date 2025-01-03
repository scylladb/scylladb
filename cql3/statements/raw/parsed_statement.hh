/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "data_dictionary/data_dictionary.hh"
#include "cql3/prepare_context.hh"
#include "cql3/column_specification.hh"

#include <seastar/core/shared_ptr.hh>

#include <vector>
#include "audit/audit.hh"

namespace cql3 {

class column_identifier;
class cql_stats;

namespace statements {

class prepared_statement;

namespace raw {

class parsed_statement {
protected:
    prepare_context _prepare_ctx;

public:
    virtual ~parsed_statement();

    prepare_context& get_prepare_context();
    const prepare_context& get_prepare_context() const;

    void set_bound_variables(const std::vector<::shared_ptr<column_identifier>>& bound_names);

    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) = 0;

protected:
    virtual audit::statement_category category() const = 0;
    virtual audit::audit_info_ptr audit_info() const = 0;
};

}

}

}
