/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "data_dictionary/data_dictionary.hh"
#include "cql3/prepare_context.hh"
#include "cql3/column_specification.hh"

#include <seastar/core/shared_ptr.hh>

#include <vector>

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
};

}

}

}
