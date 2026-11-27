/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include "data_dictionary/data_dictionary.hh"
#include "cql3/prepare_context.hh"
#include "cql3/column_specification.hh"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include <memory>
#include <vector>
#include "audit/audit.hh"
#include "utils/growth_counter.hh"

namespace cql3 {

class column_identifier;
class cql_stats;
class cql_config;

namespace statements {

class prepared_statement;

// What parsed_statement::prepare() returns.
struct prepare_result {
    std::unique_ptr<prepared_statement> prepared;
    // The bytes that prepare() allocated. The parsing cost tracker counts
    // them, and its caller cannot: other tasks allocate while a prepare()
    // yields.
    uint64_t allocated_bytes;
};

// Counts the bytes allocated since it was constructed. For a prepare() that
// does not yield, which declares it first and returns through result().
class prepare_allocations {
    utils::total_bytes_allocated_counter _allocated;
public:
    future<prepare_result> result(std::unique_ptr<prepared_statement> statement) const;
};

namespace raw {

class parsed_statement {
protected:
    prepare_context _prepare_ctx;

public:
    virtual ~parsed_statement();

    prepare_context& get_prepare_context();
    const prepare_context& get_prepare_context() const;

    void set_bound_variables(const std::vector<::shared_ptr<column_identifier>>& bound_names, dialect d);

    // Returns the statement and the bytes that preparing it allocated. Only
    // a statement that is slow to prepare yields.
    virtual future<prepare_result> prepare(data_dictionary::database db, cql_stats& stats, const cql_config& cfg) = 0;

protected:
    virtual audit::statement_category category() const = 0;
    virtual audit::audit_info_ptr audit_info() const = 0;
};

}

}

}
