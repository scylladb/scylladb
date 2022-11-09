/*
 * Copyright (C) 2022-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/statements/raw/cf_statement.hh"
#include "cql3/attributes.hh"

namespace cql3 {

namespace statements {

namespace raw {

class truncate_statement : public raw::cf_statement {
private:
    std::unique_ptr<attributes::raw> _attrs;
public:
    /**
     * Creates a new truncate_statement from a column family name, and attributes.
     *
     * @param name column family being operated on
     * @param attrs additional attributes for statement (timeout)
     */
    truncate_statement(cf_name name, std::unique_ptr<attributes::raw> attrs);

    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;
};

} // namespace raw

} // namespace statements

} // namespace cql3
