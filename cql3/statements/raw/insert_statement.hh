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
#include "cql3/column_identifier.hh"
#include "cql3/expr/expression.hh"
#include "data_dictionary/data_dictionary.hh"

#include <vector>

namespace cql3 {

namespace statements {

class modification_statement;

namespace raw {

class insert_statement : public raw::modification_statement {
private:
    const std::vector<::shared_ptr<column_identifier::raw>> _column_names;
    const std::vector<expr::expression> _column_values;
public:
    /**
     * A parsed <code>INSERT</code> statement.
     *
     * @param name column family being operated on
     * @param columnNames list of column names
     * @param columnValues list of column values (corresponds to names)
     * @param attrs additional attributes for statement (CL, timestamp, timeToLive)
     */
    insert_statement(cf_name name,
                  std::unique_ptr<attributes::raw> attrs,
                  std::vector<::shared_ptr<column_identifier::raw>> column_names,
                  std::vector<expr::expression> column_values,
                  bool if_not_exists);

    virtual ::shared_ptr<cql3::statements::modification_statement> prepare_internal(data_dictionary::database db, schema_ptr schema,
                prepare_context& ctx, std::unique_ptr<attributes> attrs, cql_stats& stats) const override;

};

class insert_json_statement : public raw::modification_statement {
private:
    cf_name _name;
    expr::expression _json_value;
    bool _if_not_exists;
    bool _default_unset;
public:
    /**
     * A parsed <code>INSERT JSON</code> statement.
     *
     * @param name column family being operated on
     * @param json_value JSON string representing names and values
     * @param attrs additional attributes for statement (CL, timestamp, timeToLive)
     */
    insert_json_statement(cf_name name, std::unique_ptr<attributes::raw> attrs, expr::expression json_value, bool if_not_exists, bool default_unset);

    virtual ::shared_ptr<cql3::statements::modification_statement> prepare_internal(data_dictionary::database db, schema_ptr schema,
                prepare_context& ctx, std::unique_ptr<attributes> attrs, cql_stats& stats) const override;

};

}

}

}
