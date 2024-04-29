/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "cql3/statements/function_statement.hh"
#include "cql3/cql3_type.hh"

namespace cql3 {

class query_processor;

namespace functions {
    class user_function;
}

namespace statements {

class create_function_statement final : public create_function_statement_base {
    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;
    future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>> prepare_schema_mutations(query_processor& qp, const query_options& options, api::timestamp_type) const override;

    virtual seastar::future<shared_ptr<db::functions::function>> create(query_processor& qp, db::functions::function* old) const override;
    sstring _language;
    sstring _body;
    std::vector<shared_ptr<column_identifier>> _arg_names;
    shared_ptr<cql3_type::raw> _return_type;
    bool _called_on_null_input;

public:
    create_function_statement(functions::function_name name, sstring language, sstring body,
            std::vector<shared_ptr<column_identifier>> arg_names, std::vector<shared_ptr<cql3_type::raw>> arg_types,
            shared_ptr<cql3_type::raw> return_type, bool called_on_null_input, bool or_replace, bool if_not_exists);
};
}
}
