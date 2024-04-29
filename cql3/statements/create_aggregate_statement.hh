/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "cql3/statements/function_statement.hh"
#include "cql3/cql3_type.hh"
#include "cql3/expr/expression.hh"
#include <optional>

namespace cql3 {

class query_processor;

namespace functions {
    class user_aggregate;
}

namespace statements {

class create_aggregate_statement final : public create_function_statement_base {
    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats) override;
    future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql3::cql_warnings_vec>> prepare_schema_mutations(query_processor& qp, const query_options& options, api::timestamp_type) const override;
    virtual future<> check_access(query_processor& qp, const service::client_state& state) const override;

    virtual seastar::future<shared_ptr<db::functions::function>> create(query_processor& qp, db::functions::function* old) const override;

    sstring _sfunc;
    shared_ptr<cql3_type::raw> _stype;
    std::optional<sstring> _rfunc;
    std::optional<sstring> _ffunc;
    std::optional<expr::expression> _ival;

public:
    create_aggregate_statement(functions::function_name name, std::vector<shared_ptr<cql3_type::raw>> arg_types,
            sstring sfunc, shared_ptr<cql3_type::raw> stype, std::optional<sstring> rfunc, std::optional<sstring> ffunc, std::optional<expr::expression> ival, bool or_replace, bool if_not_exists);
};
}
}
