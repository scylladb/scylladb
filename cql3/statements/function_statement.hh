/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "cql3/statements/schema_altering_statement.hh"
#include "cql3/functions/function_name.hh"
#include "cql3/cql3_type.hh"

namespace db {
namespace functions {
    class function;
}
}

namespace cql3 {

namespace statements {

class function_statement : public schema_altering_statement {
protected:
    virtual future<> check_access(query_processor& qp, const service::client_state& state) const override;
    virtual void prepare_keyspace(const service::client_state& state) override;
    db::functions::function_name _name;
    std::vector<shared_ptr<cql3_type::raw>> _raw_arg_types;
    mutable std::vector<data_type> _arg_types;
    static shared_ptr<cql_transport::event::schema_change> create_schema_change(
            const db::functions::function& func, bool created);
    function_statement(functions::function_name name, std::vector<shared_ptr<cql3_type::raw>> raw_arg_types);
    void create_arg_types(query_processor& qp) const;
    data_type prepare_type(query_processor& qp, cql3_type::raw &t) const;
    virtual seastar::future<shared_ptr<db::functions::function>> validate_while_executing(query_processor&) const = 0;
};

// common logic for creating UDF and UDA
class create_function_statement_base : public function_statement {
protected:
    virtual seastar::future<shared_ptr<db::functions::function>> create(query_processor& qp, db::functions::function* old) const = 0;
    virtual seastar::future<shared_ptr<db::functions::function>> validate_while_executing(query_processor&) const override;

    bool _or_replace;
    bool _if_not_exists;

    create_function_statement_base(functions::function_name name, std::vector<shared_ptr<cql3_type::raw>> raw_arg_types,
            bool or_replace, bool if_not_exists);

public:
    virtual future<> check_access(query_processor& qp, const service::client_state& state) const override;
};

// common logic for dropping UDF and UDA
class drop_function_statement_base : public function_statement {
protected:
    virtual seastar::future<shared_ptr<db::functions::function>> validate_while_executing(query_processor&) const override;

    bool _args_present;
    bool _if_exists;

    drop_function_statement_base(functions::function_name name, std::vector<shared_ptr<cql3_type::raw>> arg_types,
            bool args_present, bool if_exists);

public:
    virtual future<> check_access(query_processor& qp, const service::client_state& state) const override;
};

}
}
