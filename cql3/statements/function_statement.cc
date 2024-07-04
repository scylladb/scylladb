/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "cql3/statements/function_statement.hh"
#include "cql3/functions/functions.hh"
#include "cql3/functions/user_function.hh"
#include "data_dictionary/data_dictionary.hh"
#include "gms/feature_service.hh"
#include "service/storage_proxy.hh"
#include "cql3/query_processor.hh"

namespace cql3 {
namespace statements {

future<> function_statement::check_access(query_processor& qp, const service::client_state& state) const { return make_ready_future<>(); }

future<> create_function_statement_base::check_access(query_processor& qp, const service::client_state& state) const {
    co_await state.has_functions_access(_name.keyspace, auth::permission::CREATE);
    if (_or_replace) {
        create_arg_types(qp);
        sstring encoded_signature = auth::encode_signature(_name.name, _arg_types);

        co_await state.has_function_access(_name.keyspace, encoded_signature, auth::permission::ALTER);
    }
}

future<> drop_function_statement_base::check_access(query_processor& qp, const service::client_state& state) const
{
    create_arg_types(qp);
    shared_ptr<functions::function> func;
    if (_args_present) {
        func = functions::instance().find(_name, _arg_types);
        if (!func) {
            return make_ready_future<>();
        }
    } else {
        auto funcs = functions::instance().find(_name);
        if (!funcs.empty()) {
            auto b = funcs.begin();
            auto i = b;
            if (++i != funcs.end()) {
                return make_ready_future<>();
            }
            func = b->second;
        } else {
            return make_ready_future<>();
        }
    }

    sstring encoded_signature = auth::encode_signature(_name.name, func->arg_types());

    try {
        return state.has_function_access(_name.keyspace, encoded_signature, auth::permission::DROP);
    } catch (exceptions::invalid_request_exception&) {
        return make_ready_future();
    }
}

function_statement::function_statement(
        functions::function_name name, std::vector<shared_ptr<cql3_type::raw>> raw_arg_types)
    : _name(std::move(name)), _raw_arg_types(std::move(raw_arg_types)) {}

shared_ptr<cql_transport::event::schema_change> function_statement::create_schema_change(
        const functions::function& func, bool created) {
    using namespace cql_transport;
    std::vector<sstring> options{func.name().name};
    for (const auto& type : func.arg_types()) {
        options.push_back(type->as_cql3_type().to_string());
    }
    auto change = created ? event::schema_change::change_type::CREATED : event::schema_change::change_type::DROPPED;
    auto type = dynamic_cast<const functions::user_function*>(&func) ? event::schema_change::target_type::FUNCTION
                                                                     : event::schema_change::target_type::AGGREGATE;
    return ::make_shared<event::schema_change>(change, type, func.name().keyspace, std::move(options));
}

data_type function_statement::prepare_type(query_processor& qp, cql3_type::raw& t) const {
    if (t.is_user_type() && t.is_frozen()) {
        throw exceptions::invalid_request_exception("User defined argument and return types should not be frozen");
    }
    auto&& db = qp.db();
    // At the CQL level the argument and return types should not have
    // the frozen keyword.
    // We and cassandra 3 support only frozen UDT arguments and
    // returns, so freeze it here.
    // It looks like cassandra 4 has updated the type checking to
    // allow both frozen and non frozen arguments to a UDF (which is
    // still declared without the frozen keyword).
    auto prepared = t.prepare(db, _name.keyspace).get_type();
    if (t.is_user_type()) {
        return prepared->freeze();
    }
    return prepared;
}

void function_statement::create_arg_types(query_processor& qp) const {
    if (!qp.proxy().features().user_defined_functions) {
        throw exceptions::invalid_request_exception("User defined functions are disabled. Set enable_user_defined_functions and experimental_features:udf to enable them");
    }

    if (_arg_types.empty()) {
        for (const auto& arg_type : _raw_arg_types) {
            _arg_types.push_back(prepare_type(qp, *arg_type));
        }
    }
}

void function_statement::prepare_keyspace(const service::client_state& state) {
    if (!_name.has_keyspace()) {
        _name.keyspace = state.get_keyspace();
    }
}

create_function_statement_base::create_function_statement_base(functions::function_name name,
        std::vector<shared_ptr<cql3_type::raw>> raw_arg_types, bool or_replace, bool if_not_exists)
    : function_statement(std::move(name), std::move(raw_arg_types)), _or_replace(or_replace), _if_not_exists(if_not_exists) {}

seastar::future<shared_ptr<functions::function>> create_function_statement_base::validate_while_executing(query_processor& qp) const {
    create_arg_types(qp);
    auto old = functions::instance().find(_name, _arg_types);
    if (!old || _or_replace) {
        co_return co_await create(qp, old.get());
    }
    if (!_if_not_exists) {
        throw exceptions::invalid_request_exception(format("The function '{}' already exists", old));
    }
    co_return nullptr;
}

drop_function_statement_base::drop_function_statement_base(functions::function_name name,
        std::vector<shared_ptr<cql3_type::raw>> arg_types, bool args_present, bool if_exists)
    : function_statement(std::move(name), std::move(arg_types)), _args_present(args_present), _if_exists(if_exists) {}

seastar::future<shared_ptr<db::functions::function>> drop_function_statement_base::validate_while_executing(query_processor& qp) const {
    create_arg_types(qp);
    shared_ptr<functions::function> func;
    if (_args_present) {
        func = functions::instance().find(_name, _arg_types);
        if (!func && !_if_exists) {
            throw exceptions::invalid_request_exception(format("User function {}({}) doesn't exist", _name, _arg_types));
        }
    } else {
        auto funcs = functions::instance().find(_name);
        if (!funcs.empty()) {
            auto b = funcs.begin();
            auto i = b;
            if (++i != funcs.end()) {
                throw exceptions::invalid_request_exception(format("There are multiple functions named {}", _name));
            }
            func = b->second;
        } else {
            if (!_if_exists) {
                throw exceptions::invalid_request_exception(format("No function named {} found", _name));
            }
        }
    }

    co_return func;
}
}
}
