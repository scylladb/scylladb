/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "cql3/statements/function_statement.hh"
#include "cql3/functions/functions.hh"
#include "db/config.hh"
#include "database.hh"
#include "gms/feature_service.hh"
#include "service/storage_proxy.hh"

namespace cql3 {
namespace statements {

future<> function_statement::check_access(service::storage_proxy& proxy, const service::client_state& state) const { return make_ready_future<>(); }

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

data_type function_statement::prepare_type(service::storage_proxy& proxy, cql3_type::raw& t) const {
    if (t.is_user_type() && t.is_frozen()) {
        throw exceptions::invalid_request_exception("User defined argument and return types should not be frozen");
    }
    auto&& db = proxy.get_db().local();
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

void function_statement::create_arg_types(service::storage_proxy& proxy) const {
    if (!proxy.features().cluster_supports_user_defined_functions()) {
        throw exceptions::invalid_request_exception("User defined functions are disabled. Set enable_user_defined_functions and experimental_features:udf to enable them");
    }

    if (_arg_types.empty()) {
        for (const auto& arg_type : _raw_arg_types) {
            _arg_types.push_back(prepare_type(proxy, *arg_type));
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

void create_function_statement_base::validate(service::storage_proxy& proxy, const service::client_state& state) const {
    create_arg_types(proxy);
    auto old = functions::functions::find(_name, _arg_types);
    if (!old || _or_replace) {
        create(proxy, old.get());
        return;
    }
    if (!_if_not_exists) {
        throw exceptions::invalid_request_exception(format("The function '{}' already exists", old));
    }
}

drop_function_statement_base::drop_function_statement_base(functions::function_name name,
        std::vector<shared_ptr<cql3_type::raw>> arg_types, bool args_present, bool if_exists)
    : function_statement(std::move(name), std::move(arg_types)), _args_present(args_present), _if_exists(if_exists) {}

void drop_function_statement_base::validate(service::storage_proxy& proxy, const service::client_state& state) const {
    create_arg_types(proxy);
    if (_args_present) {
        _func = functions::functions::find(_name, _arg_types);
        if (!_func && !_if_exists) {
            throw exceptions::invalid_request_exception(format("User function {}({}) doesn't exist", _name, _arg_types));
        }
    } else {
        auto funcs = functions::functions::find(_name);
        if (funcs.empty()) {
            if (!_if_exists) {
                throw exceptions::invalid_request_exception(format("No function named {} found", _name));
            }
            return;
        }
        auto b = funcs.begin();
        auto i = b;
        if (++i != funcs.end()) {
            throw exceptions::invalid_request_exception(format("There are multiple functions named {}", _name));
        }
        _func = b->second;
    }
}
}
}
