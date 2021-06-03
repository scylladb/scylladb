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

#include "cql3/statements/create_function_statement.hh"
#include "cql3/functions/functions.hh"
#include "prepared_statement.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "lua.hh"
#include "database.hh"
#include "cql3/query_processor.hh"

namespace cql3 {

namespace statements {

void create_function_statement::create(service::storage_proxy& proxy, functions::function* old) const {
    if (old && !dynamic_cast<functions::user_function*>(old)) {
        throw exceptions::invalid_request_exception(format("Cannot replace '{}' which is not a user defined function", *old));
    }
    if (_language != "lua") {
        throw exceptions::invalid_request_exception(format("Language '{}' is not supported", _language));
    }
    data_type return_type = prepare_type(proxy, *_return_type);
    std::vector<sstring> arg_names;
    for (const auto& arg_name : _arg_names) {
        arg_names.push_back(arg_name->to_string());
    }

    auto&& db = proxy.get_db().local();
    lua::runtime_config cfg = lua::make_runtime_config(db.get_config());

    // Checking that the function compiles also produces bitcode
    auto bitcode = lua::compile(cfg, arg_names, _body);

    _func = ::make_shared<functions::user_function>(_name, _arg_types, std::move(arg_names), _body, _language,
        std::move(return_type), _called_on_null_input, std::move(bitcode), std::move(cfg));
    return;
}

std::unique_ptr<prepared_statement> create_function_statement::prepare(database& db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<create_function_statement>(*this));
}

future<shared_ptr<cql_transport::event::schema_change>> create_function_statement::announce_migration(
        query_processor& qp) const {
    if (!_func) {
        return make_ready_future<::shared_ptr<cql_transport::event::schema_change>>();
    }
    return qp.get_migration_manager().announce_new_function(_func).then([this] {
        return create_schema_change(*_func, true);
    });
}

create_function_statement::create_function_statement(functions::function_name name, sstring language, sstring body,
        std::vector<shared_ptr<column_identifier>> arg_names, std::vector<shared_ptr<cql3_type::raw>> arg_types,
        shared_ptr<cql3_type::raw> return_type, bool called_on_null_input, bool or_replace, bool if_not_exists)
    : create_function_statement_base(std::move(name), std::move(arg_types), or_replace, if_not_exists),
      _language(std::move(language)), _body(std::move(body)), _arg_names(std::move(arg_names)),
      _return_type(std::move(return_type)), _called_on_null_input(called_on_null_input) {}
}
}
