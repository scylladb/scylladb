/*
 * Copyright (C) 2019 ScyllaDB
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

namespace cql3 {
namespace statements {

future<> function_statement::check_access(const service::client_state& state) { return make_ready_future<>(); }

function_statement::function_statement(
        functions::function_name name, std::vector<shared_ptr<cql3_type::raw>> raw_arg_types)
    : _name(std::move(name)), _raw_arg_types(std::move(raw_arg_types)) {}

create_function_statement_base::create_function_statement_base(functions::function_name name,
        std::vector<shared_ptr<cql3_type::raw>> raw_arg_types, bool or_replace, bool if_not_exists)
    : function_statement(std::move(name), std::move(raw_arg_types)), _or_replace(or_replace), _if_not_exists(if_not_exists) {}

void create_function_statement_base::validate(service::storage_proxy& proxy, const service::client_state& state) {}

drop_function_statement_base::drop_function_statement_base(functions::function_name name,
        std::vector<shared_ptr<cql3_type::raw>> arg_types, bool args_present, bool if_exists)
    : function_statement(std::move(name), std::move(arg_types)), _args_present(args_present), _if_exists(if_exists) {}

void drop_function_statement_base::validate(service::storage_proxy& proxy, const service::client_state& state) {}

}
}
