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

#include "user_function.hh"
#include "lua.hh"
#include "log.hh"
#include "cql_serialization_format.hh"

#include <seastar/core/thread.hh>

namespace cql3 {
namespace functions {

extern logging::logger log;

user_function::user_function(function_name name, std::vector<data_type> arg_types, std::vector<sstring> arg_names,
        sstring body, sstring language, data_type return_type, bool called_on_null_input, sstring bitcode,
        lua::runtime_config cfg)
    : abstract_function(std::move(name), std::move(arg_types), std::move(return_type)),
      _arg_names(std::move(arg_names)), _body(std::move(body)), _language(std::move(language)),
      _called_on_null_input(called_on_null_input), _bitcode(std::move(bitcode)),
      _cfg(std::move(cfg)) {}

bool user_function::is_pure() const { return true; }

bool user_function::is_native() const { return false; }

bool user_function::is_aggregate() const { return false; }

bool user_function::requires_thread() const { return true; }

bytes_opt user_function::execute(cql_serialization_format sf, const std::vector<bytes_opt>& parameters) {
    const auto& types = arg_types();
    if (parameters.size() != types.size()) {
        throw std::logic_error("Wrong number of parameters");
    }

    std::vector<data_value> values;
    values.reserve(parameters.size());
    for (int i = 0, n = types.size(); i != n; ++i) {
        const data_type& type = types[i];
        const bytes_opt& bytes = parameters[i];
        if (!bytes && !_called_on_null_input) {
            return std::nullopt;
        }
        values.push_back(bytes ? type->deserialize(*bytes) : data_value::make_null(type));
    }
    if (!seastar::thread::running_in_thread()) {
        on_internal_error(log, "User function cannot be executed in this context");
    }
    return lua::run_script(lua::bitcode_view{_bitcode}, values, return_type(), _cfg).get0();
}
}
}
