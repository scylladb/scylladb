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

#pragma once

#include "cql3/statements/schema_altering_statement.hh"

namespace cql3 {
namespace statements {

class function_statement : public schema_altering_statement {
protected:
    virtual future<> check_access(const service::client_state& state) override;
    functions::function_name _name;
    std::vector<shared_ptr<cql3_type::raw>> _raw_arg_types;
    function_statement(functions::function_name name, std::vector<shared_ptr<cql3_type::raw>> raw_arg_types);
};

// common logic for creating UDF and UDA
class create_function_statement_base : public function_statement {
protected:
    virtual void validate(service::storage_proxy& proxy, const service::client_state& state) override;

    bool _or_replace;
    bool _if_not_exists;

    create_function_statement_base(functions::function_name name, std::vector<shared_ptr<cql3_type::raw>> raw_arg_types,
            bool or_replace, bool if_not_exists);
};

// common logic for dropping UDF and UDA
class drop_function_statement_base : public function_statement {
protected:
    virtual void validate(service::storage_proxy&, const service::client_state& state) override;

    bool _args_present;
    bool _if_exists;

    drop_function_statement_base(functions::function_name name, std::vector<shared_ptr<cql3_type::raw>> arg_types,
            bool args_present, bool if_exists);
};

}
}
