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

#pragma once

#include "cql3/statements/schema_altering_statement.hh"
#include "cql3/functions/user_function.hh"

namespace cql3 {
namespace statements {

class function_statement : public schema_altering_statement {
protected:
    virtual future<> check_access(service::storage_proxy& proxy, const service::client_state& state) const override;
    virtual void prepare_keyspace(const service::client_state& state) override;
    functions::function_name _name;
    std::vector<shared_ptr<cql3_type::raw>> _raw_arg_types;
    mutable std::vector<data_type> _arg_types;
    static shared_ptr<cql_transport::event::schema_change> create_schema_change(
            const functions::function& func, bool created);
    function_statement(functions::function_name name, std::vector<shared_ptr<cql3_type::raw>> raw_arg_types);
    void create_arg_types(service::storage_proxy& proxy) const;
    data_type prepare_type(service::storage_proxy& proxy, cql3_type::raw &t) const;
};

// common logic for creating UDF and UDA
class create_function_statement_base : public function_statement {
protected:
    virtual void validate(service::storage_proxy& proxy, const service::client_state& state) const override;
    virtual void create(service::storage_proxy& proxy, functions::function* old) const = 0;

    bool _or_replace;
    bool _if_not_exists;

    create_function_statement_base(functions::function_name name, std::vector<shared_ptr<cql3_type::raw>> raw_arg_types,
            bool or_replace, bool if_not_exists);
};

// common logic for dropping UDF and UDA
class drop_function_statement_base : public function_statement {
protected:
    virtual void validate(service::storage_proxy&, const service::client_state& state) const override;

    bool _args_present;
    bool _if_exists;

    mutable shared_ptr<functions::function> _func{};

    drop_function_statement_base(functions::function_name name, std::vector<shared_ptr<cql3_type::raw>> arg_types,
            bool args_present, bool if_exists);
};

}
}
