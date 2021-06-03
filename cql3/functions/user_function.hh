/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
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

#include "abstract_function.hh"
#include "scalar_function.hh"
#include "lua.hh"

namespace cql3 {
namespace functions {

class user_function final : public abstract_function, public scalar_function {
    std::vector<sstring> _arg_names;
    sstring _body;
    sstring _language;
    bool _called_on_null_input;
    sstring _bitcode;

    // FIXME: We should not need a copy in each function. It is here
    // because user_function::execute is only passed the
    // cql_serialization_format and the runtime arguments.  We could
    // avoid it by having a runtime->execute(user_function) instead,
    // but that is a large refactoring. We could also store a
    // lua_runtime in a thread_local variable, but that is one extra
    // global.
    lua::runtime_config _cfg;

public:
    user_function(function_name name, std::vector<data_type> arg_types, std::vector<sstring> arg_names, sstring body,
            sstring language, data_type return_type, bool called_on_null_input, sstring bitcode,
            lua::runtime_config cfg);

    const std::vector<sstring>& arg_names() const { return _arg_names; }

    const sstring& body() const { return _body; }

    const sstring& language() const { return _language; }

    bool called_on_null_input() const { return _called_on_null_input; }

    virtual bool is_pure() const override;
    virtual bool is_native() const override;
    virtual bool is_aggregate() const override;
    virtual bool requires_thread() const override;
    virtual bytes_opt execute(cql_serialization_format sf, const std::vector<bytes_opt>& parameters) override;
};

}
}
