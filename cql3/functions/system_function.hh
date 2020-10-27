/*
 * Copyright (C) 2020 ScyllaDB
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

#include "native_scalar_function.hh"

namespace cql3::functions
{

// system function is a native function declared in system.*
// keyspace.
class system_function : public native_scalar_function {
    std::function<future<sstring>()> _func;
public:
    system_function(sstring name, data_type return_type, std::vector<data_type> args_type, std::function<future<sstring>()> func)
            : native_scalar_function(std::move(name), std::move(return_type), std::move(args_type)) 
            , _func(std::move(func))
    {}

protected:
    bool requires_thread() const override {
        return true;
    }

    bool is_pure() const override {
        return false;
    }
    bytes_opt execute(cql_serialization_format sf, const std::vector<bytes_opt>& parameters) override;
};

} // end of namespace cql3::functions::system
