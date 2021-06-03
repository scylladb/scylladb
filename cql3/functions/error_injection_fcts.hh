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

#include "native_scalar_function.hh"

namespace cql3
{

namespace functions
{

namespace error_injection
{

class failure_injection_function  : public native_scalar_function {
protected:
    failure_injection_function(sstring name, data_type return_type, std::vector<data_type> args_type)
            : native_scalar_function(std::move(name), std::move(return_type), std::move(args_type)) {
    }

    bool requires_thread() const override {
        return true;
    }
};

shared_ptr<function> make_enable_injection_function();
shared_ptr<function> make_disable_injection_function();
shared_ptr<function> make_enabled_injections_function();

} // namespace error_injection

} // namespace functions

} // namespace cql3
