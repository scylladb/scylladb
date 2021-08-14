/*
 * Copyright (C) 2021-present ScyllaDB
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
#include "aggregate_function.hh"

namespace cql3 {
namespace functions {

class user_aggregate : public abstract_function, public aggregate_function{
    bytes_opt _initcond;
    ::shared_ptr<scalar_function> _sfunc;
    ::shared_ptr<scalar_function> _finalfunc;
public:
    user_aggregate(function_name fname, bytes_opt initcond, ::shared_ptr<scalar_function> sfunc, ::shared_ptr<scalar_function> finalfunc);
    virtual std::unique_ptr<aggregate_function::aggregate> new_aggregate() override;
    virtual bool is_pure() const override;
    virtual bool is_native() const override;
    virtual bool is_aggregate() const override;
    virtual bool requires_thread() const override;

    const scalar_function& sfunc() const {
        return *_sfunc;
    }
    const scalar_function& finalfunc() const {
        return *_finalfunc;
    }
    const bytes_opt& initcond() const {
        return _initcond;
    }
};

}
}
