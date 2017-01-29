/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Modified by ScyllaDB
 *
 * Copyright (C) 2015 ScyllaDB
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

#include "function.hh"
#include "scalar_function.hh"
#include "cql3/term.hh"
#include "exceptions/exceptions.hh"

namespace cql3 {
namespace functions {

class function_call : public non_terminal {
    const shared_ptr<scalar_function> _fun;
    const std::vector<shared_ptr<term>> _terms;
public:
    function_call(shared_ptr<scalar_function> fun, std::vector<shared_ptr<term>> terms)
            : _fun(std::move(fun)), _terms(std::move(terms)) {
    }
    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const override;
    virtual void collect_marker_specification(shared_ptr<variable_specifications> bound_names) override;
    virtual shared_ptr<terminal> bind(const query_options& options) override;
    virtual cql3::raw_value_view bind_and_get(const query_options& options) override;
private:
    static bytes_opt execute_internal(cql_serialization_format sf, scalar_function& fun, std::vector<bytes_opt> params);
public:
    virtual bool contains_bind_marker() const override;
private:
    static shared_ptr<terminal> make_terminal(shared_ptr<function> fun, cql3::raw_value result, cql_serialization_format sf);
public:
    class raw : public term::raw {
        function_name _name;
        std::vector<shared_ptr<term::raw>> _terms;
    public:
        raw(function_name name, std::vector<shared_ptr<term::raw>> terms)
            : _name(std::move(name)), _terms(std::move(terms)) {
        }
        virtual ::shared_ptr<term> prepare(database& db, const sstring& keyspace, ::shared_ptr<column_specification> receiver) override;
    private:
        // All parameters must be terminal
        static bytes_opt execute(scalar_function& fun, std::vector<shared_ptr<term>> parameters);
    public:
        virtual assignment_testable::test_result test_assignment(database& db, const sstring& keyspace, shared_ptr<column_specification> receiver) override;
        virtual sstring to_string() const override;
    };
};

}
}
