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
 * Copyright 2014 Cloudius Systems
 *
 * Modified by Cloudius Systems
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

#include "cql3/variable_specifications.hh"
#include "cql3/column_specification.hh"
#include "cql3/column_identifier.hh"
#include "cql3/cql_statement.hh"

#include "core/shared_ptr.hh"

#include <experimental/optional>
#include <vector>

namespace cql3 {

namespace statements {

class parsed_statement {
private:
    ::shared_ptr<variable_specifications> _variables;

public:
    virtual ~parsed_statement()
    { }

    shared_ptr<variable_specifications> get_bound_variables() {
        return _variables;
    }

    // Used by the parser and preparable statement
    void set_bound_variables(const std::vector<::shared_ptr<column_identifier>>& bound_names)
    {
        _variables = ::make_shared<variable_specifications>(bound_names);
    }

    class prepared {
    public:
        const ::shared_ptr<cql_statement> statement;
        const std::vector<::shared_ptr<column_specification>> bound_names;

        prepared(::shared_ptr<cql_statement> statement_, std::vector<::shared_ptr<column_specification>> bound_names_)
            : statement(std::move(statement_))
            , bound_names(std::move(bound_names_))
        { }

        prepared(::shared_ptr<cql_statement> statement_, const variable_specifications& names)
            : prepared(statement_, names.get_specifications())
        { }

        prepared(::shared_ptr<cql_statement> statement_, variable_specifications&& names)
            : prepared(statement_, std::move(names).get_specifications())
        { }

        prepared(::shared_ptr<cql_statement>&& statement_)
            : prepared(statement_, std::vector<::shared_ptr<column_specification>>())
        { }
    };

    virtual ::shared_ptr<prepared> prepare(database& db) = 0;

    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const {
        return false;
    }
};

}

}
