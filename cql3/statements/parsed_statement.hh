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

#ifndef CQL3_STATEMENTS_PARSED_STATEMENT_HH
#define CQL3_STATEMENTS_PARSED_STATEMENT_HH

#include "cql3/variable_specifications.hh"
#include "cql3/column_specification.hh"
#include "cql3/column_identifier.hh"
#include "cql3/cql_statement.hh"

#include "core/shared_ptr.hh"

namespace cql3 {

namespace statements {

class parsed_statement {
private:
    shared_ptr<variable_specifications> _variables;

public:
    virtual ~parsed_statement()
    { }

    shared_ptr<variable_specifications> get_bound_variables() {
        return _variables;
    }

    // Used by the parser and preparable statement
    void set_bound_variables(const std::list<column_identifier>& bound_names)
    {
        _variables = ::make_shared<variable_specifications>(bound_names);
    }

    class prepared {
    public:
        const std::unique_ptr<cql_statement> statement;
        const std::list<column_specification> bound_names;

        prepared(std::unique_ptr<cql_statement>&& statement_, const std::list<column_specification>& bound_names_)
            : statement(std::move(statement_))
            , bound_names(bound_names_)
        { }

        prepared(std::unique_ptr<cql_statement>&& statement_, const variable_specifications& names)
            : prepared(std::move(statement_), names.get_specifications())
        { }

        prepared(std::unique_ptr<cql_statement>&& statement_)
            : prepared(std::move(statement_), std::list<column_specification>())
        { }
    };

    virtual std::unique_ptr<prepared> prepare(std::unique_ptr<cql_statement>&& stmt) = 0;

    virtual bool uses_function(sstring ks_name, sstring function_name) {
        return false;
    }
};

}

}

#endif
