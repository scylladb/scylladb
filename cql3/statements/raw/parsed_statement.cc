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
 * Copyright (C) 2014-present ScyllaDB
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

#include "parsed_statement.hh"

#include "cql3/statements/prepared_statement.hh"

namespace cql3 {

namespace statements {

namespace raw {

parsed_statement::~parsed_statement()
{ }

variable_specifications& parsed_statement::get_bound_variables() {
    return _variables;
}

const variable_specifications& parsed_statement::get_bound_variables() const {
    return _variables;
}

// Used by the parser and preparable statement
void parsed_statement::set_bound_variables(const std::vector<::shared_ptr<column_identifier>>& bound_names) {
    _variables.set_bound_variables(bound_names);
}

}

prepared_statement::prepared_statement(::shared_ptr<cql_statement> statement_, std::vector<lw_shared_ptr<column_specification>> bound_names_, std::vector<uint16_t> partition_key_bind_indices)
    : statement(std::move(statement_))
    , bound_names(std::move(bound_names_))
    , partition_key_bind_indices(std::move(partition_key_bind_indices))
{ }

prepared_statement::prepared_statement(::shared_ptr<cql_statement> statement_, const variable_specifications& names, const std::vector<uint16_t>& partition_key_bind_indices)
    : prepared_statement(statement_, names.get_specifications(), partition_key_bind_indices)
{ }

prepared_statement::prepared_statement(::shared_ptr<cql_statement> statement_, variable_specifications&& names, std::vector<uint16_t>&& partition_key_bind_indices)
    : prepared_statement(statement_, std::move(names).get_specifications(), std::move(partition_key_bind_indices))
{ }

prepared_statement::prepared_statement(::shared_ptr<cql_statement>&& statement_)
    : prepared_statement(statement_, std::vector<lw_shared_ptr<column_specification>>(), std::vector<uint16_t>())
{ }

}

}
