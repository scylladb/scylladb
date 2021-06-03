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
 * Copyright (C) 2015-present ScyllaDB
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

#include "cql3/statements/modification_statement.hh"
#include "cql3/statements/raw/modification_statement.hh"
#include "cql3/column_identifier.hh"
#include "cql3/term.hh"

#include "database_fwd.hh"

#include <vector>
#include "unimplemented.hh"

namespace cql3 {

namespace statements {

class update_statement;

namespace raw {

class update_statement : public raw::modification_statement {
private:
    // Provided for an UPDATE
    std::vector<std::pair<::shared_ptr<column_identifier::raw>, std::unique_ptr<operation::raw_update>>> _updates;
    std::vector<relation_ptr> _where_clause;
public:
    /**
     * Creates a new UpdateStatement from a column family name, columns map, consistency
     * level, and key term.
     *
     * @param name column family being operated on
     * @param attrs additional attributes for statement (timestamp, timeToLive)
     * @param updates a map of column operations to perform
     * @param whereClause the where clause
     */
    update_statement(cf_name name,
        std::unique_ptr<attributes::raw> attrs,
        std::vector<std::pair<::shared_ptr<column_identifier::raw>, std::unique_ptr<operation::raw_update>>> updates,
        std::vector<relation_ptr> where_clause,
        conditions_vector conditions, bool if_exists);
protected:
    virtual ::shared_ptr<cql3::statements::modification_statement> prepare_internal(database& db, schema_ptr schema,
                variable_specifications& bound_names, std::unique_ptr<attributes> attrs, cql_stats& stats) const override;
};

}

}

}
