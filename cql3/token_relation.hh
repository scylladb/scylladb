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
 * Copyright (C) 2015 ScyllaDB
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

#include <vector>

#include "core/shared_ptr.hh"
#include "to_string.hh"

#include "relation.hh"
#include "column_identifier.hh"
#include "term.hh"
#include "restrictions/restriction.hh"

namespace cql3 {

/**
 * A relation using the token function.
 * Examples:
 * <ul>
 * <li>SELECT ... WHERE token(a) &gt; token(1)</li>
 * <li>SELECT ... WHERE token(a, b) &gt; token(1, 3)</li>
 * </ul>
 */
class token_relation : public relation {
private:
    std::vector<::shared_ptr<column_identifier::raw>> _entities;
    ::shared_ptr<term::raw> _value;

    /**
     * Returns the definition of the columns to which apply the token restriction.
     *
     * @param cfm the column family metadata
     * @return the definition of the columns to which apply the token restriction.
     * @throws InvalidRequestException if the entity cannot be resolved
     */
    std::vector<const column_definition*> get_column_definitions(schema_ptr s);

    /**
     * Returns the receivers for this relation.
     *
     * @param cfm the Column Family meta data
     * @param columnDefs the column definitions
     * @return the receivers for the specified relation.
     * @throws InvalidRequestException if the relation is invalid
     */
    std::vector<::shared_ptr<column_specification>> to_receivers(schema_ptr schema, const std::vector<const column_definition*>& column_defs);

public:
    token_relation(std::vector<::shared_ptr<column_identifier::raw>> entities,
            const operator_type& type, ::shared_ptr<term::raw> value)
            : relation(type), _entities(std::move(entities)), _value(
                    std::move(value)) {
    }

    bool on_token() const override {
        return true;
    }

    ::shared_ptr<restrictions::restriction> new_EQ_restriction(database& db,
            schema_ptr schema,
            ::shared_ptr<variable_specifications> bound_names) override;

    ::shared_ptr<restrictions::restriction> new_IN_restriction(database& db,
            schema_ptr schema,
            ::shared_ptr<variable_specifications> bound_names) override;

    ::shared_ptr<restrictions::restriction> new_slice_restriction(database& db,
            schema_ptr schema,
            ::shared_ptr<variable_specifications> bound_names,
            statements::bound bound,
            bool inclusive) override;

    ::shared_ptr<restrictions::restriction> new_contains_restriction(
            database& db, schema_ptr schema,
            ::shared_ptr<variable_specifications> bound_names, bool isKey)
                    override;

    ::shared_ptr<relation> maybe_rename_identifier(const column_identifier::raw& from, column_identifier::raw to) override;

    sstring to_string() const override;

protected:
    ::shared_ptr<term> to_term(const std::vector<::shared_ptr<column_specification>>& receivers,
                                       ::shared_ptr<term::raw> raw,
                                       database& db,
                                       const sstring& keyspace,
                                       ::shared_ptr<variable_specifications> bound_names) override;
};

}
