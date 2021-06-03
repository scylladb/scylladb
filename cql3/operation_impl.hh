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

#include "operation.hh"
#include "constants.hh"
#include "maps.hh"
#include "sets.hh"
#include "lists.hh"
#include "user_types.hh"

namespace cql3 {

class operation::set_value : public raw_update {
protected:
    ::shared_ptr<term::raw> _value;
public:
    set_value(::shared_ptr<term::raw> value) : _value(std::move(value)) {}

    virtual ::shared_ptr <operation> prepare(database& db, const sstring& keyspace, const column_definition& receiver) const override;

#if 0
        protected String toString(ColumnSpecification column)
        {
            return String.format("%s = %s", column, value);
        }
#endif

    virtual bool is_compatible_with(const std::unique_ptr<raw_update>& other) const override;
};

class operation::set_counter_value_from_tuple_list : public set_value {
public:
    using set_value::set_value;
    ::shared_ptr <operation> prepare(database& db, const sstring& keyspace, const column_definition& receiver) const override;
};

class operation::column_deletion : public raw_deletion {
private:
    ::shared_ptr<column_identifier::raw> _id;
public:
    column_deletion(::shared_ptr<column_identifier::raw> id)
        : _id(std::move(id))
    { }

    virtual const column_identifier::raw& affected_column() const override {
        return *_id;
    }

    ::shared_ptr<operation> prepare(database& db, const sstring& keyspace, const column_definition& receiver) const {
        // No validation, deleting a column is always "well typed"
        return ::make_shared<constants::deleter>(receiver);
    }
};

}
