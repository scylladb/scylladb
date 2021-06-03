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

#include "exceptions.hh"
#include <seastar/core/shared_ptr.hh>
#include "cql3/column_identifier.hh"
#include "cql3/relation.hh"

namespace exceptions {

/**
 * Exception thrown when an entity is not recognized within a relation.
 */
class unrecognized_entity_exception : public invalid_request_exception {
public:
    /**
     * The unrecognized entity.
     */
    cql3::column_identifier entity;

    /**
     * The entity relation in a stringified form.
     */
    sstring relation_str;

    /**
     * Creates a new <code>UnrecognizedEntityException</code>.
     * @param entity the unrecognized entity
     * @param relation_str the entity relation string
     */
    unrecognized_entity_exception(cql3::column_identifier entity, sstring relation_str)
        : invalid_request_exception(format("Undefined name {} in where clause ('{}')", entity, relation_str))
        , entity(std::move(entity))
        , relation_str(std::move(relation_str))
    { }
};

}
