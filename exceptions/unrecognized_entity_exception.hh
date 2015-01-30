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
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#pragma once

#include "exceptions.hh"
#include "core/shared_ptr.hh"
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
    ::shared_ptr<cql3::column_identifier> entity;

    /**
     * The entity relation.
     */
    cql3::relation_ptr relation;

    /**
     * Creates a new <code>UnrecognizedEntityException</code>.
     * @param entity the unrecognized entity
     * @param relation the entity relation
     */
    unrecognized_entity_exception(::shared_ptr<cql3::column_identifier> entity, cql3::relation_ptr relation)
        : invalid_request_exception(sprint("Undefined name %s in where clause ('%s')", *entity, relation->to_string()))
        , entity(entity)
        , relation(relation)
    { }
};

}
