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

#include "relation.hh"
#include "exceptions/unrecognized_entity_exception.hh"

namespace cql3 {

const column_definition&
relation::to_column_definition(schema_ptr schema, ::shared_ptr<column_identifier::raw> entity) {
    auto id = entity->prepare_column_identifier(schema);
    auto def = get_column_definition(schema, *id);
    if (!def) {
        throw exceptions::unrecognized_entity_exception(id, shared_from_this());
    }
    return *def;
}

}
