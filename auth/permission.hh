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
 * Copyright 2016 Cloudius Systems
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

#include "enum_set.hh"

namespace auth {

enum class permission {
    //Deprecated
    READ,
    //Deprecated
    WRITE,

    // schema management
    CREATE, // required for CREATE KEYSPACE and CREATE TABLE.
    ALTER,  // required for ALTER KEYSPACE, ALTER TABLE, CREATE INDEX, DROP INDEX.
    DROP,   // required for DROP KEYSPACE and DROP TABLE.

    // data access
    SELECT, // required for SELECT.
    MODIFY, // required for INSERT, UPDATE, DELETE, TRUNCATE.

    // permission management
    AUTHORIZE, // required for GRANT and REVOKE.
};

typedef enum_set<super_enum<permission,
                permission::READ,
                permission::WRITE,
                permission::CREATE,
                permission::ALTER,
                permission::DROP,
                permission::SELECT,
                permission::MODIFY,
                permission::AUTHORIZE>> permission_set;

extern const permission_set ALL_DATA;
extern const permission_set ALL;
extern const permission_set NONE;

}
