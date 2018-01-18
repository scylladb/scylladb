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
 * Copyright (C) 2016 ScyllaDB
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

#include <unordered_set>

#include <seastar/core/sstring.hh>

#include "enum_set.hh"
#include "seastarx.hh"

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
    DESCRIBE, // required on the root-level role resource to list all roles.

};

typedef enum_set<
        super_enum<
                permission,
                permission::READ,
                permission::WRITE,
                permission::CREATE,
                permission::ALTER,
                permission::DROP,
                permission::SELECT,
                permission::MODIFY,
                permission::AUTHORIZE,
                permission::DESCRIBE>> permission_set;

bool operator<(const permission_set&, const permission_set&);

namespace permissions {

extern const permission_set ALL;
extern const permission_set NONE;

const sstring& to_string(permission);
permission from_string(const sstring&);

std::unordered_set<sstring> to_strings(const permission_set&);
permission_set from_strings(const std::unordered_set<sstring>&);

}

}
