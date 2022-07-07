/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
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

    // function/aggregate/procedure calls
    EXECUTE,
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
                permission::DESCRIBE,
                permission::EXECUTE>> permission_set;

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
