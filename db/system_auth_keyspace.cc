/*
 * Modified by ScyllaDB
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "system_auth_keyspace.hh"
#include "system_keyspace.hh"
#include "db/schema_tables.hh"
#include "schema/schema_builder.hh"

namespace db::system_auth_keyspace {

// use the same gc setting as system_schema tables
using days = std::chrono::duration<int, std::ratio<24 * 3600>>;
// FIXME: in some cases time-based gc may cause data resurrection,
// for more info see https://github.com/scylladb/scylladb/issues/15607
static constexpr auto auth_gc_grace = std::chrono::duration_cast<std::chrono::seconds>(days(7)).count();

schema_ptr roles() {
    //TODO(mmal): define fully
    static thread_local auto schema = [] {
        schema_builder builder(generate_legacy_id(NAME, ROLES), NAME, ROLES,
        // partition key
        {{"role", utf8_type}},
        // clustering key
        {},
        // regular columns
        {},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "comment"
        );
        builder.set_gc_grace_seconds(auth_gc_grace);
        builder.with_version(system_keyspace::generate_schema_version(builder.uuid()));
        return builder.build();
    }();
    return schema;
}

std::vector<schema_ptr> all_tables() {
    return {roles()};
}

} // namespace system_auth_keyspace

