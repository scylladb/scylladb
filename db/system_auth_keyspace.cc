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
#include "types/set.hh"

namespace db {

// all system auth tables use schema commitlog
namespace {
    const auto set_use_schema_commitlog = schema_builder::register_static_configurator([](const sstring& ks_name, const sstring& cf_name, schema_static_props& props) {
        if (ks_name == system_auth_keyspace::NAME) {
            props.enable_schema_commitlog();
        }
    });
} // anonymous namespace

namespace system_auth_keyspace {

// use the same gc setting as system_schema tables
using days = std::chrono::duration<int, std::ratio<24 * 3600>>;
// FIXME: in some cases time-based gc may cause data resurrection,
// for more info see https://github.com/scylladb/scylladb/issues/15607
static constexpr auto auth_gc_grace = std::chrono::duration_cast<std::chrono::seconds>(days(7)).count();

schema_ptr roles() {
    static thread_local auto schema = [] {
        schema_builder builder(generate_legacy_id(NAME, ROLES), NAME, ROLES,
        // partition key
        {{"role", utf8_type}},
        // clustering key
        {},
        // regular columns
        {
            {"can_login", boolean_type},
            {"is_superuser", boolean_type},
            {"member_of", set_type_impl::get_instance(utf8_type, true)},
            {"salted_hash", utf8_type}
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "roles for authentication and RBAC"
        );
        builder.set_gc_grace_seconds(auth_gc_grace);
        builder.with_version(system_keyspace::generate_schema_version(builder.uuid()));
        return builder.build();
    }();
    return schema;
}

schema_ptr role_members() {
    static thread_local auto schema = [] {
        schema_builder builder(generate_legacy_id(NAME, ROLE_MEMBERS), NAME, ROLE_MEMBERS,
        // partition key
        {{"role", utf8_type}},
        // clustering key
        {{"member", utf8_type}},
        // regular columns
        {},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "joins users and their granted roles in RBAC"
        );
        builder.set_gc_grace_seconds(auth_gc_grace);
        builder.with_version(system_keyspace::generate_schema_version(builder.uuid()));
        return builder.build();
    }();
    return schema;
}

schema_ptr role_attributes() {
    static thread_local auto schema = [] {
        schema_builder builder(generate_legacy_id(NAME, ROLE_ATTRIBUTES), NAME, ROLE_ATTRIBUTES,
        // partition key
        {{"role", utf8_type}},
        // clustering key
        {{"name", utf8_type}},
        // regular columns
        {
            {"value", utf8_type}
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "role permissions in RBAC"
        );
        builder.set_gc_grace_seconds(auth_gc_grace);
        builder.with_version(system_keyspace::generate_schema_version(builder.uuid()));
        return builder.build();
    }();
    return schema;
}

schema_ptr role_permissions() {
    static thread_local auto schema = [] {
        schema_builder builder(generate_legacy_id(NAME, ROLE_PERMISSIONS), NAME, ROLE_PERMISSIONS,
        // partition key
        {{"role", utf8_type}},
        // clustering key
        {{"resource", utf8_type}},
        // regular columns
        {
            {"permissions", set_type_impl::get_instance(utf8_type, true)}
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "role permissions for CassandraAuthorizer"
        );
        builder.set_gc_grace_seconds(auth_gc_grace);
        builder.with_version(system_keyspace::generate_schema_version(builder.uuid()));
        return builder.build();
    }();
    return schema;
}

std::vector<schema_ptr> all_tables() {
    return {roles(), role_members(), role_attributes(), role_permissions()};
}

} // namespace system_auth_keyspace
} // namespace db
