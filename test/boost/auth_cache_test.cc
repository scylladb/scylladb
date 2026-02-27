/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/future.hh>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>

#include "auth/cache.hh"
#include "auth/permission.hh"
#include "auth/resource.hh"
#include "auth/role_or_anonymous.hh"
#include "auth/service.hh"
#include "db/config.hh"
#include "test/lib/cql_test_env.hh"

BOOST_AUTO_TEST_SUITE(auth_cache_test)

static cql_test_config auth_on_config() {
    cql_test_config cfg;
    cfg.db_config->authorizer("CassandraAuthorizer");
    cfg.db_config->authenticator("PasswordAuthenticator");
    return cfg;
}

// Tests whether removing role drops it from cache.
SEASTAR_TEST_CASE(test_update_cache_for_dropped_role) {
    return do_with_cql_env([](cql_test_env& env) -> future<> {
        co_await env.execute_cql("CREATE ROLE to_drop WITH LOGIN = false AND PASSWORD = 'x'");

        auto& cache = env.auth_cache().local();
        co_await cache.load_all();

        BOOST_REQUIRE(cache.get("to_drop"));

        co_await env.execute_cql("DROP ROLE to_drop");
        co_await cache.load_roles({"to_drop"});

        BOOST_REQUIRE(!cache.get("to_drop"));
    }, auth_on_config());
}

// Tests whether altering base role's permission refreshes
// all descendant roles permissions. And then if dropping
// base role also drops permissions for descendants.
SEASTAR_TEST_CASE(test_three_level_role_hierarchy) {
    return do_with_cql_env([](cql_test_env& env) -> future<> {
        co_await env.execute_cql("CREATE TABLE ks.tbl_a (pk int PRIMARY KEY, v int)");
        co_await env.execute_cql("CREATE TABLE ks.tbl_b (pk int PRIMARY KEY, v int)");

        co_await env.execute_cql("CREATE ROLE r_base WITH LOGIN = false");
        co_await env.execute_cql("CREATE ROLE r_mid WITH LOGIN = false");
        co_await env.execute_cql("CREATE ROLE r_bottom WITH LOGIN = true AND PASSWORD = 'x'");

        co_await env.execute_cql("GRANT r_base TO r_mid");
        co_await env.execute_cql("GRANT r_mid TO r_bottom");

        co_await env.execute_cql("GRANT SELECT ON ks.tbl_a TO r_base");
        co_await env.execute_cql("GRANT MODIFY ON ks.tbl_b TO r_mid");
        co_await env.execute_cql("GRANT CREATE ON KEYSPACE ks TO r_bottom");
        co_await env.execute_cql("GRANT MODIFY ON ks.tbl_a TO r_bottom");

        auto& cache = env.auth_cache().local();
        co_await cache.load_all();

        auto base = auth::role_or_anonymous("r_base");
        auto mid = auth::role_or_anonymous("r_mid");
        auto bottom = auth::role_or_anonymous("r_bottom");
        auto res_tbl_a = auth::make_data_resource("ks", "tbl_a");
        auto res_tbl_b = auth::make_data_resource("ks", "tbl_b");

        // Initial check of relationships.
        BOOST_REQUIRE(cache.get("r_base")->member_of.empty());
        BOOST_REQUIRE(cache.get("r_mid")->member_of.contains("r_base"));
        BOOST_REQUIRE(cache.get("r_bottom")->member_of.contains("r_mid"));

        // Update base permissions.
        co_await env.execute_cql("REVOKE SELECT ON ks.tbl_a FROM r_base");
        co_await env.execute_cql("GRANT ALTER ON ks.tbl_a TO r_base");
        co_await cache.load_roles({"r_base"});

        auto perms_mid_a = co_await cache.get_permissions(mid, res_tbl_a);
        BOOST_REQUIRE(!perms_mid_a.contains(auth::permission::SELECT));
        BOOST_REQUIRE(perms_mid_a.contains(auth::permission::ALTER));

        auto perms_bottom_a = co_await cache.get_permissions(bottom, res_tbl_a);
        BOOST_REQUIRE(!perms_bottom_a.contains(auth::permission::SELECT));
        BOOST_REQUIRE(perms_bottom_a.contains(auth::permission::ALTER));

        // Drop base role.
        co_await env.execute_cql("DROP ROLE r_base");
        co_await cache.load_roles({"r_base"});

        perms_mid_a = co_await cache.get_permissions(mid, res_tbl_a);
        BOOST_REQUIRE_EQUAL(perms_mid_a.mask(), auth::permissions::NONE.mask());

        perms_bottom_a = co_await cache.get_permissions(bottom, res_tbl_a);
        auth::permission_set expected;
        expected.set(auth::permission::MODIFY);
        BOOST_REQUIRE(perms_bottom_a.mask() == expected.mask());
    }, auth_on_config());
}

// Tests whether resource drop properly removes permissions.
SEASTAR_TEST_CASE(test_invalidate_permissions_via_drop) {
    return do_with_cql_env([](cql_test_env& env) -> future<> {
        co_await env.execute_cql("CREATE TABLE ks.t (pk int PRIMARY KEY)");
        co_await env.execute_cql("CREATE ROLE user");
        co_await env.execute_cql("GRANT SELECT ON ks.t TO user");

        auto& cache = env.auth_cache().local();
        co_await cache.load_all();

        auto r = auth::make_data_resource("ks", "t");
        auto role = auth::role_or_anonymous("user");

        auto perms_before = co_await cache.get_permissions(role, r);
        BOOST_REQUIRE(perms_before.contains(auth::permission::SELECT));

        co_await env.execute_cql("DROP TABLE ks.t");
        co_await cache.prune(r);

        auto perms_after = co_await cache.get_permissions(role, r);
        BOOST_REQUIRE(!perms_after.contains(auth::permission::SELECT));
    }, auth_on_config());
}

// Checks if permissions are not accidentally added to anonymous role.
SEASTAR_TEST_CASE(test_anonymous_not_granted_other_roles_permissions) {
    return do_with_cql_env([](cql_test_env& env) -> future<> {
        co_await env.execute_cql("CREATE TABLE ks.t (pk int PRIMARY KEY)");
        co_await env.execute_cql("CREATE ROLE granted_role");
        co_await env.execute_cql("GRANT SELECT ON ks.t TO granted_role");

        auto& cache = env.auth_cache().local();
        co_await cache.load_all();

        auto r = auth::make_data_resource("ks", "t");

        auto role_perms = co_await cache.get_permissions(auth::role_or_anonymous("granted_role"), r);
        BOOST_REQUIRE(role_perms.contains(auth::permission::SELECT));

        auto anon_perms = co_await cache.get_permissions(auth::role_or_anonymous(), r);
        BOOST_REQUIRE(!anon_perms.contains(auth::permission::SELECT));
    }, auth_on_config());
}

BOOST_AUTO_TEST_SUITE_END()
