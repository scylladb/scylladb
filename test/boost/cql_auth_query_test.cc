/*
 * Copyright (C) 2017-present ScyllaDB
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

#include <string_view>

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/util/defer.hh>

#include "auth/authenticated_user.hh"
#include "auth/permission.hh"
#include "auth/service.hh"
#include "cql3/query_processor.hh"
#include "cql3/role_name.hh"
#include "cql3/role_options.hh"
#include "cql3/statements/create_role_statement.hh"
#include "db/config.hh"
#include "exceptions/exceptions.hh"
#include "seastarx.hh"
#include "service/client_state.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"

static const auto alice = std::string_view("alice");
static const auto bob = std::string_view("bob");

static shared_ptr<db::config> db_config_with_auth() {
    shared_ptr<db::config> config_ptr = make_shared<db::config>();
    auto& config = *config_ptr;
    config.authorizer.set("CassandraAuthorizer");
    config.authenticator.set("PasswordAuthenticator");

    // Disable time-based caching so that changing permissions of a user is reflected immediately.
    config.permissions_validity_in_ms.set(0);

    return config_ptr;
}

//
// These functions must be called inside Seastar threads.
//

static void create_user_if_not_exists(cql_test_env& env, std::string_view user_name) {
    env.execute_cql(format("CREATE USER IF NOT EXISTS {} WITH PASSWORD '{}'", user_name, user_name)).get();
}

// Invoke `f` as though the user indicated with `user_name` had logged in. The current logged in user is restored after
// `f` is invoked.
static void with_user(cql_test_env& env, std::string_view user_name, noncopyable_function<void ()> f) {
    auto& cs = env.local_client_state();
    std::optional<auth::authenticated_user> old_user = cs.user();

    create_user_if_not_exists(env, user_name);
    cs.set_login(auth::authenticated_user(sstring(user_name)));

    const auto reset = defer([&cs, old_user = std::move(old_user)] {
        cs.set_login(std::move(*old_user));
    });

    f();
}

// Ensure that invoking the CQL query as a specific user throws `exceptions::unauthorized_exception`, but that, after
// invoking `resolve` as a superuser, the same CQL query does not throw.
void verify_unauthorized_then_ok(
        cql_test_env& env,
        std::string_view user_name,
        std::string_view cql_query,
        noncopyable_function<void ()> resolve) {
    const auto cql_query_string = sstring(cql_query);

    with_user(env, user_name, [&env, &cql_query_string] {
        BOOST_REQUIRE_THROW(env.execute_cql(cql_query_string).get0(), exceptions::unauthorized_exception);
    });

    resolve();

    with_user(env, user_name, [&env, &cql_query_string] {
        env.execute_cql(cql_query_string).get0();
    });
}

//
// CREATE ROLE
//

SEASTAR_TEST_CASE(create_role_restrictions) {
    return do_with_cql_env_thread([](auto&& env) {
        //
        // A user cannot create a role without CREATE on <all roles>.
        //

        verify_unauthorized_then_ok(env, alice, "CREATE ROLE lord", [&env] {
            env.execute_cql("GRANT CREATE ON ALL ROLES TO alice").get0();
        });

        //
        // Only a superuser can create a superuser role.
        //

        verify_unauthorized_then_ok(env, bob, "CREATE ROLE emperor WITH SUPERUSER = true", [&env] {
            env.execute_cql("ALTER USER bob SUPERUSER").get0();
        });
    }, db_config_with_auth());
}

//
// ALTER ROLE
//

SEASTAR_TEST_CASE(alter_role_restrictions) {
    return do_with_cql_env_thread([](auto&& env) {
        env.execute_cql("CREATE ROLE lord").get0();

        //
        // A user cannot alter a role without ALTER on the role.
        //

        verify_unauthorized_then_ok(env, alice, "ALTER ROLE lord WITH LOGIN = true", [&env] {
            env.execute_cql("GRANT ALTER ON ROLE lord TO alice").get0();
        });

        //
        // A user can alter themselves without any permissions.
        //

        with_user(env, bob, [&env] {
            env.execute_cql("ALTER ROLE bob WITH LOGIN = true").get0();
        });

        //
        // Only superusers can alter superuser status.
        //

        verify_unauthorized_then_ok(env, bob, "ALTER ROLE lord WITH SUPERUSER = true", [&env] {
            env.execute_cql("ALTER USER bob SUPERUSER").get0();
        });

        //
        // A user cannot alter their own superuser status.
        //
        // Note that `bob` is still a superuser.

        with_user(env, bob, [&env] {
            BOOST_REQUIRE_THROW(
                    env.execute_cql("ALTER ROLE bob WITH SUPERUSER = true").get0(),
                    exceptions::unauthorized_exception);
        });
    }, db_config_with_auth());
}

//
// DROP ROLE
//

SEASTAR_TEST_CASE(drop_role_restrictions) {
    return do_with_cql_env_thread([](auto&& env) {
        env.execute_cql("CREATE ROLE LORD").get0();

        //
        // A user cannot drop a role without DROP on the role.
        //

        verify_unauthorized_then_ok(env, alice, "DROP ROLE lord", [&env] {
            env.execute_cql("GRANT DROP ON ROLE lord TO alice").get0();
        });

        //
        // A logged-in user cannot drop themselves.
        //

        with_user(env, alice, [&env] {
            BOOST_REQUIRE_THROW(env.execute_cql("DROP ROLE alice").get0(), exceptions::request_validation_exception);
        });

        //
        // Only a superuser can drop a role that has been granted a superuser role.
        //

        env.execute_cql("CREATE ROLE emperor WITH SUPERUSER = true").get0();

        verify_unauthorized_then_ok(env, bob, "DROP ROLE emperor", [&env] {
            env.execute_cql("ALTER USER bob SUPERUSER").get0();
        });

    }, db_config_with_auth());
}

//
// LIST ROLES
//

SEASTAR_TEST_CASE(list_roles_restrictions) {
    return do_with_cql_env_thread([](auto&& env) {
        env.execute_cql("CREATE ROLE lord").get0();

        //
        // A user cannot list all roles of a role not granted to them.
        //

        verify_unauthorized_then_ok(env, alice, "LIST ROLES OF lord", [&env] {
            env.execute_cql("GRANT lord TO alice").get0();
        });
    }, db_config_with_auth());
}

//
// GRANT ROLE
//

SEASTAR_TEST_CASE(grant_role_restrictions) {
    return do_with_cql_env_thread([](auto&& env) {
        env.execute_cql("CREATE ROLE lord").get0();

        //
        // A user cannot grant a role to another user without AUTHORIZE on the role being granted.
        //

        verify_unauthorized_then_ok(env, alice, "GRANT lord TO alice", [&env] {
            env.execute_cql("GRANT AUTHORIZE ON ROLE lord TO alice").get0();
        });
    }, db_config_with_auth());
}

//
// REVOKE ROLE
//

SEASTAR_TEST_CASE(revoke_role_restrictions) {
    return do_with_cql_env_thread([](auto&& env) {
        create_user_if_not_exists(env, alice);

        env.execute_cql("CREATE ROLE lord").get0();
        env.execute_cql("GRANT lord TO alice").get0();

        //
        // A user cannot revoke a role from another user without AUTHORIZE on the role being revoked.
        //

        verify_unauthorized_then_ok(env, alice, "REVOKE lord FROM alice", [&env] {
            env.execute_cql("GRANT AUTHORIZE ON ROLE lord TO alice").get0();
        });
    }, db_config_with_auth());
}

//
// The creator of a database object is granted all applicable permissions on it.
//

///
/// Grant a user appropriate permissions (with `grant_query`) then create a new database object (with `creation_query`).
/// Verify that the user has been granted all applicable permissions on the new object.
///
static void verify_default_permissions(
        cql_test_env& env,
        std::string_view user,
        std::string_view grant_query,
        std::string_view creation_query,
        const auth::resource& r) {
    create_user_if_not_exists(env, user);
    env.execute_cql(sstring(grant_query)).get0();

    with_user(env, user, [&env, creation_query] {
        env.execute_cql(sstring(creation_query)).get0();
    });

    const auto default_permissions = auth::get_permissions(
            env.local_auth_service(),
            auth::authenticated_user(user),
            r).get0();

    BOOST_REQUIRE_EQUAL(
            auth::permissions::to_strings(default_permissions),
            auth::permissions::to_strings(r.applicable_permissions()));
}

SEASTAR_TEST_CASE(create_role_default_permissions) {
    return do_with_cql_env_thread([](auto&& env) {
        verify_default_permissions(
                env,
                alice,
                "GRANT CREATE ON ALL ROLES TO alice",
                "CREATE ROLE lord",
                auth::make_role_resource("lord"));
    }, db_config_with_auth());
}

SEASTAR_TEST_CASE(create_keyspace_default_permissions) {
    return do_with_cql_env_thread([](auto&& env) {
        verify_default_permissions(
                env,
                alice,
                "GRANT CREATE ON ALL KEYSPACES TO alice",
                "CREATE KEYSPACE armies WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }",
                auth::make_data_resource("armies"));
    }, db_config_with_auth());
}

SEASTAR_TEST_CASE(create_table_default_permissions) {
    return do_with_cql_env_thread([](auto&& env) {
        verify_default_permissions(
                env,
                alice,
                "GRANT CREATE ON KEYSPACE ks TO alice",
                "CREATE TABLE orcs (id int PRIMARY KEY, strength int)",
                auth::make_data_resource("ks", "orcs"));
    }, db_config_with_auth());
}

SEASTAR_TEST_CASE(modify_table_with_view) {
    return do_with_cql_env_thread([](auto&& env) {
        cquery_nofail(env, "CREATE TABLE t (p int PRIMARY KEY)");
        cquery_nofail(env, "CREATE MATERIALIZED VIEW v AS SELECT * FROM t WHERE p > 0 PRIMARY KEY (p)");
        create_user_if_not_exists(env, bob);
        cquery_nofail(env, "GRANT ALL PERMISSIONS ON t TO bob");
        with_user(env, bob, [&env] {
            cquery_nofail(env, "INSERT INTO t (p) values (1337)");
        });
        assert_that(cquery_nofail(env, "SELECT * FROM v;")).is_rows().with_rows({{int32_type->decompose(1337)}});
    }, db_config_with_auth());
}

SEASTAR_TEST_CASE(modify_table_with_index) {
    return do_with_cql_env_thread([](auto&& env) {
        cquery_nofail(env, "CREATE TABLE t (p int PRIMARY KEY, v text)");
        cquery_nofail(env, "CREATE INDEX ON t(v)");
        create_user_if_not_exists(env, bob);
        cquery_nofail(env, "GRANT ALL PERMISSIONS ON t TO bob");
        with_user(env, bob, [&env] {
            cquery_nofail(env, "INSERT INTO t (p, v) values (14, 'aaa')");
        });
        assert_that(cquery_nofail(env, "SELECT p FROM t WHERE v='aaa' ALLOW FILTERING;")).is_rows().with_rows(
                {{int32_type->decompose(14)}});
    }, db_config_with_auth());
}
