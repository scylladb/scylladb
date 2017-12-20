/*
 * Copyright (C) 2017 ScyllaDB
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

#include <experimental/string_view>

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>

#include "auth/authenticated_user.hh"
#include "cql3/query_processor.hh"
#include "cql3/role_name.hh"
#include "cql3/role_options.hh"
#include "cql3/statements/create_role_statement.hh"
#include "db/config.hh"
#include "exceptions/exceptions.hh"
#include "stdx.hh"
#include "seastarx.hh"
#include "service/client_state.hh"
#include "tests/cql_test_env.hh"
#include "tests/test-utils.hh"

static const auto alice = stdx::string_view("alice");
static const auto bob = stdx::string_view("bob");

static db::config db_config_with_auth() {
    db::config config;
    config.authorizer("CassandraAuthorizer");
    config.authenticator("PasswordAuthenticator");

    // Disable time-based caching so that changing permissions of a user is reflected immediately.
    config.permissions_validity_in_ms(0);

    return config;
}

//
// These functions must be called inside Seastar threads.
//

static void create_user_if_not_exists(cql_test_env& env, stdx::string_view user_name) {
    //
    // When roles replace users, creating a user will be equivalent to creating a role with the same name. Until then,
    // we must manually ensure that a role and a user of the same name exist.
    //

    env.execute_cql(sprint("CREATE USER IF NOT EXISTS %s WITH PASSWORD '%s'", user_name, user_name)).get();
    env.execute_cql(sprint("CREATE ROLE IF NOT EXISTS %s", user_name)).get();
}

// Invoke `f` as though the user indicated with `user_name` had logged in. The current logged in user is restored after
// `f` is invoked.
template <typename Function>
static void with_user(cql_test_env& env, stdx::string_view user_name, Function&& f) {
    auto& cs = env.local_client_state();
    auto old_user = cs.user();

    create_user_if_not_exists(env, user_name);
    cs.set_login(::make_shared<auth::authenticated_user>(sstring(user_name)));

    const auto reset = defer([&cs, old_user = std::move(old_user)] {
        cs.set_login(std::move(old_user));
    });

    f();
}

// Ensure that invoking the CQL query as a specific user throws `exceptions::unauthorized_exception`, but that, after
// invoking `resolve` as a superuser, the same CQL query does not throw.
template <class Function>
void verify_unauthorized_then_ok(
        cql_test_env& env,
        stdx::string_view user_name,
        stdx::string_view cql_query,
        Function&& resolve) {
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

        verify_unauthorized_then_ok(env, bob, "CREATE ROLE emperor SUPERUSER", [&env] {
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

        verify_unauthorized_then_ok(env, alice, "ALTER ROLE lord LOGIN", [&env] {
            env.execute_cql("GRANT ALTER ON ROLE lord TO alice").get0();
        });

        //
        // A user can alter themselves without any permissions.
        //

        with_user(env, bob, [&env] {
            env.execute_cql("ALTER ROLE bob LOGIN").get0();
        });

        //
        // Only superusers can alter superuser status.
        //

        verify_unauthorized_then_ok(env, bob, "ALTER ROLE lord SUPERUSER", [&env] {
            env.execute_cql("ALTER USER bob SUPERUSER").get0();
        });

        //
        // A user cannot alter their own superuser status.
        //
        // Note that `bob` is still a superuser.

        with_user(env, bob, [&env] {
            BOOST_REQUIRE_THROW(env.execute_cql("ALTER ROLE bob SUPERUSER").get0(), exceptions::unauthorized_exception);
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

        env.execute_cql("CREATE ROLE emperor SUPERUSER").get0();

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
