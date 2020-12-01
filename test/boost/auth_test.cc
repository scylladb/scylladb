/*
 * Copyright (C) 2016 ScyllaDB
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


#include <boost/range/irange.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/test/unit_test.hpp>
#include <stdint.h>

#include <seastar/core/future-util.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/thread.hh>

#include <seastar/testing/test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/exception_utils.hh"

#include "auth/allow_all_authenticator.hh"
#include "auth/authenticator.hh"
#include "auth/password_authenticator.hh"
#include "auth/service.hh"
#include "auth/authenticated_user.hh"
#include "auth/resource.hh"

#include "db/config.hh"
#include "cql3/query_processor.hh"
#include "types/map.hh"

SEASTAR_TEST_CASE(test_default_authenticator) {
    return do_with_cql_env([](cql_test_env& env) {
        auto& a = env.local_auth_service().underlying_authenticator();
        BOOST_REQUIRE(!a.require_authentication());
        BOOST_REQUIRE_EQUAL(a.qualified_java_name(), auth::allow_all_authenticator_name);
        return make_ready_future();
    });
}

SEASTAR_TEST_CASE(test_password_authenticator_attributes) {
    auto cfg = make_shared<db::config>();
    cfg->authenticator(sstring(auth::password_authenticator_name));

    return do_with_cql_env([](cql_test_env& env) {
        auto& a = env.local_auth_service().underlying_authenticator();
        BOOST_REQUIRE(a.require_authentication());
        BOOST_REQUIRE_EQUAL(a.qualified_java_name(), auth::password_authenticator_name);
        return make_ready_future();
    }, cfg);
}

static future<auth::authenticated_user>
authenticate(cql_test_env& env, std::string_view username, std::string_view password) {
    auto& c = env.local_client_state();
    auto& a = env.local_auth_service().underlying_authenticator();

    return do_with(
            auth::authenticator::credentials_map{
                    {auth::authenticator::USERNAME_KEY, sstring(username)},
                    {auth::authenticator::PASSWORD_KEY, sstring(password)}},
            [&a, &c, username](const auto& credentials) {
        return a.authenticate(credentials).then([&c, username](auth::authenticated_user u) {
            c.set_login(std::move(u));
            return c.check_user_can_login().then([&c] { return *c.user(); });
        });
    });
}

template <typename Exception, typename... Args>
future<> require_throws(seastar::future<Args...> fut) {
    return fut.then_wrapped([](auto completed_fut) {
        try {
            completed_fut.get();
            BOOST_FAIL("Required an exception to be thrown");
        } catch (const Exception&) {
            // Ok.
        }
    });
}

SEASTAR_TEST_CASE(test_password_authenticator_operations) {
    auto cfg = make_shared<db::config>();
    cfg->authenticator(sstring(auth::password_authenticator_name));

    /**
     * Not using seastar::async due to apparent ASan bug.
     * Enjoy the slightly less readable code.
     */
    return do_with_cql_env([](cql_test_env& env) {
        static const sstring username("fisk");
        static const sstring password("notter");

        // check non-existing user
        return require_throws<exceptions::authentication_exception>(
                authenticate(env, username, password)).then([&env] {
            return do_with(auth::role_config{}, auth::authentication_options{}, [&env](auto& config, auto& options) {
                config.can_login = true;
                options.password = password;

                return auth::create_role(env.local_auth_service(), username, config, options).then([&env] {
                    return authenticate(env, username, password).then([](auth::authenticated_user user) {
                        BOOST_REQUIRE(!auth::is_anonymous(user));
                        BOOST_REQUIRE_EQUAL(*user.name, username);
                    });
                });
            });
        }).then([&env] {
            return require_throws<exceptions::authentication_exception>(authenticate(env, username, "hejkotte"));
        }).then([&env] {
            //
            // A role must be explicitly marked as being allowed to log in.
            //

            return do_with(
                    auth::role_config_update{},
                    auth::authentication_options{},
                    [&env](auto& config_update, const auto& options) {
                config_update.can_login = false;

                return auth::alter_role(env.local_auth_service(), username, config_update, options).then([&env] {
                    return require_throws<exceptions::authentication_exception>(authenticate(env, username, password));
                });
            });
        }).then([&env] {
            // sasl
            auto& a = env.local_auth_service().underlying_authenticator();
            auto sasl = a.new_sasl_challenge();

            BOOST_REQUIRE(!sasl->is_complete());

            bytes b;
            int8_t i = 0;
            b.append(&i, 1);
            b.insert(b.end(), username.begin(), username.end());
            b.append(&i, 1);
            b.insert(b.end(), password.begin(), password.end());

            sasl->evaluate_response(b);
            BOOST_REQUIRE(sasl->is_complete());

            return sasl->get_authenticated_user().then([](auth::authenticated_user user) {
                BOOST_REQUIRE(!auth::is_anonymous(user));
                BOOST_REQUIRE_EQUAL(*user.name, username);
            });
        }).then([&env] {
            // check deleted user
            return auth::drop_role(env.local_auth_service(), username).then([&env] {
                return require_throws<exceptions::authentication_exception>(authenticate(env, username, password));
            });
        });
    }, cfg);
}

namespace {

/// Asserts that table is protected from alterations that can brick a node.
void require_table_protected(cql_test_env& env, const char* table) {
    using exception_predicate::message_contains;
    using unauth = exceptions::unauthorized_exception;
    const auto q = [&] (const char* stmt) { return env.execute_cql(fmt::format(stmt, table)).get(); };
    BOOST_TEST_INFO(table);
    BOOST_REQUIRE_EXCEPTION(q("ALTER TABLE {} ALTER role TYPE blob"), unauth, message_contains("is protected"));
    BOOST_REQUIRE_EXCEPTION(q("ALTER TABLE {} RENAME role TO user"), unauth, message_contains("is protected"));
    BOOST_REQUIRE_EXCEPTION(q("ALTER TABLE {} DROP role"), unauth, message_contains("is protected"));
    BOOST_REQUIRE_EXCEPTION(q("DROP TABLE {}"), unauth, message_contains("is protected"));
}

cql_test_config auth_on() {
    cql_test_config cfg;
    cfg.db_config->authorizer("CassandraAuthorizer");
    cfg.db_config->authenticator("PasswordAuthenticator");
    return cfg;
}

} // anonymous namespace

SEASTAR_TEST_CASE(roles_table_is_protected) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        require_table_protected(env, "system_auth.roles");
    }, auth_on());
}

SEASTAR_TEST_CASE(role_members_table_is_protected) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        require_table_protected(env, "system_auth.role_members");
    }, auth_on());
}

SEASTAR_TEST_CASE(role_permissions_table_is_protected) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        require_table_protected(env, "system_auth.role_permissions");
    }, auth_on());
}

SEASTAR_TEST_CASE(alter_opts_on_system_auth_tables) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        cquery_nofail(env, "ALTER TABLE system_auth.roles WITH speculative_retry = 'NONE'");
        cquery_nofail(env, "ALTER TABLE system_auth.role_members WITH gc_grace_seconds = 123");
        cquery_nofail(env, "ALTER TABLE system_auth.role_permissions WITH min_index_interval = 456");
    }, auth_on());
}

SEASTAR_TEST_CASE(test_alter_with_timeouts) {
    auto cfg = make_shared<db::config>();
    cfg->authenticator(sstring(auth::password_authenticator_name));

    return do_with_cql_env_thread([] (cql_test_env& e) {
        auth::role_config config {
            .can_login = true,
        };
        auth::authentication_options opts {
            .password = "pass"
        };
        auth::create_role(e.local_auth_service(), "user", config, opts).get();
        auth::create_role(e.local_auth_service(), "also_user", config, opts).get();
        authenticate(e, "user", "pass").get();

        cquery_nofail(e, "CREATE TABLE t (id int PRIMARY KEY, v int)");
        cquery_nofail(e, "ALTER ROLE user WITH options = {'read_timeout': 5ms, 'write_timeout': 1h30m}");

	    auto my_map_type = map_type_impl::get_instance(utf8_type, utf8_type, false);

	    auto msg = cquery_nofail(e, "SELECT options FROM system_auth.roles WHERE role = 'user'");
        assert_that(msg).is_rows().with_rows({{
            my_map_type->decompose(make_map_value(my_map_type, map_type_impl::native_type({{"read_timeout", "5ms"}, {"write_timeout", "1h30m"}}))),
        }});

        cquery_nofail(e, "ALTER ROLE user WITH options = {'write_timeout': 35s}");

	    msg = cquery_nofail(e, "SELECT options FROM system_auth.roles WHERE role = 'user'");
        assert_that(msg).is_rows().with_rows({{
            my_map_type->decompose(make_map_value(my_map_type, map_type_impl::native_type({{"write_timeout", "35s"}}))),
        }});

        // Setting a timeout value of 0 makes little sense, but it's great for testing
        cquery_nofail(e, "ALTER ROLE user WITH options = {'read_timeout': 0s, 'write_timeout': 0s}");
        BOOST_REQUIRE_THROW(e.execute_cql("SELECT * FROM t").get(), exceptions::read_timeout_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("INSERT INTO t (id, v) VALUES (1,2)").get(), exceptions::mutation_write_failure_exception);

        cquery_nofail(e, "ALTER ROLE user WITH options = {}");
        cquery_nofail(e, "SELECT * FROM t");
        cquery_nofail(e, "INSERT INTO t (id, v) VALUES (1,2)");

        // Only valid timeout values are accepted
        BOOST_REQUIRE_THROW(e.execute_cql("ALTER ROLE user WITH options = {'read_timeout': 'I am not a valid duration'}").get(), marshal_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("ALTER ROLE user WITH options = {'read_timeout': 5us}").get(), exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("ALTER ROLE user WITH options = {'read_timeout': -5s}").get(), exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("ALTER ROLE user WITH options = {'read_timeout': 48h}").get(), exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("ALTER ROLE user WITH options = {'read_timeout': 2y6mo5d}").get(), exceptions::invalid_request_exception);

        // When multiple per-role timeouts apply, the smallest value is always effective
        cquery_nofail(e, "ALTER ROLE user WITH options = {'write_timeout': 1s}");
        cquery_nofail(e, "ALTER ROLE also_user WITH options = {'write_timeout': 0s, 'read_timeout': 0s}");
        cquery_nofail(e, "GRANT also_user TO user");
        BOOST_REQUIRE_THROW(e.execute_cql("SELECT * FROM t").get(), exceptions::read_timeout_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("INSERT INTO t (id, v) VALUES (1,2)").get(), exceptions::mutation_write_failure_exception);

        // Effective timeouts are also immediately visible in system.clients table
        cquery_nofail(e, "ALTER ROLE user WITH options = {'write_timeout': 12h}");
        cquery_nofail(e, "ALTER ROLE also_user WITH options = {'write_timeout': 1h30m, 'read_timeout': 500ms}");
	    msg = cquery_nofail(e, "SELECT params FROM system.clients");
        assert_that(msg).is_rows().with_rows({{
            my_map_type->decompose(make_map_value(my_map_type, map_type_impl::native_type({{"read_timeout", "500ms"}, {"write_timeout", "1h30m"}}))),
        }});
    }, cfg);
}
