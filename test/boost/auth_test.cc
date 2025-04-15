/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */


#include <stdint.h>
#include <fmt/ranges.h>

#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/thread.hh>

#include "cql3/CqlParser.hpp"
#include "exceptions/exceptions.hh"
#include "service/raft/raft_group0_client.hh"

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/exception_utils.hh"

#include "auth/allow_all_authenticator.hh"
#include "auth/authenticator.hh"
#include "auth/password_authenticator.hh"
#include "auth/service.hh"
#include "auth/authenticated_user.hh"

#include "db/config.hh"

BOOST_AUTO_TEST_SUITE(auth_test)

cql_test_config auth_on(bool with_authorizer = true) {
    cql_test_config cfg;
    if (with_authorizer) {
        cfg.db_config->authorizer("CassandraAuthorizer");
    }
    cfg.db_config->authenticator("PasswordAuthenticator");
    return cfg;
}

SEASTAR_TEST_CASE(test_default_authenticator) {
    return do_with_cql_env([](cql_test_env& env) {
        auto& a = env.local_auth_service().underlying_authenticator();
        BOOST_REQUIRE(!a.require_authentication());
        BOOST_REQUIRE_EQUAL(a.qualified_java_name(), auth::allow_all_authenticator_name);
        return make_ready_future();
    });
}

SEASTAR_TEST_CASE(test_password_authenticator_attributes) {
    return do_with_cql_env([](cql_test_env& env) {
        auto& a = env.local_auth_service().underlying_authenticator();
        BOOST_REQUIRE(a.require_authentication());
        BOOST_REQUIRE_EQUAL(a.qualified_java_name(), auth::password_authenticator_name);
        return make_ready_future();
    }, auth_on(false));
}

static future<auth::authenticated_user>
authenticate(cql_test_env& env, std::string_view username, std::string_view password) {
    auto& c = env.local_client_state();
    auto& a = env.local_auth_service().underlying_authenticator();

    return do_with(
            auth::authenticator::credentials_map{
                    {auth::authenticator::USERNAME_KEY, sstring(username)},
                    {auth::authenticator::PASSWORD_KEY, sstring(password)}},
            [&a, &c](const auto& credentials) {
        return a.authenticate(credentials).then([&c](auth::authenticated_user u) {
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
            return seastar::async([&env] () {
                cquery_nofail(env, format("CREATE ROLE {} WITH PASSWORD = '{}' AND LOGIN = true", username, password));
            }).then([&env] () {
                return authenticate(env, username, password);
            }).then([] (auth::authenticated_user user) {
                BOOST_REQUIRE(!auth::is_anonymous(user));
                BOOST_REQUIRE_EQUAL(*user.name, username);
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

                return seastar::async([&env] {
                    do_with_mc(env, [&env] (auto& mc) {
                        auth::authentication_options opts;
                        auth::role_config_update conf;
                        conf.can_login = false;
                        auth::alter_role(env.local_auth_service(), username, conf, opts, mc).get();
                    });
                    // has to be in a separate transaction to observe results of alter role
                    do_with_mc(env, [&env] (auto& mc) {
                        require_throws<exceptions::authentication_exception>(authenticate(env, username, password)).get();
                    });
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
            return seastar::async([&env] {
                do_with_mc(env, [&env] (auto& mc) {
                    auth::drop_role(env.local_auth_service(), username, mc).get();
                    require_throws<exceptions::authentication_exception>(authenticate(env, username, password)).get();
                });
            });
        });
    }, auth_on(false));
}

namespace {

/// Asserts that table is protected from alterations that can brick a node.
void require_table_protected(cql_test_env& env, const char* table) {
    using exception_predicate::message_matches;
    using unauth = exceptions::unauthorized_exception;
    const auto q = [&] (const char* stmt) { return env.execute_cql(fmt::format(fmt::runtime(stmt), table)).get(); };
    const char* pattern = ".*(is protected)|(is not user-modifiable).*";
    BOOST_TEST_INFO(table);
    BOOST_REQUIRE_EXCEPTION(q("ALTER TABLE {} ALTER role TYPE blob"), unauth, message_matches(pattern));
    BOOST_REQUIRE_EXCEPTION(q("ALTER TABLE {} RENAME role TO user"), unauth, message_matches(pattern));
    BOOST_REQUIRE_EXCEPTION(q("ALTER TABLE {} DROP role"), unauth, message_matches(pattern));
    BOOST_REQUIRE_EXCEPTION(q("DROP TABLE {}"), unauth, message_matches(pattern));
}

} // anonymous namespace

SEASTAR_TEST_CASE(roles_table_is_protected) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        require_table_protected(env, "system.roles");
    }, auth_on());
}

SEASTAR_TEST_CASE(role_members_table_is_protected) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        require_table_protected(env, "system.role_members");
    }, auth_on());
}

SEASTAR_TEST_CASE(role_permissions_table_is_protected) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        require_table_protected(env, "system.role_permissions");
    }, auth_on());
}

SEASTAR_TEST_CASE(test_alter_with_timeouts) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "CREATE ROLE user1 WITH PASSWORD = 'pass' AND LOGIN = true");
        cquery_nofail(e, "CREATE ROLE user2 WITH PASSWORD = 'pass' AND LOGIN = true");
        cquery_nofail(e, "CREATE ROLE user3 WITH PASSWORD = 'pass' AND LOGIN = true");
        cquery_nofail(e, "CREATE ROLE user4 WITH PASSWORD = 'pass' AND LOGIN = true");
        authenticate(e, "user1", "pass").get();

        cquery_nofail(e, "CREATE SERVICE LEVEL sl WITH timeout = 5ms");
        cquery_nofail(e, "ATTACH SERVICE LEVEL sl TO user1");

        cquery_nofail(e, "CREATE TABLE t (id int, v int, PRIMARY KEY(id, v))");
        cquery_nofail(e, "INSERT INTO t (id, v) VALUES (1, 2)");
        cquery_nofail(e, "INSERT INTO t (id, v) VALUES (2, 3)");
        cquery_nofail(e, "INSERT INTO t (id, v) VALUES (3, 4)");
        // Avoid reading from memtables, which does not check timeouts due to being too fast
        e.db().invoke_on_all([] (replica::database& db) { return db.flush_all_memtables(); }).get();

        auto sl_is_v2 = e.local_client_state().get_service_level_controller().is_v2();
	    auto msg = cquery_nofail(e, format("SELECT timeout FROM {}", sl_is_v2 ? "system.service_levels_v2" : "system_distributed.service_levels"));
        assert_that(msg).is_rows().with_rows({{
            duration_type->from_string("5ms")
        }});

        cquery_nofail(e, "ALTER SERVICE LEVEL sl WITH timeout = 35s");

	    msg = cquery_nofail(e, format("SELECT timeout FROM {} WHERE service_level = 'sl'", sl_is_v2 ? "system.service_levels_v2" : "system_distributed.service_levels"));
        assert_that(msg).is_rows().with_rows({{
            duration_type->from_string("35s")
        }});

        // Setting a timeout value of 0 makes little sense, but it's great for testing
        cquery_nofail(e, "ALTER SERVICE LEVEL sl WITH timeout = 0s");
        e.refresh_client_state().get();
        BOOST_REQUIRE_THROW(e.execute_cql("SELECT * FROM t BYPASS CACHE").get(), exceptions::read_timeout_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("INSERT INTO t (id, v) VALUES (1,2)").get(), exceptions::mutation_write_timeout_exception);

        cquery_nofail(e, "ALTER SERVICE LEVEL sl WITH timeout = null");
        e.refresh_client_state().get();
        cquery_nofail(e, "SELECT * FROM t BYPASS CACHE");
        cquery_nofail(e, "INSERT INTO t (id, v) VALUES (1,2)");

        // Only valid timeout values are accepted
        BOOST_REQUIRE_THROW(e.execute_cql("ALTER SERVICE LEVEL sl WITH timeout = 'I am not a valid duration'").get(), exceptions::syntax_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("ALTER SERVICE LEVEL sl WITH timeout = 5us").get(), exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("ALTER SERVICE LEVEL sl WITH timeout = 2y6mo5d").get(), exceptions::invalid_request_exception);

        // When multiple per-role timeouts apply, the smallest value is always effective
        cquery_nofail(e, "CREATE SERVICE LEVEL sl2 WITH timeout = 2s");
        cquery_nofail(e, "CREATE SERVICE LEVEL sl3 WITH timeout = 0s");
        cquery_nofail(e, "CREATE SERVICE LEVEL sl4 WITH timeout = 3s");
        cquery_nofail(e, "ATTACH SERVICE LEVEL sl2 TO user2");
        cquery_nofail(e, "ATTACH SERVICE LEVEL sl3 TO user3");
        cquery_nofail(e, "ATTACH SERVICE LEVEL sl4 TO user4");
        cquery_nofail(e, "ALTER SERVICE LEVEL sl WITH timeout = 5s");
        // The roles are granted as follows:
         //  user4  user3
        //      \ /
        //      user2
        //      /
        //    user1
        //
        // which means that user1 should inherit timeouts from all other users
        cquery_nofail(e, "GRANT user2 TO user1"); 
        cquery_nofail(e, "GRANT user3 TO user2");
        cquery_nofail(e, "GRANT user4 TO user2");
        e.refresh_client_state().get();
        // Avoid reading from memtables, which does not check timeouts due to being too fast
        e.db().invoke_on_all([] (replica::database& db) { return db.flush_all_memtables(); }).get();
        // For user1, operations should time out
        BOOST_REQUIRE_THROW(e.execute_cql("SELECT * FROM t where id = 1 BYPASS CACHE").get(), exceptions::read_timeout_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("SELECT * FROM t BYPASS CACHE").get(), exceptions::read_timeout_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("INSERT INTO t (id, v) VALUES (1,2)").get(), exceptions::mutation_write_timeout_exception);
        // after switching to user2, same thing should be observed
        authenticate(e, "user2", "pass").get();
        e.refresh_client_state().get();
        BOOST_REQUIRE_THROW(e.execute_cql("SELECT * FROM t where id = 1 BYPASS CACHE").get(), exceptions::read_timeout_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("SELECT * FROM t BYPASS CACHE").get(), exceptions::read_timeout_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("INSERT INTO t (id, v) VALUES (1,2)").get(), exceptions::mutation_write_timeout_exception);
        // after switching to user3, same thing should be observed
        authenticate(e, "user3", "pass").get();
        e.refresh_client_state().get();
        BOOST_REQUIRE_THROW(e.execute_cql("SELECT * FROM t where id = 1 BYPASS CACHE").get(), exceptions::read_timeout_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("SELECT * FROM t BYPASS CACHE").get(), exceptions::read_timeout_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("INSERT INTO t (id, v) VALUES (1,2)").get(), exceptions::mutation_write_timeout_exception);
        // after switching to user4, everything should work fine
        authenticate(e, "user4", "pass").get();
        e.refresh_client_state().get();
        cquery_nofail(e, "SELECT * FROM t where id = 1 BYPASS CACHE");
        cquery_nofail(e, "SELECT * FROM t BYPASS CACHE");
        cquery_nofail(e, "INSERT INTO t (id, v) VALUES (1,2)");
    }, auth_on(false));
}

SEASTAR_TEST_CASE(test_alter_with_workload_type) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "CREATE ROLE user1 WITH PASSWORD = 'pass' AND LOGIN = true");
        cquery_nofail(e, "CREATE ROLE user2 WITH PASSWORD = 'pass' AND LOGIN = true");
        cquery_nofail(e, "CREATE ROLE user3 WITH PASSWORD = 'pass' AND LOGIN = true");
        cquery_nofail(e, "CREATE ROLE user4 WITH PASSWORD = 'pass' AND LOGIN = true");
        authenticate(e, "user1", "pass").get();

        cquery_nofail(e, "CREATE SERVICE LEVEL sl");
        cquery_nofail(e, "ATTACH SERVICE LEVEL sl TO user1");

        auto sl_is_v2 = e.local_client_state().get_service_level_controller().is_v2();
	    auto msg = cquery_nofail(e, format("SELECT workload_type FROM {}", sl_is_v2 ? "system.service_levels_v2" : "system_distributed.service_levels"));
        assert_that(msg).is_rows().with_rows({{
            {}
        }});

        e.refresh_client_state().get();
        // Default workload type is `unspecified`
        BOOST_REQUIRE_EQUAL(e.local_client_state().get_workload_type(), service::client_state::workload_type::unspecified);

        // When multiple per-role timeouts apply, the smallest value is always effective
        cquery_nofail(e, "CREATE SERVICE LEVEL sl2 WITH workload_type = null");
        cquery_nofail(e, "CREATE SERVICE LEVEL sl3 WITH workload_type = 'batch'");
        cquery_nofail(e, "CREATE SERVICE LEVEL sl4 WITH workload_type = 'interactive'");
        cquery_nofail(e, "ATTACH SERVICE LEVEL sl2 TO user2");
        cquery_nofail(e, "ATTACH SERVICE LEVEL sl3 TO user3");
        cquery_nofail(e, "ATTACH SERVICE LEVEL sl4 TO user4");
        cquery_nofail(e, "ALTER SERVICE LEVEL sl WITH workload_type = 'interactive'");
        // The roles are granted as follows:
         //  user4  user3
        //      \ /
        //      user2
        //      /
        //    user1
        //
        // which means that user1 should inherit workload types from all other users
        cquery_nofail(e, "GRANT user2 TO user1"); 
        cquery_nofail(e, "GRANT user3 TO user2");
        cquery_nofail(e, "GRANT user4 TO user2");
        e.refresh_client_state().get();
        // For user1, the effective workload type should be batch
        BOOST_REQUIRE_EQUAL(e.local_client_state().get_workload_type(), service::client_state::workload_type::batch);
        // after switching to user2, still batch
        authenticate(e, "user2", "pass").get();
        e.refresh_client_state().get();
        BOOST_REQUIRE_EQUAL(e.local_client_state().get_workload_type(), service::client_state::workload_type::batch);
        // after switching to user3, batch again
        authenticate(e, "user3", "pass").get();
        e.refresh_client_state().get();
        BOOST_REQUIRE_EQUAL(e.local_client_state().get_workload_type(), service::client_state::workload_type::batch);
        // after switching to user4, the workload is interactive
        authenticate(e, "user4", "pass").get();
        e.refresh_client_state().get();
        BOOST_REQUIRE_EQUAL(e.local_client_state().get_workload_type(), service::client_state::workload_type::interactive);
    }, auth_on(false));
}

SEASTAR_TEST_CASE(test_try_to_create_role_with_hashed_password_and_password) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        BOOST_REQUIRE_THROW(
            env.execute_cql("CREATE ROLE jane WITH HASHED PASSWORD = 'something' AND PASSWORD = 'something'").get(),
            exceptions::syntax_exception);
    }, auth_on(false));
}

SEASTAR_TEST_CASE(test_try_to_create_role_with_password_and_hashed_password) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        BOOST_REQUIRE_THROW(
            env.execute_cql("CREATE ROLE jane WITH PASSWORD = 'something' AND HASHED PASSWORD = 'something'").get(),
            exceptions::syntax_exception);
    }, auth_on(false));
}

SEASTAR_TEST_CASE(test_try_create_role_with_hashed_password_as_anonymous_user) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        env.local_client_state().set_login(auth::anonymous_user());
        env.refresh_client_state().get();
        BOOST_REQUIRE(auth::is_anonymous(*env.local_client_state().user()));
        BOOST_REQUIRE_THROW(env.execute_cql("CREATE ROLE my_new_role WITH HASHED PASSWORD = 'myhash'").get(), exceptions::unauthorized_exception);
    }, auth_on(true));
}

SEASTAR_TEST_CASE(test_create_roles_with_hashed_password_and_log_in) {
    // This test ensures that Scylla allows for creating roles with hashed passwords
    // following the format of one of the supported algorithms, as well as logging in
    // as that role is performed successfully.
    return do_with_cql_env_thread([] (cql_test_env& env) {
        // Pairs of form (password, hashed password).
        constexpr std::pair<std::string_view, std::string_view> passwords[] = {
            // bcrypt's.
            {"myPassword", "$2a$05$ae4qyC7lYe47n8K2f/fgKuW/TCRCCpEvcYrA4Dl14VYJAjAEz3tli"},
            {"myPassword", "$2b$05$ae4qyC7lYe47n8K2f/fgKuW/TCRCCpEvcYrA4Dl14VYJAjAEz3tli"},
            {"myPassword", "$2x$05$ae4qyC7lYe47n8K2f/fgKuW/TCRCCpEvcYrA4Dl14VYJAjAEz3tli"},
            {"myPassword", "$2y$05$ae4qyC7lYe47n8K2f/fgKuW/TCRCCpEvcYrA4Dl14VYJAjAEz3tli"},
            // sha512.
            {"myPassword", "$6$pffOF1SkGYpLPe7h$tsYwSqUvbzh2O79dtMNadUsYawCrHMfK06XWFh3vJIMwqaVsaiFsubB2a7uZshDVpJWhTCnGWGKsy3fAteFw9/"},
            // sha256.
            {"myPassword", "$5$AKS.nD1e18H.7gu9$IWy7QB0K.qoYkrWmFn6rZ4BO6Y.FWdCchrFg3beXfx8"},
            // md5.
            {"myPassword", "$1$rVcnG0Et$qAhrrNev1JVV9Zu5qhnry1"}
        };
        for (auto [pwd, hash] : passwords) {
            env.execute_cql(seastar::format("CREATE ROLE r WITH HASHED PASSWORD = '{}' AND LOGIN = true", hash)).get();
            // First, try to log in using an incorrect password.
            BOOST_REQUIRE_EXCEPTION(authenticate(env, "r", "notThePassword").get(), exceptions::authentication_exception,
                    exception_predicate::message_equals("Username and/or password are incorrect"));
            // Now use the correct one.
            authenticate(env, "r", pwd).get();

            // We need to log in as a superuser to be able to drop the role.
            authenticate(env, "cassandra", "cassandra").get();
            env.execute_cql("DROP ROLE r").get();
        }
    }, auth_on(true));
}

SEASTAR_TEST_CASE(test_try_login_after_creating_roles_with_hashed_password) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        // Note: crypt(5) specifies:
        //
        //    "Hashed passphrases are always entirely printable ASCII, and do not contain any whitespace
        //     or the characters `:`, `;`, `*`, `!`, or `\`.   (These  characters  are
        //     used as delimiters and special markers in the passwd(5) and shadow(5) files.)"

        env.execute_cql("CREATE ROLE invalid_role WITH HASHED PASSWORD = ';' AND LOGIN = true").get();
        env.execute_cql("CREATE ROLE valid_role WITH HASHED PASSWORD = 'hashed_password' AND LOGIN = true").get();
        BOOST_REQUIRE_EXCEPTION(authenticate(env, "invalid_role", "pwd").get(), exceptions::authentication_exception,
                exception_predicate::message_equals("Could not verify password"));
        BOOST_REQUIRE_EXCEPTION(authenticate(env, "valid_role", "pwd").get(), exceptions::authentication_exception,
                exception_predicate::message_equals("Username and/or password are incorrect"));
    }, auth_on(true));
}

SEASTAR_TEST_CASE(test_try_describe_schema_with_internals_and_passwords_as_anonymous_user) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        env.local_client_state().set_login(auth::anonymous_user());
        env.refresh_client_state().get();
        BOOST_REQUIRE(auth::is_anonymous(*env.local_client_state().user()));
        BOOST_REQUIRE_EXCEPTION(env.execute_cql("DESC SCHEMA WITH INTERNALS AND PASSWORDS").get(), exceptions::unauthorized_exception,
                exception_predicate::message_equals("DESCRIBE SCHEMA WITH INTERNALS AND PASSWORDS can only be issued by a superuser"));
    }, auth_on(true));
}

BOOST_AUTO_TEST_SUITE_END()
