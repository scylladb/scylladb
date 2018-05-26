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

#include "tests/test-utils.hh"
#include "tests/cql_test_env.hh"
#include "tests/cql_assertions.hh"

#include "auth/allow_all_authenticator.hh"
#include "auth/authenticator.hh"
#include "auth/password_authenticator.hh"
#include "auth/service.hh"
#include "auth/authenticated_user.hh"
#include "auth/resource.hh"

#include "db/config.hh"
#include "cql3/query_processor.hh"

SEASTAR_TEST_CASE(test_default_authenticator) {
    return do_with_cql_env([](cql_test_env& env) {
        auto& a = env.local_auth_service().underlying_authenticator();
        BOOST_REQUIRE_EQUAL(a.require_authentication(), false);
        BOOST_REQUIRE_EQUAL(a.qualified_java_name(), auth::allow_all_authenticator_name());
        return make_ready_future();
    });
}

SEASTAR_TEST_CASE(test_password_authenticator_attributes) {
    db::config cfg;
    cfg.authenticator(auth::password_authenticator_name());

    return do_with_cql_env([](cql_test_env& env) {
        auto& a = env.local_auth_service().underlying_authenticator();
        BOOST_REQUIRE_EQUAL(a.require_authentication(), true);
        BOOST_REQUIRE_EQUAL(a.qualified_java_name(), auth::password_authenticator_name());
        return make_ready_future();
    }, cfg);
}

SEASTAR_TEST_CASE(test_password_authenticator_operations) {
    db::config cfg;
    cfg.authenticator(auth::password_authenticator_name());

    /**
     * Not using seastar::async due to apparent ASan bug.
     * Enjoy the slightly less readable code.
     */
    return do_with_cql_env([](cql_test_env& env) {
        sstring username("fisk");
        sstring password("notter");

        using namespace auth;

        auto USERNAME_KEY = authenticator::USERNAME_KEY;
        auto PASSWORD_KEY = authenticator::PASSWORD_KEY;

        auto& a = env.local_auth_service().underlying_authenticator();

        // check non-existing user
        return a.authenticate({ { USERNAME_KEY, username }, { PASSWORD_KEY, password } }).then_wrapped([&a](future<auth::authenticated_user>&& f) {
            try {
                f.get();
                BOOST_FAIL("should not reach");
            } catch (exceptions::authentication_exception&) {
                // ok
            }
        }).then([=, &a] {
            authentication_options options;
            options.password = password;

            return a.create(username, std::move(options)).then([=, &a] {
                return a.authenticate({ { USERNAME_KEY, username }, { PASSWORD_KEY, password } }).then([=](auth::authenticated_user user) {
                    BOOST_REQUIRE_EQUAL(auth::is_anonymous(user), false);
                    BOOST_REQUIRE_EQUAL(*user.name, username);
                });
            });
        }).then([=, &a] {
            // check wrong password
            return a.authenticate( { {USERNAME_KEY, username}, {PASSWORD_KEY, "hejkotte"}}).then_wrapped([](future<auth::authenticated_user>&& f) {
                try {
                    f.get();
                    BOOST_FAIL("should not reach");
                } catch (exceptions::authentication_exception&) {
                    // ok
                }
            });
        }).then([=, &a] {
            // sasl
            auto sasl = a.new_sasl_challenge();

            BOOST_REQUIRE_EQUAL(sasl->is_complete(), false);

            bytes b;
            int8_t i = 0;
            b.append(&i, 1);
            b.insert(b.end(), username.begin(), username.end());
            b.append(&i, 1);
            b.insert(b.end(), password.begin(), password.end());

            sasl->evaluate_response(b);
            BOOST_REQUIRE_EQUAL(sasl->is_complete(), true);

            return sasl->get_authenticated_user().then([=](auth::authenticated_user user) {
                BOOST_REQUIRE_EQUAL(auth::is_anonymous(user), false);
                BOOST_REQUIRE_EQUAL(*user.name, username);
            });
        }).then([=, &a] {
            // check deleted user
            return a.drop(username).then([=, &a] {
                return a.authenticate({ { USERNAME_KEY, username }, { PASSWORD_KEY, password } }).then_wrapped([](future<auth::authenticated_user>&& f) {
                    try {
                        f.get();
                        BOOST_FAIL("should not reach");
                    } catch (exceptions::authentication_exception&) {
                        // ok
                    }
                });
            });
        });
    }, cfg);
}


SEASTAR_TEST_CASE(test_cassandra_hash) {
    db::config cfg;
    cfg.authenticator(auth::password_authenticator_name());

    return do_with_cql_env([](cql_test_env& env) {
        /**
         * Try to check password against hash from origin.
         * Allow for specific failure if glibc cannot handle the
         * hash algo (i.e. blowfish).
         */

        sstring username("fisk");
        sstring password("cassandra");
        sstring salted_hash("$2a$10$8cz4EZ5v8f/aTZFkNEQafe.z66ZvjOonOpHCApwx0ksWp3aKf.Roq");

        // This is extremely whitebox. We'll just go right ahead and know
        // what the tables etc are called. Oy wei...
        auto f = env.local_qp().process("INSERT into system_auth.roles (role, salted_hash) values (?, ?)", db::consistency_level::ONE,
                        infinite_timeout_config,
                        { username, salted_hash }).discard_result();

        return f.then([=, &env] {
            auto& a = env.local_auth_service().underlying_authenticator();

            auto USERNAME_KEY = auth::authenticator::USERNAME_KEY;
            auto PASSWORD_KEY = auth::authenticator::PASSWORD_KEY;

            // try to verify our user with a cassandra-originated salted_hash
            return a.authenticate({ { USERNAME_KEY, username }, { PASSWORD_KEY, password } }).then_wrapped([](future<auth::authenticated_user> f) {
                try {
                    f.get();
                } catch (exceptions::authentication_exception& e) {
                    try {
                        std::rethrow_if_nested(e);
                        BOOST_FAIL(std::string("Unexcepted exception ") + e.what());
                    } catch (std::system_error & e) {
                        bool is_einval = e.code().category() == std::system_category() && e.code().value() == EINVAL;
                        BOOST_WARN_MESSAGE(is_einval, "Could not verify cassandra password hash due to glibc limitation");
                        if (!is_einval) {
                            BOOST_FAIL(std::string("Unexcepted system error ") + e.what());
                        }
                    }
                }
            });
        });
    }, cfg);
}


