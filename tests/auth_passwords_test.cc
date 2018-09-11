/*
 * Copyright (C) 2018 ScyllaDB
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

#define BOOST_TEST_MODULE core

#include <array>
#include <random>
#include <unordered_set>

#include "auth/passwords.hh"

#include <boost/test/unit_test.hpp>
#include <seastar/core/sstring.hh>

#include "seastarx.hh"

static auto rng_for_salt = std::default_random_engine(std::random_device{}());

//
// The same password hashed multiple times will result in different strings because the salt will be different.
//
BOOST_AUTO_TEST_CASE(passwords_are_salted) {
    const char* const cleartext = "my_excellent_password";
    std::unordered_set<sstring> observed_passwords{};

    for (int i = 0; i < 10; ++i) {
        const sstring e = auth::passwords::hash(cleartext, rng_for_salt);
        BOOST_REQUIRE_EQUAL(observed_passwords.count(e), 0);
        observed_passwords.insert(e);
    }
}

//
// A hashed password will authenticate against the same password in cleartext.
//
BOOST_AUTO_TEST_CASE(correct_passwords_authenticate) {
    // Common passwords.
    std::array<const char*, 3> passwords{
        "12345",
        "1_am_the_greatest!",
        "password1"
    };

    for (const char* p : passwords) {
        BOOST_REQUIRE(auth::passwords::check(p, auth::passwords::hash(p, rng_for_salt)));
    }
}

//
// A hashed password that does not match the password in cleartext does not authenticate.
//
BOOST_AUTO_TEST_CASE(incorrect_passwords_do_not_authenticate) {
    const sstring hashed_password = auth::passwords::hash("actual_password", rng_for_salt);
    BOOST_REQUIRE(!auth::passwords::check("password_guess", hashed_password));
}
