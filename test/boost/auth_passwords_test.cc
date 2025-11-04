/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/testing/test_case.hh>

#include <array>
#include <random>
#include <unordered_set>

#include "auth/passwords.hh"

#include <boost/test/unit_test.hpp>
#include <seastar/core/sstring.hh>
#include <seastar/core/coroutine.hh>

#include "seastarx.hh"

extern "C" {
#include <crypt.h>
#include <unistd.h>
}

static auto rng_for_salt = std::default_random_engine(std::random_device{}());

//
// The same password hashed multiple times will result in different strings because the salt will be different.
//
SEASTAR_TEST_CASE(passwords_are_salted) {
    const char* const cleartext = "my_excellent_password";
    std::unordered_set<sstring> observed_passwords{};

    for (int i = 0; i < 10; ++i) {
        const sstring e = auth::passwords::hash(cleartext, rng_for_salt, auth::passwords::scheme::sha_512);
        BOOST_REQUIRE(!observed_passwords.contains(e));
        observed_passwords.insert(e);
    }
    co_return;
}

//
// A hashed password will authenticate against the same password in cleartext.
//
SEASTAR_TEST_CASE(correct_passwords_authenticate) {
    // Common passwords.
    std::array<const char*, 3> passwords{
        "12345",
        "1_am_the_greatest!",
        "password1"
    };

    for (const char* p : passwords) {
        BOOST_REQUIRE(co_await auth::passwords::check(p, auth::passwords::hash(p, rng_for_salt, auth::passwords::scheme::sha_512)));
    }
}

//
// A hashed password that does not match the password in cleartext does not authenticate.
//
SEASTAR_TEST_CASE(incorrect_passwords_do_not_authenticate) {
    const sstring hashed_password = auth::passwords::hash("actual_password", rng_for_salt,auth::passwords::scheme::sha_512);
    BOOST_REQUIRE(!co_await auth::passwords::check("password_guess", hashed_password));
}
