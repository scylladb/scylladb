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

std::string long_password(uint32_t len) {
    std::string out;
    auto pattern = "0123456789";
    for (uint32_t i = 0; i < len; ++i) {
        out.push_back(pattern[i % strlen(pattern)]);
    }

    return out;
}

SEASTAR_TEST_CASE(same_hashes_as_crypt_h) {

    std::string long_pwd_254 = long_password(254);
    std::string long_pwd_255 = long_password(255);
    std::string long_pwd_511 = long_password(511);

    std::array<const char*, 8> passwords{
        "12345",
        "1_am_the_greatest!",
        "password1",
        // Some special characters
        "!@#$%^&*()_+-=[]{}|\n;:'\",.<>/?",
        // UTF-8 characters
        "こんにちは、世界！",
        // Passwords close to __crypt_sha512 length limit
        long_pwd_254.c_str(),
        long_pwd_255.c_str(),
        // Password of maximal accepted length
        long_pwd_511.c_str(),
    };

    auto salt = "$6$aaaabbbbccccdddd";

    for (const char* p : passwords) {
        auto res = co_await auth::passwords::detail::hash_with_salt_async(p, salt);
        BOOST_REQUIRE(res == auth::passwords::detail::hash_with_salt(p, salt));
    }
}

//
// A hashed password that does not match the password in cleartext does not authenticate.
//
SEASTAR_TEST_CASE(incorrect_passwords_do_not_authenticate) {
    const sstring hashed_password = auth::passwords::hash("actual_password", rng_for_salt,auth::passwords::scheme::sha_512);
    BOOST_REQUIRE(!co_await auth::passwords::check("password_guess", hashed_password));
}
