/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
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

    std::array<const char*, 9> passwords{
        "",
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

    for (const char* salt : {"$6$aaaabbbbccccdddd", "$6$rounds=1000$aaaabbbbccccdddd"}) {
        for (const char* p : passwords) {
            auto res = co_await auth::passwords::detail::hash_with_salt_async(p, salt);
            BOOST_REQUIRE(res == auth::passwords::detail::hash_with_salt(p, salt));
        }
    }
}

struct sha512_crypt_vector {
    const char* key;
    const char* setting;
    const char* hash;
};

// Test vectors from the SHA-crypt specification
// (http://people.redhat.com/drepper/SHA-crypt.txt), followed by the musl
// libc self-test vector.
static const std::array<sha512_crypt_vector, 8> sha512_crypt_vectors{{
    {"Hello world!", "$6$saltstring",
        "$6$saltstring$svn8UoSVapNtMuq1ukKS4tPQd8iKwSMHWjl/O817G3uBnIFNjnQJuesI68u4OTLiBFdcbYEdFCoEOfaS35inz1"},
    {"Hello world!", "$6$rounds=10000$saltstringsaltstring",
        "$6$rounds=10000$saltstringsaltst$OW1/O6BYHV6BcXZu8QVeXbDWra3Oeqh0sbHbbMCVNSnCM/UrjmM0Dp8vOuZeHBy/YTBmSK6H9qs/y3RnOaw5v."},
    {"This is just a test", "$6$rounds=5000$toolongsaltstring",
        "$6$rounds=5000$toolongsaltstrin$lQ8jolhgVRVhY4b5pZKaysCLi0QBxGoNeKQzQ3glMhwllF7oGDZxUhx1yxdYcz/e1JSbq3y6JMxxl8audkUEm0"},
    {"a very much longer text to encrypt.  This one even stretches over morethan one line.", "$6$rounds=1400$anotherlongsaltstring",
        "$6$rounds=1400$anotherlongsalts$POfYwTEok97VWcjxIiSOjiykti.o/pQs.wPvMxQ6Fm7I6IoYN3CmLs66x9t0oSwbtEW7o7UmJEiDwGqd8p4ur1"},
    {"we have a short salt string but not a short password", "$6$rounds=77777$short",
        "$6$rounds=77777$short$WuQyW2YR.hBNpjjRhpYD/ifIw05xdfeEyQoMxIXbkvr0gge1a1x3yRULJ5CCaUeOxFmtlcGZelFl5CxtgfiAc0"},
    {"a short string", "$6$rounds=123456$asaltof16chars..",
        "$6$rounds=123456$asaltof16chars..$BtCwjqMJGx5hrJhZywWvt0RLE8uZ4oPwcelCjmw2kSYu.Ec6ycULevoBK25fs2xXgMNrCzIMVcgEJAstJeonj1"},
    // libxcrypt rejects rounds below the minimum, so only __crypt_sha512 clamps them as the specification says.
    {"the minimum number is still observed", "$6$rounds=10$roundstoolow",
        "$6$rounds=1000$roundstoolow$kUMsbe306n21p9R.FRkW3IGn.S9NPN0x50YhH1xhLsPuWGsUSklZt58jaTfF4ZEQpyUNGc0dqbpBYYBaHHrsX."},
    {"Xy01@#\x01\x02\x80\x7f\xff\r\n\x81\t !", "$6$rounds=1234$abc0123456789$",
        "$6$rounds=1234$abc0123456789$BCpt8zLrc/RcyuXmCDOE1ALqMXB2MH6n1g891HhFj8.w7LxGv.FTkqq6Vxc/km3Y0jE0j24jY5PIv/oOu6reg1"},
}};

SEASTAR_TEST_CASE(sha512_crypt_known_answers) {
    for (const auto& v : sha512_crypt_vectors) {
        BOOST_REQUIRE_EQUAL(co_await auth::passwords::detail::hash_with_salt_async(v.key, v.setting), v.hash);
        // Verifying against the full hash is how a login checks a password.
        BOOST_REQUIRE_EQUAL(co_await auth::passwords::detail::hash_with_salt_async(v.key, v.hash), v.hash);
        BOOST_REQUIRE(co_await auth::passwords::check(v.key, v.hash));
        BOOST_REQUIRE(!co_await auth::passwords::check(sstring(v.key) + "x", v.hash));
    }
}

SEASTAR_TEST_CASE(too_long_password) {
    auto p1 = long_password(71);
    auto p2 = long_password(72);
    auto p3 = long_password(73);
    auto too_long_password = long_password(512);

    auto salt_bcrypt = "$2a$05$mAyzaIeJu41dWUkxEbn8hO";
    auto h1_bcrypt = co_await auth::passwords::detail::hash_with_salt_async(p1, salt_bcrypt);
    auto h2_bcrypt = co_await auth::passwords::detail::hash_with_salt_async(p2, salt_bcrypt);
    auto h3_bcrypt = co_await auth::passwords::detail::hash_with_salt_async(p3, salt_bcrypt);
    BOOST_REQUIRE(h1_bcrypt != h2_bcrypt);

    // The check below documents the behavior of the current bcrypt
    // implementation that compares only the first 72 bytes of the password.
    // Although we don't typically use bcrypt for password hashing, it is
    // possible to insert such a hash using`CREATE ROLE ... WITH HASHED PASSWORD ...`.
    // Refs: scylladb/scylladb#26842
    BOOST_REQUIRE(h2_bcrypt == h3_bcrypt);

    // The current implementation of bcrypt password hasing fails with passwords of length 512 and above
    BOOST_CHECK_THROW(co_await auth::passwords::detail::hash_with_salt_async(too_long_password, salt_bcrypt), std::system_error);

    auto salt_sha512 = "$6$aaaabbbbccccdddd";
    auto h1_sha512 = co_await auth::passwords::detail::hash_with_salt_async(p1, salt_sha512);
    auto h2_sha512 = co_await auth::passwords::detail::hash_with_salt_async(p2, salt_sha512);
    auto h3_sha512 = co_await auth::passwords::detail::hash_with_salt_async(p3, salt_sha512);
    BOOST_REQUIRE(h1_sha512 != h2_sha512);
    BOOST_REQUIRE(h2_sha512 != h3_sha512);
    // The current implementation of SHA-512 password hasing fails with passwords of length 512 and above
    BOOST_CHECK_THROW(co_await auth::passwords::detail::hash_with_salt_async(too_long_password, salt_sha512), std::system_error);
}

//
// A hashed password that does not match the password in cleartext does not authenticate.
//
SEASTAR_TEST_CASE(incorrect_passwords_do_not_authenticate) {
    const sstring hashed_password = auth::passwords::hash("actual_password", rng_for_salt,auth::passwords::scheme::sha_512);
    BOOST_REQUIRE(!co_await auth::passwords::check("password_guess", hashed_password));
}
