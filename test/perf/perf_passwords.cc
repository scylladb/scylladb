/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/testing/perf_tests.hh>

#include "auth/passwords.hh"
#include "utils/crypt_sha512.hh"

// Cost of verifying a CQL password against a stored SHA-512 salted hash.
// "fresh" models a login with credentials not seen before, "repeated" a
// reconnect with the same credentials, and "wrong" a failed login.
template <size_t PasswordLength>
struct password_fixture {
    const sstring password = make_password('p');
    const sstring wrong_password = make_password('q');
    const sstring salted_hash = auth::passwords::detail::hash_with_salt(password, "$6$aaaabbbbccccdddd");

    static sstring make_password(char c) {
        sstring p(PasswordLength, c);
        p[0] = 'A';
        return p;
    }

    future<> crypt_sha512() {
        char buf[128];
        perf_tests::do_not_optimize(co_await __crypt_sha512(password.c_str(), salted_hash.c_str(), buf));
    }

    future<> check_fresh() {
        perf_tests::do_not_optimize(co_await auth::passwords::check(password, salted_hash));
    }

    future<> check_repeated() {
        perf_tests::do_not_optimize(co_await auth::passwords::check(password, salted_hash));
    }

    future<> check_wrong() {
        perf_tests::do_not_optimize(co_await auth::passwords::check(wrong_password, salted_hash));
    }
};

using password_5 = password_fixture<5>;
using password_16 = password_fixture<16>;
using password_64 = password_fixture<64>;

PERF_TEST_C(password_5, crypt_sha512) { return crypt_sha512(); }
PERF_TEST_C(password_5, check_fresh) { return check_fresh(); }
PERF_TEST_C(password_5, check_repeated) { return check_repeated(); }
PERF_TEST_C(password_5, check_wrong) { return check_wrong(); }

PERF_TEST_C(password_16, crypt_sha512) { return crypt_sha512(); }
PERF_TEST_C(password_16, check_fresh) { return check_fresh(); }
PERF_TEST_C(password_16, check_repeated) { return check_repeated(); }
PERF_TEST_C(password_16, check_wrong) { return check_wrong(); }

PERF_TEST_C(password_64, crypt_sha512) { return crypt_sha512(); }
PERF_TEST_C(password_64, check_fresh) { return check_fresh(); }
PERF_TEST_C(password_64, check_repeated) { return check_repeated(); }
PERF_TEST_C(password_64, check_wrong) { return check_wrong(); }
