/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "auth/passwords.hh"
#include "utils/crypt_sha512.hh"
#include <seastar/core/coroutine.hh>

#include <cerrno>

extern "C" {
#include <crypt.h>
#include <unistd.h>
}

namespace auth::passwords {

static thread_local crypt_data tlcrypt = {};

namespace detail {

void verify_hashing_output(const char * res) {
    if (!res || (res[0] == '*')) {
        throw std::system_error(errno, std::system_category());
    }
}

void verify_scheme(scheme scheme) {
    const sstring random_part_of_salt = "aaaabbbbccccdddd";

    const sstring salt = sstring(prefix_for_scheme(scheme)) + random_part_of_salt;
    const char* e = crypt_r("fisk", salt.c_str(), &tlcrypt);
    try {
        verify_hashing_output(e);
    } catch (const std::system_error& ex) {
        throw no_supported_schemes();
    }
}

sstring hash_with_salt(const sstring& pass, const sstring& salt) {
    auto res = crypt_r(pass.c_str(), salt.c_str(), &tlcrypt);
    verify_hashing_output(res);
    return res;
}

seastar::future<sstring> hash_with_salt_async(const sstring& pass, const sstring& salt) {
    sstring res;
    // Only SHA-512 hashes for passphrases shorter than 256 bytes can be computed using
    // the __crypt_sha512 method. For other computations, we fall back to the
    // crypt_r implementation from `<crypt.h>`, which can stall.
    if (salt.starts_with(prefix_for_scheme(scheme::sha_512)) && pass.size() <= 255) {
        char buf[128];
        const char * output_ptr = co_await __crypt_sha512(pass.c_str(), salt.c_str(), buf);
        verify_hashing_output(output_ptr);
        res = output_ptr;
    } else {
        const char * output_ptr = crypt_r(pass.c_str(), salt.c_str(), &tlcrypt);
        verify_hashing_output(output_ptr);
        res = output_ptr;
    }
    co_return res;
}

std::string_view prefix_for_scheme(scheme c) noexcept {
    switch (c) {
    case scheme::bcrypt_y: return "$2y$";
    case scheme::bcrypt_a: return "$2a$";
    case scheme::sha_512: return "$6$";
    case scheme::sha_256: return "$5$";
    case scheme::md5: return "$1$";
    }
}

} // namespace detail

no_supported_schemes::no_supported_schemes()
        : std::runtime_error("No allowed hashing schemes are supported on this system") {
}

seastar::future<bool> check(const sstring& pass, const sstring& salted_hash) {
    const auto pwd_hash = co_await detail::hash_with_salt_async(pass, salted_hash);
    co_return pwd_hash == salted_hash;
}

} // namespace auth::passwords
