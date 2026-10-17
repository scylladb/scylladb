/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "auth/passwords.hh"
#include "utils/crypt_sha512.hh"
#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <cctype>
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

seastar::future<bool> is_valid_salted_hash(const sstring& salted_hash) {
    sstring dummy_hash;
    try {
        dummy_hash = co_await detail::hash_with_salt_async("dummy", salted_hash);
    } catch (const std::system_error&) {
        // crypt(3) doesn't support this hashing scheme or its parameters.
        co_return false;
    }
    // crypt(3) reads only the setting - the scheme, its parameters and the
    // salt - from the beginning of the salted hash, and ignores the rest.
    // The rest is an encoding of the output of the scheme's hash function,
    // using the characters [./0-9A-Za-z] (crypt(5) documents the exact format
    // of each scheme, e.g., for bcrypt, "\$2[abxy]\$[0-9]{2}\$[./A-Za-z0-9]{53}").
    // So check that the given salted hash has the same beginning up to its
    // last '$' (the scheme and its parameters, and in most schemes also the
    // salt) as the hash which crypt(3) computed for a dummy password, and that
    // the rest consists of these characters and has the right length -
    // otherwise, no password's hash can ever be equal to it.
    const std::string_view hash(salted_hash);
    const auto setting_size = hash.rfind('$') + 1; // 0 if no '$'
    const auto rest = hash.substr(setting_size);
    if (rest.empty() || hash.substr(0, setting_size) != std::string_view(dummy_hash).substr(0, setting_size)
            || !std::ranges::all_of(rest, [] (char c) { return std::isalnum(static_cast<unsigned char>(c)) || c == '.' || c == '/'; })) {
        co_return false;
    }
    if (setting_size == 0) {
        // A hash without '$' is of the traditional DES-based scheme, 13
        // characters long, or of its "bigcrypt" extension, whose hash grows by
        // 11 characters for every 8 characters of the password beyond 8.
        co_return hash.size() >= 13 && (hash.size() - 13) % 11 == 0;
    }
    // In the other schemes, the hash function's output has a fixed size, so
    // for a given setting, the hashes of all passwords have the same length.
    co_return dummy_hash.size() == salted_hash.size();
}

} // namespace auth::passwords
