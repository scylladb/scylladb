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

#include "auth/passwords.hh"

#include <cerrno>
#include <optional>
#include <random>

extern "C" {
#include <crypt.h>
#include <unistd.h>
}

// TODO: blowfish
// Origin uses Java bcrypt library, i.e. blowfish salt
// generation and hashing, which is arguably a "better"
// password hash than sha/md5 versions usually available in
// crypt_r. Otoh, glibc 2.7+ uses a modified sha512 algo
// which should be the same order of safe, so the only
// real issue should be salted hash compatibility with
// origin if importing system tables from there.
//
// Since bcrypt/blowfish is _not_ (afaict) not available
// as a dev package/lib on most linux distros, we'd have to
// copy and compile for example OWL  crypto
// (http://cvsweb.openwall.com/cgi/cvsweb.cgi/Owl/packages/glibc/crypt_blowfish/)
// to be fully bit-compatible.
//
// Until we decide this is needed, let's just use crypt_r,
// and some old-fashioned random salt generation.

namespace auth::passwords {

static thread_local crypt_data tlcrypt = { 0, };

no_supported_schemes::no_supported_schemes()
        : std::runtime_error("No allowed hashing schemes are supported on this system") {
}

static sstring hash(const sstring& pass, const sstring& salt) {
    auto res = crypt_r(pass.c_str(), salt.c_str(), &tlcrypt);
    if (!res || (res[0] == '*')) {
        throw std::system_error(errno, std::system_category());
    }
    return res;
}

static const char* prefix_for_scheme(scheme c) noexcept {
    switch (c) {
    case scheme::bcrypt_y: return "$2y$";
    case scheme::bcrypt_a: return "$2a$";
    case scheme::sha_512: return "$6$";
    case scheme::sha_256: return "$5$";
    case scheme::md5: return "$1$";
    default: return nullptr;
    }
}

///
/// Test each allowed hashing scheme and report the best supported one on the current system.
///
/// \throws \ref no_supported_schemes when none of the known schemes is supported.
///
static scheme identify_best_supported_scheme() {
    const auto all_schemes = { scheme::bcrypt_y, scheme::bcrypt_a, scheme::sha_512, scheme::sha_256, scheme::md5 };
    // "Random", for testing schemes.
    const sstring random_part_of_salt = "aaaabbbbccccdddd";

    for (scheme c : all_schemes) {
        const sstring salt = sstring(prefix_for_scheme(c)) + random_part_of_salt;
        const char* e = crypt_r("fisk", salt.c_str(), &tlcrypt);

        if (e && (e[0] != '*')) {
            return c;
        }
    }

    throw no_supported_schemes();
}

static sstring generate_random_salt_bytes() {
    static thread_local std::random_device rd{};
    static const sstring valid_bytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789./";
    static constexpr std::size_t num_bytes = 16;
    std::default_random_engine rng{rd()};
    std::uniform_int_distribution<char> dist;
    sstring result(num_bytes, 0);

    for (char& c : result) {
        c = valid_bytes[dist(rng) % valid_bytes.size()];
    }

    return result;
}

bool check(const sstring& pass, const sstring& salted_hash) {
    auto tmp = hash(pass, salted_hash);
    return tmp == salted_hash;
}

///
/// Generate a implementation-specific salt string for hashing passwords.
///
/// The \ref std::default_random_engine is used to generate the string, which is an implementation-specific length.
///
/// \throws \ref no_supported_schemes when no known hashing schemes are supported on the system.
///
static sstring generate_salt() {
    static const scheme scheme = identify_best_supported_scheme();
    static const sstring prefix = sstring(prefix_for_scheme(scheme));
    return prefix + generate_random_salt_bytes();
}

sstring hash(const sstring& pass) {
    return hash(pass, generate_salt());
}

}
