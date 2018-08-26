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

extern "C" {
#include <crypt.h>
#include <unistd.h>
}

namespace auth::passwords {

static thread_local crypt_data tlcrypt = { 0, };

namespace detail {

scheme identify_best_supported_scheme() {
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

sstring hash_with_salt(const sstring& pass, const sstring& salt) {
    auto res = crypt_r(pass.c_str(), salt.c_str(), &tlcrypt);
    if (!res || (res[0] == '*')) {
        throw std::system_error(errno, std::system_category());
    }
    return res;
}

const char* prefix_for_scheme(scheme c) noexcept {
    switch (c) {
    case scheme::bcrypt_y: return "$2y$";
    case scheme::bcrypt_a: return "$2a$";
    case scheme::sha_512: return "$6$";
    case scheme::sha_256: return "$5$";
    case scheme::md5: return "$1$";
    default: return nullptr;
    }
}

} // namespace detail

no_supported_schemes::no_supported_schemes()
        : std::runtime_error("No allowed hashing schemes are supported on this system") {
}

bool check(const sstring& pass, const sstring& salted_hash) {
    return detail::hash_with_salt(pass, salted_hash) == salted_hash;
}

} // namespace auth::paswords
