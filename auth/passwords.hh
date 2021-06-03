/*
 * Copyright (C) 2018-present ScyllaDB
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

#pragma once

#include <random>
#include <stdexcept>

#include <seastar/core/sstring.hh>

#include "seastarx.hh"

namespace auth::passwords {

class no_supported_schemes : public std::runtime_error {
public:
    no_supported_schemes();
};

///
/// Apache Cassandra uses a library to provide the bcrypt scheme. Many Linux implementations do not support bcrypt, so
/// we support alternatives. The cost is loss of direct compatibility with Apache Cassandra system tables.
///
enum class scheme {
    bcrypt_y,
    bcrypt_a,
    sha_512,
    sha_256,
    md5
};

namespace detail {

template <typename RandomNumberEngine>
sstring generate_random_salt_bytes(RandomNumberEngine& g) {
    static const sstring valid_bytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789./";
    static constexpr std::size_t num_bytes = 16;
    std::uniform_int_distribution<std::size_t> dist(0, valid_bytes.size() - 1);
    sstring result(num_bytes, 0);

    for (char& c : result) {
        c = valid_bytes[dist(g)];
    }

    return result;
}

///
/// Test each allowed hashing scheme and report the best supported one on the current system.
///
/// \throws \ref no_supported_schemes when none of the known schemes is supported.
///
scheme identify_best_supported_scheme();

const char* prefix_for_scheme(scheme) noexcept;

///
/// Generate a implementation-specific salt string for hashing passwords.
///
/// The `RandomNumberEngine` is used to generate the string, which is an implementation-specific length.
///
/// \throws \ref no_supported_schemes when no known hashing schemes are supported on the system.
///
template <typename RandomNumberEngine>
sstring generate_salt(RandomNumberEngine& g) {
    static const scheme scheme = identify_best_supported_scheme();
    static const sstring prefix = sstring(prefix_for_scheme(scheme));
    return prefix + generate_random_salt_bytes(g);
}

///
/// Hash a password combined with an implementation-specific salt string.
///
/// \throws \ref std::system_error when an unexpected implementation-specific error occurs.
///
sstring hash_with_salt(const sstring& pass, const sstring& salt);

} // namespace detail

///
/// Run a one-way hashing function on cleartext to produce encrypted text.
///
/// Prior to applying the hashing function, random salt is amended to the cleartext. The random salt bytes are generated
/// according to the random number engine `g`.
///
/// The result is the encrypted cyphertext, and also the salt used but in a implementation-specific format.
///
/// \throws \ref std::system_error when the implementation-specific implementation fails to hash the cleartext.
///
template <typename RandomNumberEngine>
sstring hash(const sstring& pass, RandomNumberEngine& g) {
    return detail::hash_with_salt(pass, detail::generate_salt(g));
}

///
/// Check that cleartext matches previously hashed cleartext with salt.
///
/// \ref salted_hash is the result of invoking \ref hash, which is the implementation-specific combination of the hashed
/// password and the salt that was generated for it.
///
/// \returns `true` if the cleartext matches the salted hash.
///
/// \throws \ref std::system_error when an unexpected implementation-specific error occurs.
///
bool check(const sstring& pass, const sstring& salted_hash);

} // namespace auth::passwords
