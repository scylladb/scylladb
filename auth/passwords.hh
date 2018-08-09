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

#pragma once

#include <seastar/core/sstring.hh>

#include "seastarx.hh"

namespace auth::passwords {

///
/// Generate a implementation-specific salt string for hashing passwords.
///
/// The \ref std::default_random_engine is used to generate the string, which is an implementation-specific length.
///
/// \note This function must be invoked once prior to invoking \ref hash or \ref check in order to initialize the
/// implementation-specific state of the other functions. After being invoked once, the function is thread-safe.
/// However, the function must be invoked initially only by a single thread.
///
/// \throws \ref std::runtime_error when the state cannot be initialized.
///
sstring gensalt();

///
/// Run a one-way hashing function on cleartext to produce encrypted text.
///
/// Prior to applying the hashing function, random salt is amended to the cleartext with \ref gensalt.
///
/// The result is the encrypted cyphertext, and also the salt used but in a implementation-specific format.
///
/// \throws \ref std::system_error when the implementation-specific implementation fails to hash the cleartext.
///
sstring hash(const sstring& pass);

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

}
