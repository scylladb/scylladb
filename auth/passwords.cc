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

static constexpr size_t rand_bytes = 16;
static thread_local crypt_data tlcrypt = { 0, };

static sstring hash(const sstring& pass, const sstring& salt) {
    auto res = crypt_r(pass.c_str(), salt.c_str(), &tlcrypt);
    if (!res || (res[0] == '*')) {
        throw std::system_error(errno, std::system_category());
    }
    return res;
}

bool check(const sstring& pass, const sstring& salted_hash) {
    auto tmp = hash(pass, salted_hash);
    return tmp == salted_hash;
}

sstring gensalt() {
    static thread_local std::random_device rd{};
    static sstring prefix;

    std::default_random_engine e1{rd()};
    std::uniform_int_distribution<char> dist;

    sstring valid_salt = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789./";
    sstring input(rand_bytes, 0);

    for (char&c : input) {
        c = valid_salt[dist(e1) % valid_salt.size()];
    }

    sstring salt;

    if (!prefix.empty()) {
        return prefix + input;
    }

    // Try in order:
    // blowfish 2011 fix, blowfish, sha512, sha256, md5
    for (sstring pfx : { "$2y$", "$2a$", "$6$", "$5$", "$1$" }) {
        salt = pfx + input;
        const char* e = crypt_r("fisk", salt.c_str(), &tlcrypt);

        if (e && (e[0] != '*')) {
            prefix = pfx;
            return salt;
        }
    }
    throw std::runtime_error("Could not initialize hashing algorithm");
}

sstring hash(const sstring& pass) {
    return hash(pass, gensalt());
}

}
