/*
 * Copyright (C) 2015 ScyllaDB
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


#include "UUID.hh"
#include "net/byteorder.hh"
#include <random>
#include <boost/iterator/function_input_iterator.hpp>
#include <boost/algorithm/string.hpp>
#include <string>
#include "core/sstring.hh"
#include "utils/serialization.hh"
#include "marshal_exception.hh"

namespace utils {

UUID
make_random_uuid() {
    // FIXME: keep in userspace
    static thread_local std::random_device urandom;
    static thread_local std::uniform_int_distribution<uint8_t> dist(0, 255);
    union {
        uint8_t b[16];
        struct {
            uint64_t msb, lsb;
        } w;
    } v;
    for (auto& b : v.b) {
        b = dist(urandom);
    }
    v.b[6] &= 0x0f;
    v.b[6] |= 0x40; // version 4
    v.b[8] &= 0x3f;
    v.b[8] |= 0x80; // IETF variant
    return UUID(net::hton(v.w.msb), net::hton(v.w.lsb));
}

std::ostream& operator<<(std::ostream& out, const UUID& uuid) {
    return out << uuid.to_sstring();
}

UUID::UUID(sstring_view uuid) {
    sstring uuid_string(uuid.begin(), uuid.end());
    boost::erase_all(uuid_string, "-");
    auto size = uuid_string.size() / 2;
    if (size != 16) {
        throw marshal_exception(sprint("UUID string size mismatch: '%s'", uuid));
    }
    sstring most = sstring(uuid_string.begin(), uuid_string.begin() + size);
    sstring least = sstring(uuid_string.begin() + size, uuid_string.end());
    int base = 16;
    this->most_sig_bits = std::stoull(most, nullptr, base);
    this->least_sig_bits = std::stoull(least, nullptr, base);
}

}
