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
    static thread_local std::mt19937_64 engine(std::random_device().operator()());
    static thread_local std::uniform_int_distribution<uint64_t> dist;
    uint64_t msb, lsb;
    msb = dist(engine);
    lsb = dist(engine);
    msb &= ~uint64_t(0x0f << 12);
    msb |= 0x4 << 12; // version 4
    lsb &= ~(uint64_t(0x3) << 62);
    lsb |= uint64_t(0x2) << 62; // IETF variant
    return UUID(msb, lsb);
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
