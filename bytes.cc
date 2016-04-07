/*
 * Copyright (C) 2014 ScyllaDB
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

#include "bytes.hh"
#include "core/print.hh"

static inline int8_t hex_to_int(unsigned char c) {
    switch (c) {
        case '0': return 0;
        case '1': return 1;
        case '2': return 2;
        case '3': return 3;
        case '4': return 4;
        case '5': return 5;
        case '6': return 6;
        case '7': return 7;
        case '8': return 8;
        case '9': return 9;
        case 'a': case 'A': return 10;
        case 'b': case 'B': return 11;
        case 'c': case 'C': return 12;
        case 'd': case 'D': return 13;
        case 'e': case 'E': return 14;
        case 'f': case 'F': return 15;
        default:
            return -1;
    }
}

bytes from_hex(sstring_view s) {
    if (s.length() % 2 == 1) {
        throw std::invalid_argument("An hex string representing bytes must have an even length");
    }
    bytes out{bytes::initialized_later(), s.length() / 2};
    unsigned end = out.size();
    for (unsigned i = 0; i != end; i++) {
        auto half_byte1 = hex_to_int(s[i * 2]);
        auto half_byte2 = hex_to_int(s[i * 2 + 1]);
        if (half_byte1 == -1 || half_byte2 == -1) {
            throw std::invalid_argument(sprint("Non-hex characters in %s", s));
        }
        out[i] = (half_byte1 << 4) | half_byte2;
    }
    return out;
}

sstring to_hex(bytes_view b) {
    static char digits[] = "0123456789abcdef";
    sstring out(sstring::initialized_later(), b.size() * 2);
    unsigned end = b.size();
    for (unsigned i = 0; i != end; ++i) {
        uint8_t x = b[i];
        out[2*i] = digits[x >> 4];
        out[2*i+1] = digits[x & 0xf];
    }
    return out;
}

sstring to_hex(const bytes& b) {
    return to_hex(bytes_view(b));
}

sstring to_hex(const bytes_opt& b) {
    return !b ? "null" : to_hex(*b);
}

std::ostream& operator<<(std::ostream& os, const bytes& b) {
    return os << to_hex(b);
}

std::ostream& operator<<(std::ostream& os, const bytes_opt& b) {
    if (b) {
        return os << *b;
    }
    return os << "null";
}

namespace std {

std::ostream& operator<<(std::ostream& os, const bytes_view& b) {
    return os << to_hex(b);
}

}
