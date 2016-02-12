/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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

// Abstraction of transport protocol-dependent serialization format
// Protocols v1, v2 used 16 bits for collection sizes, while v3 and
// above use 32 bits.  But letting every bit of the code know what
// transport protocol we're using (and in some cases, we aren't using
// any transport -- it's for internal storage) is bad, so abstract it
// away here.

class cql_serialization_format {
    bool _use_32_bit;
private:
    explicit cql_serialization_format(bool use_32_bit) : _use_32_bit(use_32_bit) {}
public:
    static cql_serialization_format use_16_bit() { return cql_serialization_format(false); }
    static cql_serialization_format use_32_bit() { return cql_serialization_format(true); }
    static cql_serialization_format internal() { return use_32_bit(); }
    bool using_32_bits_for_collections() const { return _use_32_bit; }
    bool operator==(cql_serialization_format x) const { return _use_32_bit == x._use_32_bit; }
    bool operator!=(cql_serialization_format x) const { return !operator==(x); }
};
