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

#pragma once

#include <iosfwd>

using cql_protocol_version_type = uint8_t;

// Abstraction of transport protocol-dependent serialization format
// Protocols v1, v2 used 16 bits for collection sizes, while v3 and
// above use 32 bits.  But letting every bit of the code know what
// transport protocol we're using (and in some cases, we aren't using
// any transport -- it's for internal storage) is bad, so abstract it
// away here.

class cql_serialization_format {
    cql_protocol_version_type _version;
public:
    static constexpr cql_protocol_version_type latest_version = 4;
    explicit cql_serialization_format(cql_protocol_version_type version) : _version(version) {}
    static cql_serialization_format latest() { return cql_serialization_format{latest_version}; }
    static cql_serialization_format internal() { return latest(); }
    bool using_32_bits_for_collections() const { return _version >= 3; }
    bool operator==(cql_serialization_format x) const { return _version == x._version; }
    bool operator!=(cql_serialization_format x) const { return !operator==(x); }
    cql_protocol_version_type protocol_version() const { return _version; }
    friend std::ostream& operator<<(std::ostream& out, const cql_serialization_format& sf) {
        return out << static_cast<int>(sf._version);
    }
    bool collection_format_unchanged(cql_serialization_format other = cql_serialization_format::latest()) const {
        return using_32_bits_for_collections() == other.using_32_bits_for_collections();
    }
};
