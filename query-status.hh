/*
 * Copyright (C) 2020 ScyllaDB
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
#include <cinttypes>

namespace ser {
template <typename T>
class serializer;
}

namespace query {

// The query status can be used to provide more context for a particular
// query. The status is propagated from a replica to its coordinator
// via RPC.
//
// Main motivation for introducing this class is that returning an error
// via RPC layer lacks context - the coordinator does not know why the query
// failed, only  that it did. In order to remedy that, an additional status
// field is sent.
//
// Status values and their meanings:
// * OK
//     The query succeeded without issues and the response contains valid data.
// * OVERLOADED
//     The query was rejected due to target replica being overloaded.
//     Results returned by the response should be ignored, and a proper
//     exception should be propagated to the client instead.
class status {
public:
    enum value { OK, OVERLOADED };
private:
    value _value;
public:
    status(value value) : _value(value) {}
    explicit status(uint8_t v) : _value(static_cast<value>(v)) {}
    uint8_t get_status() const {
        return static_cast<uint8_t>(_value);
    }
    friend struct ser::serializer<status>;
    bool operator==(const status& other) const = default;
};

}
