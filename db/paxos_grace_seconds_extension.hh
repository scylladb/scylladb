/*
 * Copyright 2020 ScyllaDB
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
 * You should have received a copy of the GNU Affero General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "serializer.hh"
#include "schema.hh"
#include "log.hh"

extern logging::logger dblog;

namespace db {

/**
 * \brief Schema extension which represents `paxos_grace_seconds` per-table option.
 *
 * The option defines the TTL value which used for inserts/updates to the paxos
 * tables when LWT queries are used against the base table.
 *
 * This value is intentionally decoupled from `gc_grace_seconds` since,
 * in general, the base table could use completely different strategy to garbage
 * collect entries, e.g. can set `gc_grace_seconds` to 0 if it doesn't use
 * deletions and hence doesn't need to repair.
 *
 * However, paxos tables still rely on repair to achieve consistency, and
 * the user is required to execute repair within `paxos_grace_seconds`.
 */
class paxos_grace_seconds_extension : public schema_extension {
    int32_t _paxos_gc_sec;
public:
    static constexpr auto NAME = "paxos_grace_seconds";

    paxos_grace_seconds_extension() = default;
    
    explicit paxos_grace_seconds_extension(int32_t seconds)
        : _paxos_gc_sec(seconds)
    {}

    explicit paxos_grace_seconds_extension(const std::map<sstring, sstring>& map) {
        on_internal_error(dblog, "Cannot create paxos_grace_seconds_extensions from map");
    }

    explicit paxos_grace_seconds_extension(bytes b) : _paxos_gc_sec(deserialize(b))
    {}

    explicit paxos_grace_seconds_extension(const sstring& s)
        : _paxos_gc_sec(std::stoi(s))
    {}

    bytes serialize() const override {
        return ser::serialize_to_buffer<bytes>(_paxos_gc_sec);
    }

    static int32_t deserialize(const bytes_view& buffer) {
        return ser::deserialize_from_buffer(buffer, boost::type<int32_t>());
    }

    int32_t get_paxos_grace_seconds() const {
        return _paxos_gc_sec;
    }
};

} // namespace db
