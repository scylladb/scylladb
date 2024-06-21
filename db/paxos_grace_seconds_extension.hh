/*
 * Copyright 2020-present ScyllaDB
 */
/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "serializer.hh"
#include "schema/schema.hh"
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

    std::string options_to_string() const override {
        return std::to_string(_paxos_gc_sec);
    }

    static int32_t deserialize(const bytes_view& buffer) {
        return ser::deserialize_from_buffer(buffer, boost::type<int32_t>());
    }

    int32_t get_paxos_grace_seconds() const {
        return _paxos_gc_sec;
    }
};

} // namespace db
