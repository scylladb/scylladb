// Copyright (C) 2026-present ScyllaDB
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#pragma once

#include <map>

#include "bytes.hh"
#include "mutation/timestamp.hh"

namespace cql3::expr {

// Per-element timestamps and TTLs for a cell of a map, set or UDT (populated
// when a WRITETIME() or TTL() of col[key] or col.field are in the query.
// Keys are the raw serialized keys or serialized field index.
struct collection_cell_metadata {
    std::map<bytes, api::timestamp_type> timestamps;
    std::map<bytes, int32_t> ttls; // remaining TTL in seconds (-1 if no TTL)
};

} // namespace cql3::expr
