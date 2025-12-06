/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/format.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/enum.hh>

namespace unimplemented {

enum class cause {
    API,         // REST API features not implemented (force_user_defined_compaction, split_output in major compaction)
    INDEXES,     // Secondary index features (filtering on collections, clustering columns)
    TRIGGERS,    // Trigger support in schema tables and storage proxy
    METRICS,     // Query processor metrics
    VALIDATION,  // Schema validation in DDL statements (drop keyspace, truncate, token functions)
    REVERSED,    // Reversed types in CQL protocol
    HINT,        // Hint replaying in batchlog manager
    SUPER,       // Super column families (legacy Cassandra feature, never supported)
};

[[noreturn]] void fail(cause what);
void warn(cause what);

}

namespace std {

template <>
struct hash<unimplemented::cause> : seastar::enum_hash<unimplemented::cause> {};

}
