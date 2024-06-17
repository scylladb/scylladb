/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/print.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/enum.hh>

namespace unimplemented {

enum class cause {
    API,
    INDEXES,
    LWT,
    PAGING,
    AUTH,
    PERMISSIONS,
    TRIGGERS,
    COUNTERS,
    METRICS,
    MIGRATIONS,
    GOSSIP,
    TOKEN_RESTRICTION,
    LEGACY_COMPOSITE_KEYS,
    COLLECTION_RANGE_TOMBSTONES,
    RANGE_DELETES,
    VALIDATION,
    REVERSED,
    COMPRESSION,
    NONATOMIC,
    CONSISTENCY,
    HINT,
    SUPER,
    WRAP_AROUND, // Support for handling wrap around ranges in queries on database level and below
    STORAGE_SERVICE,
    SCHEMA_CHANGE,
    MIXED_CF,
    SSTABLE_FORMAT_M,
};

[[noreturn]] void fail(cause what);
void warn(cause what);

}

namespace std {

template <>
struct hash<unimplemented::cause> : seastar::enum_hash<unimplemented::cause> {};

}
