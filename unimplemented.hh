/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include <iostream>
#include "core/print.hh"
#include "core/sstring.hh"
#include "core/enum.hh"

namespace unimplemented {

enum class cause {
    INDEXES,
    LWT,
    PAGING,
    AUTH,
    PERMISSIONS,
    TRIGGERS,
    COUNTERS,
    METRICS,
    MIGRATIONS,
    COMPACT_TABLES,
    GOSSIP,
    TOKEN_RESTRICTION,
    LEGACY_COMPOSITE_KEYS,
    COLLECTION_RANGE_TOMBSTONES,
    RANGE_QUERIES,
    RANGE_DELETES,
    THRIFT,
    VALIDATION,
    REVERSED,
    COMPRESSION,
    NONATOMIC,
    CONSISTENCY,
    HINT,
    SUPER,
    WRAP_AROUND, // Support for handling wrap around ranges in queries on database level and below
};

void fail(cause what) __attribute__((noreturn));
void warn(cause what);

}

namespace std {

template <>
struct hash<unimplemented::cause> : enum_hash<unimplemented::cause> {};

}
