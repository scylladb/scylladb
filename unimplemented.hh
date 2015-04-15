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
    COLLECTIONS,
    COUNTERS,
    METRICS,
    MIGRATIONS,
    COMPACT_TABLES,
    GOSSIP,
    TOKEN_RESTRICTION,
    LEGACY_COMPOSITE_KEYS,
    COLLECTION_RANGE_TOMBSTONES,
    RANGE_QUERIES,
    THRIFT,
    VALIDATION,
    REVERSED,
    COMPRESSION,
};

void fail(cause what) __attribute__((noreturn));
void warn(cause what);

}

namespace std {

template <>
struct hash<unimplemented::cause> : enum_hash<unimplemented::cause> {};

}
