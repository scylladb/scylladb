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
    COMPACT_TABLES,
    GOSSIP
};

void fail(cause what) __attribute__((noreturn));
void warn(cause what);

}

namespace std {

template <>
struct hash<unimplemented::cause> : enum_hash<unimplemented::cause> {};

}
