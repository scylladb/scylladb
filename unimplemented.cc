/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include <unordered_map>
#include "unimplemented.hh"
#include "core/sstring.hh"
#include "core/enum.hh"

namespace unimplemented {

static thread_local std::unordered_map<cause, bool> _warnings;

std::ostream& operator<<(std::ostream& out, cause c) {
    switch(c) {
        case cause::INDEXES: return out << "INDEXES";
        case cause::LWT: return out << "LWT";
        case cause::PAGING: return out << "PAGING";
        case cause::AUTH: return out << "AUTH";
        case cause::PERMISSIONS: return out << "PERMISSIONS";
        case cause::TRIGGERS: return out << "TRIGGERS";
        case cause::COLLECTIONS: return out << "COLLECTIONS";
        case cause::COUNTERS: return out << "COUNTERS";
        case cause::METRICS: return out << "METRICS";
        case cause::COMPACT_TABLES: return out << "COMPACT_TABLES";
        case cause::GOSSIP: return out << "GOSSIP";
        case cause::TOKEN_RESTRICTION: return out << "TOKEN_RESTRICTION";
        case cause::LEGACY_COMPOSITE_KEYS: return out << "LEGACY_COMPOSITE_KEYS";
        case cause::RANGE_QUERIES: return out << "RANGE_QUERIES";
    }
    assert(0);
}

void warn(cause c) {
    auto i = _warnings.find(c);
    if (i == _warnings.end()) {
        _warnings.insert({c, true});
        std::cerr << "WARNING: Not implemented: " << c << std::endl;
    }
}

void fail(cause c) {
    throw std::runtime_error(sprint("Not implemented: %s", c));
}

}
