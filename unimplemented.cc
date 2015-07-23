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
        case cause::COUNTERS: return out << "COUNTERS";
        case cause::METRICS: return out << "METRICS";
        case cause::MIGRATIONS: return out << "MIGRATIONS";
        case cause::GOSSIP: return out << "GOSSIP";
        case cause::TOKEN_RESTRICTION: return out << "TOKEN_RESTRICTION";
        case cause::LEGACY_COMPOSITE_KEYS: return out << "LEGACY_COMPOSITE_KEYS";
        case cause::COLLECTION_RANGE_TOMBSTONES: return out << "COLLECTION_RANGE_TOMBSTONES";
        case cause::RANGE_QUERIES: return out << "RANGE_QUERIES";
        case cause::RANGE_DELETES: return out << "RANGE_DELETES";
        case cause::THRIFT: return out << "THRIFT";
        case cause::VALIDATION: return out << "VALIDATION";
        case cause::REVERSED: return out << "REVERSED";
        case cause::COMPRESSION: return out << "COMPRESSION";
        case cause::NONATOMIC: return out << "NONATOMIC";
        case cause::CONSISTENCY: return out << "CONSISTENCY";
        case cause::HINT: return out << "HINT";
        case cause::SUPER: return out << "SUPER";
        case cause::WRAP_AROUND: return out << "WRAP_AROUND";
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
