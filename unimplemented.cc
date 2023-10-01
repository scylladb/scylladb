/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <unordered_map>
#include "unimplemented.hh"
#include <seastar/core/sstring.hh>
#include <seastar/core/enum.hh>
#include "log.hh"
#include "seastarx.hh"

namespace unimplemented {

static thread_local std::unordered_map<cause, bool> _warnings;

static logging::logger ulogger("unimplemented");

std::ostream& operator<<(std::ostream& out, cause c) {
    switch (c) {
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
        case cause::STORAGE_SERVICE: return out << "STORAGE_SERVICE";
        case cause::API: return out << "API";
        case cause::SCHEMA_CHANGE: return out << "SCHEMA_CHANGE";
        case cause::MIXED_CF: return out << "MIXED_CF";
        case cause::SSTABLE_FORMAT_M: return out << "SSTABLE_FORMAT_M";
    }
    abort();
}

void warn(cause c) {
    if (!_warnings.contains(c)) {
        _warnings.insert({c, true});
        ulogger.debug("{}", c);
    }
}

void fail(cause c) {
    throw std::runtime_error(format("Not implemented: {}", c));
}

}

template <> struct fmt::formatter<unimplemented::cause> : fmt::ostream_formatter {};
