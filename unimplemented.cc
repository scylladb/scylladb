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

std::string_view format_as(cause c) {
    switch (c) {
        case cause::INDEXES: return "INDEXES";
        case cause::LWT: return "LWT";
        case cause::PAGING: return "PAGING";
        case cause::AUTH: return "AUTH";
        case cause::PERMISSIONS: return "PERMISSIONS";
        case cause::TRIGGERS: return "TRIGGERS";
        case cause::COUNTERS: return "COUNTERS";
        case cause::METRICS: return "METRICS";
        case cause::MIGRATIONS: return "MIGRATIONS";
        case cause::GOSSIP: return "GOSSIP";
        case cause::TOKEN_RESTRICTION: return "TOKEN_RESTRICTION";
        case cause::LEGACY_COMPOSITE_KEYS: return "LEGACY_COMPOSITE_KEYS";
        case cause::COLLECTION_RANGE_TOMBSTONES: return "COLLECTION_RANGE_TOMBSTONES";
        case cause::RANGE_DELETES: return "RANGE_DELETES";
        case cause::VALIDATION: return "VALIDATION";
        case cause::REVERSED: return "REVERSED";
        case cause::COMPRESSION: return "COMPRESSION";
        case cause::NONATOMIC: return "NONATOMIC";
        case cause::CONSISTENCY: return "CONSISTENCY";
        case cause::HINT: return "HINT";
        case cause::SUPER: return "SUPER";
        case cause::WRAP_AROUND: return "WRAP_AROUND";
        case cause::STORAGE_SERVICE: return "STORAGE_SERVICE";
        case cause::API: return "API";
        case cause::SCHEMA_CHANGE: return "SCHEMA_CHANGE";
        case cause::MIXED_CF: return "MIXED_CF";
        case cause::SSTABLE_FORMAT_M: return "SSTABLE_FORMAT_M";
    }
    abort();
}

void warn(cause c) {
    if (!_warnings.contains(c)) {
        _warnings.insert({c, true});
        ulogger.debug("{}", format_as(c));
    }
}

void fail(cause c) {
    throw std::runtime_error(fmt::format("Not implemented: {}", format_as(c)));
}

}
