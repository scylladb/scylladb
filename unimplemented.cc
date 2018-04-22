/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <unordered_map>
#include "unimplemented.hh"
#include "core/sstring.hh"
#include "core/enum.hh"
#include "log.hh"
#include "seastarx.hh"

namespace unimplemented {

static thread_local std::unordered_map<cause, bool> _warnings;

static logging::logger ulogger("unimplemented");

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
    auto i = _warnings.find(c);
    if (i == _warnings.end()) {
        _warnings.insert({c, true});
        ulogger.debug("{}", c);
    }
}

void fail(cause c) {
    throw std::runtime_error(sprint("Not implemented: %s", c));
}

}
