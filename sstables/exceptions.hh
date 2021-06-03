/*
 * Copyright (C) 2015-present ScyllaDB
 *
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

#pragma once

#include <seastar/core/print.hh>

#include "seastarx.hh"

namespace sstables {
class malformed_sstable_exception : public std::exception {
    sstring _msg;
public:
    malformed_sstable_exception(sstring msg, sstring filename)
        : malformed_sstable_exception{format("{} in sstable {}", msg, filename)}
    {}
    malformed_sstable_exception(sstring s) : _msg(s) {}
    const char *what() const noexcept {
        return _msg.c_str();
    }
};

struct bufsize_mismatch_exception : malformed_sstable_exception {
    bufsize_mismatch_exception(size_t size, size_t expected) :
        malformed_sstable_exception(format("Buffer improperly sized to hold requested data. Got: {:d}. Expected: {:d}", size, expected))
    {}
};

class compaction_stop_exception : public std::exception {
    sstring _msg;
    bool _retry;
public:
    compaction_stop_exception(sstring ks, sstring cf, sstring reason, bool retry = false) :
        _msg(format("Compaction for {}/{} was stopped due to: {}", ks, cf, reason)), _retry(retry) {}
    bool retry() const {
        return _retry;
    }
    const char *what() const noexcept {
        return _msg.c_str();
    }
};

}
