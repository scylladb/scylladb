/*
 * Copyright (C) 2021-present ScyllaDB
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

#include <map>
#include <chrono>
#include <seastar/core/sstring.hh>

enum class tombstone_gc_mode : uint8_t { timeout, disabled, immediate, repair };

class tombstone_gc_options {
private:
    tombstone_gc_mode _mode = tombstone_gc_mode::timeout;
    std::chrono::seconds _propagation_delay_in_seconds = std::chrono::seconds(3600);
public:
    tombstone_gc_options() = default;
    const tombstone_gc_mode& mode() const { return _mode; }
    explicit tombstone_gc_options(const std::map<seastar::sstring, seastar::sstring>& map);
    const std::chrono::seconds& propagation_delay_in_seconds() const {
        return _propagation_delay_in_seconds;
    }
    std::map<seastar::sstring, seastar::sstring> to_map() const;
    seastar::sstring to_sstring() const;
    bool operator==(const tombstone_gc_options& other) const;
    bool operator!=(const tombstone_gc_options& other) const;
};

std::ostream& operator<<(std::ostream& os, const tombstone_gc_mode& m);
