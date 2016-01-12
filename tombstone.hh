/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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

#include "timestamp.hh"
#include "gc_clock.hh"
#include "hashing.hh"

/**
 * Represents deletion operation. Can be commuted with other tombstones via apply() method.
 * Can be empty.
 */
struct tombstone final {
    api::timestamp_type timestamp;
    gc_clock::time_point deletion_time;

    tombstone(api::timestamp_type timestamp, gc_clock::time_point deletion_time)
        : timestamp(timestamp)
        , deletion_time(deletion_time)
    { }

    tombstone()
        : tombstone(api::missing_timestamp, {})
    { }

    int compare(const tombstone& t) const {
        if (timestamp < t.timestamp) {
            return -1;
        } else if (timestamp > t.timestamp) {
            return 1;
        } else if (deletion_time < t.deletion_time) {
            return -1;
        } else if (deletion_time > t.deletion_time) {
            return 1;
        } else {
            return 0;
        }
    }

    bool operator<(const tombstone& t) const {
        return compare(t) < 0;
    }

    bool operator<=(const tombstone& t) const {
        return compare(t) <= 0;
    }

    bool operator>(const tombstone& t) const {
        return compare(t) > 0;
    }

    bool operator>=(const tombstone& t) const {
        return compare(t) >= 0;
    }

    bool operator==(const tombstone& t) const {
        return compare(t) == 0;
    }

    bool operator!=(const tombstone& t) const {
        return compare(t) != 0;
    }

    explicit operator bool() const {
        return timestamp != api::missing_timestamp;
    }

    void apply(const tombstone& t) {
        if (*this < t) {
            *this = t;
        }
    }

    friend std::ostream& operator<<(std::ostream& out, const tombstone& t) {
        if (t) {
            return out << "{tombstone: timestamp=" << t.timestamp << ", deletion_time=" << t.deletion_time.time_since_epoch().count() << "}";
        } else {
            return out << "{tombstone: none}";
        }
    }
};

template<>
struct appending_hash<tombstone> {
    template<typename Hasher>
    void operator()(Hasher& h, const tombstone& t) const {
        feed_hash(h, t.timestamp);
        feed_hash(h, t.deletion_time);
    }
};
