/*
 * Copyright (C) 2018-present ScyllaDB
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

#include "gc_clock.hh"
#include "timestamp.hh"
#include "utils/extremum_tracking.hh"

// Stores statistics on all the updates done to a memtable
// The collected statistics are used for flushing memtable to the disk
struct encoding_stats {

    // The fixed epoch corresponds to the one used by Origin - 22/09/2015, 00:00:00, GMT-0:
    //        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT-0"), Locale.US);
    //        c.set(Calendar.YEAR, 2015);
    //        c.set(Calendar.MONTH, Calendar.SEPTEMBER);
    //        c.set(Calendar.DAY_OF_MONTH, 22);
    //        c.set(Calendar.HOUR_OF_DAY, 0);
    //        c.set(Calendar.MINUTE, 0);
    //        c.set(Calendar.SECOND, 0);
    //        c.set(Calendar.MILLISECOND, 0);
    //
    //        long TIMESTAMP_EPOCH = c.getTimeInMillis() * 1000; // timestamps should be in microseconds by convention
    //        int DELETION_TIME_EPOCH = (int)(c.getTimeInMillis() / 1000); // local deletion times are in seconds
    // Encoding stats are used for delta-encoding, so we want some default values
    // that are just good enough so we take some recent date in the past
    static constexpr int32_t deletion_time_epoch = 1442880000;
    static constexpr api::timestamp_type timestamp_epoch = api::timestamp_type(deletion_time_epoch) * 1000 * 1000;
    static constexpr int32_t ttl_epoch = 0;

    api::timestamp_type min_timestamp = timestamp_epoch;
    gc_clock::time_point min_local_deletion_time = gc_clock::time_point(gc_clock::duration(deletion_time_epoch));
    gc_clock::duration min_ttl = gc_clock::duration(ttl_epoch);
};

class encoding_stats_collector {
private:
    min_tracker<api::timestamp_type> min_timestamp;
    min_tracker<gc_clock::time_point> min_local_deletion_time;
    min_tracker<gc_clock::duration> min_ttl;

public:
    encoding_stats_collector()
        : min_timestamp(api::max_timestamp)
        , min_local_deletion_time(gc_clock::time_point::max())
        , min_ttl(gc_clock::duration::max())
    {}

    void update_timestamp(api::timestamp_type ts) {
        min_timestamp.update(ts);
    }

    void update_local_deletion_time(gc_clock::time_point local_deletion_time) {
        min_local_deletion_time.update(local_deletion_time);
    }

    void update_ttl(gc_clock::duration ttl) {
        min_ttl.update(ttl);
    }

    void update(const encoding_stats& other) {
        update_timestamp(other.min_timestamp);
        update_local_deletion_time(other.min_local_deletion_time);
        update_ttl(other.min_ttl);
    }

    encoding_stats get() const {
        return { min_timestamp.get(), min_local_deletion_time.get(), min_ttl.get() };
    }
};
