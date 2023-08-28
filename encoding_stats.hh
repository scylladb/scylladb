/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

protected:
    void update_timestamp(api::timestamp_type ts) noexcept {
        min_timestamp.update(ts);
    }

    void update_local_deletion_time(gc_clock::time_point local_deletion_time) noexcept {
        min_local_deletion_time.update(local_deletion_time);
    }

    void update_ttl(gc_clock::duration ttl) noexcept {
        min_ttl.update(ttl);
    }

public:
    encoding_stats_collector() noexcept
        : min_timestamp(api::max_timestamp)
        , min_local_deletion_time(gc_clock::time_point::max())
        , min_ttl(gc_clock::duration::max())
    {}

    void update(const encoding_stats& other) noexcept {
        update_timestamp(other.min_timestamp);
        update_local_deletion_time(other.min_local_deletion_time);
        update_ttl(other.min_ttl);
    }

    encoding_stats get() const noexcept {
        return { min_timestamp.get(), min_local_deletion_time.get(), min_ttl.get() };
    }
};
