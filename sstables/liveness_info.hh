/*
 * Copyright (C) 2018-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "timestamp.hh"
#include "gc_clock.hh"
#include "sstables/types.hh"
#include "mutation/mutation_partition.hh"

namespace sstables {

struct liveness_info {
    api::timestamp_type _timestamp = api::missing_timestamp;
    gc_clock::duration _ttl = gc_clock::duration::zero();
    gc_clock::time_point _local_deletion_time = gc_clock::time_point::max();
    bool is_set() const {
        return _timestamp != api::missing_timestamp
               || _ttl != gc_clock::duration::zero()
               || _local_deletion_time != gc_clock::time_point::max();
    }
public:
    void set_timestamp(api::timestamp_type timestamp) {
        _timestamp = timestamp;
    }
    void set_ttl(gc_clock::duration ttl) {
        _ttl = ttl;
    }
    void set_local_deletion_time(gc_clock::time_point local_deletion_time) {
        _local_deletion_time = local_deletion_time;
    }
    api::timestamp_type timestamp() const { return _timestamp; }
    gc_clock::duration ttl() const { return _ttl; }
    gc_clock::time_point local_deletion_time() const { return _local_deletion_time; }
    row_marker to_row_marker() const {
        if (!is_set()) {
            return row_marker();
        }
        if (is_expired_liveness_ttl(_ttl)) {
            return row_marker{tombstone{_timestamp, _local_deletion_time}};
        } else if (_ttl != gc_clock::duration::zero() || _local_deletion_time != gc_clock::time_point::max()) {
            return row_marker{_timestamp, _ttl, _local_deletion_time};
        }

        return row_marker{_timestamp};
    }
};

}
