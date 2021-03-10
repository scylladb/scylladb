/*
 * Copyright (C) 2018 ScyllaDB
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


#include <variant>
#include "flat_mutation_reader.hh"
#include "timestamp.hh"
#include "gc_clock.hh"
#include "mutation_fragment.hh"
#include "schema_fwd.hh"
#include "row.hh"
#include "clustering_ranges_walker.hh"
#include "utils/data_input.hh"
#include "utils/overloaded_functor.hh"
#include "liveness_info.hh"
#include "mutation_fragment_filter.hh"
#include "types.hh"
#include "keys.hh"
#include "clustering_bounds_comparator.hh"
#include "range_tombstone.hh"
#include "types/user.hh"
#include "concrete_types.hh"

namespace sstables {

namespace kl {
    class mp_row_consumer_k_l;
}

namespace mx {
    class mp_row_consumer_m;
}

class mp_row_consumer_reader : public flat_mutation_reader::impl {
    friend class sstables::kl::mp_row_consumer_k_l;
    friend class sstables::mx::mp_row_consumer_m;
protected:
    shared_sstable _sst;

    // Whether index lower bound is in current partition
    bool _index_in_current_partition = false;

    // True iff the consumer finished generating fragments for a partition and hasn't
    // entered the new partition yet.
    // Implies that partition_end was emitted for the last partition.
    // Will cause the reader to skip to the next partition if !_before_partition.
    bool _partition_finished = true;

    // When set, the consumer is positioned right before a partition or at end of the data file.
    // _index_in_current_partition applies to the partition which is about to be read.
    bool _before_partition = true;

    std::optional<dht::decorated_key> _current_partition_key;
public:
    mp_row_consumer_reader(schema_ptr s, reader_permit permit, shared_sstable sst)
        : impl(std::move(s), std::move(permit))
        , _sst(std::move(sst))
    { }

    // Called when all fragments relevant to the query range or fast forwarding window
    // within the current partition have been pushed.
    // If no skipping is required, this method may not be called before transitioning
    // to the next partition.
    virtual void on_out_of_clustering_range() = 0;

    void on_next_partition(dht::decorated_key key, tombstone tomb);
};

struct new_mutation {
    partition_key key;
    tombstone tomb;
};

inline atomic_cell make_atomic_cell(const abstract_type& type,
                                    api::timestamp_type timestamp,
                                    bytes_view value,
                                    gc_clock::duration ttl,
                                    gc_clock::time_point expiration,
                                    atomic_cell::collection_member cm) {
    if (ttl != gc_clock::duration::zero()) {
        return atomic_cell::make_live(type, timestamp, value, expiration, ttl, cm);
    } else {
        return atomic_cell::make_live(type, timestamp, value, cm);
    }
}

atomic_cell make_counter_cell(api::timestamp_type timestamp, bytes_view value);

}
