/*
 * Copyright (C) 2021 ScyllaDB
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

#include "sstable_mutation_reader.hh"

namespace sstables {

position_in_partition_view get_slice_upper_bound(const schema& s, const query::partition_slice& slice, dht::ring_position_view key) {
    const auto& ranges = slice.row_ranges(s, *key.key());
    if (ranges.empty()) {
        return position_in_partition_view::for_static_row();
    }
    if (slice.options.contains(query::partition_slice::option::reversed)) {
        return position_in_partition_view::for_range_end(ranges.front());
    }
    return position_in_partition_view::for_range_end(ranges.back());
}

void mp_row_consumer_reader::on_next_partition(dht::decorated_key key, tombstone tomb) {
    _partition_finished = false;
    _before_partition = false;
    _end_of_stream = false;
    _current_partition_key = std::move(key);
    push_mutation_fragment(
        mutation_fragment(*_schema, _permit, partition_start(*_current_partition_key, tomb)));
    _sst->get_stats().on_partition_read();
}

} // namespace sstables
