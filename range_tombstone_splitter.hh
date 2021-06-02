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

#pragma once

#include "clustering_ranges_walker.hh"
#include "mutation_fragment.hh"

template<typename T>
concept SplitterFragmentConsumer = std::invocable<T, mutation_fragment>;

/// Takes a stream of range tombstone fragments and trims them to the boundaries of clustering key restrictions.
class range_tombstone_splitter {
    clustering_ranges_walker& _walker;
    range_tombstone_stream _rts;
public:
    range_tombstone_splitter(const schema& s, reader_permit permit, clustering_ranges_walker& w)
        : _walker(w)
        , _rts(s, std::move(permit))
    { }

    template<SplitterFragmentConsumer C>
    void flush(position_in_partition_view pos, C consumer) {
        while (auto rto = _rts.get_next(pos)) {
            consumer(std::move(*rto));
        }
    }

    template<SplitterFragmentConsumer C>
    void consume(range_tombstone rt, C consumer) {
        if (auto rto = _walker.split_tombstone(std::move(rt), _rts)) {
            _rts.apply(std::move(*rto));
        }
        flush(rt.position(), std::move(consumer));
    }
};
