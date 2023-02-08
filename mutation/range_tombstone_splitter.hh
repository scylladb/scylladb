/*
 * Copyright (C) 2021 ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "clustering_ranges_walker.hh"
#include "mutation/mutation_fragment.hh"

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
