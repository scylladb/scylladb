/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "utils/chunked_vector.hh"
#include "cdc/generation.hh"
#include <generator>

namespace cdc {
    class stream_id;
}

namespace alternator {
    class stream_id_range {
        // helper class for manipulating (possibly wrapped around) range of stream_ids
        // it holds one or two ranges [lo1, end1) and [lo2, end2)
        // if the range doesn't wrap around, then lo2 == end2 == items.end()
        // if the range wraps around, then
        // `lo1 == items.begin() and end2 == items.end()` must be true
        // the object doesn't own `items`, but it does manipulate it - it will
        // reorder elements (so both ranges were next to each other) and sort them by unsigned comparison
        // usage - create an object with needed ranges. before iteration call `prepare_for_iterating` method -
        // it will reorder elements of `items` array to what is needed and then call begin / end pair.
        // note - `items` array will be modified - elements will be reordered, but no elements will be added or removed.
        // `items` array must stay intact as long as iteration is in progress.
        utils::chunked_vector<cdc::stream_id>::iterator _lo1 = {}, _end1 = {}, _lo2 = {}, _end2 = {};
        const cdc::stream_id* _skip_to = nullptr;
        bool _prepared = false;
    public:
        stream_id_range(
                utils::chunked_vector<cdc::stream_id> &items,
                utils::chunked_vector<cdc::stream_id>::iterator lo1,
                utils::chunked_vector<cdc::stream_id>::iterator end1);
        stream_id_range(
                utils::chunked_vector<cdc::stream_id> &items,
                utils::chunked_vector<cdc::stream_id>::iterator lo1,
                utils::chunked_vector<cdc::stream_id>::iterator end1,
                utils::chunked_vector<cdc::stream_id>::iterator lo2,
                utils::chunked_vector<cdc::stream_id>::iterator end2);

        void set_starting_position(const cdc::stream_id &update_to);
        // Must be called after construction and after set_starting_position()
        // (if used), but before begin()/end() iteration.
        void prepare_for_iterating();

        utils::chunked_vector<cdc::stream_id>::iterator begin() const { return _lo1; }
        utils::chunked_vector<cdc::stream_id>::iterator end() const { return _end1; }
    };

    stream_id_range find_children_range_from_parent_token(
        const utils::chunked_vector<cdc::stream_id>& parent_streams,
        utils::chunked_vector<cdc::stream_id>& current_streams,
        cdc::stream_id parent,
        bool uses_tablets
    );
}
