/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/chunked_vector.hh"
#include <generator>

namespace cdc {
    class stream_id;
}

namespace alternator {
    class stream_id_range {
        utils::chunked_vector<cdc::stream_id> *items;
        utils::chunked_vector<cdc::stream_id>::iterator lo1, end1, lo2, end2;
        const cdc::stream_id *skip_to = nullptr;
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

        bool apply_starting_position_update(const cdc::stream_id &update_to);
        std::generator<const cdc::stream_id&> iterate();
    };

    stream_id_range find_children_range_from_parent_token(
        const utils::chunked_vector<cdc::stream_id>& parent_streams,
        utils::chunked_vector<cdc::stream_id>& current_streams,
        cdc::stream_id parent,
        bool uses_tablets
    );    
}
