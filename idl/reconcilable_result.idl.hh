/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "idl/full_position.idl.hh"

class partition {
    uint32_t row_count_low_bits();
    frozen_mutation mut();
    uint32_t row_count_high_bits() [[version 4.3]] = 0;
};

struct partition_skip {
    uint32_t partition;
    position_in_partition position;
};

class reconcilable_result {
    uint32_t row_count_low_bits();
    utils::chunked_vector<partition> partitions();
    query::short_read is_short_read() [[version 1.6]] = query::short_read::no;
    uint32_t row_count_high_bits() [[version 4.3]] = 0;
    // The frontier's stop, if the command asked for a frontier. See
    // query::partition_slice::option::send_read_frontier.
    std::optional<full_position> wire_position() [[version 2026.4]];
    std::vector<partition_skip> skips() [[version 2026.4]];
};
