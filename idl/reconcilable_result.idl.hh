/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

class partition {
    uint32_t row_count_low_bits();
    frozen_mutation mut();
    uint32_t row_count_high_bits() [[version 4.3]] = 0;
};

class reconcilable_result {
    uint32_t row_count_low_bits();
    utils::chunked_vector<partition> partitions();
    query::short_read is_short_read() [[version 1.6]] = query::short_read::no;
    uint32_t row_count_high_bits() [[version 4.3]] = 0;
};
