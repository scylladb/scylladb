/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

namespace query {

class result_digest final {
    std::array<uint8_t, 16> get();
};

class result {
    bytes buf();
    std::optional<query::result_digest> digest();
    api::timestamp_type last_modified() [ [version 1.2] ] = api::missing_timestamp;
    query::short_read is_short_read() [[version 1.6]] = query::short_read::no;
    std::optional<uint32_t> row_count_low_bits() [[version 2.1]];
    std::optional<uint32_t> partition_count() [[version 2.1]];
    std::optional<uint32_t> row_count_high_bits() [[version 4.3]];
    std::optional<full_position> last_position() [[version 5.1]];
};

}
