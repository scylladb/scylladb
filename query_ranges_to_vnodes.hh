/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "dht/i_partitioner_fwd.hh"
#include "locator/token_range_splitter.hh"

class query_ranges_to_vnodes_generator {
    schema_ptr _s;
    dht::partition_range_vector _ranges;
    dht::partition_range_vector::iterator _i; // iterator to current range in _ranges
    bool _local;
    std::unique_ptr<locator::token_range_splitter> _splitter;
    void process_one_range(size_t n, dht::partition_range_vector& ranges);
public:
    query_ranges_to_vnodes_generator(std::unique_ptr<locator::token_range_splitter> splitter, schema_ptr s, dht::partition_range_vector ranges, bool local = false);
    query_ranges_to_vnodes_generator(const query_ranges_to_vnodes_generator&) = delete;
    query_ranges_to_vnodes_generator(query_ranges_to_vnodes_generator&&) = default;
    // generate next 'n' vnodes, may return less than requested number of ranges
    // which means either that there are no more ranges
    // (in which case empty() == true), or too many ranges
    // are requested
    dht::partition_range_vector operator()(size_t n);
    bool empty() const;
};
