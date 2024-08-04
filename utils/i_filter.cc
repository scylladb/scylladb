
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "log.hh"
#include "bloom_filter.hh"
#include "bloom_calculations.hh"
#include "utils/assert.hh"
#include "utils/murmur_hash.hh"
#include <seastar/core/thread.hh>

namespace utils {
static logging::logger filterlog("bloom_filter");

filter_ptr i_filter::get_filter(int64_t num_elements, double max_false_pos_probability, filter_format fformat) {
    SCYLLA_ASSERT(seastar::thread::running_in_thread());

    if (max_false_pos_probability > 1.0) {
        throw std::invalid_argument(format("Invalid probability {:f}: must be lower than 1.0", max_false_pos_probability));
    }

    if (max_false_pos_probability == 1.0) {
        return std::make_unique<filter::always_present_filter>();
    }

    int buckets_per_element = bloom_calculations::max_buckets_per_element(num_elements);
    auto spec = bloom_calculations::compute_bloom_spec(buckets_per_element, max_false_pos_probability);
    return filter::create_filter(spec.K, num_elements, spec.buckets_per_element, fformat);
}

size_t i_filter::get_filter_size(int64_t num_elements, double max_false_pos_probability) {
    if (max_false_pos_probability >= 1.0) {
        return 0;
    }

    int buckets_per_element = bloom_calculations::max_buckets_per_element(num_elements);
    auto spec = bloom_calculations::compute_bloom_spec(buckets_per_element, max_false_pos_probability);

    return filter::get_bitset_size(num_elements, spec.buckets_per_element) / 8;
}

hashed_key make_hashed_key(bytes_view b) {
    std::array<uint64_t, 2> h;
    utils::murmur_hash::hash3_x64_128(b, 0, h);
    return { h };
}

}
