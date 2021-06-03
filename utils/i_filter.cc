
/*
 * Copyright (C) 2015-present ScyllaDB
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

#include "log.hh"
#include "bloom_filter.hh"
#include "bloom_calculations.hh"
#include <seastar/core/thread.hh>

namespace utils {
static logging::logger filterlog("bloom_filter");

filter_ptr i_filter::get_filter(int64_t num_elements, double max_false_pos_probability, filter_format fformat) {
    assert(seastar::thread::running_in_thread());

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

hashed_key make_hashed_key(bytes_view b) {
    std::array<uint64_t, 2> h;
    utils::murmur_hash::hash3_x64_128(b, 0, h);
    return { h };
}

}
