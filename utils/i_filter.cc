
/*
 * Copyright (C) 2015 ScyllaDB
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

namespace utils {
static logging::logger filterlog("bloom_filter");

filter_ptr i_filter::get_filter(long num_elements, double max_false_pos_probability) {
    if (max_false_pos_probability > 1.0) {
        throw std::invalid_argument(sprint("Invalid probability %f: must be lower than 1.0", max_false_pos_probability));
    }

    if (max_false_pos_probability == 1.0) {
        return std::make_unique<filter::always_present_filter>();
    }

    int buckets_per_element = bloom_calculations::max_buckets_per_element(num_elements);
    auto spec = bloom_calculations::compute_bloom_spec(buckets_per_element, max_false_pos_probability);
    return filter::create_filter(spec.K, num_elements, spec.buckets_per_element);
}

filter_ptr i_filter::get_filter(long num_elements, int target_buckets_per_elem) {
    int max_buckets_per_element = std::max(1, bloom_calculations::max_buckets_per_element(num_elements));
    int buckets_per_element = std::min(target_buckets_per_elem, max_buckets_per_element);

    if (buckets_per_element < target_buckets_per_elem) {
        filterlog.warn("Cannot provide an optimal bloom_filter for {} elements ({}/{} buckets per element).", num_elements, buckets_per_element, target_buckets_per_elem);
    }
    auto spec = bloom_calculations::compute_bloom_spec(buckets_per_element);
    return filter::create_filter(spec.K, num_elements, spec.buckets_per_element);
}
}
