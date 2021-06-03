/*
 * Copyright (C) 2020-present ScyllaDB
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

#pragma once

#include <cinttypes>

class reader_concurrency_semaphore;

namespace query {

struct max_result_size {
    uint64_t soft_limit = 0;
    uint64_t hard_limit = 0;

    max_result_size() = default;
    explicit max_result_size(uint64_t max_size) : soft_limit(max_size), hard_limit(max_size) { }
    explicit max_result_size(uint64_t soft_limit, uint64_t hard_limit) : soft_limit(soft_limit), hard_limit(hard_limit) { }
};

inline bool operator==(const max_result_size& a, const max_result_size& b) {
    return a.soft_limit == b.soft_limit && a.hard_limit == b.hard_limit;
}

struct query_class_config {
    reader_concurrency_semaphore& semaphore;
    max_result_size max_memory_for_unlimited_query;
};

}
