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

namespace query {

struct max_result_size {
    uint64_t soft_limit;
    uint64_t hard_limit;
    uint64_t unlimited_soft_limit;
    uint64_t unlimited_hard_limit;

    uint64_t get_unlimited_soft_limit() const {
        return unlimited_soft_limit == 0 ? soft_limit : unlimited_soft_limit;
    }

    uint64_t get_unlimited_hard_limit() const {
        return unlimited_hard_limit == 0 ? hard_limit : unlimited_hard_limit;
    }

    max_result_size() = delete;
    explicit max_result_size(uint64_t max_size)
        : soft_limit(max_size), hard_limit(max_size), unlimited_soft_limit(max_size), unlimited_hard_limit(max_size)
    { }
    explicit max_result_size(uint64_t soft_limit, uint64_t hard_limit)
        : soft_limit(soft_limit), hard_limit(hard_limit), unlimited_soft_limit(soft_limit), unlimited_hard_limit(hard_limit)
    { }
    explicit max_result_size(uint64_t soft_limit, uint64_t hard_limit, uint64_t unlimited_soft_limit, uint64_t unlimited_hard_limit)
        : soft_limit(soft_limit), hard_limit(hard_limit), unlimited_soft_limit(unlimited_soft_limit), unlimited_hard_limit(unlimited_hard_limit)
    { }
};

inline bool operator==(const max_result_size& a, const max_result_size& b) {
    return a.soft_limit == b.soft_limit && a.hard_limit == b.hard_limit && a.unlimited_soft_limit == b.unlimited_soft_limit && a.unlimited_hard_limit == b.unlimited_hard_limit;
}

}
