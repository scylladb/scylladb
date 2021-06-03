/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
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

#include <cstdint>

namespace sstables {
class sstable;
}

class filter_tracker {
    uint64_t false_positive = 0;
    uint64_t true_positive = 0;

    uint64_t last_false_positive = 0;
    uint64_t last_true_positive = 0;
public:
    void add_false_positive() {
        false_positive++;
    }

    void add_true_positive() {
        true_positive++;
    }

    friend class sstables::sstable;
};
