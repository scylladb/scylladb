/*
 * Copyright 2016-present ScyllaDB
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
