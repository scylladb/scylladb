/*
 * Copyright 2016 ScyllaDB
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

namespace query {

class result_digest final {
    std::array<uint8_t, 16> get();
};

class result {
    bytes buf();
    std::experimental::optional<query::result_digest> digest();
    api::timestamp_type last_modified() [ [version 1.2] ] = api::missing_timestamp;
    query::short_read is_short_read() [[version 1.6]] = query::short_read::no;
    std::experimental::optional<uint32_t> row_count() [[version 2.1]];
    std::experimental::optional<uint32_t> partition_count() [[version 2.1]];
};

}
