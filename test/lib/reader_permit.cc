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

#include "test/lib/reader_permit.hh"
#include "reader_concurrency_semaphore.hh"

namespace tests {

thread_local reader_concurrency_semaphore the_semaphore{reader_concurrency_semaphore::no_limits{}, "global_test_semaphore"};

reader_concurrency_semaphore& semaphore() {
    return the_semaphore;
}

reader_permit make_permit() {
    return the_semaphore.make_permit(nullptr, "test");
}

query::query_class_config make_query_class_config() {
    return query::query_class_config{the_semaphore, query::max_result_size(std::numeric_limits<uint64_t>::max())};
}

} // namespace tests
