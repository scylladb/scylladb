/*
 * Copyright (C) 2018 ScyllaDB
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

#include "resource_manager.hh"

namespace db {
namespace hints {

static logging::logger resource_manager_logger("hints_resource_manager");

future<semaphore_units<semaphore_default_exception_factory>> resource_manager::get_send_units_for(size_t buf_size) {
    // Let's approximate the memory size the mutation is going to consume by the size of its serialized form
    size_t hint_memory_budget = std::max(_min_send_hint_budget, buf_size);
    // Allow a very big mutation to be sent out by consuming the whole shard budget
    hint_memory_budget = std::min(hint_memory_budget, _max_send_in_flight_memory);
    resource_manager_logger.trace("memory budget: need {} have {}", hint_memory_budget, _send_limiter.available_units());
    return get_units(_send_limiter, hint_memory_budget);
}

}
}
