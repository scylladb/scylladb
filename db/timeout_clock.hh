/*
 * Copyright (C) 2017-present ScyllaDB
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

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/semaphore.hh>
#include <chrono>

namespace db {
using timeout_clock = seastar::lowres_clock;
using timeout_semaphore = seastar::basic_semaphore<seastar::default_timeout_exception_factory, timeout_clock>;
using timeout_semaphore_units = seastar::semaphore_units<seastar::default_timeout_exception_factory, timeout_clock>;
static constexpr timeout_clock::time_point no_timeout = timeout_clock::time_point::max();
}
