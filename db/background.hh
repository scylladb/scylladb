/*
 * Copyright (C) 2020 ScyllaDB
 *
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

#include "utils/phased_barrier.hh"

namespace db {

extern thread_local utils::phased_barrier background_jobs;

/// Those functions are used for tracking background tasks initiated by
/// requests coming into the system.

// Starts tracking a new background operation.
// The operation ends when the returned object is destroyed.
inline
utils::phased_barrier::operation start_background_job() noexcept { return background_jobs.start(); }

// Waits for all background jobs started before this call.
// Background jobs are started with start_background_job().
// Jobs started after this call but before the future resolves will not be waited for.
inline
future<> await_background_jobs() { return background_jobs.advance_and_await(); }

}
