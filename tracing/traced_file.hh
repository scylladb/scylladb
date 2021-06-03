/*
 * Copyright (C) 2019-present ScyllaDB
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

#include <seastar/core/file.hh>
#include "tracing/trace_state.hh"

namespace tracing {

// Creates a wrapper over `f` that writes CQL trace messages using `trace_state`
// before and after each operation performed on `f` that returns a future
// (all operations except `dup` and `list_directory`).

// To identify messages for this particular file, `trace_prefix` is prepended to every such message.

// Note: calling dup() on the wrapper returns a handle to the underlying file.
seastar::file make_traced_file(seastar::file f, trace_state_ptr trace_state, seastar::sstring trace_prefix);

} // namespace tracing
