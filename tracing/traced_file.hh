/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
