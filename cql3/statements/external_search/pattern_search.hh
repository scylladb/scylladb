/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/values.hh"
#include "seastarx.hh"

#include <seastar/core/sstring.hh>

/// The parts of running a pattern search that are specific to it.
namespace cql3::statements::pattern_search {

/// The pattern an evaluated query value holds.
sstring query_pattern(const cql3::raw_value& value);

} // namespace cql3::statements::pattern_search
