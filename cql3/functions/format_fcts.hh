/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/functions/function.hh"
#include "types/types.hh"

namespace cql3::functions {

// format_bytes(value [, source_unit] [, target_unit]) formats a size, given
// in bytes or in source_unit, as a human-readable text like "1.46 GiB". It
// converts the size to target_unit, or picks a suitable unit if none is
// given. The units are B, KiB, MiB and GiB.
//
// format_time(value [, source_unit] [, target_unit]) does the same for a
// duration, given in milliseconds or in source_unit, e.g., "2.5 h". The units
// are ns, us (or µs), ms, s, m, h and d.
//
// The value can be of any of the types in format_value_types() - a string
// must be a number - and the units are text.
const std::vector<data_type>& format_value_types();
shared_ptr<function> make_format_bytes_function(std::vector<data_type> arg_types);
shared_ptr<function> make_format_time_function(std::vector<data_type> arg_types);

}
