/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/future.hh>

#include "bytes_fwd.hh"
#include "seastarx.hh"

namespace redis {

    future<bytes> lolwut5(const int cols, const int squares_per_row, const int squares_per_col);

}
