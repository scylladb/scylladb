/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include "types/types.hh"

namespace redis {

    future<bytes> lolwut5(const int cols, const int squares_per_row, const int squares_per_col);

}
