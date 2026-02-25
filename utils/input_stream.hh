/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/simple-stream.hh>
#include "bytes_ostream.hh"

namespace utils {

using input_stream = seastar::memory_input_stream<bytes_ostream::fragment_iterator>;

}
