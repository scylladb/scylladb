/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
 
#pragma once

#include "seastarx.hh"
#include <seastar/core/iostream.hh>

namespace sstables {

using digest_integrity_error_handler = std::function<void(sstring)>;
input_stream<char> make_digest_checked_input_stream(input_stream<char> source, size_t len, uint32_t digest, digest_integrity_error_handler error_handler);

} // namespace sstables