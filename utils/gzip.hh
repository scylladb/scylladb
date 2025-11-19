/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/future.hh>
#include "utils/rjson.hh"

namespace utils {

/**
 * Decompresses gzip-compressed data stored in chunked_content format.
 * 
 * @param compressed_body The gzip-compressed data in chunked_content format
 * @param length_limit Maximum allowed size of the uncompressed data (throws if exceeded)
 * @return A future containing the uncompressed data in chunked_content format
 * 
 * Features:
 * - Supports concatenated gzip files (multiple gzip files appended together)
 * - Throws exception if input is not valid gzip
 * - Throws exception if input is truncated
 * - Throws exception if non-gzip junk is appended
 * - Throws exception if uncompressed size exceeds length_limit
 */
seastar::future<rjson::chunked_content> ungzip(rjson::chunked_content&& compressed_body, size_t length_limit);

} // namespace utils
