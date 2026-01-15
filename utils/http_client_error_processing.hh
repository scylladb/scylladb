/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include <seastar/util/bool_class.hh>

namespace utils::http {

using retryable = seastar::bool_class<struct is_retryable>;

} // namespace utils::http
