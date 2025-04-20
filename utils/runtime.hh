/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <chrono>

namespace runtime {

void init_uptime();

std::chrono::steady_clock::time_point get_boot_time();

/// Returns the uptime of the system.
std::chrono::steady_clock::duration get_uptime();

}
