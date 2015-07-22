/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include <chrono>

namespace runtime {

void init_uptime();

/// Returns the uptime of the system in milliseconds.
uint64_t get_uptime();

}
