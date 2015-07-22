/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "utils/runtime.hh"

#include <chrono>

namespace runtime {

static std::chrono::steady_clock::time_point boot_time;

void init_uptime()
{
    boot_time = std::chrono::steady_clock::now();
}

uint64_t get_uptime()
{
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(now - boot_time).count();
}

}
