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

std::chrono::steady_clock::duration get_uptime()
{
    return std::chrono::steady_clock::now() - boot_time;
}

}
