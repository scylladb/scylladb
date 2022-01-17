/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once


#include <cstdint>
#include <utility>
#include <unistd.h>

struct perf_event_attr; // from <linux/perf_event.h>

class linux_perf_event {
    int _fd = -1;
public:
    linux_perf_event(const struct ::perf_event_attr& attr, pid_t pid, int cpu, int group_fd, unsigned long flags);
    linux_perf_event(linux_perf_event&& x) noexcept : _fd(std::exchange(x._fd, -1)) {}
    linux_perf_event& operator=(linux_perf_event&& x) noexcept;
    ~linux_perf_event();
    uint64_t read();
    void enable();
    void disable();
public:
    static linux_perf_event user_instructions_retired();
};

