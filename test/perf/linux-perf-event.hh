/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
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

