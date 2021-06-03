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


#include "linux-perf-event.hh"

#include <linux/perf_event.h>
#include <linux/hw_breakpoint.h>
#include <sys/ioctl.h>
#include <asm/unistd.h>

linux_perf_event::linux_perf_event(const struct ::perf_event_attr& attr, pid_t pid, int cpu, int group_fd, unsigned long flags) {
    int ret = syscall(__NR_perf_event_open, &attr, pid, cpu, group_fd, flags);
    if (ret != -1) {
        _fd = ret; // ignore failures, can happen in constrained environments such as containers
    }
}

linux_perf_event::~linux_perf_event() {
    if (_fd != -1) {
        ::close(_fd);
    }
}

linux_perf_event&
linux_perf_event::operator=(linux_perf_event&& x) noexcept {
    if (this != &x) {
        if (_fd != -1) {
            ::close(_fd);
        }
        _fd = std::exchange(x._fd, -1);
    }
    return *this;
}

uint64_t
linux_perf_event::read() {
    if (_fd == -1) {
        return 0;
    }
    uint64_t ret;
    ::read(_fd, &ret, sizeof(ret));
    return ret;
}

void
linux_perf_event::enable() {
    if (_fd == -1) {
        return;
    }
    ::ioctl(_fd, PERF_EVENT_IOC_ENABLE, 0);
}

void
linux_perf_event::disable() {
    if (_fd == -1) {
        return;
    }
    ::ioctl(_fd, PERF_EVENT_IOC_DISABLE, 0);
}

linux_perf_event
linux_perf_event::user_instructions_retired() {
    return linux_perf_event(perf_event_attr{
            .type = PERF_TYPE_HARDWARE,
            .size = sizeof(struct perf_event_attr),
            .config = PERF_COUNT_HW_INSTRUCTIONS,
            .disabled = 1,
            .exclude_kernel = 1,
            .exclude_hv = 1,
            }, 0, -1, -1, 0);
}
