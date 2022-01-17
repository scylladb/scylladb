/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
