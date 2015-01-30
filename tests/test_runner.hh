/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include <memory>

#include "core/reactor.hh"
#include "core/future.hh"
#include "exchanger.hh"

class test_runner {
private:
    std::unique_ptr<posix_thread> _thread;
    std::atomic<bool> _started{false};
    exchanger<std::function<future<>()>> _task;
    bool _done = false;
private:
    void start(std::function<void()> pre_start);
    void stop();
public:
    static test_runner& launch_or_get(std::function<void()> pre_start);
    ~test_runner();
    void run_sync(std::function<future<>()> task);
};
