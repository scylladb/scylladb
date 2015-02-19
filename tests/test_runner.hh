/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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
