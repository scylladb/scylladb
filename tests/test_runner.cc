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

#include <iostream>

#include "core/app-template.hh"
#include "core/future-util.hh"
#include "test_runner.hh"

static test_runner instance;

test_runner::~test_runner() {
    if (_thread) {
        stop();
        _thread->join();
    }
}

void
test_runner::start(std::function<void()> pre_start) {
    bool expected = false;
    if (!_started.compare_exchange_strong(expected, true, std::memory_order_acquire)) {
        return;
    }

    _thread = std::make_unique<posix_thread>([this, pre_start = std::move(pre_start)]() mutable {
        char* av[] = {};
        int ac = 0;

        pre_start();

        app_template app;
        auto exit_code = app.run(ac, av, [&] {
            return do_until([this] { return _done; }, [this] {
                // this will block the reactor briefly, but we don't care
                auto func = _task.take();
                return func();
            });
        });
        if (exit_code) {
            exit(exit_code);
        }
    });
}

void
test_runner::stop() {
    assert(_started.load());
    _task.give([this] {
        _done = true;
        engine().exit(0);
        return now();
    });
}

void
test_runner::run_sync(std::function<future<>()> task) {
    exchanger<std::experimental::optional<std::exception_ptr>> e;
    _task.give([task = std::move(task), &e] {
        try {
            return task().then_wrapped([&e](auto&& f) {
                try {
                    f.get();
                    e.give({});
                } catch (...) {
                    e.give({std::current_exception()});
                }
            });
        } catch (...) {
            e.give({std::current_exception()});
            return make_ready_future<>();
        }
    });
    auto maybe_exception = e.take();
    if (maybe_exception) {
        std::rethrow_exception(*maybe_exception);
    }
}

test_runner&
test_runner::launch_or_get(std::function<void()> pre_start) {
    instance.start(std::move(pre_start));
    return instance;
}
