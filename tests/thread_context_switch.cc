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

#include "core/thread.hh"
#include "core/semaphore.hh"
#include "core/app-template.hh"
#include "core/do_with.hh"
#include "core/distributed.hh"
#include "core/sleep.hh"

using namespace seastar;
using namespace std::chrono_literals;

class context_switch_tester {
    uint64_t _switches{0};
    semaphore _s1{0};
    semaphore _s2{0};
    bool _done1{false};
    bool _done2{false};
    thread _t1{[this] { main1(); }};
    thread _t2{[this] { main2(); }};
private:
    void main1() {
        while (!_done1) {
            _s1.wait().get();
            ++_switches;
            _s2.signal();
        }
        _done2 = true;
    }
    void main2() {
        while (!_done2) {
            _s2.wait().get();
            ++_switches;
            _s1.signal();
        }
    }
public:
    void begin_measurement() {
        _s1.signal();
    }
    future<uint64_t> measure() {
        _done1 = true;
        return _t1.join().then([this] {
            return _t2.join();
        }).then([this] {
            return _switches;
        });
    }
    future<> stop() {
        return make_ready_future<>();
    }
};

int main(int ac, char** av) {
    static const auto test_time = 5s;
    return app_template().run(ac, av, [] {
        return do_with(distributed<context_switch_tester>(), [] (distributed<context_switch_tester>& dcst) {
            return dcst.start().then([&dcst] {
                return dcst.invoke_on_all(&context_switch_tester::begin_measurement);
            }).then([] {
                return sleep(test_time);
            }).then([&dcst] {
                return dcst.map_reduce0(std::mem_fn(&context_switch_tester::measure), uint64_t(), std::plus<uint64_t>());
            }).then([] (uint64_t switches) {
                switches /= smp::count;
                print("context switch time: %5.1f ns\n",
                      double(std::chrono::duration_cast<std::chrono::nanoseconds>(test_time).count()) / switches);
            }).then([&dcst] {
                return dcst.stop();
            }).then([] {
                engine_exit(0);
            });
        });
    });
}
