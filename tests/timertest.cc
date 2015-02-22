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
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "core/app-template.hh"
#include "core/reactor.hh"
#include "core/print.hh"
#include <chrono>

using namespace std::chrono_literals;

#define BUG() do { \
        std::cerr << "ERROR @ " << __FILE__ << ":" << __LINE__ << std::endl; \
        throw std::runtime_error("test failed"); \
    } while (0)

#define OK() do { \
        std::cerr << "OK @ " << __FILE__ << ":" << __LINE__ << std::endl; \
    } while (0)

template <typename Clock>
struct timer_test {
    timer<Clock> t1;
    timer<Clock> t2;
    timer<Clock> t3;
    timer<Clock> t4;
    timer<Clock> t5;
    promise<> pr1;
    promise<> pr2;

    future<> run() {
        t1.set_callback([this] {
            OK();
            print(" 500ms timer expired\n");
            if (!t4.cancel()) {
                BUG();
            }
            if (!t5.cancel()) {
                BUG();
            }
            t5.arm(1100ms);
        });
        t2.set_callback([this] { OK(); print(" 900ms timer expired\n"); });
        t3.set_callback([this] { OK(); print("1000ms timer expired\n"); });
        t4.set_callback([this] { OK(); print("  BAD cancelled timer expired\n"); });
        t5.set_callback([this] { OK(); print("1600ms rearmed timer expired\n"); pr1.set_value(); });

        t1.arm(500ms);
        t2.arm(900ms);
        t3.arm(1000ms);
        t4.arm(700ms);
        t5.arm(800ms);

        return pr1.get_future().then([this] { test_timer_cancelling(); });
    }

    future<> test_timer_cancelling() {
        timer<Clock>& t1 = *new timer<Clock>();
        t1.set_callback([] { BUG(); });
        t1.arm(100ms);
        t1.cancel();

        t1.arm(100ms);
        t1.cancel();

        t1.set_callback([this] { OK(); pr2.set_value(); });
        t1.arm(100ms);
        return pr2.get_future();
    }
};

int main(int ac, char** av) {
    app_template app;
    timer_test<std::chrono::high_resolution_clock> t1;
    timer_test<lowres_clock> t2;
    return app.run(ac, av, [&t1, &t2] {
        print("=== Start High res clock test\n");
        t1.run().then([&t2] {
            print("=== Start Low  res clock test\n");
            return t2.run();
        }).then([] {
            print("Done\n");
            engine().exit(0);
        });
    });
}
