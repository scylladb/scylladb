/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "core/reactor.hh"
#include "core/print.hh"
#include <chrono>

using namespace std::chrono_literals;

#define BUG() do { \
        std::cerr << "ERROR @ " << __FILE__ << ":" << __LINE__ << std::endl; \
        throw std::runtime_error("test failed"); \
    } while (0)

#define OK() { \
        std::cerr << "OK @ " << __FILE__ << ":" << __LINE__ << std::endl; \
    } while (0)

struct timer_test {
    timer<> t1;
    timer<> t2;
    timer<> t3;
    timer<> t4;
    timer<> t5;

    void run() {
        t1.set_callback([this] {
            print(" 500ms timer expired\n");
            if (!t4.cancel()) {
                BUG();
            }
            if (!t5.cancel()) {
                BUG();
            }
            t5.arm(1100ms);
        });
        t2.set_callback([this] { print(" 900ms timer expired\n"); });
        t3.set_callback([this] { print("1000ms timer expired\n"); });
        t4.set_callback([this] { print("  BAD cancelled timer expired\n"); });
        t5.set_callback([this] { print("1600ms rearmed timer expired\n"); });

        t1.arm(500ms);
        t2.arm(900ms);
        t3.arm(1000ms);
        t4.arm(700ms);
        t5.arm(800ms);

        test_timer_cancelling();
    }

    void test_timer_cancelling() {
        timer<>& t1 = *new timer<>();
        t1.set_callback([] { BUG(); });
        t1.arm(100ms);
        t1.cancel();

        t1.arm(100ms);
        t1.cancel();

        t1.set_callback([] { OK(); });
        t1.arm(100ms);
    }
};

int main(int ac, char** av) {
    timer_test t;
    engine.when_started().then([&t] { t.run(); });
    engine.run();
}
