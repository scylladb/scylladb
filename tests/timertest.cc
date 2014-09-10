/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "core/reactor.hh"
#include "core/print.hh"
#include <chrono>

using namespace std::chrono_literals;

struct timer_test {
    timer t1;
    timer t2;
    timer t3;
    timer t4;
    timer t5;

    void run() {
        t1.arm(500ms);
        t2.arm(900ms);
        t3.arm(1000ms);
        t4.arm(700ms);
        t5.arm(800ms);
        t1.expired().then([this] {
            print(" 500ms timer expired\n");
            t4.suspend();
            t5.suspend();
            t5.arm(1100ms);
        });
        t2.expired().then([this] { print(" 900ms timer expired\n"); });
        t3.expired().then([this] { print("1000ms timer expired\n"); });
        t4.expired().then([this] { print("  BAD cancelled timer expired\n"); });
        t5.expired().then([this] { print("1600ms rearmed timer expired\n"); });
    }
};

int main(int ac, char** av) {
    timer_test t;
    engine.start().then([&t] { t.run(); });
    engine.run();
}


