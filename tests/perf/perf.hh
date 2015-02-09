/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include <chrono>
#include <iostream>

template<typename Func>
void time_it(Func func, int iterations = 5) {
    using clk = std::chrono::high_resolution_clock;

    for (int i = 0; i < iterations; i++) {
        auto start = clk::now();
        auto end_at = start + std::chrono::seconds(1);
        uint64_t count = 0;

        while (clk::now() < end_at) {
            for (int i = 0; i < 10000; i++) { // amortize clock reading cost
                func();
                count++;
            }
        }

        auto end = clk::now();
        auto duration = std::chrono::duration<double>(end - start).count();
        std::cout << sprint("%.2f", (double)count / duration) << " tps\n";
    }
}
