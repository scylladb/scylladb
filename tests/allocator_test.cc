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
 * Copyright 2014 Cloudius Systems
 */

#include "core/memory.hh"
#include <random>
#include <cmath>
#include <iostream>
#include <iomanip>
#include <algorithm>
#include <cassert>
#include <memory>
#include <chrono>
#include <boost/program_options.hpp>

template <size_t N>
void test_aligned_allocator() {
    using aptr = std::unique_ptr<char[]>;
    std::vector<aptr> v;
    for (unsigned i = 0; i < 1000; ++i) {
        aptr p(new (with_alignment(64)) char[N]);
        assert(reinterpret_cast<uintptr_t>(p.get()) % 64 == 0);
        v.push_back(std::move(p));
    }
}

struct allocation {
    size_t n;
    std::unique_ptr<char[]> data;
    char poison;
    allocation(size_t n, char poison) : n(n), data(new char[n]), poison(poison) {
        std::fill_n(data.get(), n, poison);
    }
    ~allocation() {
        verify();
    }
    allocation(allocation&& x) noexcept = default;
    void verify() {
        if (data) {
            assert(std::find_if(data.get(), data.get() + n, [this] (char c) {
                return c != poison;
            }) == data.get() + n);
        }
    }
    allocation& operator=(allocation&& x) {
        verify();
        if (this != &x) {
            data = std::move(x.data);
            n = x.n;
            poison = x.poison;
        }
        return *this;
    }
};

int main(int ac, char** av) {
    namespace bpo = boost::program_options;
    bpo::options_description opts("Allowed options");
    opts.add_options()
            ("help", "produce this help message")
            ("iterations", bpo::value<unsigned>(), "run s specified number of iterations")
            ("time", bpo::value<float>()->default_value(5.0), "run for a specified amount of time, in seconds")
            ;
    bpo::variables_map vm;
    bpo::store(bpo::parse_command_line(ac, av, opts), vm);
    bpo::notify(vm);
    test_aligned_allocator<1>();
    test_aligned_allocator<4>();
    test_aligned_allocator<80>();
    std::default_random_engine random_engine;
    std::exponential_distribution<> distr(0.2);
    std::uniform_int_distribution<> type(0, 1);
    std::uniform_int_distribution<char> poison(-128, 127);
    std::uniform_real_distribution<> which(0, 1);
    std::vector<allocation> allocations;
    auto iteration = [&] {
        auto typ = type(random_engine);
        switch (typ) {
        case 0: {
            auto n = std::min<size_t>(std::exp(distr(random_engine)), 1 << 25);
            try {
                allocations.emplace_back(n, poison(random_engine));
            } catch (std::bad_alloc&) {

            }
            break;
        }
        case 1: {
            if (allocations.empty()) {
                break;
            }
            size_t i = which(random_engine) * allocations.size();
            allocations[i] = std::move(allocations.back());
            allocations.pop_back();
            break;
        }
        }
    };
    if (vm.count("help")) {
        std::cout << opts << "\n";
        return 1;
    }
    if (vm.count("iterations")) {
        auto iterations = vm["iterations"].as<unsigned>();
        for (unsigned i = 0; i < iterations; ++i) {
            iteration();
        }
    } else {
        auto time = vm["time"].as<float>();
        using clock = std::chrono::high_resolution_clock;
        auto end = clock::now() + std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::seconds(1) * time);
        while (clock::now() < end) {
            for (unsigned i = 0; i < 1000; ++i) {
                iteration();
            }
        }
    }
    return 0;
}


