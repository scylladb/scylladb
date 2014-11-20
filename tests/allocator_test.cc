/*
 * Copyright 2014 Cloudius Systems
 */

#include <random>
#include <cmath>
#include <iostream>
#include <iomanip>
#include <algorithm>
#include <cassert>
#include <memory>
#include <chrono>
#include <boost/program_options.hpp>

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


