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
    std::default_random_engine random_engine;
    std::exponential_distribution<> distr(0.2);
    std::uniform_int_distribution<> type(0, 1);
    std::uniform_int_distribution<char> poison(-128, 127);
    std::uniform_real_distribution<> which(0, 1);
    std::vector<allocation> allocations;
    for (auto i = 0; i < 200000; ++i) {
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
                continue;
            }
            size_t i = which(random_engine) * allocations.size();
            allocations[i] = std::move(allocations.back());
            allocations.pop_back();
            break;
        }
        }
    }
    return 0;
}


