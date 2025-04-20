/*
 * Copyright (C) 2021 ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/distributed.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/reactor.hh>

#include <fmt/core.h>
#include <random>

#include "utils/allocation_strategy.hh"
#include "utils/logalloc.hh"
#include "utils/log.hh"
#include "test/perf/perf.hh"

class piggie {
    size_t _extra_size;

public:
    size_t storage_size() const noexcept { return sizeof(piggie) + _extra_size; }
    piggie(size_t sz) noexcept : _extra_size(sz) {}
};

static constexpr unsigned nr_seq_allocations = 1024;
static constexpr unsigned nr_iterations = 20000;
static constexpr unsigned nr_sizes = 32;

int main(int argc, char** argv) {
    app_template app;
    return app.run(argc, argv, [] {
        return seastar::async([&] {
            logalloc::prime_segment_pool(memory::stats().total_memory(), memory::min_free_memory()).get();
            logalloc::region reg;

            std::array<piggie*, nr_seq_allocations> objects;
            std::array<size_t, nr_sizes> sizes;

            std::random_device rd;
            std::mt19937 g(rd());
            for (unsigned i = 0; i < nr_sizes; i++) {
                sizes[i] = i;
            }

            auto& allocator = reg.allocator();

            std::chrono::duration<double> total;

            for (unsigned iter = 0; iter < nr_iterations; iter++) {
                std::shuffle(sizes.begin(), sizes.end(), g);

                void* mem = allocator.alloc<piggie>(sizeof(piggie) + sizes[0]);
                objects[0] = new (mem) piggie(sizes[0]);

                auto start = std::chrono::steady_clock::now();
                for (unsigned i = 1; i < nr_seq_allocations; i++) {
                    void* mem = allocator.alloc<piggie>(sizeof(piggie) + sizes[i % nr_sizes]);
                    objects[i] = new (mem) piggie(sizes[i % nr_sizes]);
                }
                total += std::chrono::steady_clock::now() - start;

                for (unsigned i = 0; i < nr_seq_allocations; i++) {
                    allocator.destroy(objects[i]);
                }

                reg.full_compaction();
            }

            fmt::print("Total time: {} s\n", total.count());
        });
    });
}
