/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <seastar/core/distributed.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>

#include "utils/managed_bytes.hh"
#include "utils/logalloc.hh"
#include "utils/managed_ref.hh"
#include "test/perf/perf.hh"
#include "log.hh"

#include <random>

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("region-object-size", bpo::value<unsigned>()->default_value(1024), "size of region object")
        ("standard-object-size", bpo::value<unsigned>()->default_value(2048), "size of standard object")
        ("debug", "enable debug logging")
        ("count", bpo::value<unsigned>()->default_value(1024 * 200), "number of standard objects to allocate before");

    return app.run(argc, argv, [&app] {
        auto reg_obj_size = app.configuration()["region-object-size"].as<unsigned>();
        auto std_obj_size = app.configuration()["standard-object-size"].as<unsigned>();
        auto obj_count = app.configuration()["count"].as<unsigned>();

        if (app.configuration().contains("debug")) {
            logging::logger_registry().set_all_loggers_level(logging::log_level::debug);
        }

        return seastar::async([reg_obj_size, std_obj_size, obj_count] {
            logalloc::region r;
            logalloc::prime_segment_pool(memory::stats().total_memory(), memory::min_free_memory()).get();

            with_allocator(r.allocator(), [&] {
                std::deque<managed_bytes> refs;

                std::cout << "Filling whole memory with managed objects..." << std::endl;
                try {
                    while (true) {
                        refs.push_back(managed_bytes(managed_bytes::initialized_later(), reg_obj_size));
                    }
                } catch (const std::bad_alloc&) {
                    // expected
                }

                // Evict in random order to stress more
                std::random_device rd;
                std::shuffle(refs.begin(), refs.end(), std::default_random_engine(rd()));
                r.make_evictable([&] {
                    return with_allocator(r.allocator(), [&] {
                        if (refs.empty()) {
                            return memory::reclaiming_result::reclaimed_nothing;
                        }
                        refs.pop_front();
                        return memory::reclaiming_result::reclaimed_something;
                    });
                });

                auto print_region_stats = [&r] {
                    std::cout << "Region occupancy: " << r.occupancy()
                        << format(", {:.2f}% of all memory", (float)r.occupancy().total_space() * 100 / memory::stats().total_memory()) << std::endl;
                };

                std::cout << "Allocated " << refs.size() << " evictable objects" << std::endl;
                print_region_stats();

                using clk = std::chrono::steady_clock;
                auto start = clk::now();

                // Allocate native memory, should evict
                for (unsigned i = 0; i < obj_count; ++i) {
                    new bytes(bytes::initialized_later(), std_obj_size); // intentionally leaked to minimize work
                }

                auto duration = clk::now() - start;

                std::cout << "Allocated " << obj_count << " (" << obj_count * std_obj_size << " B) standard objects in "
                    << format("{:.4f} [s]", std::chrono::duration_cast<std::chrono::duration<float>>(duration).count()) << "\n";
                print_region_stats();
            });
            return 0;
        });
    });
}
