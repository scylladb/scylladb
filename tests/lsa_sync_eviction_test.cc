/*
 * Copyright 2015 Cloudius Systems
 */

#include <core/distributed.hh>
#include <core/app-template.hh>
#include <core/sstring.hh>
#include <core/thread.hh>

#include "utils/managed_bytes.hh"
#include "utils/logalloc.hh"
#include "utils/managed_ref.hh"
#include "tests/perf/perf.hh"
#include "log.hh"

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

        if (app.configuration().count("debug")) {
            logging::logger_registry().set_all_loggers_level(logging::log_level::debug);
        }

        return seastar::async([reg_obj_size, std_obj_size, obj_count] {
            logalloc::region r;

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
                std::random_shuffle(refs.begin(), refs.end());
                r.make_evictable([&] {
                    with_allocator(r.allocator(), [&] {
                        if (!refs.empty()) {
                            refs.pop_front();
                        }
                    });
                });

                auto print_region_stats = [&r] {
                    std::cout << "Region occupancy: " << r.occupancy()
                        << sprint(", %.2f%% of all memory", (float)r.occupancy().total_space() * 100 / memory::stats().total_memory()) << std::endl;
                };

                std::cout << "Allocated " << refs.size() << " evictable objects" << std::endl;
                print_region_stats();

                using clk = std::chrono::high_resolution_clock;
                auto start = clk::now();

                // Allocate native memory, should evict
                for (unsigned i = 0; i < obj_count; ++i) {
                    new bytes(bytes::initialized_later(), std_obj_size); // intentionally leaked to minimize work
                }

                auto duration = clk::now() - start;

                std::cout << "Allocated " << obj_count << " (" << obj_count * std_obj_size << " B) standard objects in "
                    << sprint("%.4f [s]", std::chrono::duration_cast<std::chrono::duration<float>>(duration).count()) << "\n";
                print_region_stats();
            });
            return 0;
        });
    });
}
