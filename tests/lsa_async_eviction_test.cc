/*
 * Copyright (C) 2015 ScyllaDB
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

#include <core/distributed.hh>
#include <core/app-template.hh>
#include <core/sstring.hh>
#include <core/thread.hh>
#include <seastar/util/defer.hh>

#include "utils/managed_bytes.hh"
#include "utils/logalloc.hh"
#include "utils/managed_ref.hh"
#include "tests/perf/perf.hh"
#include "log.hh"

void print_stats() {
    std::cout << "free " << memory::stats().free_memory()
        << " used " << logalloc::shard_tracker().occupancy().used_space()
        << " total " << logalloc::shard_tracker().occupancy().total_space()
        << std::endl;
}

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("size", bpo::value<unsigned>()->default_value(1024), "size of allocated object")
        ("batch", bpo::value<unsigned>()->default_value(1024), "number of allocated objects between deferring points")
        ("count", bpo::value<unsigned>()->default_value(1024*200), "number of objects to allocate before exiting");

    logging::logger_registry().set_all_loggers_level(logging::log_level::debug);

    return app.run(argc, argv, [&app] {
        auto obj_size = app.configuration()["size"].as<unsigned>();
        auto obj_count = app.configuration()["count"].as<unsigned>();
        auto objects_in_batch = app.configuration()["batch"].as<unsigned>();

        return seastar::async([obj_size, obj_count, objects_in_batch] {
            chunked_fifo<managed_bytes> refs;
            logalloc::region r;

            with_allocator(r.allocator(), [&] {
                auto clear_refs = defer([&refs] { refs.clear(); });

                r.make_evictable([&] {
                    return with_allocator(r.allocator(), [&] {
                        if (refs.empty()) {
                            return memory::reclaiming_result::reclaimed_nothing;
                        }
                        refs.pop_front();
                        return memory::reclaiming_result::reclaimed_something;
                    });
                });

                uint64_t counter = 0;
                logalloc::allocating_section alloc_sect;
                alloc_sect.set_lsa_reserve(0);
                alloc_sect.set_std_reserve(0);

                while (counter < obj_count) {
                    alloc_sect(r, [&] {
                        auto obj = managed_bytes(managed_bytes::initialized_later(), obj_size);
                        logalloc::reclaim_lock l(r);
                        refs.push_back(std::move(obj));
                    });

                    ++counter;

                    if (counter % objects_in_batch == 0) {
                        print_stats();
                        seastar::thread::yield();
                    }
                }
            });
            return 0;
        });
    });
}
