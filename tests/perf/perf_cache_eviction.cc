/*
 * Copyright (C) 2017 ScyllaDB
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

#include <boost/range/irange.hpp>
#include "seastarx.hh"
#include "tests/simple_schema.hh"
#include "tests/cql_test_env.hh"
#include "core/app-template.hh"
#include "database.hh"
#include "db/config.hh"
#include "partition_slice_builder.hh"
#include "utils/int_range.hh"
#include "utils/div_ceil.hh"
#include <seastar/core/reactor.hh>

logging::logger test_log("test");

static thread_local bool cancelled = false;

using namespace std::chrono_literals;

template<typename T>
class monotonic_counter {
    std::function<T()> _getter;
    T _prev;
public:
    monotonic_counter(std::function<T()> getter)
        : _getter(std::move(getter)) {
        _prev = _getter();
    }
    // Return change in value since the last call to change() or rate().
    auto change() {
        auto now = _getter();
        return now - std::exchange(_prev, now);
    }
};

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("trace", "Enables trace-level logging for the test actions")
        ("no-reads", "Disable reads during the test")
        ("seconds", bpo::value<unsigned>()->default_value(60), "Duration [s] after which the test terminates with a success")
        ;

    return app.run(argc, argv, [&app] {
        if (app.configuration().count("trace")) {
            test_log.set_level(seastar::log_level::trace);
        }

        db::config cfg;
        cfg.enable_commitlog(false);
        cfg.enable_cache(true);

        return do_with_cql_env_thread([&app] (cql_test_env& env) {
            auto reads_enabled = !app.configuration().count("no-reads");
            auto seconds = app.configuration()["seconds"].as<unsigned>();

            engine().at_exit([] {
                cancelled = true;
                return make_ready_future();
            });

            timer<> completion_timer;
            completion_timer.set_callback([&] {
                test_log.info("Test done.");
                cancelled = true;
            });
            completion_timer.arm(std::chrono::seconds(seconds));

            env.execute_cql("CREATE TABLE ks.cf (pk text, ck int, v text, PRIMARY KEY (pk, ck)) WITH CLUSTERING ORDER BY (ck DESC)").get();
            database& db = env.local_db();
            auto s = db.find_schema("ks", "cf");
            column_family& cf = db.find_column_family(s->id());
            cf.set_compaction_strategy(sstables::compaction_strategy_type::null);

            uint64_t mutations = 0;
            uint64_t reads = 0;
            utils::estimated_histogram reads_hist;
            utils::estimated_histogram writes_hist;

            auto& tracker = env.local_db().row_cache_tracker();

            timer<> stats_printer;
            monotonic_counter<uint64_t> reads_ctr([&] { return reads; });
            monotonic_counter<uint64_t> mutations_ctr([&] { return mutations; });
            monotonic_counter<uint64_t> pmerges_ctr([&] { return tracker.get_stats().partition_merges; });
            monotonic_counter<uint64_t> eviction_ctr([&] { return tracker.get_stats().row_evictions; });
            monotonic_counter<uint64_t> miss_ctr([&] { return tracker.get_stats().reads_with_misses; });
            stats_printer.set_callback([&] {
                auto MB = 1024 * 1024;
                std::cout << sprint("rd/s: %d, wr/s: %d, ev/s: %d, pmerge/s: %d, miss/s: %d, cache: %d/%d [MB], LSA: %d/%d [MB], std free: %d [MB]",
                    reads_ctr.change(),
                    mutations_ctr.change(),
                    eviction_ctr.change(),
                    pmerges_ctr.change(),
                    miss_ctr.change(),
                    tracker.region().occupancy().used_space() / MB,
                    tracker.region().occupancy().total_space() / MB,
                    logalloc::shard_tracker().region_occupancy().used_space() / MB,
                    logalloc::shard_tracker().region_occupancy().total_space() / MB,
                    seastar::memory::stats().free_memory() / MB) << "\n\n";

                auto print_percentiles = [] (const utils::estimated_histogram& hist) {
                    return sprint("min: %-6d, 50%%: %-6d, 90%%: %-6d, 99%%: %-6d, 99.9%%: %-6d, max: %-6d [us]",
                        hist.percentile(0),
                        hist.percentile(0.5),
                        hist.percentile(0.9),
                        hist.percentile(0.99),
                        hist.percentile(0.999),
                        hist.percentile(1.0)
                    );
                };

                std::cout << "reads : " << print_percentiles(reads_hist) << "\n";
                std::cout << "writes: " << print_percentiles(writes_hist) << "\n";
                std::cout << "\n";
                reads_hist.clear();
                writes_hist.clear();
            });
            stats_printer.arm_periodic(1s);

            auto make_pkey = [s] (sstring pk) {
                auto key = partition_key::from_single_value(*s, data_value(pk).serialize());
                return dht::global_partitioner().decorate_key(*s, key);
            };

            auto pkey = make_pkey("key1");
            sstring value = sstring(sstring::initialized_later(), 1024);

            using clock = std::chrono::steady_clock;

            auto reader = seastar::async([&] {
                if (!reads_enabled) {
                    return;
                }
                auto id = env.prepare("select * from ks.cf where pk = 'key1' limit 10;").get0();
                while (!cancelled) {
                    auto t0 = clock::now();
                    env.execute_prepared(id, {}).get();
                    reads_hist.add(std::chrono::duration_cast<std::chrono::microseconds>(clock::now() - t0).count());
                    ++reads;
                    sleep(100ms).get(); // So that reads don't overwhelm the CPU
                }
            });

            auto mutator = seastar::async([&] {
                int32_t ckey_seq = 0;
                while (!cancelled) {
                    mutation m(s, pkey);
                    auto ck = clustering_key::from_single_value(*s, data_value(ckey_seq++).serialize());
                    auto&& col = *s->get_column_definition(to_bytes("v"));
                    m.set_clustered_cell(ck, col, atomic_cell::make_live(*col.type, api::new_timestamp(), data_value(value).serialize()));
                    auto t0 = clock::now();
                    db.apply(s, freeze(m)).get();
                    writes_hist.add(std::chrono::duration_cast<std::chrono::microseconds>(clock::now() - t0).count());
                    ++mutations;
                }
            });

            mutator.get();
            reader.get();
            stats_printer.cancel();
            completion_timer.cancel();
        }, cfg);
    });
}
