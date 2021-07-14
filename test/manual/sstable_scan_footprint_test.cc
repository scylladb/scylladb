/*
 * Copyright (C) 2019-present ScyllaDB
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

#include "test/lib/cql_test_env.hh"
#include "test/lib/memtable_snapshot_source.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/log.hh"

#include "schema_builder.hh"
#include "row_cache.hh"
#include "database.hh"
#include "db/config.hh"

#include <boost/range/irange.hpp>
#include <seastar/core/app-template.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/units.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/fstream.hh>
#include <seastar/util/log.hh>

app_template app;

class memory_demand_probe {

};

namespace seastar::testing {

thread_local std::default_random_engine local_random_engine;

}

namespace {

class stats_collector {
public:
    struct params {
        sstring output_file;
        std::chrono::milliseconds period;
    };

    class collect_guard {
        timer<> _t;
    private:
        explicit collect_guard(stats_collector& sc, std::chrono::milliseconds period)
            : _t(std::bind(&stats_collector::capture_snapshot, std::ref(sc))) {
            if (period.count()) {
                _t.arm_periodic(period);
                testlog.info("Start collecting stats");
                sc.capture_snapshot();
            }
        }
    private:
        friend class stats_collector;
    public:
        ~collect_guard() {
            testlog.info("Finish collecting stats");
        }
    };

private:
    struct data_point {
        size_t lsa_used_memory;
        size_t lsa_free_memory;
        size_t non_lsa_used_memory;
        size_t non_lsa_free_memory;
        size_t reads_memory_consumption;
        size_t reads;
    };

private:
    std::optional<params> _params;
    reader_concurrency_semaphore& _sem;
    const reader_resources _initial_res;
    utils::chunked_vector<data_point> _data_points;

private:
    void capture_snapshot() {
        const auto mem_stats = memory::stats();
        const auto lsa_stats = logalloc::shard_tracker().region_occupancy();
        const auto res = _sem.available_resources();

        data_point dp;
        dp.lsa_used_memory = lsa_stats.used_space();
        dp.lsa_free_memory = lsa_stats.total_space() - lsa_stats.used_space();
        dp.non_lsa_used_memory = mem_stats.total_memory() - mem_stats.free_memory() - dp.lsa_used_memory;
        dp.non_lsa_free_memory = mem_stats.free_memory() - dp.lsa_free_memory;
        dp.reads_memory_consumption = _initial_res.memory - res.memory;
        dp.reads = _initial_res.count - res.count;

        _data_points.push_back(dp);
    }

public:
    static std::optional<params> parse_params(boost::program_options::variables_map& app_config) {
        if (!app_config.contains("collect-stats")) {
            return {};
        }
        return params{
            app_config["stats-file"].as<sstring>(),
            std::chrono::milliseconds(app_config["stats-period-ms"].as<unsigned>())};
    }
    stats_collector(reader_concurrency_semaphore& sem, std::optional<params> p)
        : _params(std::move(p))
        , _sem(sem)
        , _initial_res(_sem.available_resources()) {
    }
    stats_collector(const stats_collector&) = delete;
    stats_collector(stats_collector&&) = delete;
    collect_guard collect() {
        return collect_guard{*this, _params ? _params->period : std::chrono::milliseconds(0)};
    }
    future<> write_stats() {
        if (!_params) {
            return make_ready_future<>();
        }
        return seastar::async([this] {
            auto f = open_file_dma(_params->output_file, open_flags::create | open_flags::wo).get0();
            auto os = make_file_output_stream(f, file_output_stream_options{}).get0();

            {
                const auto header = "lsa_used_memory,lsa_free_memory,non_lsa_used_memory,non_lsa_free_memory,reads_memory_consumption,reads\n";
                os.write(header).get();
            }

            for (const auto& dp : _data_points) {
                const auto line = format("{},{},{},{},{},{}\n", dp.lsa_used_memory, dp.lsa_free_memory, dp.non_lsa_used_memory, dp.non_lsa_free_memory,
                        dp.reads_memory_consumption, dp.reads);
                os.write(line.c_str()).get();
            }

            os.close().get();

            testlog.info("Stats written to {}", _params->output_file);
        });
    }
};

void execute_reads(const schema& s, reader_concurrency_semaphore& sem, unsigned reads, unsigned concurrency, std::function<future<>(unsigned)> read) {
    const reader_resources initial_res = sem.available_resources();
    unsigned n = 0;
    gate g;
    std::exception_ptr e;

    while (n < reads && !e) {
        try {
            // we wait indirectly via the gate
            (void)with_gate(g, [reads, read, &n, concurrency] {
                const auto start = n;
                n = std::min(reads, n + concurrency);
                return parallel_for_each(boost::irange(start, n), read);
            }).handle_exception([&e, &sem, initial_res] (std::exception_ptr eptr) {
                const auto res = sem.available_resources();
                testlog.error("Read failed: {}", eptr);
                testlog.trace("Reads remaining: count: {}/{}, memory: {}/{}, waiters: {}", (initial_res.count - res.count), initial_res.count,
                        (initial_res.memory - res.memory), initial_res.memory, sem.waiters());
                e = std::move(eptr);
            });
            thread::yield();
        } catch (...) {
            e = std::current_exception();
        }

        const auto res = sem.available_resources();
        testlog.trace("Initiated reads: {}/{}, count: {}/{}, memory: {}/{}, waiters: {}", n, reads, (initial_res.count - res.count), initial_res.count,
                (initial_res.memory - res.memory), initial_res.memory, sem.waiters());

        if (sem.waiters()) {
            testlog.trace("Waiting for queue to drain");
            sem.obtain_permit(&s, "drain", 1, db::no_timeout).get();
        }
    }

    testlog.debug("Closing gate");
    g.close().get();

    if (e) {
        std::rethrow_exception(e);
    }
}

void test_main_thread(cql_test_env& env) {
    bool with_compression = app.configuration().contains("with-compression");
    auto compressor = with_compression ? "LZ4Compressor" : "";
    uint64_t sstable_size = app.configuration()["sstable-size"].as<uint64_t>();
    uint64_t sstables = app.configuration()["sstables"].as<uint64_t>();
    const auto clustering_row_size = app.configuration()["clustering-row-size"].as<uint64_t>();
    auto reads = app.configuration()["reads"].as<unsigned>();
    auto read_concurrency = app.configuration()["read-concurrency"].as<unsigned>();

    std::optional<stats_collector::params> stats_collector_params;
    try {
        stats_collector_params = stats_collector::parse_params(app.configuration());
    } catch (...) {
        testlog.error("Error parsing stats collection parameters: {}", std::current_exception());
        return;
    }

    env.execute_cql(format("{} WITH compression = {{ 'class': '{}' }} "
                           "AND compaction = {{'class' : 'NullCompactionStrategy'}};",
        "create table test (pk int, ck int, value blob, primary key (pk,ck))", compressor)).get();

    table& tab = env.local_db().find_column_family("ks", "test");
    auto s = tab.schema();

    auto value = serialized(tests::random::get_bytes(clustering_row_size));
    auto& value_cdef = *s->get_column_definition("value");
    auto pk = partition_key::from_single_value(*s, serialized(0));
    uint64_t rows = 0;
    auto gen = [s, &rows, ck = 0, pk, &value_cdef, value] (uint64_t sstable_size) mutable -> mutation {
        auto ts = api::new_timestamp();
        mutation m(s, pk);
        uint64_t size = m.partition().external_memory_usage(*s);
        while (size < sstable_size) {
            auto ckey = clustering_key::from_single_value(*s, serialized(ck));
            auto& row = m.partition().clustered_row(*s, ckey);
            row.cells().apply(value_cdef, atomic_cell::make_live(*value_cdef.type, ts, value));
            size += row.cells().external_memory_usage(*s, column_kind::regular_column);
            ++rows;
            ++ck;
            thread::maybe_yield();
        }
        return m;
    };

    testlog.info("Populating");

    for (uint64_t i = 0; i < sstables; ++i) {
        auto m = gen(sstable_size);
        env.local_db().apply(s, freeze(m), tracing::trace_state_ptr(), db::commitlog::force_sync::no, db::no_timeout).get();
        tab.flush().get();
        thread::maybe_yield();
    }

    env.local_db().flush_all_memtables().get();

    testlog.info("Live disk space used: {}", tab.get_stats().live_disk_space_used);
    testlog.info("Live sstables: {}", tab.get_stats().live_sstable_count);

    testlog.info("Preparing dummy cache");
    memtable_snapshot_source underlying(s);
    cache_tracker& tr = env.local_db().row_cache_tracker();
    row_cache c(s, snapshot_source([&] { return underlying(); }), tr, is_continuous::yes);
    auto prev_evictions = tr.get_stats().row_evictions;
    while (tr.get_stats().row_evictions == prev_evictions) {
        auto mt = make_lw_shared<memtable>(s);
        mt->apply(gen(sstable_size));
        c.update(row_cache::external_updater([] {}), *mt).get();
        thread::maybe_yield();
    }

    auto prev_occupancy = logalloc::shard_tracker().occupancy();
    testlog.info("Occupancy before: {}", prev_occupancy);

    auto& sem = env.local_db().get_reader_concurrency_semaphore();

    testlog.info("Reading");
    stats_collector sc(sem, stats_collector_params);
    try {
        auto _ = sc.collect();
        memory::set_heap_profiling_enabled(true);
        execute_reads(*s, sem, reads, read_concurrency, [&] (unsigned i) {
            return env.execute_cql(format("select * from ks.test where pk = 0 and ck > {} limit 100;",
                    tests::random::get_int(rows / 2))).discard_result();
        });
    } catch (...) {
        testlog.error("Reads aborted due to exception: {}", std::current_exception());
    }
    memory::set_heap_profiling_enabled(false);
    sc.write_stats().get();

    auto occupancy = logalloc::shard_tracker().occupancy();
    testlog.info("Occupancy after: {}", occupancy);
    testlog.info("Max demand: {}", prev_occupancy.total_space() - occupancy.total_space());
    testlog.info("Max sstables per read: {}", tab.get_stats().estimated_sstable_per_read.max());
}

} // anonymous namespace

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;

    app.add_options()
        ("enable-cache", "Enables cache")
        ("with-compression", "Generates compressed sstables")
        ("reads", bpo::value<unsigned>()->default_value(100), "Total reads")
        ("read-concurrency", bpo::value<unsigned>()->default_value(1), "Concurrency of reads, the amount of reads to fire at once")
        ("sstables", bpo::value<uint64_t>()->default_value(100), "Number of sstables to generate")
        ("sstable-size", bpo::value<uint64_t>()->default_value(10000000), "Size of generated sstables")
        ("sstable-format", bpo::value<std::string>()->default_value("md"), "Sstable format version to use during population")
        ("clustering-row-size", bpo::value<uint64_t>()->default_value(100), "Size of a clustering row")
        ("collect-stats", "Enable collecting statistics.")
        ("stats-file", bpo::value<sstring>()->default_value("stats.csv"), "Store statistics in the specified file.")
        ("stats-period-ms", bpo::value<unsigned>()->default_value(100), "Tick period of the stats collection.")
        ;

    testing::local_random_engine.seed(std::random_device()());

    return app.run(argc, argv, [] {
      return async([] {
        cql_test_config test_cfg;

        auto& db_cfg = *test_cfg.db_config;

        db_cfg.enable_cache(app.configuration().contains("enable-cache"));
        db_cfg.enable_commitlog(false);
        db_cfg.virtual_dirty_soft_limit(1.0);

        auto sstable_format_name = app.configuration()["sstable-format"].as<std::string>();
        if (sstable_format_name == "md") {
            db_cfg.enable_sstables_md_format(true);
        } else if (sstable_format_name == "mc") {
            db_cfg.enable_sstables_md_format(false);
        } else {
            throw std::runtime_error(format("Unsupported sstable format: {}", sstable_format_name));
        }

        do_with_cql_env([] (cql_test_env& env) {
            return with_scheduling_group(env.local_db().get_statement_scheduling_group(), [&] {
                return seastar::async([&] {
                    test_main_thread(env);
                });
            });
        }, test_cfg).get();
      });
    });
}
