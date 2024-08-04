/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <fstream>

#include <fmt/ranges.h>
#include <boost/range/irange.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <json/json.h>

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/testing/test_runner.hh>

#include "test/lib/tmpdir.hh"
#include "test/perf/perf.hh"
#include "test/lib/random_utils.hh"

#include "db/config.hh"
#include "schema/schema_builder.hh"
#include "release.hh"
#include "db/config.hh"
#include "db/extensions.hh"
#include "db/commitlog/commitlog.hh"
#include "utils/assert.hh"
#include "utils/UUID_gen.hh"

struct test_config {
    unsigned concurrency;
    unsigned duration_in_seconds;
    unsigned operations_per_shard = 0;

    size_t min_data_size;
    size_t max_data_size;

    uint64_t min_flush_delay_in_ms;
    uint64_t max_flush_delay_in_ms;
};

using clperf_result = perf_result_with_aio_writes;

static void write_json_result(std::string result_file, const test_config& cfg, clperf_result median, double mad, double max, double min) {
    Json::Value results;

    Json::Value params;
    params["concurrency"] = cfg.concurrency;
    params["cpus"] = smp::count;
    params["duration"] = cfg.duration_in_seconds;

    params["min-data-size"] = cfg.min_data_size;
    params["max-data-size"] = cfg.max_data_size;
    params["min-flush-delay-in-ms"] = cfg.min_flush_delay_in_ms;
    params["max-flush-delay-in-ms"] = cfg.max_flush_delay_in_ms;

    params["concurrency,cpus,duration"] = fmt::format("{},{},{}", cfg.concurrency, smp::count, cfg.duration_in_seconds);
    results["parameters"] = std::move(params);

    Json::Value stats;
    stats["median tps"] = median.throughput;
    stats["allocs_per_op"] = median.mallocs_per_op;
    stats["logallocs_per_op"] = median.logallocs_per_op;
    stats["tasks_per_op"] = median.tasks_per_op;
    stats["instructions_per_op"] = median.instructions_per_op;
    stats["cpu_cycles_per_op"] = median.cpu_cycles_per_op;
    stats["aio_writes"] = median.aio_writes;
    stats["aio_write_bytes"] = median.aio_write_bytes;
    stats["mad tps"] = mad;
    stats["max tps"] = max;
    stats["min tps"] = min;
    results["stats"] = std::move(stats);

    std::string test_type = "commitlog_write";
    results["test_properties"]["type"] = test_type;

    // <version>-<release>
    auto version_components = std::vector<std::string>{};
    auto sver = scylla_version();
    boost::algorithm::split(version_components, sver, boost::is_any_of("-"));
    // <scylla-build>.<date>.<git-hash>
    auto release_components = std::vector<std::string>{};
    boost::algorithm::split(release_components, version_components[1], boost::is_any_of("."));

    Json::Value version;
    version["commit_id"] = release_components[2];
    version["date"] = release_components[1];
    version["version"] = version_components[0];

    // It'd be nice to have std::chrono::format(), wouldn't it?
    auto current_time = std::time(nullptr);
    char time_str[100];
    ::tm time_buf;
    std::strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", ::localtime_r(&current_time, &time_buf));
    version["run_date_time"] = time_str;

    results["versions"]["scylla-server"] = std::move(version);

    auto out = std::ofstream(result_file);
    out << results;
}

struct commitlog_service {
    test_config cfg;
    std::uniform_int_distribution<unsigned> delay_dist;
    std::uniform_int_distribution<size_t> size_dist;
    std::optional<db::commitlog> log;
    std::optional<db::commitlog::flush_handler_anchor> fa;
    timer<> flush_timer;

    commitlog_service(const test_config& c)
        : cfg(c)
        , delay_dist(cfg.min_flush_delay_in_ms, cfg.max_flush_delay_in_ms)
        , size_dist(cfg.min_data_size, cfg.max_data_size)
    {}

    future<> init(const db::commitlog::config& cfg) {
        SCYLLA_ASSERT(!log);
        log.emplace(co_await db::commitlog::create_commitlog(cfg));
        fa.emplace(log->add_flush_handler(std::bind(&commitlog_service::flush_handler, this, std::placeholders::_1, std::placeholders::_2)));
    }
    future<> stop() {
        if (log) {
            co_await log->shutdown();
            co_await log->clear();
        }
    }
    void flush_handler(db::cf_id_type id, db::replay_position pos) {
        if (!flush_timer.armed()) {
            flush_timer.set_callback([id, this] { log->discard_completed_segments(id); });
            flush_timer.arm(std::chrono::milliseconds(delay_dist(tests::random::gen())));
        }
    }
};

static std::vector<clperf_result> do_commitlog_test(distributed<commitlog_service>& cls, test_config& cfg) {
    auto uuid = table_id(utils::UUID_gen::get_time_UUID());

    return time_parallel_ex<clperf_result>([&] {
        auto& log = cls.local();
        size_t size = log.size_dist(tests::random::gen());
        return log.log->add_mutation(uuid, size, db::commitlog::force_sync::no, [size](db::commitlog::output& dst) {
            dst.fill('1', size);
        }).then([](db::rp_handle h) {
            h.release();
        });
    }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard, true, &clperf_result::update);
}

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("random-seed", boost::program_options::value<unsigned>(), "Random number generator seed")
        ("duration", bpo::value<unsigned>()->default_value(5), "test duration in seconds")
        ("concurrency", bpo::value<unsigned>()->default_value(100), "workers per core")
        ("operations-per-shard", bpo::value<unsigned>(), "run this many operations per shard (overrides duration)")

        ("commitlog-sync", bpo::value<sstring>(), "commitlog sync method (pediodic/batch)")
        ("commitlog-segment-size-in-mb", bpo::value<unsigned>(), "commitlog segment size")
        ("commitlog-total-space-in-mb", bpo::value<unsigned>(), "total commitlog size")
        ("commitlog-sync-period-in-ms", bpo::value<unsigned>(), "how long the system waits for other writes before performing a sync in \"periodic\" mode")
        ("commitlog-use-o-dsync", bpo::value<bool>()->default_value(true), "whether or not to use O_DSYNC mode for commitlog segments io")
        ("commitlog-use-hard-size-limit", bpo::value<bool>()->default_value(true), "whether or not to use a hard size limit for commitlog disk usage")

        ("min-data-size", bpo::value<size_t>()->default_value(200), "minimum size of data element added")
        ("max-data-size", bpo::value<size_t>()->default_value(32/2 * 1024 * 1024 - 1), "maximum size of data element added")

        ("min-flush-delay-in-ms", bpo::value<uint64_t>()->default_value(10), "minimum flush response delay")
        ("max-flush-delay-in-ms", bpo::value<uint64_t>()->default_value(800), "maximum flush response delay")

        ("json-result", bpo::value<std::string>(), "name of the json result file")
        ;

    set_abort_on_internal_error(true);

    return app.run(argc, argv, [&app_in = app]() -> future<> {
        auto& app = app_in;
        auto conf_seed = app.configuration()["random-seed"];
        auto seed = conf_seed.empty() ? std::random_device()() : conf_seed.as<unsigned>();
        std::cout << "random-seed=" << seed << '\n';

        co_await smp::invoke_on_all([seed] {
            seastar::testing::local_random_engine.seed(seed + this_shard_id());
        });

        auto ext = std::make_shared<db::extensions>(); // TODO: maybe add commitlog file extension + delays or errors.
        auto db_cfg = ::make_shared<db::config>(ext);

        if (app.configuration().contains("commitlog-sync")) {
            db_cfg->commitlog_sync(app.configuration()["commitlog-sync"].as<sstring>());
        }
        if (app.configuration().contains("commitlog-segment-size-in-mb")) {
            db_cfg->commitlog_segment_size_in_mb(app.configuration()["commitlog-segment-size-in-mb"].as<unsigned>());
        }
        if (app.configuration().contains("commitlog-total-space-in-mb")) {
            db_cfg->commitlog_total_space_in_mb(app.configuration()["commitlog-total-space-in-mb"].as<unsigned>());
        }
        if (app.configuration().contains("commitlog-sync-period-in-ms")) {
            db_cfg->commitlog_sync_period_in_ms(app.configuration()["commitlog-sync-period-in-ms"].as<unsigned>());
        }
        if (app.configuration().contains("commitlog-use-o-dsync")) {
            db_cfg->commitlog_use_o_dsync(app.configuration()["commitlog-use-o-dsync"].as<bool>());
        }
        if (app.configuration().contains("commitlog-use-hard-size-limit")) {
            db_cfg->commitlog_use_hard_size_limit(app.configuration()["commitlog-use-hard-size-limit"].as<bool>());
        }

        auto cfg = test_config();
        cfg.duration_in_seconds = app.configuration()["duration"].as<unsigned>();
        cfg.concurrency = app.configuration()["concurrency"].as<unsigned>();
        if (app.configuration().contains("operations-per-shard")) {
            cfg.operations_per_shard = app.configuration()["operations-per-shard"].as<unsigned>();
        }
        cfg.min_data_size = app.configuration()["min-data-size"].as<size_t>();
        cfg.max_data_size = app.configuration()["max-data-size"].as<size_t>();
        cfg.min_flush_delay_in_ms = app.configuration()["min-flush-delay-in-ms"].as<uint64_t>();
        cfg.max_flush_delay_in_ms = app.configuration()["min-flush-delay-in-ms"].as<uint64_t>();

        if (cfg.min_data_size > cfg.max_data_size) {
            cfg.max_data_size = cfg.min_data_size;
        }
        if (cfg.min_flush_delay_in_ms > cfg.max_flush_delay_in_ms) {
            cfg.max_flush_delay_in_ms = cfg.min_flush_delay_in_ms;
        }

        db::commitlog::config cl_cfg = db::commitlog::config::from_db_config(*db_cfg, current_scheduling_group(), memory::stats().total_memory());
        tmpdir tmp;
        cl_cfg.commit_log_location = tmp.path().string();

        distributed<commitlog_service> test_commitlog;

        //logging::logger_registry().set_logger_level("commitlog", logging::log_level::debug);

        co_await test_commitlog.start(cfg);
        co_await test_commitlog.invoke_on_all(std::mem_fn(&commitlog_service::init), cl_cfg);
        std::exception_ptr ex;

        try {
            if (cfg.max_data_size > test_commitlog.local().log->max_record_size()) {
                throw std::invalid_argument(sstring("Too large max data size: ") + std::to_string(cfg.max_data_size));
            }
            // test "framework" expects seastar thread
            auto results = co_await seastar::async([&] {
                return do_commitlog_test(test_commitlog, cfg);
            });

            auto compare_throughput = [] (perf_result a, perf_result b) { return a.throughput < b.throughput; };
            std::sort(results.begin(), results.end(), compare_throughput);
            auto median_result = results[results.size() / 2];
            auto median = median_result.throughput;
            auto min = results[0].throughput;
            auto max = results[results.size() - 1].throughput;
            auto absolute_deviations = boost::copy_range<std::vector<double>>(
                    results
                    | boost::adaptors::transformed(std::mem_fn(&perf_result::throughput))
                    | boost::adaptors::transformed([&] (double r) { return abs(r - median); }));
            std::sort(absolute_deviations.begin(), absolute_deviations.end());
            auto mad = absolute_deviations[results.size() / 2];
            std::cout << format("\nmedian {}\nmedian absolute deviation: {:.2f}\nmaximum: {:.2f}\nminimum: {:.2f}\n", median_result, mad, max, min);

            if (app.configuration().contains("json-result")) {
                write_json_result(app.configuration()["json-result"].as<std::string>(), cfg, median_result, mad, max, min);
            }
        } catch (...) {
            ex = std::current_exception();
        }

        co_await test_commitlog.stop();

        if (ex) {
            std::rethrow_exception(ex);
        }
    });
}
