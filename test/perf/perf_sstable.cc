/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/program_options/errors.hpp>
#include <seastar/core/distributed.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/reactor.hh>
#include <random>

// hack: perf_sstable falsely depends on Boost.Test, but we can't include it with
// with statically linked boost
#define BOOST_REQUIRE(x) (void)(x)
#define BOOST_CHECK_NO_THROW(x) (void)(x)

#include "test/perf/perf_sstable.hh"

using namespace sstables;

static unsigned iterations = 30;
static unsigned parallelism = 1;

future<> test_write(distributed<perf_sstable_test_env>& dt) {
    return seastar::async([&dt] {
        dt.invoke_on_all([] (perf_sstable_test_env &t) {
            return t.fill_memtable();
        }).then([&dt] {
            return time_runs(iterations, parallelism, dt, &perf_sstable_test_env::flush_memtable);
        }).get();
    });
}

future<> test_compaction(distributed<perf_sstable_test_env>& dt) {
    return seastar::async([&dt] {
        dt.invoke_on_all([] (perf_sstable_test_env &t) {
            return t.fill_memtable();
        }).then([&dt] {
            return time_runs(iterations, parallelism, dt, &perf_sstable_test_env::compaction);
        }).get();
    });
}

future<> test_index_read(distributed<perf_sstable_test_env>& dt) {
    return time_runs(iterations, parallelism, dt, &perf_sstable_test_env::read_all_indexes);
}

future<> test_sequential_read(distributed<perf_sstable_test_env>& dt) {
    return time_runs(iterations, parallelism, dt, &perf_sstable_test_env::read_sequential_partitions);
}

future<> test_crawling_streaming(distributed<perf_sstable_test_env>& dt) {
    return time_runs(iterations, parallelism, dt, &perf_sstable_test_env::crawling_streaming);
}

future<> test_partitioned_streaming(distributed<perf_sstable_test_env>& dt) {
    return time_runs(iterations, parallelism, dt, &perf_sstable_test_env::partitioned_streaming);
}

enum class test_modes {
    sequential_read,
    index_read,
    write,
    index_write,
    compaction,
    crawling_streaming,
    partitioned_streaming,
};

static const std::unordered_map<sstring, test_modes> test_mode = {
    {"sequential_read", test_modes::sequential_read },
    {"index_read", test_modes::index_read },
    {"write", test_modes::write },
    {"index_write", test_modes::index_write },
    {"compaction", test_modes::compaction },
    {"crawling_streaming", test_modes::crawling_streaming },
    {"parititioned_streaming", test_modes::partitioned_streaming },
};

std::istream& operator>>(std::istream& is, test_modes& mode) {
    std::string token;
    is >> token;
    try {
        mode = test_mode.at(token);
    } catch (const std::out_of_range&) {
        throw boost::program_options::invalid_option_value(token);
    }
    return is;
}

std::ostream& operator<<(std::ostream& os, test_modes mode) {
    for (auto& [name, m] : test_mode) {
        if (m == mode) {
            return os << name;
        }
    }
    assert(false && "unreachable");
}

namespace perf {

int scylla_sstable_main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("parallelism", bpo::value<unsigned>()->default_value(1), "number parallel requests")
        ("iterations", bpo::value<unsigned>()->default_value(30), "number of iterations")
        ("partitions", bpo::value<unsigned>()->default_value(5000000), "number of partitions")
        ("buffer_size", bpo::value<unsigned>()->default_value(64), "sstable buffer size, in KB")
        ("key_size", bpo::value<unsigned>()->default_value(128), "size of partition key")
        ("num_columns", bpo::value<unsigned>()->default_value(5), "number of columns per row")
        ("column_size", bpo::value<unsigned>()->default_value(64), "size in bytes for each column")
        ("sstables", bpo::value<unsigned>()->default_value(1), "number of sstables (valid only for compaction mode)")
        ("mode", bpo::value<test_modes>()->default_value(test_modes::index_write), "one of: sequential_read, index_read, write, compaction, index_write, crawling_streaming, partitioned_streaming")
        ("testdir", bpo::value<sstring>()->default_value("/var/lib/scylla/perf-tests"), "directory in which to store the sstables")
        ("compaction-strategy", bpo::value<sstring>()->default_value("SizeTieredCompactionStrategy"), "compaction strategy to use, one of "
             "(SizeTieredCompactionStrategy, LeveledCompactionStrategy, DateTieredCompactionStrategy, TimeWindowCompactionStrategy)")
        ("timestamp-range", bpo::value<api::timestamp_type>()->default_value(0), "Timestamp values to use, chosen uniformly from: [-x, +x]");

    return app.run_deprecated(argc, argv, [&app] {
        auto test = make_lw_shared<distributed<perf_sstable_test_env>>();

        auto cfg = perf_sstable_test_env::conf();
        iterations = app.configuration()["iterations"].as<unsigned>();
        parallelism = app.configuration()["parallelism"].as<unsigned>();
        cfg.partitions = app.configuration()["partitions"].as<unsigned>();
        cfg.key_size = app.configuration()["key_size"].as<unsigned>();
        cfg.buffer_size = app.configuration()["buffer_size"].as<unsigned>() << 10;
        cfg.sstables = app.configuration()["sstables"].as<unsigned>();
        sstring dir = app.configuration()["testdir"].as<sstring>();
        cfg.dir = dir;
        auto mode = app.configuration()["mode"].as<test_modes>();
        if ((mode == test_modes::index_read) || (mode == test_modes::index_write)) {
            cfg.num_columns = 0;
            cfg.column_size = 0;
        } else {
            cfg.num_columns = app.configuration()["num_columns"].as<unsigned>();
            cfg.column_size = app.configuration()["column_size"].as<unsigned>();
        }
        cfg.compaction_strategy = sstables::compaction_strategy::type(app.configuration()["compaction-strategy"].as<sstring>());
        cfg.timestamp_range = app.configuration()["timestamp-range"].as<api::timestamp_type>();
        return test->start(std::move(cfg)).then([mode, dir, test] {
            engine().at_exit([test] { return test->stop(); });
            switch (mode) {
            using enum test_modes;
            case index_read:
                [[fallthrough]];
            case sequential_read:
                [[fallthrough]];
            case crawling_streaming:
                [[fallthrough]];
            case partitioned_streaming:
                return test->invoke_on_all([] (perf_sstable_test_env &t) {
                    return t.load_sstables(iterations);
                }).then_wrapped([] (future<> f) {
                    try {
                        f.get();
                    } catch (...) {
                        std::cerr << "An error occurred when trying to load test sstables. Did you run write mode yet?" << std::endl;
                        throw;
                    }
                });
            case index_write:
                [[fallthrough]];
            case write:
                [[fallthrough]];
            case compaction:
                return test_setup::create_empty_test_dir(dir);
            }
        }).then([test, mode] {
            switch (mode) {
            using enum test_modes;
            case index_read:
                return test_index_read(*test).then([test] {});
            case sequential_read:
                return test_sequential_read(*test).then([test] {});
            case crawling_streaming:
                return test_crawling_streaming(*test).then([test] {});
            case partitioned_streaming:
                return test_partitioned_streaming(*test).then([test] {});
            case index_write:
                [[fallthrough]];
            case write:
                return test_write(*test).then([test] {});
            case compaction:
                return test_compaction(*test).then([test] {});
            }
        }).then([] {
            return engine().exit(0);
        }).or_terminate();
    });
}

} // namespace perf
