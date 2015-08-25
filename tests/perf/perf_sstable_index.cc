/*
 * Copyright 2015 Cloudius Systems
 */

#include <boost/test/unit_test.hpp>
#include <core/distributed.hh>
#include <core/app-template.hh>
#include <core/sstring.hh>
#include <random>
#include "perf_sstable.hh"

using namespace sstables;

static unsigned iterations = 30;
static unsigned parallelism = 1;

future<> test_write(distributed<test_env>& dt) {
    return dt.invoke_on_all([] (test_env &t) {
        t.fill_memtable();
    }).then([&dt] {
        return time_runs(iterations, parallelism, dt, &test_env::flush_memtable);
    });
}

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("parallelism", bpo::value<unsigned>()->default_value(1), "number parallel requests")
        ("iterations", bpo::value<unsigned>()->default_value(30), "number of iterations")
        ("partitions", bpo::value<unsigned>()->default_value(5000000), "number of partitions")
        ("key_size", bpo::value<unsigned>()->default_value(128), "size of partition key")
        ("testdir", bpo::value<sstring>()->default_value("/var/lib/cassandra/perf-tests"), "directory in which to store the sstables");

    return app.run(argc, argv, [&app] {
        auto test = make_lw_shared<distributed<test_env>>();

        auto cfg = test_env::conf();
        iterations = app.configuration()["iterations"].as<unsigned>();
        parallelism = app.configuration()["parallelism"].as<unsigned>();
        cfg.partitions = app.configuration()["partitions"].as<unsigned>();
        cfg.key_size = app.configuration()["key_size"].as<unsigned>();
        sstring dir = app.configuration()["testdir"].as<sstring>();
        cfg.dir = dir;
        return test->start(std::move(cfg)).then([dir, test] {
            engine().at_exit([test] { return test->stop(); });
            return test_setup::create_empty_test_dir(dir);
        }).then([test] {
            return test_write(*test).then([test] {});
        }).then([] {
            return engine().exit(0);
        }).or_terminate();
    });
}
