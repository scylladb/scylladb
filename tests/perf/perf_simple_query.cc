/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include <boost/range/irange.hpp>
#include "tests/urchin/cql_test_env.hh"
#include "tests/perf/perf.hh"
#include "core/app-template.hh"

static const sstring table_name = "cf";

static sstring make_key(int sequence) {
    return sprint("0xdeadbeefcafebabe%04d", sequence);
};

static auto execute_update_for_key(cql_test_env& env, const sstring& key) {
    return env.execute_cql(sprint("UPDATE cf SET "
        "\"C0\" = 0x8f75da6b3dcec90c8a404fb9a5f6b0621e62d39c69ba5758e5f41b78311fbb26cc7a,"
        "\"C1\" = 0xa8761a2127160003033a8f4f3d1069b7833ebe24ef56b3beee728c2b686ca516fa51,"
        "\"C2\" = 0x583449ce81bfebc2e1a695eb59aad5fcc74d6d7311fc6197b10693e1a161ca2e1c64,"
        "\"C3\" = 0x62bcb1dbc0ff953abc703bcb63ea954f437064c0c45366799658bd6b91d0f92908d7,"
        "\"C4\" = 0x222fcbe31ffa1e689540e1499b87fa3f9c781065fccd10e4772b4c7039c2efd0fb27 "
        "WHERE \"KEY\"=%s;", key)).discard_result();
};

static auto constexpr n_partitons = 1000;
static auto constexpr concurrency = 100;

future<> do_test(cql_test_env& env) {
    std::cout << "Concurrency = " << concurrency << " x " << smp::count << std::endl;
    auto keys = make_shared<std::vector<bytes>>();

    return env.create_table([] (auto ks_name) {
        return schema(ks_name, "cf",
                {{"KEY", bytes_type}},
                {},
                {{"C0", bytes_type}, {"C1", bytes_type}, {"C2", bytes_type}, {"C3", bytes_type}, {"C4", bytes_type}},
                {},
                utf8_type);
    }).then([&env, keys] {
        std::cout << "Creating " << n_partitons << " partitions..." << std::endl;
        auto partitions = boost::irange(0, n_partitons);
        return do_for_each(partitions.begin(), partitions.end(), [&env, keys] (int sequence) {
            auto key = make_key(sequence);
            keys->push_back(bytes(key));
            return execute_update_for_key(env, key);
        });
    }).then([&env] {
        return env.prepare("select \"C0\", \"C1\", \"C2\", \"C3\", \"C4\" from cf where \"KEY\" = ?");
    }).then([&env, keys] (auto id) {
        std::cout << "Timing single key selects..." << std::endl;
        return time_parallel([&env, id, keys] {
                auto& key = keys->at(0);
                return env.execute_prepared(id, {{key}}).discard_result();
            }, concurrency)
        .then([&env, id, keys] () {
            std::cout << "Timing random key selects..." << std::endl;
            return time_parallel([&env, id, keys] {
                auto& key = keys->at(std::rand() % keys->size());
                return env.execute_prepared(id, {{key}}).discard_result();
            }, concurrency);
        });
    });
}

int main(int argc, char** argv) {
    app_template app;
    return app.run(argc, argv, [] {
        make_env_for_test().then([] (auto env) {
            return do_test(*env).finally([env] {
                return env->stop().finally([env] {});
            });
        }).then([] {
            return engine().exit(0);
        }).or_terminate();
    });
}
