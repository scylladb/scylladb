/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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
#include "tests/cql_test_env.hh"
#include "tests/perf/perf.hh"
#include "core/app-template.hh"

#include "disk-error-handler.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

static const sstring table_name = "cf";

static bytes make_key(uint64_t sequence) {
    bytes b(bytes::initialized_later(), sizeof(sequence));
    auto i = b.begin();
    write<uint64_t>(i, sequence);
    return b;
};

static auto execute_update_for_key(cql_test_env& env, const bytes& key) {
    return env.execute_cql(sprint("UPDATE cf SET "
        "\"C0\" = 0x8f75da6b3dcec90c8a404fb9a5f6b0621e62d39c69ba5758e5f41b78311fbb26cc7a,"
        "\"C1\" = 0xa8761a2127160003033a8f4f3d1069b7833ebe24ef56b3beee728c2b686ca516fa51,"
        "\"C2\" = 0x583449ce81bfebc2e1a695eb59aad5fcc74d6d7311fc6197b10693e1a161ca2e1c64,"
        "\"C3\" = 0x62bcb1dbc0ff953abc703bcb63ea954f437064c0c45366799658bd6b91d0f92908d7,"
        "\"C4\" = 0x222fcbe31ffa1e689540e1499b87fa3f9c781065fccd10e4772b4c7039c2efd0fb27 "
        "WHERE \"KEY\"= 0x%s;", to_hex(key))).discard_result();
};

struct test_config {
    enum class run_mode { read, write };

    run_mode mode;
    unsigned partitions;
    unsigned concurrency;
    bool query_single_key;
    unsigned duration_in_seconds;
};

std::ostream& operator<<(std::ostream& os, const test_config::run_mode& m) {
    switch (m) {
        case test_config::run_mode::write: return os << "write";
        case test_config::run_mode::read: return os << "read";
    }
    assert(0);
}

std::ostream& operator<<(std::ostream& os, const test_config& cfg) {
    return os << "{partitions=" << cfg.partitions
           << ", concurrency=" << cfg.concurrency
           << ", mode=" << cfg.mode
           << ", query_single_key=" << (cfg.query_single_key ? "yes" : "no")
           << "}";
}

future<> test_read(cql_test_env& env, test_config& cfg) {
    std::cout << "Creating " << cfg.partitions << " partitions..." << std::endl;
    auto partitions = boost::irange(0, (int)cfg.partitions);
    return do_for_each(partitions.begin(), partitions.end(), [&env](int sequence) {
        return execute_update_for_key(env, make_key(sequence));
    }).then([&env] {
        return env.prepare("select \"C0\", \"C1\", \"C2\", \"C3\", \"C4\" from cf where \"KEY\" = ?");
    }).then([&env, &cfg](auto id) {
        return time_parallel([&env, &cfg, id] {
            bytes key = make_key(cfg.query_single_key ? 0 : std::rand() % cfg.partitions);
            return env.execute_prepared(id, {{std::move(key)}}).discard_result();
        }, cfg.concurrency, cfg.duration_in_seconds);
    });
}

future<> test_write(cql_test_env& env, test_config& cfg) {
    return env.prepare("UPDATE cf SET "
                           "\"C0\" = 0x8f75da6b3dcec90c8a404fb9a5f6b0621e62d39c69ba5758e5f41b78311fbb26cc7a,"
                           "\"C1\" = 0xa8761a2127160003033a8f4f3d1069b7833ebe24ef56b3beee728c2b686ca516fa51,"
                           "\"C2\" = 0x583449ce81bfebc2e1a695eb59aad5fcc74d6d7311fc6197b10693e1a161ca2e1c64,"
                           "\"C3\" = 0x62bcb1dbc0ff953abc703bcb63ea954f437064c0c45366799658bd6b91d0f92908d7,"
                           "\"C4\" = 0x222fcbe31ffa1e689540e1499b87fa3f9c781065fccd10e4772b4c7039c2efd0fb27 "
                           "WHERE \"KEY\" = ?;")
        .then([&env, &cfg](auto id) {
            return time_parallel([&env, &cfg, id] {
                bytes key = make_key(cfg.query_single_key ? 0 : std::rand() % cfg.partitions);
                return env.execute_prepared(id, {{std::move(key)}}).discard_result();
            }, cfg.concurrency, cfg.duration_in_seconds);
        });
}

future<> do_test(cql_test_env& env, test_config& cfg) {
    std::cout << "Running test with config: " << cfg << std::endl;
    return env.create_table([] (auto ks_name) {
        return schema({}, ks_name, "cf",
                {{"KEY", bytes_type}},
                {},
                {{"C0", bytes_type}, {"C1", bytes_type}, {"C2", bytes_type}, {"C3", bytes_type}, {"C4", bytes_type}},
                {},
                utf8_type);
    }).then([&env, &cfg] {
        switch (cfg.mode) {
            case test_config::run_mode::read:
                return test_read(env, cfg);
            case test_config::run_mode::write:
                return test_write(env, cfg);
        };
        assert(0);
    });
}

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("partitions", bpo::value<unsigned>()->default_value(10000), "number of partitions")
        ("write", "test write path instead of read path")
        ("duration", bpo::value<unsigned>()->default_value(5), "test duration in seconds")
        ("query-single-key", "test write path instead of read path")
        ("concurrency", bpo::value<unsigned>()->default_value(100), "workers per core");

    return app.run(argc, argv, [&app] {
        return do_with_cql_env([&app] (auto&& env) {
            auto cfg = make_lw_shared<test_config>();
            cfg->partitions = app.configuration()["partitions"].as<unsigned>();
            cfg->duration_in_seconds = app.configuration()["duration"].as<unsigned>();
            cfg->concurrency = app.configuration()["concurrency"].as<unsigned>();
            cfg->mode = app.configuration().count("write") ? test_config::run_mode::write : test_config::run_mode::read;
            cfg->query_single_key = app.configuration().count("query-single-key");
            return do_test(env, *cfg).finally([cfg] {});
        });
    });
}
