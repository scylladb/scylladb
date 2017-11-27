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

#include <boost/range/irange.hpp>
#include "tests/cql_test_env.hh"
#include "tests/perf/perf.hh"
#include "core/app-template.hh"
#include "schema_builder.hh"

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

static auto execute_counter_update_for_key(cql_test_env& env, const bytes& key) {
    return env.execute_cql(sprint("UPDATE cf SET "
        "\"C0\" = \"C0\" + 1,"
        "\"C1\" = \"C1\" + 2,"
        "\"C2\" = \"C2\" + 3,"
        "\"C3\" = \"C3\" + 4,"
        "\"C4\" = \"C4\" + 5 "
        "WHERE \"KEY\"= 0x%s;", to_hex(key))).discard_result();
};

struct test_config {
    enum class run_mode { read, write, del };

    run_mode mode;
    unsigned partitions;
    unsigned concurrency;
    bool query_single_key;
    unsigned duration_in_seconds;
    bool counters;
    unsigned operations_per_shard = 0;
};

std::ostream& operator<<(std::ostream& os, const test_config::run_mode& m) {
    switch (m) {
        case test_config::run_mode::write: return os << "write";
        case test_config::run_mode::read: return os << "read";
        case test_config::run_mode::del: return os << "delete";
    }
    abort();
}

std::ostream& operator<<(std::ostream& os, const test_config& cfg) {
    return os << "{partitions=" << cfg.partitions
           << ", concurrency=" << cfg.concurrency
           << ", mode=" << cfg.mode
           << ", query_single_key=" << (cfg.query_single_key ? "yes" : "no")
           << ", counters=" << (cfg.counters ? "yes" : "no")
           << "}";
}

future<> create_partitions(cql_test_env& env, test_config& cfg) {
    std::cout << "Creating " << cfg.partitions << " partitions..." << std::endl;
    auto partitions = boost::irange(0, (int)cfg.partitions);
    return do_for_each(partitions.begin(), partitions.end(), [&] (int sequence) {
        if (cfg.counters) {
            return execute_counter_update_for_key(env, make_key(sequence));
        }
        return execute_update_for_key(env, make_key(sequence));
    });
}

future<> test_read(cql_test_env& env, test_config& cfg) {
    return create_partitions(env, cfg).then([&env] {
        return env.prepare("select \"C0\", \"C1\", \"C2\", \"C3\", \"C4\" from cf where \"KEY\" = ?");
    }).then([&env, &cfg](auto id) {
        return time_parallel([&env, &cfg, id] {
            bytes key = make_key(cfg.query_single_key ? 0 : std::rand() % cfg.partitions);
            return env.execute_prepared(id, {{cql3::raw_value::make_value(std::move(key))}}).discard_result();
        }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard);
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
                return env.execute_prepared(id, {{cql3::raw_value::make_value(std::move(key))}}).discard_result();
            }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard);
        });
}

future<> test_delete(cql_test_env& env, test_config& cfg) {
    return create_partitions(env, cfg).then([&env] {
        return env.prepare("DELETE \"C0\", \"C1\", \"C2\", \"C3\", \"C4\" FROM cf WHERE \"KEY\" = ?");
    }).then([&env, &cfg](auto id) {
        return time_parallel([&env, &cfg, id] {
            bytes key = make_key(cfg.query_single_key ? 0 : std::rand() % cfg.partitions);
            return env.execute_prepared(id, {{cql3::raw_value::make_value(std::move(key))}}).discard_result();
        }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard);
    });
}

future<> test_counter_update(cql_test_env& env, test_config& cfg) {
    return env.prepare("UPDATE cf SET "
                           "\"C0\" = \"C0\" + 1,"
                           "\"C1\" = \"C1\" + 2,"
                           "\"C2\" = \"C2\" + 3,"
                           "\"C3\" = \"C3\" + 4,"
                           "\"C4\" = \"C4\" + 5 "
                           "WHERE \"KEY\" = ?;")
        .then([&env, &cfg] (auto id) {
            return time_parallel([&env, &cfg, id] {
                bytes key = make_key(cfg.query_single_key ? 0 : std::rand() % cfg.partitions);
                return env.execute_prepared(id, {{cql3::raw_value::make_value(std::move(key))}}).discard_result();
            }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard);
        });
}

schema_ptr make_counter_schema(const sstring& ks_name) {
    return schema_builder(ks_name, "cf")
            .with_column("KEY", bytes_type, column_kind::partition_key)
            .with_column("C0", counter_type)
            .with_column("C1", counter_type)
            .with_column("C2", counter_type)
            .with_column("C3", counter_type)
            .with_column("C4", counter_type)
            .build();
}

future<> do_test(cql_test_env& env, test_config& cfg) {
    std::cout << "Running test with config: " << cfg << std::endl;
    return env.create_table([&cfg] (auto ks_name) {
        if (cfg.counters) {
            return *make_counter_schema(ks_name);
        }
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
                if (cfg.counters) {
                    return test_counter_update(env, cfg);
                } else {
                    return test_write(env, cfg);
                }
            case test_config::run_mode::del:
                return test_delete(env, cfg);
        };
        abort();
    });
}

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("partitions", bpo::value<unsigned>()->default_value(10000), "number of partitions")
        ("write", "test write path instead of read path")
        ("delete", "test delete path instead of read path")
        ("duration", bpo::value<unsigned>()->default_value(5), "test duration in seconds")
        ("query-single-key", "test reading with a single key instead of random keys")
        ("concurrency", bpo::value<unsigned>()->default_value(100), "workers per core")
        ("operations-per-shard", bpo::value<unsigned>(), "run this many operations per shard (overrides duration)")
        ("counters", "test counters");

    return app.run(argc, argv, [&app] {
        return do_with_cql_env([&app] (auto&& env) {
            auto cfg = make_lw_shared<test_config>();
            cfg->partitions = app.configuration()["partitions"].as<unsigned>();
            cfg->duration_in_seconds = app.configuration()["duration"].as<unsigned>();
            cfg->concurrency = app.configuration()["concurrency"].as<unsigned>();
            cfg->query_single_key = app.configuration().count("query-single-key");
            cfg->counters = app.configuration().count("counters");
            if (app.configuration().count("write")) {
                cfg->mode = test_config::run_mode::write;
            } else if (app.configuration().count("delete")) {
                cfg->mode = test_config::run_mode::del;
            } else {
                cfg->mode = test_config::run_mode::read;
            };
            if (app.configuration().count("operations-per-shard")) {
                cfg->operations_per_shard = app.configuration()["operations-per-shard"].as<unsigned>();
            }
            return do_test(env, *cfg).finally([cfg] {});
        });
    });
}
