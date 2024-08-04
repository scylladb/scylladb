/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/range/irange.hpp>
#include <json/json.h>
#include <fmt/ranges.h>

#include "test/lib/cql_test_env.hh"
#include "test/lib/alternator_test_env.hh"
#include "test/perf/perf.hh"
#include <seastar/core/app-template.hh>
#include <seastar/testing/test_runner.hh>
#include "test/lib/random_utils.hh"

#include "db/config.hh"
#include "schema/schema_builder.hh"
#include "release.hh"
#include <fstream>
#include "service/storage_proxy.hh"
#include "cql3/query_processor.hh"
#include "db/config.hh"
#include "db/extensions.hh"
#include "db/tags/extension.hh"
#include "gms/gossiper.hh"

static const sstring table_name = "cf";

static bytes make_key(uint64_t sequence) {
    bytes b(bytes::initialized_later(), sizeof(sequence));
    auto i = b.begin();
    write<uint64_t>(i, sequence);
    return b;
};

static void execute_update_for_key(cql_test_env& env, const bytes& key) {
    env.execute_cql(fmt::format("UPDATE cf SET "
        "\"C0\" = 0x8f75da6b3dcec90c8a404fb9a5f6b0621e62d39c69ba5758e5f41b78311fbb26cc7a,"
        "\"C1\" = 0xa8761a2127160003033a8f4f3d1069b7833ebe24ef56b3beee728c2b686ca516fa51,"
        "\"C2\" = 0x583449ce81bfebc2e1a695eb59aad5fcc74d6d7311fc6197b10693e1a161ca2e1c64,"
        "\"C3\" = 0x62bcb1dbc0ff953abc703bcb63ea954f437064c0c45366799658bd6b91d0f92908d7,"
        "\"C4\" = 0x222fcbe31ffa1e689540e1499b87fa3f9c781065fccd10e4772b4c7039c2efd0fb27 "
        "WHERE \"KEY\"= 0x{};", to_hex(key))).get();
};

static void execute_counter_update_for_key(cql_test_env& env, const bytes& key) {
    env.execute_cql(fmt::format("UPDATE cf SET "
        "\"C0\" = \"C0\" + 1,"
        "\"C1\" = \"C1\" + 2,"
        "\"C2\" = \"C2\" + 3,"
        "\"C3\" = \"C3\" + 4,"
        "\"C4\" = \"C4\" + 5 "
        "WHERE \"KEY\"= 0x{};", to_hex(key))).get();
};

struct test_config {
    enum class run_mode { read, write, del };
    enum class frontend_type { cql, alternator };

    run_mode mode;
    frontend_type frontend;
    unsigned partitions;
    unsigned concurrency;
    bool query_single_key;
    unsigned duration_in_seconds;
    bool counters;
    bool flush_memtables;
    unsigned memtable_partitions = 0;
    unsigned operations_per_shard = 0;
    bool stop_on_error;
    sstring timeout;
    bool bypass_cache;
    std::optional<unsigned> initial_tablets;
};

std::ostream& operator<<(std::ostream& os, const test_config::run_mode& m) {
    switch (m) {
        case test_config::run_mode::write: return os << "write";
        case test_config::run_mode::read: return os << "read";
        case test_config::run_mode::del: return os << "delete";
    }
    abort();
}

std::ostream& operator<<(std::ostream& os, const test_config::frontend_type& f) {
    switch (f) {
        case test_config::frontend_type::cql: return os << "cql";
        case test_config::frontend_type::alternator: return os << "alternator";
    }
    abort();
}

std::ostream& operator<<(std::ostream& os, const test_config& cfg) {
    return os << "{partitions=" << cfg.partitions
           << ", concurrency=" << cfg.concurrency
           << ", mode=" << cfg.mode
           << ", frontend=" << cfg.frontend
           << ", query_single_key=" << (cfg.query_single_key ? "yes" : "no")
           << ", counters=" << (cfg.counters ? "yes" : "no")
           << "}";
}

static void create_partitions(cql_test_env& env, test_config& cfg) {
    std::cout << "Creating " << cfg.partitions << " partitions..." << std::endl;
    unsigned next_flush = (cfg.memtable_partitions > 0 ? cfg.memtable_partitions : cfg.partitions);
    for (unsigned sequence = 0; sequence < cfg.partitions; ++sequence) {
        if (cfg.counters) {
            execute_counter_update_for_key(env, make_key(sequence));
        } else {
            execute_update_for_key(env, make_key(sequence));
        }
        if (sequence + 1 >= next_flush) {
            env.db().invoke_on_all(&replica::database::flush_all_memtables).get();
            next_flush += cfg.memtable_partitions;
        }
    }

    if (cfg.flush_memtables) {
        std::cout << "Flushing partitions..." << std::endl;
        env.db().invoke_on_all(&replica::database::flush_all_memtables).get();
    }
}

static int64_t make_random_seq(test_config& cfg) {
    return cfg.query_single_key ? 0 : tests::random::get_int<uint64_t>(cfg.partitions - 1);
}

static bytes make_random_key(test_config& cfg) {
    return make_key(make_random_seq(cfg));
}

static std::vector<perf_result> test_read(cql_test_env& env, test_config& cfg) {
    create_partitions(env, cfg);
    sstring query = "select \"C0\", \"C1\", \"C2\", \"C3\", \"C4\" from cf where \"KEY\" = ?";
    if (cfg.bypass_cache) {
        query += " bypass cache";
    }
    if (!cfg.timeout.empty()) {
        query += " using timeout " + cfg.timeout;
    }
    auto id = env.prepare(query).get();
    return time_parallel([&env, &cfg, id] {
            bytes key = make_random_key(cfg);
            return env.execute_prepared(id, {{cql3::raw_value::make_value(std::move(key))}}).discard_result();
        }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard, cfg.stop_on_error);
}

static std::vector<perf_result> test_write(cql_test_env& env, test_config& cfg) {
    sstring usings;
    if (!cfg.timeout.empty()) {
        usings += "USING TIMEOUT " + cfg.timeout;
    }
    sstring query = format("UPDATE cf {}SET "
            "\"C0\" = 0x8f75da6b3dcec90c8a404fb9a5f6b0621e62d39c69ba5758e5f41b78311fbb26cc7a,"
            "\"C1\" = 0xa8761a2127160003033a8f4f3d1069b7833ebe24ef56b3beee728c2b686ca516fa51,"
            "\"C2\" = 0x583449ce81bfebc2e1a695eb59aad5fcc74d6d7311fc6197b10693e1a161ca2e1c64,"
            "\"C3\" = 0x62bcb1dbc0ff953abc703bcb63ea954f437064c0c45366799658bd6b91d0f92908d7,"
            "\"C4\" = 0x222fcbe31ffa1e689540e1499b87fa3f9c781065fccd10e4772b4c7039c2efd0fb27 "
            "WHERE \"KEY\" = ?", usings);
    auto id = env.prepare(query).get();
    return time_parallel([&env, &cfg, id] {
            bytes key = make_random_key(cfg);
            return env.execute_prepared(id, {{cql3::raw_value::make_value(std::move(key))}}).discard_result();
        }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard, cfg.stop_on_error);
}

static std::vector<perf_result> test_delete(cql_test_env& env, test_config& cfg) {
    create_partitions(env, cfg);
    sstring usings;
    if (!cfg.timeout.empty()) {
        usings += "USING TIMEOUT " + cfg.timeout;
    }
    sstring query = format("DELETE \"C0\", \"C1\", \"C2\", \"C3\", \"C4\" FROM cf {}WHERE \"KEY\" = ?", usings);
    auto id = env.prepare(query).get();
    return time_parallel([&env, &cfg, id] {
            bytes key = make_random_key(cfg);
            return env.execute_prepared(id, {{cql3::raw_value::make_value(std::move(key))}}).discard_result();
        }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard, cfg.stop_on_error);
}

static std::vector<perf_result> test_counter_update(cql_test_env& env, test_config& cfg) {
    sstring usings;
    if (!cfg.timeout.empty()) {
        usings += "USING TIMEOUT " + cfg.timeout;
    }
    sstring query = format("UPDATE cf {}SET "
            "\"C0\" = \"C0\" + 1,"
            "\"C1\" = \"C1\" + 2,"
            "\"C2\" = \"C2\" + 3,"
            "\"C3\" = \"C3\" + 4,"
            "\"C4\" = \"C4\" + 5 "
            "WHERE \"KEY\" = ?", usings);
    auto id = env.prepare(query).get();
    return time_parallel([&env, &cfg, id] {
            bytes key = make_random_key(cfg);
            return env.execute_prepared(id, {{cql3::raw_value::make_value(std::move(key))}}).discard_result();
        }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard, cfg.stop_on_error);
}

static schema_ptr make_counter_schema(std::string_view ks_name) {
    return schema_builder(ks_name, "cf")
            .with_column("KEY", bytes_type, column_kind::partition_key)
            .with_column("C0", counter_type)
            .with_column("C1", counter_type)
            .with_column("C2", counter_type)
            .with_column("C3", counter_type)
            .with_column("C4", counter_type)
            .build();
}

static void create_alternator_table(service::client_state& state, alternator::executor& executor) {
    executor.create_table(state, tracing::trace_state_ptr(), empty_service_permit(), rjson::parse(
        R"(
            {
                "AttributeDefinitions": [{
                        "AttributeName": "p",
                        "AttributeType": "S"
                    }
                ],
                "TableName": "alternator_table",
                "BillingMode": "PAY_PER_REQUEST",
                "KeySchema": [{
                        "AttributeName": "p",
                        "KeyType": "HASH"
                    }
                ]
            }
        )"
    )).get();
}

static void execute_alternator_update_for_key(service::client_state& state, alternator::executor& executor, int64_t sequence) {
    std::string prefix = R"(
        {
            "TableName": "alternator_table",
            "Key": {
                "p": {
                    "S": ")";
     std::string postfix = R"("
                }
            },
            "UpdateExpression": "set C0 = :C0, C1 = :C1, C2 = :C2, C3 = :C3, C4 = :C4",
            "ExpressionAttributeValues": {
                ":C0": {
                    "S": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                },
                ":C1": {
                    "S": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                },
                ":C2": {
                    "S": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                },
                ":C3": {
                    "S": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                },
                ":C4": {
                    "S": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                }
            },
            "ReturnValues": "NONE"
        }
    )";
    auto key = std::to_string(sequence);
    // Chunked content is used to minimize string copying, and thus extra allocations
    rjson::chunked_content content;
    content.reserve(3);
    content.emplace_back(prefix.data(), prefix.size(), deleter{});
    content.emplace_back(key.data(), key.size(), deleter{});
    content.emplace_back(postfix.data(), postfix.size(), deleter{});
    executor.update_item(state, tracing::trace_state_ptr(), empty_service_permit(), rjson::parse(std::move(content))).get();
};

static void create_alternator_partitions(service::client_state& state, noncopyable_function<void()> flush_memtables,
        alternator::executor& executor, test_config& cfg) {
    std::cout << "Creating " << cfg.partitions << " partitions..." << std::endl;
    for (unsigned sequence = 0; sequence < cfg.partitions; ++sequence) {
        execute_alternator_update_for_key(state, executor, sequence);
    }

    if (cfg.flush_memtables) {
        flush_memtables();
    }
}

static std::vector<perf_result> test_alternator_read(service::client_state& state, noncopyable_function<void()> flush_memtables,
        alternator::executor& executor, test_config& cfg) {
    create_alternator_partitions(state, std::move(flush_memtables), executor, cfg);
    std::string prefix = R"(
        {
            "TableName": "alternator_table",
            "Key": {
                "p": {
                    "S": ")";
    std::string postfix = R"("
                    }
                },
                "ConsistentRead": false,
                "ReturnConsumedCapacity": "TOTAL"
            }
        )";
    return time_parallel([&] {
        auto key = std::to_string(make_random_seq(cfg));
        // Chunked content is used to minimize string copying, and thus extra allocations
        rjson::chunked_content content;
        content.reserve(3);
        content.emplace_back(prefix.data(), prefix.size(), deleter{});
        content.emplace_back(key.data(), key.size(), deleter{});
        content.emplace_back(postfix.data(), postfix.size(), deleter{});
        return executor.get_item(state, tracing::trace_state_ptr(), empty_service_permit(), rjson::parse(std::move(content))).discard_result();
    }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard, cfg.stop_on_error);
}

static std::vector<perf_result> test_alternator_write(service::client_state& state, alternator::executor& executor, test_config& cfg) {
    return time_parallel([&] {
        std::string prefix = R"(
            {
                "TableName": "alternator_table",
                "Key": {
                    "p": {
                        "S": ")";
        std::string postfix = R"("
                    }
                },
                "UpdateExpression": "set C0 = :C0, C1 = :C1, C2 = :C2, C3 = :C3, C4 = :C4",
                "ExpressionAttributeValues": {
                    ":C0": {
                        "S": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                    },
                    ":C1": {
                        "S": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                    },
                    ":C2": {
                        "S": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                    },
                    ":C3": {
                        "S": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                    },
                    ":C4": {
                        "S": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                    }
                },
                "ReturnValues": "NONE"
            }
        )";
        auto key = std::to_string(make_random_seq(cfg));
        // Chunked content is used to minimize string copying, and thus extra allocations
        rjson::chunked_content content;
        content.reserve(3);
        content.emplace_back(prefix.data(), prefix.size(), deleter{});
        content.emplace_back(key.data(), key.size(), deleter{});
        content.emplace_back(postfix.data(), postfix.size(), deleter{});
        return executor.update_item(state, tracing::trace_state_ptr(), empty_service_permit(), rjson::parse(std::move(content))).discard_result();
    }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard, cfg.stop_on_error);
}

static std::vector<perf_result> test_alternator_delete(service::client_state& state, noncopyable_function<void()> flush_memtables,
        alternator::executor& executor, test_config& cfg) {
    create_alternator_partitions(state, std::move(flush_memtables), executor, cfg);
    return time_parallel([&] {
        std::string json = R"(
            {
                "TableName": "alternator_table",
                "Key": {
                    "p": {
                        "S": ")" + std::to_string(make_random_seq(cfg)) + R"("
                    }
                },
                "ReturnValues": "NONE"
            }
        )";
        return executor.delete_item(state, tracing::trace_state_ptr(), empty_service_permit(), rjson::parse(json)).discard_result();
    }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard, cfg.stop_on_error);
}

static std::vector<perf_result> do_alternator_test(std::string isolation_level,
        service::client_state& state,
        sharded<cql3::query_processor>& qp,
        sharded<service::migration_manager>& mm,
        sharded<gms::gossiper>& gossiper,
        test_config& cfg) {
    SCYLLA_ASSERT(cfg.frontend == test_config::frontend_type::alternator);
    std::cout << "Running test with config: " << cfg << std::endl;

    alternator_test_env env(gossiper, qp.local().proxy().container(), mm, qp);
    env.start(isolation_level).get();
    auto stop_env = defer([&] {
        env.stop().get();
    });
    alternator::executor& executor = env.executor();

    try {
        create_alternator_table(state, executor);
        auto flush_memtables = [&] {
            std::cout << "Flushing partitions..." << std::endl;
            env.flush_memtables().get();
        };

        switch (cfg.mode) {
        case test_config::run_mode::read:
            return test_alternator_read(state, std::move(flush_memtables), executor, cfg);
        case test_config::run_mode::write:
            return test_alternator_write(state, executor, cfg);
        case test_config::run_mode::del:
            return test_alternator_delete(state, std::move(flush_memtables), executor, cfg);
        };
    } catch (const alternator::api_error& e) {
        std::cout << "Alternator API error: " << e._msg << std::endl;
        throw;
    }
    abort();
}

static std::vector<perf_result> do_cql_test(cql_test_env& env, test_config& cfg) {
    SCYLLA_ASSERT(cfg.frontend == test_config::frontend_type::cql);

    std::cout << "Running test with config: " << cfg << std::endl;
    env.create_table([&cfg] (auto ks_name) {
        if (cfg.counters) {
            return *make_counter_schema(ks_name);
        }
        return *schema_builder(ks_name, "cf")
                .with_column("KEY", bytes_type, column_kind::partition_key)
                .with_column("C0", bytes_type)
                .with_column("C1", bytes_type)
                .with_column("C2", bytes_type)
                .with_column("C3", bytes_type)
                .with_column("C4", bytes_type)
                .build();
    }).get();

    std::cout << "Disabling auto compaction" << std::endl;
    env.db().invoke_on_all([] (auto& db) {
        auto& cf = db.find_column_family("ks", "cf");
        return cf.disable_auto_compaction();
    }).get();

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
}

void write_json_result(std::string result_file, const test_config& cfg, const aggregated_perf_results& agg) {
    Json::Value results;

    Json::Value params;
    params["concurrency"] = cfg.concurrency;
    params["partitions"] = cfg.partitions;
    params["cpus"] = smp::count;
    params["duration"] = cfg.duration_in_seconds;
    params["concurrency,partitions,cpus,duration"] = fmt::format("{},{},{},{}", cfg.concurrency, cfg.partitions, smp::count, cfg.duration_in_seconds);
    if (cfg.initial_tablets) {
        params["initial_tablets"] = cfg.initial_tablets.value();
    }
    results["parameters"] = std::move(params);

    Json::Value stats;
    auto med = agg.median_by_throughput;
    stats["median tps"] = med.throughput;
    stats["allocs_per_op"] = med.mallocs_per_op;
    stats["logallocs_per_op"] = med.logallocs_per_op;
    stats["tasks_per_op"] = med.tasks_per_op;
    stats["instructions_per_op"] = med.instructions_per_op;
    stats["cpu_cycles_per_op"] = med.cpu_cycles_per_op;
    const auto& tps = agg.stats.at("throughput");
    stats["mad tps"] = tps.median_absolute_deviation;
    stats["max tps"] = tps.max;
    stats["min tps"] = tps.min;
    results["stats"] = std::move(stats);

    std::string test_type;
    switch (cfg.mode) {
    case test_config::run_mode::read: test_type = "read"; break;
    case test_config::run_mode::write: test_type = "write"; break;
    case test_config::run_mode::del: test_type = "delete"; break;
    }
    if (cfg.counters) {
        test_type += "_counters";
    }
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

namespace perf {

int scylla_simple_query_main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("random-seed", boost::program_options::value<unsigned>(), "Random number generator seed")
        ("partitions", bpo::value<unsigned>()->default_value(10000), "number of partitions")
        ("write", "test write path instead of read path")
        ("delete", "test delete path instead of read path")
        ("duration", bpo::value<unsigned>()->default_value(5), "test duration in seconds")
        ("query-single-key", "test reading with a single key instead of random keys")
        ("concurrency", bpo::value<unsigned>()->default_value(100), "workers per core")
        ("operations-per-shard", bpo::value<unsigned>(), "run this many operations per shard (overrides duration)")
        ("counters", "test counters")
        ("tablets", "use tablets")
        ("initial-tablets", bpo::value<unsigned>()->default_value(128), "initial number of tablets")
        ("flush", "flush memtables before test")
        ("memtable-partitions", bpo::value<unsigned>(), "apply this number of partitions to memtable, then flush")
        ("json-result", bpo::value<std::string>(), "name of the json result file")
        ("enable-cache", bpo::value<bool>()->default_value(true), "enable row cache")
        ("alternator", bpo::value<std::string>(), "use alternator frontend instead of CQL with given write isolation")
        ("stop-on-error", bpo::value<bool>()->default_value(true), "stop after encountering the first error")
        ("timeout", bpo::value<std::string>()->default_value(""), "use timeout")
        ("bypass-cache", "use bypass cache when querying")
        ;

    set_abort_on_internal_error(true);

    return app.run(argc, argv, [&app] {
        auto conf_seed = app.configuration()["random-seed"];
        auto seed = conf_seed.empty() ? std::random_device()() : conf_seed.as<unsigned>();
        std::cout << "random-seed=" << seed << '\n';
        return smp::invoke_on_all([seed] {
            seastar::testing::local_random_engine.seed(seed + this_shard_id());
        }).then([&app] () -> future<> {
            auto ext = std::make_shared<db::extensions>();
            ext->add_schema_extension<db::tags_extension>(db::tags_extension::NAME);
            auto db_cfg = ::make_shared<db::config>(ext);

            const auto enable_cache = app.configuration()["enable-cache"].as<bool>();
            std::cout << "enable-cache=" << enable_cache << '\n';
            db_cfg->enable_cache(enable_cache);
            cql_test_config cfg(db_cfg);
            if (app.configuration().contains("tablets")) {
                cfg.db_config->enable_tablets.set(true);
                cfg.initial_tablets = app.configuration()["initial-tablets"].as<unsigned>();
            }
          return do_with_cql_env_thread([&app] (auto&& env) {
            auto cfg = test_config();
            cfg.partitions = app.configuration()["partitions"].as<unsigned>();
            cfg.duration_in_seconds = app.configuration()["duration"].as<unsigned>();
            cfg.concurrency = app.configuration()["concurrency"].as<unsigned>();
            cfg.query_single_key = app.configuration().contains("query-single-key");
            cfg.counters = app.configuration().contains("counters");
            cfg.flush_memtables = app.configuration().contains("flush");
            if (app.configuration().contains("tablets")) {
                cfg.initial_tablets = app.configuration()["initial-tablets"].as<unsigned>();
            }
            if (app.configuration().contains("write")) {
                cfg.mode = test_config::run_mode::write;
            } else if (app.configuration().contains("delete")) {
                cfg.mode = test_config::run_mode::del;
            } else {
                cfg.mode = test_config::run_mode::read;
            };
            if (app.configuration().contains("alternator")) {
                cfg.frontend = test_config::frontend_type::alternator;
            } else {
                cfg.frontend = test_config::frontend_type::cql;
            }
            if (app.configuration().contains("operations-per-shard")) {
                cfg.operations_per_shard = app.configuration()["operations-per-shard"].as<unsigned>();
            }
            if (app.configuration().contains("memtable-partitions")) {
                cfg.memtable_partitions = app.configuration()["memtable-partitions"].as<unsigned>();
            }
            cfg.stop_on_error = app.configuration()["stop-on-error"].as<bool>();
            cfg.timeout = app.configuration()["timeout"].as<std::string>();
            cfg.bypass_cache = app.configuration().contains("bypass-cache");
            auto results = cfg.frontend == test_config::frontend_type::cql
                    ? do_cql_test(env, cfg)
                    : do_alternator_test(app.configuration()["alternator"].as<std::string>(),
                            env.local_client_state(), env.qp(), env.migration_manager(), env.gossiper(), cfg);
            aggregated_perf_results agg(results);
            std::cout << agg << std::endl;
            if (app.configuration().contains("json-result")) {
                write_json_result(app.configuration()["json-result"].as<std::string>(), cfg, agg);
            }
          }, std::move(cfg));
        });
    });
}

} // namespace perf
