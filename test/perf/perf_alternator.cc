/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <functional>
#include <memory>
#include <signal.h>
#include <seastar/core/future.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/app-template.hh>
#include <seastar/http/client.hh>
#include <seastar/http/request.hh>
#include <seastar/http/reply.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/short_streams.hh>
#include <tuple>
#include <boost/program_options.hpp>

#include "db/config.hh"
#include "test/perf/perf.hh"
#include "test/lib/random_utils.hh"


namespace perf {

using namespace seastar;
namespace bpo = boost::program_options;

struct test_config {
    std::string workload;
    int port;
    unsigned partitions;
    bool prepopulate_partitions;
    unsigned duration_in_seconds;
    unsigned operations_per_shard;
    unsigned concurrency;
    unsigned scan_total_segments;
    bool flush;
    std::string remote_host;
    bool continue_after_error;
};

std::ostream& operator<<(std::ostream& os, const test_config& cfg) {
    return os << "{workload=" << cfg.workload
           << ", partitions=" << cfg.partitions
           << ", concurrency=" << cfg.concurrency
           << ", duration_in_seconds=" << cfg.duration_in_seconds
           << ", operations-per-shard=" << cfg.operations_per_shard
           << ", flush=" << cfg.flush
           << "}";
}

static http::experimental::client get_client(const test_config& c, int port = 0) {
    std::string host = "127.0.0.1";
    if (!c.remote_host.empty()) {
        host = c.remote_host;
    }
    if (port == 0) {
        port = c.port;
    }
    return http::experimental::client(socket_address(net::inet_address(host), port));
}

static future<> make_request(http::experimental::client& cli, sstring operation, sstring body) {
    auto req = http::request::make("POST", "localhost", "/");
    req._headers["X-Amz-Target:"] = "DynamoDB_20120810." + operation;
    req.write_body("application/x-amz-json-1.0", std::move(body));
    return cli.make_request(std::move(req), [] (const http::reply& rep, input_stream<char>&& in_) {
        return do_with(std::move(in_), [] (auto& in) {
            return util::skip_entire_stream(in).then([&in] () {
                return in.close();
            });
        });
    });
}

static void delete_alternator_table(http::experimental::client& cli) {
    try {
        make_request(cli, "DeleteTable", R"({"TableName": "workloads_test"})").get();
    } catch(...) {
        // table may exist or not
    }
}

static void create_alternator_table(http::experimental::client& cli) {
    delete_alternator_table(cli); // cleanup in case of leftovers
    make_request(cli, "CreateTable", R"(
        {
            "AttributeDefinitions": [{
                    "AttributeName": "p",
                    "AttributeType": "S"
                },
                {
                    "AttributeName": "c",
                    "AttributeType": "S"
                }
            ],
            "TableName": "workloads_test",
            "BillingMode": "PAY_PER_REQUEST",
            "KeySchema": [{
                    "AttributeName": "p",
                    "KeyType": "HASH"
                },
                {
                    "AttributeName": "c",
                    "KeyType": "RANGE"
                }
            ]
        }
    )").get();
}

static void create_alternator_table_with_gsi(http::experimental::client& cli) {
    delete_alternator_table(cli); // cleanup in case of leftovers
    make_request(cli, "CreateTable", R"(
        {
            "AttributeDefinitions": [{
                    "AttributeName": "p",
                    "AttributeType": "S"
                },
                {
                    "AttributeName": "c",
                    "AttributeType": "S"
                },
                {
                    "AttributeName": "C0",
                    "AttributeType": "S"
                },
                {
                    "AttributeName": "C1",
                    "AttributeType": "S"
                },
                {
                    "AttributeName": "C2",
                    "AttributeType": "S"
                },
                {
                    "AttributeName": "C3",
                    "AttributeType": "S"
                },
                {
                    "AttributeName": "C4",
                    "AttributeType": "S"
                }
            ],
            "TableName": "workloads_test",
            "BillingMode": "PAY_PER_REQUEST",
            "KeySchema": [{
                    "AttributeName": "p",
                    "KeyType": "HASH"
                },
                {
                    "AttributeName": "c",
                    "KeyType": "RANGE"
                }
            ],
            "GlobalSecondaryIndexes": [
                {   "IndexName": "idx_C0",
                    "KeySchema": [
                        { "AttributeName": "C0", "KeyType": "HASH" }
                    ],
                    "Projection": { "ProjectionType": "ALL" }
                },
                {   "IndexName": "idx_C1",
                    "KeySchema": [
                        { "AttributeName": "C1", "KeyType": "HASH" }
                    ],
                    "Projection": { "ProjectionType": "ALL" }
                },
                {   "IndexName": "idx_C2",
                    "KeySchema": [
                        { "AttributeName": "C2", "KeyType": "HASH" }
                    ],
                    "Projection": { "ProjectionType": "ALL" }
                },
                {   "IndexName": "idx_C3",
                    "KeySchema": [
                        { "AttributeName": "C3", "KeyType": "HASH" }
                    ],
                    "Projection": { "ProjectionType": "ALL" }
                },
                {   "IndexName": "idx_C4",
                    "KeySchema": [
                        { "AttributeName": "C4", "KeyType": "HASH" }
                    ],
                    "Projection": { "ProjectionType": "ALL" }
                }
            ]
        }
    )").get();
}

// Exercise various types documented here: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html
static constexpr auto update_item_suffix = R"(
        "UpdateExpression": "set C0 = :C0, C1 = :C1, C2 = :C2, C3 = :C3, C4 = :C4, C5 = :C5, C6 = :C6, C7 = :C7, C8 = :C8, C9 = :C9",
        "ExpressionAttributeValues": {{
            {}
            ":C0": {{
                "B": "dGhpcyB0ZXh0IGlzIGJhc2U2NC1lbmNvZGVk"
            }},
            ":C1": {{
                "BOOL": true
            }},
            ":C2": {{
                "BS": ["U3Vubnk=", "UmFpbnk=", "U25vd3k="]
            }},
            ":C3": {{
                "L": [ {{"S": "Cookies"}} , {{"S": "Coffee"}}, {{"N": "3.14159"}}]
            }},
            ":C4": {{
                "M": {{"Name": {{"S": "Joe"}}, "Age": {{"N": "35"}}}}
            }},
            ":C5": {{
                "N": "123.45"
            }},
            ":C6": {{
                "NS": ["42.2", "-19", "7.5", "3.14"]
            }},
            ":C7": {{
                "NULL": true
            }},
            ":C8": {{
                "S": "Hello"
            }},
            ":C9": {{
                "SS": ["Giraffe", "Hippo" ,"Zebra"]
            }}
        }},
        "ReturnValues": "NONE"
    }}
)";

static future<> update_item(const test_config& _, http::experimental::client& cli, uint64_t seq) {
    auto prefix = format(R"({{
            "TableName": "workloads_test",
            "Key": {{
                "p": {{
                    "S": "{}"
                }},
                "c": {{
                    "S": "{}"
                }}
            }},)", seq, seq);

    return make_request(cli, "UpdateItem", prefix + format(update_item_suffix, ""));
}

static future<> update_item_gsi(const test_config& _, http::experimental::client& cli, uint64_t seq) {
    auto prefix = format(R"({{
            "TableName": "workloads_test",
            "Key": {{
                "p": {{
                    "S": "{}"
                }},
                "c": {{
                    "S": "{}"
                }}
            }},
            "UpdateExpression": "set C0 = :C0, C1 = :C1, C2 = :C2, C3 = :C3, C4 = :C4",
            "ExpressionAttributeValues": {{
                ":C0": {{
                    "S": "{}"
                }},
                ":C1": {{
                    "S": "{}"
                }},
                ":C2": {{
                    "S": "{}"
                }},
                ":C3": {{
                    "S": "{}"
                }},
                ":C4": {{
                    "S": "{}"
                }}
            }},
        "ReturnValues": "NONE"
    }})", seq, seq, seq>>1, seq>>2, seq>>3, seq>>4, seq>>5); // different values so that some gsi (mv) updates will land on different shards
    return make_request(cli, "UpdateItem", prefix);
}

static future<> update_item_rmw(const test_config& _, http::experimental::client& cli, uint64_t seq) {
    auto prefix = format(R"({{
            "TableName": "workloads_test",
            "Key": {{
                "p": {{
                    "S": "{}"
                }},
                "c": {{
                    "S": "{}"
                }}
            }},)", seq, seq);
    // making conditional write is one way of making sure scylla will do read before write (rmw)
    // for our static data this condition is always true to simplify things
    auto condition_exp = R"(
         "ConditionExpression": "((NOT attribute_exists(C2)) OR size(C6) <= :val1) AND (C8 <> :val2 OR C6 IN (:val1)) ",
    )";
    auto condition_attribute_values = R"(
        ":val1": {
                "N": "10"
        },
        ":val2": {
                "S": "some_value"
        },
    )";
    return make_request(cli, "UpdateItem", prefix + condition_exp +
                        format(update_item_suffix, condition_attribute_values));
}

static future<> get_item(const test_config& _, http::experimental::client& cli, uint64_t seq) {
    auto body = format(R"({{
        "TableName": "workloads_test",
        "Key": {{
            "p": {{
                "S": "{}"
            }},
            "c": {{
                "S": "{}"
            }}
        }},
        "ProjectionExpression": "C0, C1, C2, C3, C4, C5, C6, C7, C8, C9",
        "ConsistentRead": false,
        "ReturnConsumedCapacity": "TOTAL"
    }})",seq, seq);
    co_await make_request(cli, "GetItem", std::move(body));
}

static future<> scan(const test_config& c, http::experimental::client& cli, uint64_t seq) {
    // This uses "parallel scan" feature, see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan
    auto body = format(R"({{
        "TableName": "workloads_test",
        "Select": "ALL_ATTRIBUTES",
        "Segment": {},
        "TotalSegments": {},
        "ConsistentRead": false
    }})", seq % c.scan_total_segments, c.scan_total_segments);
    co_await make_request(cli, "Scan", std::move(body));
}

static void flush_table(const test_config& c) {
    auto cli = get_client(c, 10000);
    auto req = http::request::make("POST", "localhost", "/storage_service/keyspace_flush/alternator_workloads_test");
    cli.make_request(std::move(req), [] (const http::reply& rep, input_stream<char>&& in) {
        return make_ready_future<>();
    }).get();
    cli.close().get();
}

static void create_partitions(const test_config& c, http::experimental::client& cli) {
    std::cout << "Creating " << c.partitions << " partitions..." << std::endl;
    for (unsigned seq = 0; seq < c.partitions; ++seq) {
        update_item(c, cli, seq).get();
    }
    if (c.flush) {
        std::cout << "Flushing partitions..." << std::endl;
        flush_table(c);
    }
}

auto make_client_pool(const test_config& c) {
    std::vector<http::experimental::client> res;
    res.reserve(c.concurrency);
    for (unsigned i = 0; i < c.concurrency; i++) {
        res.push_back(get_client(c));
    }
    return res;
}

void workload_main(const test_config& c) {
    std::cout << "Running test with config: " << c << std::endl;

    auto cli = get_client(c);
    auto finally = defer([&] {
        delete_alternator_table(cli);
        cli.close().get();
    });
    if (c.workload != "write_gsi") {
        create_alternator_table(cli);
    } else {
        create_alternator_table_with_gsi(cli);
    }

    using fun_t = std::function<future<>(const test_config&, http::experimental::client&, uint64_t)>;
    std::map<std::string, fun_t> workloads = {
        {"read",  get_item},
        {"scan", scan},
        {"write", update_item},
        {"write_gsi", update_item_gsi},
        // needs to be executed together with --alternator-write-isolation only_rmw_uses_lwt
        // for realistic scenario
        {"write_rmw", update_item_rmw},
    };

    if (c.prepopulate_partitions && (c.workload == "read" || c.workload == "scan")) {
        create_partitions(c, cli);
    }

    auto it = workloads.find(c.workload);
    if (it == workloads.end()) {
        throw std::runtime_error(format("unknown workload '{}'", c.workload));
    }
    fun_t fun = it->second;

    auto results = time_parallel([&] {
        static thread_local auto sharded_cli_pool = make_client_pool(c); // for simplicity never closed as it lives for the whole process runtime
        static thread_local auto cli_iter = -1;
        auto seq = tests::random::get_int<uint64_t>(c.partitions - 1);
        return fun(c, sharded_cli_pool[++cli_iter % c.concurrency], seq);
    }, c.concurrency, c.duration_in_seconds, c.operations_per_shard, !c.continue_after_error);

    std::cout << aggregated_perf_results(results) << std::endl;
}

std::tuple<int,char**> cut_arg(int ac, char** av, std::string name, int num_args = 2) {
    for (int i = 1 ; i < ac - 1; i++) {
        if (std::string(av[i]) == name) {
            std::shift_left(av + i, av + ac, num_args);
            ac -= num_args;
            break;
        }
    }
    return std::make_tuple(ac, av);
}

std::function<int(int, char**)> alternator(std::function<int(int, char**)> scylla_main, std::function<void(lw_shared_ptr<db::config> cfg)>* after_init_func) {
    return [=](int ac, char** av) -> int {
        test_config c;
       
        bpo::options_description opts_desc;
        opts_desc.add_options()
            ("workload", bpo::value<std::string>()->default_value(""), "which workload type to run")
            ("partitions", bpo::value<unsigned>()->default_value(10000), "number of partitions")
            ("prepopulate-partitions", bpo::value<bool>()->default_value(true), "relevant for read workloads, can be disabled when data is prepopulated externally")
            ("duration", bpo::value<unsigned>()->default_value(5), "test duration in seconds")
            ("operations-per-shard", bpo::value<unsigned>()->default_value(0), "run this many operations per shard (overrides duration)")
            ("concurrency", bpo::value<unsigned>()->default_value(100), "workers per core")
            ("flush", bpo::value<bool>()->default_value(true), "flush memtables before test")
            ("remote-host", bpo::value<std::string>()->default_value(""), "address of remote alternator service, use localhost by default")
            ("scan-total-segments", bpo::value<unsigned>()->default_value(10), "single scan operation will retrieve 1/scan-total-segments portion of a table")
            ("continue-after-error", bpo::value<bool>()->default_value(false), "continue test after failed request")
        ;
        bpo::variables_map opts;
        bpo::store(bpo::command_line_parser(ac, av).options(opts_desc).allow_unregistered().run(), opts);

        c.workload = opts["workload"].as<std::string>();
        c.partitions = opts["partitions"].as<unsigned>();
        c.prepopulate_partitions = opts["prepopulate-partitions"].as<bool>();
        c.duration_in_seconds = opts["duration"].as<unsigned>();
        c.operations_per_shard = opts["operations-per-shard"].as<unsigned>();
        c.concurrency = opts["concurrency"].as<unsigned>();
        c.flush = opts["flush"].as<bool>();
        c.remote_host = opts["remote-host"].as<std::string>();
        c.scan_total_segments = opts["scan-total-segments"].as<unsigned>();
        c.continue_after_error = opts["continue-after-error"].as<bool>();

        if (c.scan_total_segments < 1 || c.scan_total_segments > 1'000'000) {
            throw std::invalid_argument("scan-total-segments must be between 1 and 1'000'000");
        }

        // Remove test options to not disturb scylla main app
        for (auto& opt : opts_desc.options()) {
            auto name = opt->canonical_display_name(bpo::command_line_style::allow_long);
            std::tie(ac, av) = cut_arg(ac, av, name);
        }

        if (c.workload.empty()) {
            std::cerr << "Missing --workload command-line value!" << std::endl;
            return 1;
        }
        
        if (!c.remote_host.empty()) {
            c.port = 8000; // TODO: make configurable
            app_template app;
            return app.run(ac, av, [c = std::move(c)] () -> future<> {
                return seastar::async([c = std::move(c)] () {
                    workload_main(c);
                });
            });
        }

        *after_init_func = [c = std::move(c)] (lw_shared_ptr<db::config> cfg) mutable {
            c.port = cfg->alternator_port();
            (void)seastar::async([c = std::move(c)] {
                try {
                    workload_main(c);
                } catch(...) {
                    std::cerr << "Test failed: " << std::current_exception() << std::endl;
                    raise(SIGKILL); // request abnormal shutdown
                }
                raise(SIGINT); // request shutdown
            });
        };
        return scylla_main(ac, av);
    };
}

} // namespace perf
