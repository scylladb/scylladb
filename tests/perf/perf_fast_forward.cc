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
#include "tests/cql_test_env.hh"
#include "tests/perf/perf.hh"
#include "core/app-template.hh"
#include "schema_builder.hh"
#include "database.hh"
#include "db/config.hh"
#include "partition_slice_builder.hh"
#include <seastar/core/reactor.hh>
#include "transport/messages/result_message.hh"

#include "disk-error-handler.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

using namespace std::chrono_literals;

reactor::io_stats s;

struct metrics_snapshot {
    std::chrono::high_resolution_clock::time_point hr_clock;
    steady_clock_type::duration busy_time;
    steady_clock_type::duration idle_time;
    reactor::io_stats io;
    sstables::shared_index_lists::stats index;

    metrics_snapshot() {
        reactor& r = *local_engine;
        io = r.get_io_stats();
        busy_time = r.total_busy_time();
        idle_time = r.total_idle_time();
        hr_clock = std::chrono::high_resolution_clock::now();
        index = sstables::shared_index_lists::shard_stats();
    }
};

class make_printable {
    using func_type = std::function<void(std::ostream&)>;
    func_type _func;
public:
    make_printable(func_type func) : _func(std::move(func)) {}
    friend std::ostream& operator<<(std::ostream& out, const make_printable& p) {
        p._func(out);
        return out;
    }
};

struct test_result {
    uint64_t fragments_read;
    metrics_snapshot before;
    metrics_snapshot after;

    test_result(metrics_snapshot before, uint64_t fragments_read)
        : fragments_read(fragments_read)
        , before(before)
    { }

    double duration_in_seconds() const {
        return std::chrono::duration<double>(after.hr_clock - before.hr_clock).count();
    }

    double fragment_rate() const { return double(fragments_read) / duration_in_seconds(); }

    uint64_t aio_reads() const { return after.io.aio_reads - before.io.aio_reads; }
    uint64_t aio_read_bytes() const { return after.io.aio_read_bytes - before.io.aio_read_bytes; }
    uint64_t read_aheads_discarded() const { return after.io.fstream_read_aheads_discarded - before.io.fstream_read_aheads_discarded; }
    uint64_t reads_blocked() const { return after.io.fstream_reads_blocked - before.io.fstream_reads_blocked; }

    uint64_t index_hits() const { return after.index.hits - before.index.hits; }
    uint64_t index_misses() const { return after.index.misses - before.index.misses; }
    uint64_t index_blocks() const { return after.index.blocks - before.index.blocks; }

    float cpu_utilization() const {
        auto busy_delta = after.busy_time.count() - before.busy_time.count();
        auto idle_delta = after.idle_time.count() - before.idle_time.count();
        return float(busy_delta) / (busy_delta + idle_delta);
    }

    static auto table_header() {
        return make_printable([] (std::ostream& out) {
            out << sprint("%10s %9s %10s %6s %10s %7s %7s %8s %8s %8s %6s",
                "time [s]", "frags", "frag/s", "aio", "[KiB]", "blocked", "dropped",
                "idx hit", "idx miss", "idx blk", "cpu");
        });
    }

    auto table_row() {
        return make_printable([this] (std::ostream& out) {
            out << sprint("%10.6f %9d %10.0f %6d %10d %7d %7d %8d %8d %8d %5.1f%%",
                duration_in_seconds(), fragments_read, fragment_rate(),
                aio_reads(), aio_read_bytes() / 1024, reads_blocked(), read_aheads_discarded(),
                index_hits(), index_misses(), index_blocks(),
                cpu_utilization() * 100);
        });
    }
};

static
uint64_t consume_all(streamed_mutation& sm) {
    uint64_t fragments = 0;
    while (1) {
        mutation_fragment_opt mfo = sm().get0();
        if (!mfo) {
            break;
        }
        ++fragments;
    }
    return fragments;
}

static
uint64_t consume_all(mutation_reader& rd) {
    uint64_t fragments = 0;
    while (1) {
        streamed_mutation_opt smo = rd().get0();
        if (!smo) {
            break;
        }
        fragments += consume_all(*smo);
    }
    return fragments;
}

// cf should belong to ks.test
static test_result scan_rows_with_stride(column_family& cf, int n_rows, int n_read = 1, int n_skip = 0) {
    auto rd = cf.make_reader(cf.schema(),
        query::full_partition_range,
        query::full_slice,
        default_priority_class(),
        nullptr,
        n_skip ? streamed_mutation::forwarding::yes : streamed_mutation::forwarding::no);

    metrics_snapshot before;

    streamed_mutation_opt smo = rd().get0();
    assert(smo);
    streamed_mutation& sm = *smo;

    uint64_t fragments = 0;
    int ck = 0;
    while (ck < n_rows) {
        if (n_skip) {
            sm.fast_forward_to(position_range(
                position_in_partition(position_in_partition::clustering_row_tag_t(), clustering_key::from_singular(*cf.schema(), ck)),
                position_in_partition(position_in_partition::clustering_row_tag_t(), clustering_key::from_singular(*cf.schema(), ck + n_read))
            )).get();
        }
        fragments += consume_all(sm);
        ck += n_read + n_skip;
    }

    return {before, fragments};
}

static dht::decorated_key make_pkey(const schema& s, int n) {
    return dht::global_partitioner().decorate_key(s, partition_key::from_singular(s, n));
}

std::vector<dht::decorated_key> make_pkeys(schema_ptr s, int n) {
    std::vector<dht::decorated_key> keys;
    for (int i = 0; i < n; ++i) {
        keys.push_back(make_pkey(*s, i));
    }
    std::sort(keys.begin(), keys.end(), dht::decorated_key::less_comparator(s));
    return keys;
}

static test_result scan_with_stride_partitions(column_family& cf, int n, int n_read = 1, int n_skip = 0) {
    auto keys = make_pkeys(cf.schema(), n + n_read);

    int pk = 0;
    auto pr = n_skip ? dht::partition_range::make_ending_with(dht::partition_range::bound(keys[0], false)) // covering none
                     : query::full_partition_range;
    auto rd = cf.make_reader(cf.schema(), pr, query::full_slice);

    metrics_snapshot before;

    uint64_t fragments = 0;
    while (pk < n) {
        // FIXME: fast_forward_to() cannot be called on a reader from which nothing was read yet.
        if (pk && n_skip) {
            rd.fast_forward_to(dht::partition_range(
                dht::partition_range::bound(keys[pk], true),
                dht::partition_range::bound(keys[pk + n_read], false)
            )).get();
        }
        fragments += consume_all(rd);
        pk += n_read + n_skip;
    }

    return {before, fragments};
}

static test_result slice_rows(column_family& cf, int offset = 0, int n_read = 1) {
    auto rd = cf.make_reader(cf.schema(),
        query::full_partition_range,
        query::full_slice,
        default_priority_class(),
        nullptr,
        streamed_mutation::forwarding::yes);

    metrics_snapshot before;
    streamed_mutation_opt smo = rd().get0();
    assert(smo);
    streamed_mutation& sm = *smo;
    sm.fast_forward_to(position_range(
            position_in_partition::for_key(clustering_key::from_singular(*cf.schema(), offset)),
            position_in_partition::for_key(clustering_key::from_singular(*cf.schema(), offset + n_read)))).get();
    uint64_t fragments = consume_all(sm);

    fragments += consume_all(rd);

    return {before, fragments};
}

static test_result select_spread_rows(column_family& cf, int stride = 0, int n_read = 1) {
    auto sb = partition_slice_builder(*cf.schema());
    for (int i = 0; i < n_read; ++i) {
        sb.with_range(query::clustering_range::make_singular(clustering_key::from_singular(*cf.schema(), i * stride)));
    }

    auto slice = sb.build();
    auto rd = cf.make_reader(cf.schema(),
        query::full_partition_range,
        slice);

    metrics_snapshot before;
    streamed_mutation_opt smo = rd().get0();
    assert(smo);
    streamed_mutation& sm = *smo;
    uint64_t fragments = consume_all(sm);
    fragments += consume_all(rd);

    return {before, fragments};
}

static test_result slice_rows_single_key(column_family& cf, int offset = 0, int n_read = 1) {
    auto pr = dht::partition_range::make_singular(make_pkey(*cf.schema(), 0));
    auto rd = cf.make_reader(cf.schema(), pr, query::full_slice, default_priority_class(), nullptr, streamed_mutation::forwarding::yes);

    metrics_snapshot before;
    streamed_mutation_opt smo = rd().get0();
    assert(smo);
    streamed_mutation& sm = *smo;
    sm.fast_forward_to(position_range(
        position_in_partition::for_key(clustering_key::from_singular(*cf.schema(), offset)),
        position_in_partition::for_key(clustering_key::from_singular(*cf.schema(), offset + n_read)))).get();
    uint64_t fragments = consume_all(sm);

    fragments += consume_all(rd);

    return {before, fragments};
}

// cf is for ks.small_part
static test_result slice_partitions(column_family& cf, int n, int offset = 0, int n_read = 1) {
    auto keys = make_pkeys(cf.schema(), n + n_read);
    auto pr = dht::partition_range(
        dht::partition_range::bound(keys[offset], true),
        dht::partition_range::bound(keys[offset + n_read], false)
    );

    auto rd = cf.make_reader(cf.schema(), pr, query::full_slice);
    metrics_snapshot before;

    uint64_t fragments = consume_all(rd);

    return {before, fragments};
}

static
bytes make_blob(size_t blob_size) {
    static thread_local std::independent_bits_engine<std::default_random_engine, 8, uint8_t> random_bytes;
    bytes big_blob(bytes::initialized_later(), blob_size);
    for (auto&& b : big_blob) {
        b = random_bytes();
    }
    return big_blob;
}

struct table_config {
    sstring name;
    int n_rows;
    int value_size;
};

static test_result test_forwarding_with_restriction(column_family& cf, table_config& cfg, bool single_partition) {
    auto first_key = cfg.n_rows / 2;
    auto slice = partition_slice_builder(*cf.schema())
        .with_range(query::clustering_range::make_starting_with(clustering_key::from_singular(*cf.schema(), first_key)))
        .build();

    auto pr = single_partition ? dht::partition_range::make_singular(make_pkey(*cf.schema(), 0)) : query::full_partition_range;
    auto rd = cf.make_reader(cf.schema(),
        pr,
        slice,
        default_priority_class(),
        nullptr,
        streamed_mutation::forwarding::yes);

    uint64_t fragments = 0;
    metrics_snapshot before;
    streamed_mutation_opt smo = rd().get0();
    assert(smo);
    streamed_mutation& sm = *smo;

    fragments += consume_all(sm);

    sm.fast_forward_to(position_range(
        position_in_partition::for_key(clustering_key::from_singular(*cf.schema(), 1)),
        position_in_partition::for_key(clustering_key::from_singular(*cf.schema(), 2)))).get();

    fragments += consume_all(sm);

    sm.fast_forward_to(position_range(
        position_in_partition::for_key(clustering_key::from_singular(*cf.schema(), first_key - 2)),
        position_in_partition::for_key(clustering_key::from_singular(*cf.schema(), first_key + 2)))).get();

    fragments += consume_all(sm);

    fragments += consume_all(rd);
    return {before, fragments};
}

static void drop_keyspace_if_exists(cql_test_env& env, sstring name) {
    try {
        env.local_db().find_keyspace(name);
        std::cout << "Dropping keyspace...\n";
        env.execute_cql("drop keyspace ks;").get();
    } catch (const no_such_keyspace&) {
        // expected
    }
}

static
table_config read_config(cql_test_env& env, const sstring& name) {
    auto msg = env.execute_cql(sprint("select n_rows, value_size from ks.config where name = '%s'", name)).get0();
    auto rows = dynamic_pointer_cast<transport::messages::result_message::rows>(msg);
    if (rows->rs().size() < 1) {
        throw std::runtime_error("config not found. Did you run --populate ?");
    }
    const std::vector<bytes_opt>& config_row = rows->rs().rows()[0];
    if (config_row.size() != 2) {
        throw std::runtime_error("config row has invalid size");
    }
    auto n_rows = value_cast<int>(int32_type->deserialize(*config_row[0]));
    auto value_size = value_cast<int>(int32_type->deserialize(*config_row[1]));
    return {name, n_rows, value_size};
}

static
void populate(cql_test_env& env, table_config cfg) {
    drop_keyspace_if_exists(env, "ks");

    env.execute_cql("CREATE KEYSPACE ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};").get();

    std::cout << "Saving test config...\n";
    env.execute_cql("create table config (name text primary key, n_rows int, value_size int)").get();
    env.execute_cql(sprint("insert into ks.config (name, n_rows, value_size) values ('%s', %d, %d)", cfg.name, cfg.n_rows, cfg.value_size)).get();

    std::cout << "Creating test tables...\n";

    // Large partition with lots of rows
    env.execute_cql("create table test (pk int, ck int, value blob, primary key (pk, ck))"
        " WITH compression = { 'sstable_compression' : '' };").get();

    database& db = env.local_db();

    {
        std::cout << "Populating ks.test with " << cfg.n_rows << " rows...";

        auto insert_id = env.prepare("update test set \"value\" = ? where \"pk\" = 0 and \"ck\" = ?;").get0();

        for (int ck = 0; ck < cfg.n_rows; ++ck) {
            env.execute_prepared(insert_id, {{
                                                 cql3::raw_value::make_value(data_value(make_blob(cfg.value_size)).serialize()),
                                                 cql3::raw_value::make_value(data_value(ck).serialize())
                                             }}).get();
        }

        column_family& cf = db.find_column_family("ks", "test");

        std::cout << "flushing...\n";
        cf.flush().get();

        std::cout << "compacting...\n";
        cf.compact_all_sstables().get();
    }

    // Small partitions, but lots
    env.execute_cql("create table small_part (pk int, value blob, primary key (pk))"
        " WITH compression = { 'sstable_compression' : '' };").get();

    {
        std::cout << "Populating small_part with " << cfg.n_rows << " partitions...";

        auto insert_id = env.prepare("update small_part set \"value\" = ? where \"pk\" = ?;").get0();

        for (int pk = 0; pk < cfg.n_rows; ++pk) {
            env.execute_prepared(insert_id, {{
                                                 cql3::raw_value::make_value(data_value(make_blob(cfg.value_size)).serialize()),
                                                 cql3::raw_value::make_value(data_value(pk).serialize())
                                             }}).get();
        }

        column_family& cf = db.find_column_family("ks", "small_part");

        std::cout << "flushing...\n";
        cf.flush().get();

        std::cout << "compacting...\n";
        cf.compact_all_sstables().get();
    }
}

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("populate", "populate the table")
        ("verbose", "Enables more logging")
        ("trace", "Enables trace-level logging")
        ("enable-cache", "Enables cache")
        ("keep-cache-across-test-groups", "Clears the cache between test groups")
        ("keep-cache-across-test-cases", "Clears the cache between test cases in each test group")
        ("rows", bpo::value<int>()->default_value(1000000), "Number of CQL rows in a partition. Relevant only for population.")
        ("value-size", bpo::value<int>()->default_value(100), "Size of value stored in a cell. Relevant only for population.")
        ("name", bpo::value<std::string>()->default_value("default"), "Name of the configuration")
        ;

    return app.run(argc, argv, [&app] {
        db::config cfg;
        cfg.enable_cache = app.configuration().count("enable-cache");
        cfg.enable_commitlog = false;
        cfg.data_file_directories({ "./perf_large_partition_data" }, db::config::config_source::CommandLine);

        if (!app.configuration().count("verbose")) {
            logging::logger_registry().set_all_loggers_level(seastar::log_level::warn);
        }
        if (app.configuration().count("trace")) {
            logging::logger_registry().set_logger_level("sstable", seastar::log_level::trace);
        }

        std::cout << "Data directory: " << cfg.data_file_directories() << "\n";

        return do_with_cql_env([&app] (cql_test_env& env) {
            return seastar::async([&app, &env] {
                sstring name = app.configuration()["name"].as<std::string>();

                if (app.configuration().count("populate")) {
                    int n_rows = app.configuration()["rows"].as<int>();
                    int value_size = app.configuration()["value-size"].as<int>();
                    table_config cfg{name, n_rows, value_size};
                    populate(env, cfg);
                } else {
                    database& db = env.local_db();
                    column_family& cf = db.find_column_family("ks", "test");

                    auto cfg = read_config(env, name);
                    bool cache_enabled = app.configuration().count("enable-cache");
                    bool new_test_case = false;

                    std::cout << "Config: rows: " << cfg.n_rows << ", value size: " << cfg.value_size << "\n";

                    ::sleep(1s).get(); // wait for system table flushes to quiesce

                    bool cancel = false;
                    engine().at_exit([&] {
                        cancel = true;
                        return make_ready_future();
                    });

                    auto clear_cache = [] {
                        global_cache_tracker().clear();
                    };

                    auto on_test_group = [&] {
                        if (!app.configuration().count("keep-cache-across-test-groups")
                            && !app.configuration().count("keep-cache-across-test-cases")) {
                            clear_cache();
                        }
                    };

                    auto on_test_case = [&] {
                        new_test_case = true;
                        if (!app.configuration().count("keep-cache-across-test-cases")) {
                            clear_cache();
                        }
                        if (cancel) {
                            throw std::runtime_error("interrupted");
                        }
                    };

                    cf.run_with_compaction_disabled([&] {
                        return seastar::async([&] {
                            {
                                on_test_group();
                                std::cout << "Testing scanning large partition with skips. \n"
                                          << "Reads whole range interleaving reads with skips according to read-skip pattern:\n";
                                std::cout << sprint("%-7s %-7s ", "read", "skip") << test_result::table_header() << "\n";
                                auto test = [&] (int n_read, int n_skip) {
                                    on_test_case();
                                    auto r = scan_rows_with_stride(cf, cfg.n_rows, n_read, n_skip);
                                    std::cout << sprint("%-7d %-7d ", n_read, n_skip) << r.table_row() << "\n";
                                };

                                test(1, 0);

                                test(1, 1);
                                test(1, 8);
                                test(1, 16);
                                test(1, 32);
                                test(1, 64);
                                test(1, 256);
                                test(1, 1024);
                                test(1, 4096);

                                test(64, 1);
                                test(64, 8);
                                test(64, 16);
                                test(64, 32);
                                test(64, 64);
                                test(64, 256);
                                test(64, 1024);
                                test(64, 4096);
                            }

                            {
                                on_test_group();
                                std::cout << "Testing slicing of large partition:\n";
                                std::cout << sprint("%-7s %-7s ", "offset", "read") << test_result::table_header() << "\n";
                                auto test = [&] (int offset, int read) {
                                    on_test_case();
                                    auto r = slice_rows(cf, offset, read);
                                    std::cout << sprint("%-7d %-7d ", offset, read) << r.table_row() << "\n";
                                };

                                test(0, 1);
                                test(0, 32);
                                test(0, 256);
                                test(0, 4096);

                                test(cfg.n_rows / 2, 1);
                                test(cfg.n_rows / 2, 32);
                                test(cfg.n_rows / 2, 256);
                                test(cfg.n_rows / 2, 4096);
                            }

                            {
                                on_test_group();
                                std::cout << "Testing slicing of large partition, single-partition reader:\n";
                                std::cout << sprint("%-7s %-7s ", "offset", "read") << test_result::table_header()
                                          << "\n";
                                auto test = [&](int offset, int read) {
                                    on_test_case();
                                    auto r = slice_rows_single_key(cf, offset, read);
                                    std::cout << sprint("%-7d %-7d ", offset, read) << r.table_row() << "\n";
                                };

                                test(0, 1);
                                test(0, 32);
                                test(0, 256);
                                test(0, 4096);

                                test(cfg.n_rows / 2, 1);
                                test(cfg.n_rows / 2, 32);
                                test(cfg.n_rows / 2, 256);
                                test(cfg.n_rows / 2, 4096);
                            }

                            {
                                on_test_group();
                                std::cout << "Testing selecting few rows from a large partition:\n";
                                std::cout << sprint("%-7s %-7s ", "stride", "rows") << test_result::table_header()
                                          << "\n";
                                auto test = [&](int stride, int read) {
                                    on_test_case();
                                    auto r = select_spread_rows(cf, stride, read);
                                    std::cout << sprint("%-7d %-7d ", stride, read) << r.table_row() << "\n";
                                };

                                test(cfg.n_rows / 1, 1);
                                test(cfg.n_rows / 2, 2);
                                test(cfg.n_rows / 4, 4);
                                test(cfg.n_rows / 8, 8);
                                test(cfg.n_rows / 16, 16);
                                test(2, cfg.n_rows / 2);
                            }

                            {
                                on_test_group();
                                std::cout << "Testing forwarding with clustering restriction in a large partition:\n";
                                std::cout << sprint("%-7s ", "pk-scan") << test_result::table_header() << "\n";
                                on_test_case();
                                std::cout << sprint("%-7s ", "yes") << test_forwarding_with_restriction(cf, cfg, false).table_row() << "\n";
                                on_test_case();
                                std::cout << sprint("%-7s ", "no")  << test_forwarding_with_restriction(cf, cfg, true).table_row() << "\n";
                            }
                        });
                    }).get();

                    column_family& cf2 = db.find_column_family("ks", "small_part");
                    cf2.run_with_compaction_disabled([&] {
                        return seastar::async([&] {
                            {
                                on_test_group();
                                std::cout << "Testing scanning small partitions with skips. \n"
                                          << "Reads whole range interleaving reads with skips according to read-skip pattern:\n";
                                std::cout << sprint("%-7s %-7s ", "read", "skip") << test_result::table_header() << "\n";
                                auto test = [&] (int n_read, int n_skip) {
                                    on_test_case();
                                    auto r = scan_with_stride_partitions(cf2, cfg.n_rows, n_read, n_skip);
                                    std::cout << sprint("%-7d %-7d ", n_read, n_skip) << r.table_row() << "\n";
                                };

                                test(1, 0);

                                test(1, 1);
                                test(1, 8);
                                test(1, 16);
                                test(1, 32);
                                test(1, 64);
                                test(1, 256);
                                test(1, 1024);
                                test(1, 4096);

                                test(64, 1);
                                test(64, 8);
                                test(64, 16);
                                test(64, 32);
                                test(64, 64);
                                test(64, 256);
                                test(64, 1024);
                                test(64, 4096);
                            }

                            {
                                on_test_group();
                                std::cout << "Testing slicing small partitions:\n";
                                std::cout << sprint("%-7s %-7s ", "offset", "read") << test_result::table_header() << "\n";
                                auto test = [&] (int offset, int read) {
                                    on_test_case();
                                    auto r = slice_partitions(cf2, cfg.n_rows, offset, read);
                                    std::cout << sprint("%-7d %-7d ", offset, read) << r.table_row() << "\n";
                                };

                                test(0, 1);
                                test(0, 32);
                                test(0, 256);
                                test(0, 4096);

                                test(cfg.n_rows / 2, 1);
                                test(cfg.n_rows / 2, 32);
                                test(cfg.n_rows / 2, 256);
                                test(cfg.n_rows / 2, 4096);
                            }
                        });
                    }).get();
                }
            });
        }, cfg);
    });
}
