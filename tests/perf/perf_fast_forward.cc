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

#include <boost/algorithm/string/replace.hpp>
#include <boost/range/irange.hpp>
#include "tests/cql_test_env.hh"
#include "tests/perf/perf.hh"
#include "core/app-template.hh"
#include "schema_builder.hh"
#include "database.hh"
#include "db/config.hh"
#include "partition_slice_builder.hh"
#include <seastar/core/reactor.hh>
#include "sstables/compaction_manager.hh"
#include "transport/messages/result_message.hh"
#include "sstables/shared_index_lists.hh"

#include "disk-error-handler.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

using namespace std::chrono_literals;
using int_range = nonwrapping_range<int>;

reactor::io_stats s;

static bool errors_found = false;

static void print_error(const sstring& msg) {
    std::cout << "^^^ ERROR: " << msg << "\n";
    errors_found = true;
}

struct metrics_snapshot {
    std::chrono::high_resolution_clock::time_point hr_clock;
    steady_clock_type::duration busy_time;
    steady_clock_type::duration idle_time;
    reactor::io_stats io;
    sstables::shared_index_lists::stats index;
    cache_tracker::stats cache;

    metrics_snapshot() {
        reactor& r = *local_engine;
        io = r.get_io_stats();
        busy_time = r.total_busy_time();
        idle_time = r.total_idle_time();
        hr_clock = std::chrono::high_resolution_clock::now();
        index = sstables::shared_index_lists::shard_stats();
        cache = global_cache_tracker().get_stats();
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

    uint64_t cache_hits() const { return after.cache.partition_hits - before.cache.partition_hits; }
    uint64_t cache_misses() const { return after.cache.partition_misses - before.cache.partition_misses; }
    uint64_t cache_insertions() const { return after.cache.partition_insertions - before.cache.partition_insertions; }

    float cpu_utilization() const {
        auto busy_delta = after.busy_time.count() - before.busy_time.count();
        auto idle_delta = after.idle_time.count() - before.idle_time.count();
        return float(busy_delta) / (busy_delta + idle_delta);
    }

    static auto table_header() {
        return make_printable([] (std::ostream& out) {
            out << sprint("%10s %9s %10s %6s %10s %7s %7s %8s %8s %8s %8s %8s %8s %6s",
                "time [s]", "frags", "frag/s", "aio", "[KiB]", "blocked", "dropped",
                "idx hit", "idx miss", "idx blk",
                "c hit", "c miss", "c ins",
                "cpu");
        });
    }

    auto table_row() {
        return make_printable([this] (std::ostream& out) {
            out << sprint("%10.6f %9d %10.0f %6d %10d %7d %7d %8d %8d %8d %8d %8d %8d %5.1f%%",
                duration_in_seconds(), fragments_read, fragment_rate(),
                aio_reads(), aio_read_bytes() / 1024, reads_blocked(), read_aheads_discarded(),
                index_hits(), index_misses(), index_blocks(),
                cache_hits(), cache_misses(), cache_insertions(),
                cpu_utilization() * 100);
        });
    }
};

static void check_no_disk_reads(const test_result& r) {
    if (r.aio_reads()) {
        print_error("Expected no disk reads");
    }
}

static void check_no_index_reads(const test_result& r) {
    if (r.index_hits() || r.index_misses()) {
        print_error("Expected no index reads");
    }
}

static void check_fragment_count(const test_result& r, uint64_t expected) {
    if (r.fragments_read != expected) {
        print_error(sprint("Expected to read %d fragments", expected));
    }
}

class counting_consumer {
    uint64_t _fragments = 0;
public:
    stop_iteration consume(tombstone) { return stop_iteration::no; }
    template<typename Fragment>
    stop_iteration consume(Fragment&& f) { _fragments++; return stop_iteration::no; }
    uint64_t consume_end_of_stream() { return _fragments; }
};

static
uint64_t consume_all(streamed_mutation& sm) {
    return consume(sm, counting_consumer()).get0();
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
        cf.schema()->full_slice(),
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
    auto keys = make_pkeys(cf.schema(), n);

    int pk = 0;
    auto pr = n_skip ? dht::partition_range::make_ending_with(dht::partition_range::bound(keys[0], false)) // covering none
                     : query::full_partition_range;
    auto rd = cf.make_reader(cf.schema(), pr, cf.schema()->full_slice());

    metrics_snapshot before;

    if (n_skip) {
        // FIXME: fast_forward_to() cannot be called on a reader from which nothing was read yet.
        consume_all(rd);
    }

    uint64_t fragments = 0;
    while (pk < n) {
        if (n_skip) {
            pr = dht::partition_range(
                dht::partition_range::bound(keys[pk], true),
                dht::partition_range::bound(keys[std::min(n, pk + n_read) - 1], true)
            );
            rd.fast_forward_to(pr).get();
        }
        fragments += consume_all(rd);
        pk += n_read + n_skip;
    }

    return {before, fragments};
}

static test_result slice_rows(column_family& cf, int offset = 0, int n_read = 1) {
    auto rd = cf.make_reader(cf.schema(),
        query::full_partition_range,
        cf.schema()->full_slice(),
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

static test_result test_reading_all(mutation_reader& rd) {
    metrics_snapshot before;
    return {before, consume_all(rd)};
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

    return test_reading_all(rd);
}

static test_result test_slicing_using_restrictions(column_family& cf, int_range row_range) {
    auto slice = partition_slice_builder(*cf.schema())
        .with_range(std::move(row_range).transform([&] (int i) -> clustering_key {
            return clustering_key::from_singular(*cf.schema(), i);
        }))
        .build();
    auto pr = dht::partition_range::make_singular(make_pkey(*cf.schema(), 0));
    auto rd = cf.make_reader(cf.schema(), pr, slice);
    return test_reading_all(rd);
}

static test_result slice_rows_single_key(column_family& cf, int offset = 0, int n_read = 1) {
    auto pr = dht::partition_range::make_singular(make_pkey(*cf.schema(), 0));
    auto rd = cf.make_reader(cf.schema(), pr, cf.schema()->full_slice(), default_priority_class(), nullptr, streamed_mutation::forwarding::yes);

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
    auto keys = make_pkeys(cf.schema(), n);

    auto pr = dht::partition_range(
        dht::partition_range::bound(keys[offset], true),
        dht::partition_range::bound(keys[std::min(n, offset + n_read) - 1], true)
    );

    auto rd = cf.make_reader(cf.schema(), pr, cf.schema()->full_slice());
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
    auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
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

static unsigned cardinality(int_range r) {
    assert(r.start());
    assert(r.end());
    return r.end()->value() - r.start()->value() + r.start()->is_inclusive() + r.end()->is_inclusive() - 1;
}

static unsigned cardinality(stdx::optional<int_range> ropt) {
    return ropt ? cardinality(*ropt) : 0;
}

static stdx::optional<int_range> intersection(int_range a, int_range b) {
    auto int_tri_cmp = [] (int x, int y) {
        return x < y ? -1 : (x > y ? 1 : 0);
    };
    return a.intersection(b, int_tri_cmp);
}

// Number of fragments which is expected to be received by interleaving
// n_read reads with n_skip skips when total number of fragments is n.
static int count_for_skip_pattern(int n, int n_read, int n_skip) {
    return n / (n_read + n_skip) * n_read + std::min(n % (n_read + n_skip), n_read);
}

app_template app;
bool cancel = false;
bool cache_enabled;
bool new_test_case = false;
table_config cfg;
int_range live_range;

void clear_cache() {
    global_cache_tracker().clear();
}

void on_test_group() {
    if (!app.configuration().count("keep-cache-across-test-groups")
        && !app.configuration().count("keep-cache-across-test-cases")) {
        clear_cache();
    }
};

void on_test_case() {
    new_test_case = true;
    if (!app.configuration().count("keep-cache-across-test-cases")) {
        clear_cache();
    }
    if (cancel) {
        throw std::runtime_error("interrupted");
    }
};

void test_large_partition_single_key_slice(column_family& cf) {
    std::cout << sprint("%-2s %-14s ", "", "range") << test_result::table_header() << "\n";
    struct first {
    };
    auto test = [&](int_range range) {
        auto r = test_slicing_using_restrictions(cf, range);
        std::cout << sprint("%-2s %-14s ", new_test_case ? "->" : "", sprint("%s", range))
                  << r.table_row() << "\n";
        new_test_case = false;
        check_fragment_count(r, cardinality(intersection(range, live_range)));
        return r;
    };

    on_test_case();
    test(int_range::make({0}, {1}));
    test_result r = test(int_range::make({0}, {1}));
    check_no_disk_reads(r);

    on_test_case();
    test(int_range::make({0}, {cfg.n_rows / 2}));
    r = test(int_range::make({0}, {cfg.n_rows / 2}));
    check_no_disk_reads(r);

    on_test_case();
    test(int_range::make({0}, {cfg.n_rows}));
    r = test(int_range::make({0}, {cfg.n_rows}));
    check_no_disk_reads(r);

    assert(cfg.n_rows > 200); // assumed below

    on_test_case(); // adjacent, no overlap
    test(int_range::make({1}, {100, false}));
    test(int_range::make({100}, {109}));

    on_test_case(); // adjacent, contained
    test(int_range::make({1}, {100}));
    r = test(int_range::make_singular({100}));
    check_no_disk_reads(r);

    on_test_case(); // overlap
    test(int_range::make({1}, {100}));
    test(int_range::make({51}, {150}));

    on_test_case(); // enclosed
    test(int_range::make({1}, {100}));
    r = test(int_range::make({51}, {70}));
    check_no_disk_reads(r);

    on_test_case(); // enclosing
    test(int_range::make({51}, {70}));
    test(int_range::make({41}, {80}));
    test(int_range::make({31}, {100}));

    on_test_case(); // adjacent, singular excluded
    test(int_range::make({0}, {100, false}));
    test(int_range::make_singular({100}));

    on_test_case(); // adjacent, singular excluded
    test(int_range::make({100, false}, {200}));
    test(int_range::make_singular({100}));

    on_test_case();
    test(int_range::make_ending_with({100}));
    r = test(int_range::make({10}, {20}));
    check_no_disk_reads(r);
    r = test(int_range::make_singular({-1}));
    check_no_disk_reads(r);

    on_test_case();
    test(int_range::make_starting_with({100}));
    r = test(int_range::make({150}, {159}));
    check_no_disk_reads(r);
    r = test(int_range::make_singular({cfg.n_rows - 1}));
    check_no_disk_reads(r);
    r = test(int_range::make_singular({cfg.n_rows + 1}));
    check_no_disk_reads(r);

    on_test_case(); // many gaps
    test(int_range::make({10}, {20, false}));
    test(int_range::make({30}, {40, false}));
    test(int_range::make({60}, {70, false}));
    test(int_range::make({90}, {100, false}));
    test(int_range::make({0}, {100, false}));

    on_test_case(); // many gaps
    test(int_range::make({10}, {20, false}));
    test(int_range::make({30}, {40, false}));
    test(int_range::make({60}, {70, false}));
    test(int_range::make({90}, {100, false}));
    test(int_range::make({10}, {100, false}));
}

void test_large_partition_skips(column_family& cf) {
    std::cout << sprint("%-7s %-7s ", "read", "skip") << test_result::table_header() << "\n";
    auto do_test = [&] (int n_read, int n_skip) {
        auto r = scan_rows_with_stride(cf, cfg.n_rows, n_read, n_skip);
        std::cout << sprint("%-7d %-7d ", n_read, n_skip) << r.table_row() << "\n";
        check_fragment_count(r, count_for_skip_pattern(cfg.n_rows, n_read, n_skip));
    };
    auto test = [&] (int n_read, int n_skip) {
        on_test_case();
        do_test(n_read, n_skip);
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

    if (cache_enabled) {
        std::cout << "Testing cache scan of large partition with varying row continuity.\n";
        for (auto n_read : {1, 64}) {
            for (auto n_skip : {1, 64}) {
                on_test_case();
                do_test(n_read, n_skip); // populate with gaps
                do_test(1, 0);
            }
        }
    }
}

void test_large_partition_slicing(column_family& cf) {
    std::cout << sprint("%-7s %-7s ", "offset", "read") << test_result::table_header() << "\n";
    auto test = [&] (int offset, int read) {
        on_test_case();
        auto r = slice_rows(cf, offset, read);
        std::cout << sprint("%-7d %-7d ", offset, read) << r.table_row() << "\n";
        check_fragment_count(r, std::min(cfg.n_rows - offset, read));
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

void test_large_partition_slicing_single_partition_reader(column_family& cf) {
    std::cout << sprint("%-7s %-7s ", "offset", "read") << test_result::table_header()
              << "\n";
    auto test = [&](int offset, int read) {
        on_test_case();
        auto r = slice_rows_single_key(cf, offset, read);
        std::cout << sprint("%-7d %-7d ", offset, read) << r.table_row() << "\n";
        check_fragment_count(r, std::min(cfg.n_rows - offset, read));
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

void test_large_partition_select_few_rows(column_family& cf) {
    std::cout << sprint("%-7s %-7s ", "stride", "rows") << test_result::table_header()
              << "\n";
    auto test = [&](int stride, int read) {
        on_test_case();
        auto r = select_spread_rows(cf, stride, read);
        std::cout << sprint("%-7d %-7d ", stride, read) << r.table_row() << "\n";
        check_fragment_count(r, read);
    };

    test(cfg.n_rows / 1, 1);
    test(cfg.n_rows / 2, 2);
    test(cfg.n_rows / 4, 4);
    test(cfg.n_rows / 8, 8);
    test(cfg.n_rows / 16, 16);
    test(2, cfg.n_rows / 2);
}

void test_large_partition_forwarding(column_family& cf) {
    std::cout << sprint("%-7s ", "pk-scan") << test_result::table_header() << "\n";

    on_test_case();
    auto r = test_forwarding_with_restriction(cf, cfg, false);
    check_fragment_count(r, 2);
    std::cout << sprint("%-7s ", "yes") << r.table_row() << "\n";

    on_test_case();
    r = test_forwarding_with_restriction(cf, cfg, true);
    check_fragment_count(r, 2);
    std::cout << sprint("%-7s ", "no")  << r.table_row() << "\n";
}

void test_small_partition_skips(column_family& cf2) {
    std::cout << sprint("%-2s %-7s %-7s ", "", "read", "skip") << test_result::table_header() << "\n";

    auto do_test = [&] (int n_read, int n_skip) {
        auto r = scan_with_stride_partitions(cf2, cfg.n_rows, n_read, n_skip);
        std::cout << sprint("%-2s %-7d %-7d ", new_test_case ? "->" : "", n_read, n_skip) << r.table_row() << "\n";
        new_test_case = false;
        check_fragment_count(r, count_for_skip_pattern(cfg.n_rows, n_read, n_skip));
        return r;
    };
    auto test = [&] (int n_read, int n_skip) {
        on_test_case();
        return do_test(n_read, n_skip);
    };

    auto r = test(1, 0);
    check_no_index_reads(r);

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

    if (cache_enabled) {
        std::cout << "Testing cache scan with small partitions with varying continuity.\n";
        for (auto n_read : {1, 64}) {
            for (auto n_skip : {1, 64}) {
                on_test_case();
                do_test(n_read, n_skip); // populate with gaps
                do_test(1, 0);
            }
        }
    }
}

void test_small_partition_slicing(column_family& cf2) {
    std::cout << sprint("%-7s %-7s ", "offset", "read") << test_result::table_header() << "\n";
    auto test = [&] (int offset, int read) {
        on_test_case();
        auto r = slice_partitions(cf2, cfg.n_rows, offset, read);
        std::cout << sprint("%-7d %-7d ", offset, read) << r.table_row() << "\n";
        check_fragment_count(r, std::min(cfg.n_rows - offset, read));
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

struct test_group {
    using requires_cache = seastar::bool_class<class requires_cache_tag>;
    enum type {
        large_partition,
        small_partition,
    };

    std::string name;
    std::string message;
    requires_cache needs_cache;
    type partition_type;
    void (*test_fn)(column_family& cf);
};

static std::initializer_list<test_group> test_groups = {
    {
        "large-partition-single-key-slice",
        "Testing effectiveness of caching of large partition, single-key slicing reads",
        test_group::requires_cache::yes,
        test_group::type::large_partition,
        test_large_partition_single_key_slice,
    },
    {
        "large-partition-skips",
        "Testing scanning large partition with skips.\n" \
        "Reads whole range interleaving reads with skips according to read-skip pattern",
        test_group::requires_cache::no,
        test_group::type::large_partition,
        test_large_partition_skips,
    },
    {
        "large-partition-slicing",
        "Testing slicing of large partition",
        test_group::requires_cache::no,
        test_group::type::large_partition,
        test_large_partition_slicing,
    },
    {
        "large-partition-slicing-single-key-reader",
        "Testing slicing of large partition, single-partition reader",
        test_group::requires_cache::no,
        test_group::type::large_partition,
        test_large_partition_slicing_single_partition_reader,
    },
    {
        "large-partition-select-few-rows",
        "Testing selecting few rows from a large partition",
        test_group::requires_cache::no,
        test_group::type::large_partition,
        test_large_partition_select_few_rows,
    },
    {
        "large-partition-forwarding",
        "Testing forwarding with clustering restriction in a large partition",
        test_group::requires_cache::no,
        test_group::type::large_partition,
        test_large_partition_forwarding,
    },
    {
        "small-partition-skips",
        "Testing scanning small partitions with skips.\n" \
        "Reads whole range interleaving reads with skips according to read-skip pattern",
        test_group::requires_cache::no,
        test_group::type::small_partition,
        test_small_partition_skips,
    },
    {
        "small-partition-slicing",
        "Testing slicing small partitions",
        test_group::requires_cache::no,
        test_group::type::small_partition,
        test_small_partition_slicing,
    },
};

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app.add_options()
        ("run-tests", bpo::value<std::vector<std::string>>()->default_value(
                boost::copy_range<std::vector<std::string>>(
                    test_groups | boost::adaptors::transformed([] (auto&& tc) { return tc.name; }))
                ),
            "Test groups to run")
        ("list-tests", "Show available test groups")
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

    return app.run(argc, argv, [] {
        db::config db_cfg;

        if (app.configuration().count("list-tests")) {
            std::cout << "Test groups:\n";
            for (auto&& tc : test_groups) {
                std::cout << "\tname: " << tc.name << "\n"
                          << (tc.needs_cache ? "\trequires: --enable-cache\n" : "")
                          << (tc.partition_type == test_group::type::large_partition
                              ? "\tlarge partition test\n" : "\tsmall partition test\n")
                          << "\tdescription:\n\t\t" << boost::replace_all_copy(tc.message, "\n", "\n\t\t") << "\n\n";
            }
            return make_ready_future<int>(0);
        }

        sstring datadir = "./perf_large_partition_data";
        ::mkdir(datadir.c_str(), S_IRWXU);

        db_cfg.enable_cache(app.configuration().count("enable-cache"));
        db_cfg.enable_commitlog(false);
        db_cfg.data_file_directories({datadir}, db::config::config_source::CommandLine);

        if (!app.configuration().count("verbose")) {
            logging::logger_registry().set_all_loggers_level(seastar::log_level::warn);
        }
        if (app.configuration().count("trace")) {
            logging::logger_registry().set_logger_level("sstable", seastar::log_level::trace);
        }

        std::cout << "Data directory: " << db_cfg.data_file_directories() << "\n";

        return do_with_cql_env([] (cql_test_env& env) {
            return seastar::async([&env] {
                sstring name = app.configuration()["name"].as<std::string>();

                if (app.configuration().count("populate")) {
                    int n_rows = app.configuration()["rows"].as<int>();
                    int value_size = app.configuration()["value-size"].as<int>();
                    table_config cfg{name, n_rows, value_size};
                    populate(env, cfg);
                } else {
                    if (smp::count != 1) {
                        throw std::runtime_error("The test must be run with one shard");
                    }

                    database& db = env.local_db();
                    column_family& cf = db.find_column_family("ks", "test");

                    cfg = read_config(env, name);
                    cache_enabled = app.configuration().count("enable-cache");
                    new_test_case = false;

                    std::cout << "Config: rows: " << cfg.n_rows << ", value size: " << cfg.value_size << "\n";

                    sleep(1s).get(); // wait for system table flushes to quiesce

                    engine().at_exit([&] {
                        cancel = true;
                        return make_ready_future();
                    });

                    auto requested_test_groups = boost::copy_range<std::unordered_set<std::string>>(
                            app.configuration()["run-tests"].as<std::vector<std::string>>()
                    );
                    auto enabled_test_groups = test_groups | boost::adaptors::filtered([&] (auto&& tc) {
                        return requested_test_groups.count(tc.name) != 0;
                    });

                    auto run_tests = [&] (column_family& cf, test_group::type type) {
                        cf.run_with_compaction_disabled([&] {
                            return seastar::async([&] {
                                live_range = int_range({0}, {cfg.n_rows - 1});

                                boost::for_each(
                                    enabled_test_groups
                                    | boost::adaptors::filtered([type] (auto&& tc) { return tc.partition_type == type; }),
                                    [&cf] (auto&& tc) {
                                        if (tc.needs_cache && !cache_enabled) {
                                            std::cout << "\nskipping: " << tc.name << "\n";
                                        } else {
                                            std::cout << "\nrunning: " << tc.name << "\n";
                                            on_test_group();
                                            std::cout << tc.message << ":\n";
                                            tc.test_fn(cf);
                                        }
                                    }
                                );
                            });
                        }).get();
                    };

                    run_tests(cf, test_group::type::large_partition);

                    column_family& cf2 = db.find_column_family("ks", "small_part");
                    run_tests(cf2, test_group::type::small_partition);
                }
            });
        }, db_cfg).then([] {
            return errors_found ? -1 : 0;
        });
    });
}
