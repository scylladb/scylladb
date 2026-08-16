/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "db/consistency_level_type.hh"
#include "utils/assert.hh"
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <json/json.h>
#include <fmt/ranges.h>

#include "test/lib/cql_test_env.hh"
#include "test/perf/perf.hh"
#include <seastar/core/app-template.hh>
#include <seastar/testing/test_runner.hh>
#include "test/lib/random_utils.hh"
#include "db/config.hh"

#include "db/config.hh"
#include "schema/schema_builder.hh"
#include "types/map.hh"
#include "service/storage_proxy.hh"
#include "cql3/query_processor.hh"
#include "db/config.hh"
#include "db/extensions.hh"
#include "db/tags/extension.hh"
#include "gms/gossiper.hh"
#include "audit/audit.hh"
#include "audit/audit_rule.hh"
#include "keys/keys.hh"
#include "dht/i_partitioner.hh"
#include "replica/database.hh"
#include "types/types.hh"
#include <seastar/core/sleep.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/loop.hh>

static const sstring table_name = "cf";

// Assignments for the C0..C4 regular columns, shared by every write workload so
// that they all produce identically sized rows.
static constexpr const char* regular_column_assignments =
        "\"C0\" = 0x8f75da6b3dcec90c8a404fb9a5f6b0621e62d39c69ba5758e5f41b78311fbb26cc7a,"
        "\"C1\" = 0xa8761a2127160003033a8f4f3d1069b7833ebe24ef56b3beee728c2b686ca516fa51,"
        "\"C2\" = 0x583449ce81bfebc2e1a695eb59aad5fcc74d6d7311fc6197b10693e1a161ca2e1c64,"
        "\"C3\" = 0x62bcb1dbc0ff953abc703bcb63ea954f437064c0c45366799658bd6b91d0f92908d7,"
        "\"C4\" = 0x222fcbe31ffa1e689540e1499b87fa3f9c781065fccd10e4772b4c7039c2efd0fb27";

static bytes make_key(uint64_t sequence) {
    bytes b(bytes::initialized_later(), sizeof(sequence));
    auto i = b.begin();
    write<uint64_t>(i, sequence);
    return b;
};

static sstring make_collection_literal(unsigned n) {
    if (n == 0) {
        return "{}";
    }
    // Fixed blob value for all cells, similar to C0..C4 column values
    static constexpr std::string_view cell_value =
        "0x8f75da6b3dcec90c8a404fb9a5f6b0621e62d39c69ba5758e5f41b78311fbb26cc7a";
    sstring result = "{";
    for (unsigned i = 0; i < n; ++i) {
        if (i > 0) {
            result += ", ";
        }
        // Key is the 8-byte big-endian encoding of the cell index as a blob
        result += fmt::format("0x{:016x}: {}", i, cell_value);
    }
    result += "}";
    return result;
}

static void execute_update_for_key(cql_test_env& env, const bytes& key, unsigned collection) {
    sstring col_suffix;
    if (collection > 0) {
        col_suffix = fmt::format(", \"CC\" = {}", make_collection_literal(collection));
    }
    // Strongly consistent writes need QUORUM/LOCAL_QUORUM.
    // For eventual consistency it does not matter because there is only one node involved.
    auto qo = std::make_unique<cql3::query_options>(db::consistency_level::QUORUM, std::vector<cql3::raw_value>{}, cql3::query_options::specific_options::DEFAULT);
    env.execute_cql(fmt::format("UPDATE cf SET {}{} "
        "WHERE \"KEY\"= 0x{};", regular_column_assignments, col_suffix, to_hex(key)), std::move(qo)).get();
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
    // Shape of the read issued by the clustering-key workload.
    //  partition: single-partition read spanning every clustering row
    //  slice:     read restricted by clustering bounds
    enum class clustering_query_kind { partition, slice };
    run_mode mode;
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
    unsigned collection = 0;
    db::consistency_level consistency_level;
    bool shard_aware;
    // 0 keeps the historical schema, which has no clustering key at all.
    unsigned clustering_columns = 0;
    unsigned rows_per_partition = 1;
    unsigned clustering_fanout = 64;
    clustering_query_kind clustering_query = clustering_query_kind::partition;
};

// Partition sequence numbers grouped by the shard that services reads for them,
// indexed by shard id. Lets a worker running on shard S pick a key owned by S,
// avoiding cross-shard hops. Each shard is later handed its own slice
// (a sharded<std::vector<uint64_t>>) so the hot path touches only NUMA-local
// memory.
using shard_sequences = std::vector<std::vector<uint64_t>>;

std::ostream& operator<<(std::ostream& os, const test_config::run_mode& m) {
    switch (m) {
        case test_config::run_mode::write: return os << "write";
        case test_config::run_mode::read: return os << "read";
        case test_config::run_mode::del: return os << "delete";
    }
    abort();
}

static const char* clustering_query_kind_name(test_config::clustering_query_kind q) {
    switch (q) {
        case test_config::clustering_query_kind::partition: return "partition";
        case test_config::clustering_query_kind::slice: return "slice";
    }
    abort();
}

std::ostream& operator<<(std::ostream& os, const test_config& cfg) {
    os << "{partitions=" << cfg.partitions
       << ", concurrency=" << cfg.concurrency
       << ", mode=" << cfg.mode
       << ", query_single_key=" << (cfg.query_single_key ? "yes" : "no")
       << ", counters=" << (cfg.counters ? "yes" : "no")
       << ", collection=" << cfg.collection
       << ", shard_aware=" << (cfg.shard_aware ? "yes" : "no");
    if (cfg.clustering_columns > 0) {
        os << ", clustering_columns=" << cfg.clustering_columns
           << ", rows_per_partition=" << cfg.rows_per_partition
           << ", clustering_fanout=" << cfg.clustering_fanout
           << ", clustering_query=" << clustering_query_kind_name(cfg.clustering_query);
    }
    return os << "}";
}

// ---------------------------------------------------------------------------
// Clustering key workload.
//
// The historical schema has no clustering key, so it cannot exercise the
// clustering key comparator at all. This workload adds an optional clustering
// key of a configurable number of components, alternating a variable-length
// utf8 component with a fixed-width int32 one: the length headers of the
// variable-length components are what makes walking a clustering key
// expensive.
//
// Component i of row r is r / fanout^(N-1-i), so the leading components are
// shared by `fanout` consecutive rows and the last component is r itself. Keys
// are therefore unique, sorted by row number, and comparisons between nearby
// rows walk a long *equal* prefix - which is the path being measured.
// ---------------------------------------------------------------------------

static sstring ck_column_name(unsigned component) {
    return fmt::format("CK{}", component);
}

static data_type ck_column_type(unsigned component) {
    return component % 2 == 0 ? utf8_type : int32_type;
}

static std::vector<bytes> make_ck_components(const test_config& cfg, uint64_t row) {
    std::vector<bytes> components;
    components.reserve(cfg.clustering_columns);
    for (unsigned i = 0; i < cfg.clustering_columns; ++i) {
        uint64_t div = 1;
        for (unsigned j = i + 1; j < cfg.clustering_columns; ++j) {
            div *= cfg.clustering_fanout;
        }
        const uint64_t value = row / div;
        if (i % 2 == 0) {
            // Zero padded so that byte-wise utf8 ordering matches numeric ordering.
            components.push_back(serialized(sstring(fmt::format("ck{}_{:010d}", i, value))));
        } else {
            components.push_back(serialized(int32_t(value)));
        }
    }
    return components;
}

// Bind values for the clustering key of every row, precomputed so that the
// measured loop only copies bytes instead of formatting strings.
using ck_bind_values = std::vector<std::vector<cql3::raw_value>>;

static ck_bind_values precompute_ck_bind_values(const test_config& cfg) {
    ck_bind_values all;
    all.reserve(cfg.rows_per_partition);
    for (uint64_t row = 0; row < cfg.rows_per_partition; ++row) {
        std::vector<cql3::raw_value> values;
        for (auto&& c : make_ck_components(cfg, row)) {
            values.push_back(cql3::raw_value::make_value(std::move(c)));
        }
        all.push_back(std::move(values));
    }
    return all;
}

static sstring clustering_key_equality_restrictions(const test_config& cfg) {
    sstring restrictions;
    for (unsigned i = 0; i < cfg.clustering_columns; ++i) {
        restrictions += format(" AND \"{}\" = ?", ck_column_name(i));
    }
    return restrictions;
}

// UPDATE touching a single row identified by its full primary key.
static sstring clustering_update_query(const test_config& cfg) {
    sstring usings;
    if (!cfg.timeout.empty()) {
        usings += "USING TIMEOUT " + cfg.timeout + " ";
    }
    sstring col_suffix;
    if (cfg.collection > 0) {
        col_suffix = fmt::format(", \"CC\" = {}", make_collection_literal(cfg.collection));
    }
    return format("UPDATE cf {}SET {}{} WHERE \"KEY\" = ?{}",
            usings, regular_column_assignments, col_suffix, clustering_key_equality_restrictions(cfg));
}

static std::vector<cql3::raw_value> make_row_bind_values(const bytes& key, const std::vector<cql3::raw_value>& ck) {
    std::vector<cql3::raw_value> values;
    values.reserve(ck.size() + 1);
    values.push_back(cql3::raw_value::make_value(key));
    values.insert(values.end(), ck.begin(), ck.end());
    return values;
}

// Populates cfg.partitions partitions with cfg.rows_per_partition clustering
// rows each. Rows within a partition are inserted concurrently; the content of
// a row depends only on its row number, so the resulting data set is
// deterministic.
static void create_clustering_partitions(cql_test_env& env, test_config& cfg) {
    std::cout << "Creating " << cfg.partitions << " partitions of "
              << cfg.rows_per_partition << " rows..." << std::endl;
    auto id = env.prepare(clustering_update_query(cfg)).get();
    const auto ck_values = precompute_ck_bind_values(cfg);
    constexpr unsigned concurrency = 128;
    unsigned next_flush = (cfg.memtable_partitions > 0 ? cfg.memtable_partitions : cfg.partitions);
    for (unsigned sequence = 0; sequence < cfg.partitions; ++sequence) {
        const auto key = make_key(sequence);
        for (unsigned row = 0; row < cfg.rows_per_partition; row += concurrency) {
            const unsigned n = std::min<unsigned>(concurrency, cfg.rows_per_partition - row);
            std::vector<future<>> pending;
            pending.reserve(n);
            for (unsigned i = 0; i < n; ++i) {
                pending.push_back(env.execute_prepared(id, make_row_bind_values(key, ck_values[row + i]),
                        db::consistency_level::QUORUM).discard_result());
            }
            when_all_succeed(pending.begin(), pending.end()).get();
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

static void create_partitions(cql_test_env& env, test_config& cfg) {
    if (cfg.clustering_columns > 0) {
        return create_clustering_partitions(env, cfg);
    }
    std::cout << "Creating " << cfg.partitions << " partitions..." << std::endl;
    unsigned next_flush = (cfg.memtable_partitions > 0 ? cfg.memtable_partitions : cfg.partitions);
    for (unsigned sequence = 0; sequence < cfg.partitions; ++sequence) {
        if (cfg.counters) {
            execute_counter_update_for_key(env, make_key(sequence));
        } else {
            execute_update_for_key(env, make_key(sequence), cfg.collection);
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

// Groups partition sequence numbers by their read-owning shard. The sharder is
// consulted on the local shard but reports the servicing shard for the whole
// node, so the complete table can be built in one place and then distributed.
// Returns a table with empty per-shard entries when the sequences won't be used
// (shard-awareness disabled, or a fixed single key is queried).
static shard_sequences build_shard_sequences(cql_test_env& env, test_config& cfg) {
    shard_sequences result(this_smp_shard_count());
    if (!cfg.shard_aware || cfg.query_single_key) {
        return result;
    }
    auto& cf = env.local_db().find_column_family("ks", table_name);
    auto schema = cf.schema();
    auto erm = cf.get_effective_replication_map();
    for (uint64_t seq = 0; seq < cfg.partitions; ++seq) {
        auto pk = partition_key::from_single_value(*schema, make_key(seq));
        auto shard = erm->shard_for_reads(*schema, dht::get_token(*schema, pk));
        result[shard].push_back(seq);
    }
    return result;
}

// Picks the key for the next operation on the current shard, drawing from the
// shard-local sequence numbers when shard-awareness is enabled so the query is
// serviced locally. Returns nullopt when this shard owns no partitions in
// shard-aware mode, signalling the worker to stay idle rather than issue a
// cross-shard query.
static std::optional<bytes> next_key(test_config& cfg, const std::vector<uint64_t>& shard_seqs) {
    if (cfg.query_single_key) {
        return make_key(0);
    }
    if (cfg.shard_aware) {
        if (shard_seqs.empty()) {
            return std::nullopt;
        }
        return make_key(shard_seqs[tests::random::get_int<uint64_t>(shard_seqs.size() - 1)]);
    }
    return make_key(tests::random::get_int<uint64_t>(cfg.partitions - 1));
}

static std::vector<perf_result> test_read(cql_test_env& env, test_config& cfg, sharded<std::vector<uint64_t>>& shard_seqs) {
    create_partitions(env, cfg);
    sstring query = "select \"C0\", \"C1\", \"C2\", \"C3\", \"C4\"";
    if (cfg.collection > 0) {
        query += ", \"CC\"";
    }
    query += " from cf where \"KEY\" = ?";
    if (cfg.bypass_cache) {
        query += " bypass cache";
    }
    if (!cfg.timeout.empty()) {
        query += " using timeout " + cfg.timeout;
    }
    auto id = env.prepare(query).get();
    return time_parallel([&env, &cfg, &shard_seqs, id] {
            auto key = next_key(cfg, shard_seqs.local());
            if (!key) {
                // This shard owns no partitions in shard-aware mode; idle for
                // one measurement window instead of issuing a cross-shard query.
                return seastar::sleep(std::chrono::seconds(1));
            }
            return env.execute_prepared(id, {{cql3::raw_value::make_value(std::move(*key))}}, cfg.consistency_level).discard_result();
        }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard, cfg.stop_on_error);
}

static std::vector<perf_result> test_write(cql_test_env& env, test_config& cfg, sharded<std::vector<uint64_t>>& shard_seqs) {
    sstring usings;
    if (!cfg.timeout.empty()) {
        usings += "USING TIMEOUT " + cfg.timeout;
    }
    sstring col_suffix;
    if (cfg.collection > 0) {
        col_suffix = fmt::format(", \"CC\" = {}", make_collection_literal(cfg.collection));
    }
    sstring query = format("UPDATE cf {}SET {}{} "
            "WHERE \"KEY\" = ?", usings, regular_column_assignments, col_suffix);
    auto id = env.prepare(query).get();
    return time_parallel([&env, &cfg, &shard_seqs, id] {
            auto key = next_key(cfg, shard_seqs.local());
            if (!key) {
                // This shard owns no partitions in shard-aware mode; idle for
                // one measurement window instead of issuing a cross-shard query.
                return seastar::sleep(std::chrono::seconds(1));
            }
            return env.execute_prepared(id, {{cql3::raw_value::make_value(std::move(*key))}}, cfg.consistency_level).discard_result();
        }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard, cfg.stop_on_error);
}

static std::vector<perf_result> test_delete(cql_test_env& env, test_config& cfg, sharded<std::vector<uint64_t>>& shard_seqs) {
    create_partitions(env, cfg);
    sstring usings;
    if (!cfg.timeout.empty()) {
        usings += "USING TIMEOUT " + cfg.timeout;
    }
    sstring col_suffix;
    if (cfg.collection > 0) {
        col_suffix = ", \"CC\"";
    }
    sstring query = format("DELETE \"C0\", \"C1\", \"C2\", \"C3\", \"C4\"{} FROM cf {}WHERE \"KEY\" = ?", col_suffix, usings);
    auto id = env.prepare(query).get();
    return time_parallel([&env, &cfg, &shard_seqs, id] {
            auto key = next_key(cfg, shard_seqs.local());
            if (!key) {
                // This shard owns no partitions in shard-aware mode; idle for
                // one measurement window instead of issuing a cross-shard query.
                return seastar::sleep(std::chrono::seconds(1));
            }
            return env.execute_prepared(id, {{cql3::raw_value::make_value(std::move(*key))}}, cfg.consistency_level).discard_result();
        }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard, cfg.stop_on_error);
}

static std::vector<perf_result> test_counter_update(cql_test_env& env, test_config& cfg, sharded<std::vector<uint64_t>>& shard_seqs) {
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
    return time_parallel([&env, &cfg, &shard_seqs, id] {
            auto key = next_key(cfg, shard_seqs.local());
            if (!key) {
                // This shard owns no partitions in shard-aware mode; idle for
                // one measurement window instead of issuing a cross-shard query.
                return seastar::sleep(std::chrono::seconds(1));
            }
            return env.execute_prepared(id, {{cql3::raw_value::make_value(std::move(*key))}}, cfg.consistency_level).discard_result();
        }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard, cfg.stop_on_error);
}

// Read restricted by clustering bounds: the first N-1 components are pinned to
// one prefix and the last one is given a range covering a whole fanout group,
// so every comparison against a row of that group compares equal up to the
// last component.
static std::vector<perf_result> test_clustering_slice_read(cql_test_env& env, test_config& cfg, sharded<std::vector<uint64_t>>& shard_seqs) {
    create_partitions(env, cfg);
    const unsigned last = cfg.clustering_columns - 1;
    sstring query = "select \"C0\", \"C1\", \"C2\", \"C3\", \"C4\"";
    if (cfg.collection > 0) {
        query += ", \"CC\"";
    }
    query += " from cf where \"KEY\" = ?";
    for (unsigned i = 0; i < last; ++i) {
        query += format(" and \"{}\" = ?", ck_column_name(i));
    }
    query += format(" and \"{0}\" >= ? and \"{0}\" <= ?", ck_column_name(last));
    if (cfg.bypass_cache) {
        query += " bypass cache";
    }
    if (!cfg.timeout.empty()) {
        query += " using timeout " + cfg.timeout;
    }
    auto id = env.prepare(query).get();

    // Bind values of every slice, without the partition key, precomputed once.
    const auto ck_values = precompute_ck_bind_values(cfg);
    std::vector<std::vector<cql3::raw_value>> slices;
    for (unsigned first_row = 0; first_row < cfg.rows_per_partition; first_row += cfg.clustering_fanout) {
        const unsigned last_row = std::min<unsigned>(first_row + cfg.clustering_fanout, cfg.rows_per_partition) - 1;
        auto values = ck_values[first_row];
        values.push_back(ck_values[last_row][last]);
        slices.push_back(std::move(values));
    }
    std::cout << "Slice reads over " << slices.size() << " clustering ranges of up to "
              << cfg.clustering_fanout << " rows" << std::endl;

    return time_parallel([&env, &cfg, &shard_seqs, &slices, id] {
            auto key = next_key(cfg, shard_seqs.local());
            if (!key) {
                return seastar::sleep(std::chrono::seconds(1));
            }
            const auto& slice = slices[tests::random::get_int<uint64_t>(slices.size() - 1)];
            auto values = make_row_bind_values(*key, slice);
            return env.execute_prepared(id, std::move(values), cfg.consistency_level).discard_result();
        }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard, cfg.stop_on_error);
}

// Write/delete of a single clustering row picked at random.
static std::vector<perf_result> test_clustering_row_op(cql_test_env& env, test_config& cfg, sharded<std::vector<uint64_t>>& shard_seqs, sstring query) {
    auto id = env.prepare(std::move(query)).get();
    const auto ck_values = precompute_ck_bind_values(cfg);
    return time_parallel([&env, &cfg, &shard_seqs, &ck_values, id] {
            auto key = next_key(cfg, shard_seqs.local());
            if (!key) {
                return seastar::sleep(std::chrono::seconds(1));
            }
            const auto& ck = ck_values[tests::random::get_int<uint64_t>(ck_values.size() - 1)];
            auto values = make_row_bind_values(*key, ck);
            return env.execute_prepared(id, std::move(values), cfg.consistency_level).discard_result();
        }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard, cfg.stop_on_error);
}

static std::vector<perf_result> test_clustering_write(cql_test_env& env, test_config& cfg, sharded<std::vector<uint64_t>>& shard_seqs) {
    return test_clustering_row_op(env, cfg, shard_seqs, clustering_update_query(cfg));
}

static std::vector<perf_result> test_clustering_delete(cql_test_env& env, test_config& cfg, sharded<std::vector<uint64_t>>& shard_seqs) {
    create_partitions(env, cfg);
    sstring usings;
    if (!cfg.timeout.empty()) {
        usings += "USING TIMEOUT " + cfg.timeout;
    }
    sstring col_suffix;
    if (cfg.collection > 0) {
        col_suffix = ", \"CC\"";
    }
    return test_clustering_row_op(env, cfg, shard_seqs,
            format("DELETE \"C0\", \"C1\", \"C2\", \"C3\", \"C4\"{} FROM cf {}WHERE \"KEY\" = ?{}",
                    col_suffix, usings, clustering_key_equality_restrictions(cfg)));
}

static schema_ptr make_counter_schema(std::string_view ks_name) {
    return schema_builder(this_smp_shard_count(), ks_name, "cf")
            .with_column("KEY", bytes_type, column_kind::partition_key)
            .with_column("C0", counter_type)
            .with_column("C1", counter_type)
            .with_column("C2", counter_type)
            .with_column("C3", counter_type)
            .with_column("C4", counter_type)
            .build();
}

static std::vector<perf_result> do_cql_test(cql_test_env& env, test_config& cfg) {
    std::cout << "Running test with config: " << cfg << std::endl;
    env.create_table([&cfg] (auto ks_name) {
        if (cfg.counters) {
            return *make_counter_schema(ks_name);
        }
        auto sb = schema_builder(this_smp_shard_count(), ks_name, "cf")
                .with_column("KEY", bytes_type, column_kind::partition_key);
        for (unsigned i = 0; i < cfg.clustering_columns; ++i) {
            sb.with_column(to_bytes(ck_column_name(i)), ck_column_type(i), column_kind::clustering_key);
        }
        sb.with_column("C0", bytes_type)
                .with_column("C1", bytes_type)
                .with_column("C2", bytes_type)
                .with_column("C3", bytes_type)
                .with_column("C4", bytes_type);
        if (cfg.collection > 0) {
            sb.with_column("CC", map_type_impl::get_instance(bytes_type, bytes_type, true));
        }
        return *sb.build();
    }).get();

    if (cfg.clustering_columns > 0) {
        auto s = env.local_db().find_column_family("ks", table_name).schema();
        SCYLLA_ASSERT(s->clustering_key_size() == cfg.clustering_columns);
        auto first = clustering_key::from_exploded(*s, make_ck_components(cfg, 0));
        auto last = clustering_key::from_exploded(*s, make_ck_components(cfg, cfg.rows_per_partition - 1));
        fmt::print("Clustering key has {} component(s); first row {}, last row {}\n",
                s->clustering_key_size(), first.with_schema(*s), last.with_schema(*s));
    }

    std::cout << "Disabling auto compaction" << std::endl;
    env.db().invoke_on_all([] (auto& db) {
        auto& cf = db.find_column_family("ks", "cf");
        return cf.disable_auto_compaction();
    }).get();

    // Build the shard->sequences table once, then hand each shard its own slice
    // so the hot path reads only NUMA-local memory.
    auto table = build_shard_sequences(env, cfg);
    sharded<std::vector<uint64_t>> shard_seqs;
    shard_seqs.start().get();
    auto stop_shard_seqs = defer([&shard_seqs] noexcept {
        shard_seqs.stop().get();
    });
    shard_seqs.invoke_on_all([&table] (std::vector<uint64_t>& s) {
        s = table[this_shard_id()];
    }).get();

    switch (cfg.mode) {
    case test_config::run_mode::read:
        if (cfg.clustering_columns > 0 && cfg.clustering_query == test_config::clustering_query_kind::slice) {
            return test_clustering_slice_read(env, cfg, shard_seqs);
        }
        // The partition-wide read is the same query as the keyless one, it just
        // spans cfg.rows_per_partition clustering rows.
        return test_read(env, cfg, shard_seqs);
    case test_config::run_mode::write:
        if (cfg.counters) {
            return test_counter_update(env, cfg, shard_seqs);
        } else if (cfg.clustering_columns > 0) {
            return test_clustering_write(env, cfg, shard_seqs);
        } else {
            return test_write(env, cfg, shard_seqs);
        }
    case test_config::run_mode::del:
        if (cfg.clustering_columns > 0) {
            return test_clustering_delete(env, cfg, shard_seqs);
        }
        return test_delete(env, cfg, shard_seqs);
    };
    abort();
}

void write_json_result(std::string result_file, const test_config& cfg, const aggregated_perf_results& agg) {
    Json::Value params;
    params["concurrency"] = cfg.concurrency;
    params["partitions"] = cfg.partitions;
    params["cpus"] = this_smp_shard_count();
    params["duration"] = cfg.duration_in_seconds;
    params["concurrency,partitions,cpus,duration"] = fmt::format("{},{},{},{}", cfg.concurrency, cfg.partitions, this_smp_shard_count(), cfg.duration_in_seconds);
    if (cfg.initial_tablets) {
        params["initial_tablets"] = cfg.initial_tablets.value();
    }
    if (cfg.collection > 0) {
        params["collection"] = cfg.collection;
    }
    if (cfg.clustering_columns > 0) {
        params["clustering_columns"] = cfg.clustering_columns;
        params["rows_per_partition"] = cfg.rows_per_partition;
        params["clustering_fanout"] = cfg.clustering_fanout;
    }

    std::string test_type;
    switch (cfg.mode) {
    case test_config::run_mode::read: test_type = "read"; break;
    case test_config::run_mode::write: test_type = "write"; break;
    case test_config::run_mode::del: test_type = "delete"; break;
    }
    if (cfg.counters) {
        test_type += "_counters";
    }
    if (cfg.clustering_columns > 0) {
        test_type += fmt::format("_clustering_{}", clustering_query_kind_name(cfg.clustering_query));
    }

    perf::write_json_result(result_file, agg, params, test_type);
}

/// If app configuration contains the named parameter, store its value into \p store.
static void set_from_cli(const char* name, app_template& app, utils::config_file::named_value<sstring>& store) {
    const auto& cfg = app.configuration();
    auto found = cfg.find(name);
    if (found != cfg.end()) {
        store(found->second.as<std::string>());
    }
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
        ("collection", bpo::value<unsigned>()->default_value(0), "add map<text,text> collection column with N cells per row (excludes --counters)")
        ("clustering-columns", bpo::value<unsigned>()->default_value(0), "number of clustering key components (0 keeps the clustering-key-less schema); components alternate utf8 and int32 (excludes --counters)")
        ("rows-per-partition", bpo::value<unsigned>()->default_value(1024), "clustering rows per partition, with --clustering-columns")
        ("clustering-fanout", bpo::value<unsigned>()->default_value(64), "number of consecutive rows sharing the same clustering key prefix; also the width of a slice read")
        ("clustering-query", bpo::value<std::string>()->default_value("partition"), "clustering read shape: 'partition' (whole partition) or 'slice' (clustering bounds)")
        ("tablets", "use tablets")
        ("strongly-consistent-tables", "use strongly consistent tables")
        ("consistency-level", bpo::value<std::string>()->default_value("QUORUM"), "consistency level used for read and write operations")
        ("initial-tablets", bpo::value<unsigned>()->default_value(128), "initial number of tablets")
        ("sstable-summary-ratio", bpo::value<double>(), "Generate summary entry, so that summary file size / data file size ~= this ratio")
        ("sstable-format", bpo::value<std::string>(), "SSTable format name to use")
        ("flush", "flush memtables before test")
        ("memtable-partitions", bpo::value<unsigned>(), "apply this number of partitions to memtable, then flush")
        ("json-result", bpo::value<std::string>(), "name of the json result file")
        ("enable-cache", bpo::value<bool>()->default_value(true), "enable row cache")
        ("enable-index-cache", bpo::value<bool>()->default_value(true), "enable partition index cache")
        ("stop-on-error", bpo::value<bool>()->default_value(true), "stop after encountering the first error")
        ("timeout", bpo::value<std::string>()->default_value(""), "use timeout")
        ("bypass-cache", "use bypass cache when querying")
        ("shard-aware", bpo::value<bool>()->default_value(true), "generate keys owned by the shard issuing the query (use --shard-aware 0 to disable)")
        ("audit", bpo::value<std::string>(), "value for audit config entry")
        ("audit-keyspaces", bpo::value<std::string>(), "value for audit_keyspaces config entry")
        ("audit-tables", bpo::value<std::string>(), "value for audit_tables config entry")
        ("audit-categories", bpo::value<std::string>(), "value for audit_categories config entry")
        ("audit-unix-socket-path", bpo::value<std::string>(), "value for audit_unix_socket_path config entry")
        ("audit-rules", bpo::value<std::string>(), "JSON value for audit_rules config entry")
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
            const auto enable_index_cache = app.configuration()["enable-index-cache"].as<bool>();
            std::cout << "enable-cache=" << enable_cache << '\n';
            std::cout << "enable-index-cache=" << enable_index_cache << '\n';
            db_cfg->enable_cache(enable_cache);
            db_cfg->cache_index_pages(enable_index_cache);
            if (app.configuration().contains("sstable-summary-ratio")) {
                db_cfg->sstable_summary_ratio(app.configuration()["sstable-summary-ratio"].as<double>());
            }
            std::cout << "sstable-summary-ratio=" << db_cfg->sstable_summary_ratio() << '\n';
            if (app.configuration().contains("sstable-format")) {
                db_cfg->sstable_format(app.configuration()["sstable-format"].as<std::string>());
            }
            std::cout << "sstable-format=" << db_cfg->sstable_format() << '\n';
            cql_test_config cfg(db_cfg);
            if (app.configuration().contains("tablets")) {
                cfg.db_config->tablets_mode_for_new_keyspaces.set(db::tablets_mode_t::mode::enabled);
                cfg.initial_tablets = app.configuration()["initial-tablets"].as<unsigned>();
            }
            if (app.configuration().contains("strongly-consistent-tables")) {
                cfg.db_config->experimental_features({db::experimental_features_t::feature::STRONGLY_CONSISTENT_TABLES},
                                                     db::config::config_source::CommandLine);
                cfg.strongly_consistent_tables = true;
            }
            set_from_cli("audit", app, cfg.db_config->audit);
            set_from_cli("audit-keyspaces", app, cfg.db_config->audit_keyspaces);
            set_from_cli("audit-tables", app, cfg.db_config->audit_tables);
            set_from_cli("audit-categories", app, cfg.db_config->audit_categories);
            set_from_cli("audit-unix-socket-path", app, cfg.db_config->audit_unix_socket_path);
            if (app.configuration().contains("audit-rules")) {
                cfg.db_config->audit_rules(audit::parse_audit_rules_from_json(app.configuration()["audit-rules"].as<std::string>()));
            }
          return do_with_cql_env_thread([&app] (auto&& env) {
            auto cfg = test_config();
            cfg.partitions = app.configuration()["partitions"].as<unsigned>();
            cfg.duration_in_seconds = app.configuration()["duration"].as<unsigned>();
            cfg.concurrency = app.configuration()["concurrency"].as<unsigned>();
            cfg.query_single_key = app.configuration().contains("query-single-key");
            cfg.counters = app.configuration().contains("counters");
            cfg.flush_memtables = app.configuration().contains("flush");
            cfg.collection = app.configuration()["collection"].as<unsigned>();
            if (cfg.counters && cfg.collection > 0) {
                throw std::invalid_argument("--collection and --counters are mutually exclusive");
            }
            cfg.clustering_columns = app.configuration()["clustering-columns"].as<unsigned>();
            if (cfg.clustering_columns > 0) {
                if (cfg.counters) {
                    throw std::invalid_argument("--clustering-columns and --counters are mutually exclusive");
                }
                cfg.rows_per_partition = app.configuration()["rows-per-partition"].as<unsigned>();
                cfg.clustering_fanout = app.configuration()["clustering-fanout"].as<unsigned>();
                if (cfg.rows_per_partition == 0 || cfg.clustering_fanout == 0) {
                    throw std::invalid_argument("--rows-per-partition and --clustering-fanout must be positive");
                }
                auto q = app.configuration()["clustering-query"].as<std::string>();
                if (q == "partition") {
                    cfg.clustering_query = test_config::clustering_query_kind::partition;
                } else if (q == "slice") {
                    cfg.clustering_query = test_config::clustering_query_kind::slice;
                } else {
                    throw std::invalid_argument(fmt::format("unknown --clustering-query: {}", q));
                }
                if (app.configuration()["partitions"].defaulted()) {
                    // The default partition count assumes one row per partition;
                    // populating it with rows_per_partition rows each would take
                    // far too long.
                    cfg.partitions = 100;
                }
            }
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
            if (app.configuration().contains("operations-per-shard")) {
                cfg.operations_per_shard = app.configuration()["operations-per-shard"].as<unsigned>();
            }
            if (app.configuration().contains("memtable-partitions")) {
                cfg.memtable_partitions = app.configuration()["memtable-partitions"].as<unsigned>();
            }
            cfg.stop_on_error = app.configuration()["stop-on-error"].as<bool>();
            cfg.timeout = app.configuration()["timeout"].as<std::string>();
            cfg.bypass_cache = app.configuration().contains("bypass-cache");
            cfg.shard_aware = app.configuration()["shard-aware"].as<bool>();
            cfg.consistency_level = db::consistency_level_from_string(app.configuration()["consistency-level"].as<std::string>());
            audit::audit::start_audit(env.local_db().get_config(), env.get_shared_token_metadata(), env.qp(), env.migration_manager()).handle_exception([&] (auto&& e) {
                fmt::print("audit start failed: {}", e);
            }).get();
            audit::audit::start_storage(env.local_db().get_config()).get();
            auto audit_stop = defer([] noexcept {
                audit::audit::stop_audit().get();
            });
            auto audit_storage_stop = defer([] noexcept {
                audit::audit::stop_storage().get();
            });
            audit::audit::audit_instance().invoke_on_all([] (audit::audit& a) {
                a.on_role_created("tester");
            }).get();
            auto results = do_cql_test(env, cfg);
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
