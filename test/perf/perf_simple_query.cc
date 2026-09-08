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
#include <seastar/core/sleep.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/loop.hh>
#include <ranges>

static const sstring table_name = "cf";

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

static constexpr std::string_view cell_values[] = {
    "0x8f75da6b3dcec90c8a404fb9a5f6b0621e62d39c69ba5758e5f41b78311fbb26cc7a",
    "0xa8761a2127160003033a8f4f3d1069b7833ebe24ef56b3beee728c2b686ca516fa51",
    "0x583449ce81bfebc2e1a695eb59aad5fcc74d6d7311fc6197b10693e1a161ca2e1c64",
    "0x62bcb1dbc0ff953abc703bcb63ea954f437064c0c45366799658bd6b91d0f92908d7",
    "0x222fcbe31ffa1e689540e1499b87fa3f9c781065fccd10e4772b4c7039c2efd0fb27",
};

struct test_config {
    enum class run_mode { read, write, del };
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
    // Store the table with logstor rather than with sstables. Implies tablets, which is the only
    // topology logstor is used with.
    bool logstor;
    // Compaction is off during the measurement by default, so that the hot path is measured on its
    // own. A logstor run leaves it on, since compaction is what gives free segments back and
    // without it a write test stalls once the segment pool is full of dead records.
    bool auto_compaction;
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

std::ostream& operator<<(std::ostream& os, const test_config& cfg) {
    return os << "{partitions=" << cfg.partitions
           << ", concurrency=" << cfg.concurrency
           << ", mode=" << cfg.mode
           << ", query_single_key=" << (cfg.query_single_key ? "yes" : "no")
           << ", counters=" << (cfg.counters ? "yes" : "no")
           << ", collection=" << cfg.collection
           << ", shard_aware=" << (cfg.shard_aware ? "yes" : "no")
           << ", logstor=" << (cfg.logstor ? "yes" : "no")
           << ", auto_compaction=" << (cfg.auto_compaction ? "yes" : "no")
           << "}";
}

// The statements the test measures, with the key left to be bound. Logstor holds a whole row per
// partition and takes the timestamp of its record from a row marker or a partition tombstone, so a
// logstor run writes the row with an INSERT and deletes the whole partition, while an sstable run
// keeps writing the cells with the UPDATE and the cell delete it has always measured.
static sstring make_write_query(const test_config& cfg, std::string_view usings = "") {
    if (cfg.logstor) {
        std::string collection_column;
        std::string collection_value;
        if (cfg.collection > 0) {
            collection_column = ", \"CC\"";
            collection_value = fmt::format(", {}", make_collection_literal(cfg.collection));
        }
        return format("INSERT INTO cf (\"KEY\", \"C0\", \"C1\", \"C2\", \"C3\", \"C4\"{}) "
                "VALUES (?, {}, {}, {}, {}, {}{}) {}",
                collection_column, cell_values[0], cell_values[1], cell_values[2], cell_values[3], cell_values[4],
                collection_value, usings);
    }
    std::string collection_assignment;
    if (cfg.collection > 0) {
        collection_assignment = fmt::format(", \"CC\" = {}", make_collection_literal(cfg.collection));
    }
    return format("UPDATE cf {}SET \"C0\" = {}, \"C1\" = {}, \"C2\" = {}, \"C3\" = {}, \"C4\" = {}{} "
            "WHERE \"KEY\" = ?",
            usings, cell_values[0], cell_values[1], cell_values[2], cell_values[3], cell_values[4],
            collection_assignment);
}

static sstring make_counter_update_query(std::string_view usings = "") {
    return format("UPDATE cf {}SET "
            "\"C0\" = \"C0\" + 1, \"C1\" = \"C1\" + 2, \"C2\" = \"C2\" + 3, \"C3\" = \"C3\" + 4, \"C4\" = \"C4\" + 5 "
            "WHERE \"KEY\" = ?", usings);
}

static sstring make_delete_query(const test_config& cfg, std::string_view usings = "") {
    if (cfg.logstor) {
        return format("DELETE FROM cf {}WHERE \"KEY\" = ?", usings);
    }
    std::string collection_column;
    if (cfg.collection > 0) {
        collection_column = ", \"CC\"";
    }
    return format("DELETE \"C0\", \"C1\", \"C2\", \"C3\", \"C4\"{} FROM cf {}WHERE \"KEY\" = ?", collection_column, usings);
}

// A USING clause, with the trailing space the statements above expect between it and what follows.
// The per statement `usings` strings this replaces were built without that space, so --timeout made
// the write `UPDATE cf USING TIMEOUT 5sSET "C0" = ...`.
static std::string make_timeout_using(const test_config& cfg) {
    return cfg.timeout.empty() ? std::string() : fmt::format("USING TIMEOUT {} ", std::string_view(cfg.timeout));
}

// How many partitions the loader writes at a time. A write to a logstor table completes only once
// its record has been flushed to a segment, so loading a dataset of any size one write at a time
// spends the whole populate phase waiting for the disk.
static constexpr unsigned populate_concurrency = 100;

static void create_partitions(cql_test_env& env, test_config& cfg) {
    std::cout << "Creating " << cfg.partitions << " partitions..." << std::endl;
    auto id = env.prepare(cfg.counters ? make_counter_update_query() : make_write_query(cfg)).get();
    // Strongly consistent writes need QUORUM/LOCAL_QUORUM. For eventual consistency it does not
    // matter because there is only one node involved.
    auto write = [&env, id] (unsigned sequence) {
        return env.execute_prepared(id, {{cql3::raw_value::make_value(make_key(sequence))}},
                db::consistency_level::QUORUM).discard_result();
    };
    if (cfg.memtable_partitions > 0) {
        // Flushing every so many partitions needs the writes to happen in a known order.
        unsigned next_flush = cfg.memtable_partitions;
        for (unsigned sequence = 0; sequence < cfg.partitions; ++sequence) {
            write(sequence).get();
            if (sequence + 1 >= next_flush) {
                env.db().invoke_on_all(&replica::database::flush_all_memtables).get();
                next_flush += cfg.memtable_partitions;
            }
        }
    } else {
        auto sequences = std::views::iota(0u, cfg.partitions);
        max_concurrent_for_each(sequences.begin(), sequences.end(), populate_concurrency, std::move(write)).get();
        // The loop this replaces flushed on its last iteration, which is what put the dataset into
        // the sstables a read test means to measure; a read of a memtable is not one of those. A
        // logstor table has no memtable, so this costs a logstor run nothing.
        env.db().invoke_on_all(&replica::database::flush_all_memtables).get();
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
    auto id = env.prepare(make_write_query(cfg, make_timeout_using(cfg))).get();
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
    auto id = env.prepare(make_delete_query(cfg, make_timeout_using(cfg))).get();
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
    auto id = env.prepare(make_counter_update_query(make_timeout_using(cfg))).get();
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
                .with_column("KEY", bytes_type, column_kind::partition_key)
                .with_column("C0", bytes_type)
                .with_column("C1", bytes_type)
                .with_column("C2", bytes_type)
                .with_column("C3", bytes_type)
                .with_column("C4", bytes_type);
        if (cfg.collection > 0) {
            sb.with_column("CC", map_type_impl::get_instance(bytes_type, bytes_type, true));
        }
        if (cfg.logstor) {
            sb.set_logstor();
        }
        return *sb.build();
    }).get();

    if (!cfg.auto_compaction) {
        std::cout << "Disabling auto compaction" << std::endl;
        env.db().invoke_on_all([] (auto& db) {
            auto& cf = db.find_column_family("ks", "cf");
            return cf.disable_auto_compaction();
        }).get();
    }

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
        return test_read(env, cfg, shard_seqs);
    case test_config::run_mode::write:
        if (cfg.counters) {
            return test_counter_update(env, cfg, shard_seqs);
        } else {
            return test_write(env, cfg, shard_seqs);
        }
    case test_config::run_mode::del:
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
    params["logstor"] = cfg.logstor;
    params["auto_compaction"] = cfg.auto_compaction;

    std::string test_type;
    switch (cfg.mode) {
    case test_config::run_mode::read: test_type = "read"; break;
    case test_config::run_mode::write: test_type = "write"; break;
    case test_config::run_mode::del: test_type = "delete"; break;
    }
    if (cfg.counters) {
        test_type += "_counters";
    }
    if (cfg.logstor) {
        test_type += "_logstor";
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
        ("tablets", "use tablets")
        ("logstor", "store the table with the logstor storage engine instead of sstables (implies --tablets)")
        ("logstor-disk-size-in-mb", bpo::value<unsigned>()->default_value(1024), "size of the logstor segment pool")
        ("logstor-file-size-in-mb", bpo::value<unsigned>()->default_value(32), "size of a logstor data file")
        ("logstor-format-on-startup", bpo::value<bool>()->default_value(true), "format the logstor files upfront, so that no write pays for formatting")
        ("logstor-sparse-files", bpo::value<bool>()->default_value(false), "create the logstor data files sparse instead of preallocating them. The test environment does this by default, to spare the disk of a unit test; a measurement wants the files a node has")
        ("auto-compaction", bpo::value<bool>(), "run compaction during the measurement (defaults to on for --logstor, off otherwise)")
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
            const auto logstor = app.configuration().contains("logstor");
            // Logstor is only used with tablets, and its compaction groups are the tablets of the
            // table, so a logstor run is a tablets run.
            if (app.configuration().contains("tablets") || logstor) {
                cfg.db_config->tablets_mode_for_new_keyspaces.set(db::tablets_mode_t::mode::enabled);
                cfg.initial_tablets = app.configuration()["initial-tablets"].as<unsigned>();
            }
            std::vector<enum_option<db::experimental_features_t>> experimental_features;
            if (app.configuration().contains("strongly-consistent-tables")) {
                experimental_features.push_back(db::experimental_features_t::feature::STRONGLY_CONSISTENT_TABLES);
                cfg.strongly_consistent_tables = true;
            }
            if (logstor) {
                experimental_features.push_back(db::experimental_features_t::feature::LOGSTOR);
                cfg.db_config->logstor_disk_size_in_mb(app.configuration()["logstor-disk-size-in-mb"].as<unsigned>());
                cfg.db_config->logstor_file_size_in_mb(app.configuration()["logstor-file-size-in-mb"].as<unsigned>());
                cfg.db_config->logstor_format_on_startup(app.configuration()["logstor-format-on-startup"].as<bool>());
                cfg.db_config->logstor_sparse_files(app.configuration()["logstor-sparse-files"].as<bool>());
                std::cout << "logstor-disk-size-in-mb=" << cfg.db_config->logstor_disk_size_in_mb() << '\n';
            }
            if (!experimental_features.empty()) {
                cfg.db_config->experimental_features(std::move(experimental_features), db::config::config_source::CommandLine);
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
            cfg.logstor = app.configuration().contains("logstor");
            if (cfg.counters && cfg.collection > 0) {
                throw std::invalid_argument("--collection and --counters are mutually exclusive");
            }
            if (cfg.counters && cfg.logstor) {
                throw std::invalid_argument("--counters and --logstor are mutually exclusive: logstor does not store counters");
            }
            if (app.configuration().contains("tablets") || cfg.logstor) {
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
            cfg.auto_compaction = app.configuration().contains("auto-compaction")
                    ? app.configuration()["auto-compaction"].as<bool>()
                    : cfg.logstor;
            cfg.consistency_level = db::consistency_level_from_string(app.configuration()["consistency-level"].as<std::string>());
            audit::audit::start_audit(env.local_db().get_config(), env.shared_token_metadata(), env.qp(), env.migration_manager()).handle_exception([&] (auto&& e) {
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
