/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <cmath>
#include <random>
#include <algorithm>
#include <numeric>
#include <json/json.h>

#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>

#include "test/lib/cql_test_env.hh"
#include "test/perf/perf.hh"
#include "db/config.hh"
#include "replica/database.hh"
#include "sstables/sstables.hh"
#include "schema/schema_builder.hh"
#include "schema/schema_registry.hh"

namespace {

struct test_config {
    unsigned partitions = 100000;
    uint64_t operations = 1000000;
    unsigned duration_in_seconds = 0;
    uint64_t warmup_operations = 0;
    unsigned warmup_duration = 0;
    double rewrite_ratio = 0.5;
    sstring distribution = "gaussian";
    unsigned random_seed = 0;
    bool random_seed_given = false;
    unsigned read_frequency = 1;
    unsigned default_time_to_live = 0;
    sstring compaction_strategy = "IncrementalCompactionStrategy";
    unsigned min_flush_interval = 100;
    unsigned max_flush_interval = 2000;
    uint64_t min_sstable_size = 10000;
    sstring output_format = "text";
    bool verbose = false;
};

struct metrics {
    uint64_t total_operations = 0;
    uint64_t unique_writes = 0;
    uint64_t rewrites = 0;
    uint64_t total_flushes = 0;

    // Write amplification tracking
    uint64_t flush_bytes_written = 0;      // total bytes written by memtable flushes
    uint64_t compaction_bytes_written = 0;  // total bytes written by compactions

    // Read amplification: number of sstables whose bloom filter matches per probe
    std::vector<uint64_t> read_amp_samples;

    // Compaction efficiency: (input_size - output_size) / input_size per compaction
    std::vector<double> compaction_efficiency_samples;
    uint64_t total_compaction_input_bytes = 0;
    uint64_t total_compaction_output_bytes = 0;

    std::chrono::steady_clock::time_point start_time;
    std::chrono::steady_clock::time_point end_time;

    double duration_seconds() const {
        return std::chrono::duration<double>(end_time - start_time).count();
    }

    // Compute percentile from sorted samples (0-100)
    uint64_t percentile(const std::vector<uint64_t>& sorted, double p) const {
        if (sorted.empty()) {
            return 0;
        }
        auto idx = static_cast<size_t>(std::ceil(p / 100.0 * sorted.size())) - 1;
        idx = std::min(idx, sorted.size() - 1);
        return sorted[idx];
    }

    double percentile_double(const std::vector<double>& sorted, double p) const {
        if (sorted.empty()) {
            return 0;
        }
        auto idx = static_cast<size_t>(std::ceil(p / 100.0 * sorted.size())) - 1;
        idx = std::min(idx, sorted.size() - 1);
        return sorted[idx];
    }

    void print_results(const test_config& cfg, uint64_t final_sstable_count,
                       uint64_t total_bytes_on_disk, uint64_t major_compacted_bytes) const {
        auto sorted_ra = read_amp_samples;
        std::sort(sorted_ra.begin(), sorted_ra.end());

        double avg_ra = 0;
        if (!sorted_ra.empty()) {
            avg_ra = std::accumulate(sorted_ra.begin(), sorted_ra.end(), 0.0) / sorted_ra.size();
        }

        double write_amp = flush_bytes_written > 0
            ? static_cast<double>(compaction_bytes_written) / flush_bytes_written
            : 0;

        fmt::print("\n=== Compaction Efficiency Benchmark Results ===\n");
        fmt::print("Strategy: {}\n", cfg.compaction_strategy);
        fmt::print("Random seed: {}\n", cfg.random_seed);
        fmt::print("Operations: {} ({} unique, {} rewrites)\n",
                   total_operations, unique_writes, rewrites);
        fmt::print("Duration: {:.1f}s\n", duration_seconds());
        fmt::print("Throughput: {:.0f} ops/sec\n",
                   total_operations / duration_seconds());
        fmt::print("\n");
        fmt::print("Flush control: [{}, {}] ops between flushes\n",
                   cfg.min_flush_interval, cfg.max_flush_interval);
        fmt::print("Total flushes: {}\n", total_flushes);
        fmt::print("Final sstable count: {}\n", final_sstable_count);
        fmt::print("\n");
        fmt::print("Flush bytes written: {:.1f} MB\n",
                   flush_bytes_written / 1048576.0);
        fmt::print("Compaction bytes written: {:.1f} MB\n",
                   compaction_bytes_written / 1048576.0);
        fmt::print("Total data on disk: {:.1f} MB\n",
                   total_bytes_on_disk / 1048576.0);
        fmt::print("Write amplification: {:.2f}x\n", write_amp);
        fmt::print("\n");
        fmt::print("Total fully compacted data on disk: {:.1f} MB\n",
                   major_compacted_bytes / 1048576.0);
        double space_amp = major_compacted_bytes > 0
            ? static_cast<double>(total_bytes_on_disk) / major_compacted_bytes
            : 0;
        fmt::print("Space amplification: {:.2f}x\n", space_amp);
        fmt::print("\n");
        fmt::print("Read amplification samples: {}\n", sorted_ra.size());
        if (!sorted_ra.empty()) {
            fmt::print("Read amplification (avg sstables per probe): {:.1f}\n", avg_ra);
            fmt::print("Read amplification (p50/p95/p99/max): {}/{}/{}/{}\n",
                       percentile(sorted_ra, 50),
                       percentile(sorted_ra, 95),
                       percentile(sorted_ra, 99),
                       sorted_ra.back());
        }

        if (!compaction_efficiency_samples.empty()) {
            auto sorted_ce = compaction_efficiency_samples;
            std::sort(sorted_ce.begin(), sorted_ce.end());
            double weighted_avg_ce = total_compaction_input_bytes > 0
                ? static_cast<double>(total_compaction_input_bytes - total_compaction_output_bytes) / total_compaction_input_bytes
                : 0;
            fmt::print("\nCompaction efficiency samples: {}\n", sorted_ce.size());
            fmt::print("Compaction efficiency (weighted avg): {:.4f}\n", weighted_avg_ce);
            fmt::print("Compaction efficiency (p50/p95/p99/max): {:.4f}/{:.4f}/{:.4f}/{:.4f}\n",
                       percentile_double(sorted_ce, 50),
                       percentile_double(sorted_ce, 95),
                       percentile_double(sorted_ce, 99),
                       sorted_ce.back());
        }
    }

    void write_json(const test_config& cfg, uint64_t final_sstable_count,
                    uint64_t total_bytes_on_disk, uint64_t major_compacted_bytes) const {
        auto sorted_ra = read_amp_samples;
        std::sort(sorted_ra.begin(), sorted_ra.end());

        double avg_ra = 0;
        if (!sorted_ra.empty()) {
            avg_ra = std::accumulate(sorted_ra.begin(), sorted_ra.end(), 0.0) / sorted_ra.size();
        }

        double write_amp = flush_bytes_written > 0
            ? static_cast<double>(compaction_bytes_written) / flush_bytes_written
            : 0;

        Json::Value root;
        root["strategy"] = std::string(cfg.compaction_strategy);
        root["random_seed"] = cfg.random_seed;
        root["total_operations"] = Json::Value::UInt64(total_operations);
        root["unique_writes"] = Json::Value::UInt64(unique_writes);
        root["rewrites"] = Json::Value::UInt64(rewrites);
        root["duration_seconds"] = fmt::format("{:.2f}", duration_seconds());
        root["throughput_ops_per_sec"] = fmt::format("{:.2f}", total_operations / duration_seconds());
        root["total_flushes"] = Json::Value::UInt64(total_flushes);
        root["final_sstable_count"] = Json::Value::UInt64(final_sstable_count);
        root["flush_bytes_written"] = Json::Value::UInt64(flush_bytes_written);
        root["compaction_bytes_written"] = Json::Value::UInt64(compaction_bytes_written);
        root["total_bytes_on_disk"] = Json::Value::UInt64(total_bytes_on_disk);
        root["major_compacted_bytes_on_disk"] = Json::Value::UInt64(major_compacted_bytes);
        double space_amp = major_compacted_bytes > 0
            ? static_cast<double>(total_bytes_on_disk) / major_compacted_bytes
            : 0;
        root["space_amplification"] = fmt::format("{:.2f}", space_amp);
        root["write_amplification"] = fmt::format("{:.2f}", write_amp);
        root["read_amp_samples"] = Json::Value::UInt64(sorted_ra.size());
        if (!sorted_ra.empty()) {
            root["read_amp_avg"] = fmt::format("{:.2f}", avg_ra);
            root["read_amp_p50"] = Json::Value::UInt64(percentile(sorted_ra, 50));
            root["read_amp_p95"] = Json::Value::UInt64(percentile(sorted_ra, 95));
            root["read_amp_p99"] = Json::Value::UInt64(percentile(sorted_ra, 99));
            root["read_amp_max"] = Json::Value::UInt64(sorted_ra.back());
        }

        if (!compaction_efficiency_samples.empty()) {
            auto sorted_ce = compaction_efficiency_samples;
            std::sort(sorted_ce.begin(), sorted_ce.end());
            double weighted_avg_ce = total_compaction_input_bytes > 0
                ? static_cast<double>(total_compaction_input_bytes - total_compaction_output_bytes) / total_compaction_input_bytes
                : 0;
            root["compaction_efficiency_samples"] = Json::Value::UInt64(sorted_ce.size());
            root["compaction_efficiency_weighted_avg"] = fmt::format("{:.4f}", weighted_avg_ce);
            root["compaction_efficiency_p50"] = fmt::format("{:.4f}", percentile_double(sorted_ce, 50));
            root["compaction_efficiency_p95"] = fmt::format("{:.4f}", percentile_double(sorted_ce, 95));
            root["compaction_efficiency_p99"] = fmt::format("{:.4f}", percentile_double(sorted_ce, 99));
            root["compaction_efficiency_max"] = fmt::format("{:.4f}", sorted_ce.back());
        }

        Json::StreamWriterBuilder builder;
        builder["indentation"] = "  ";
        auto json_str = Json::writeString(builder, root);
        fmt::print("{}\n", json_str);
    }
};

// Workload state: tracks all partitions and their max clustering key
struct workload_state {
    // pk -> max ck written (0-based)
    std::unordered_map<int64_t, int64_t> partition_rows;
    int64_t next_partition_id = 0;

    std::mt19937_64 rng;
    std::uniform_real_distribution<double> uniform_01{0.0, 1.0};
    std::uniform_int_distribution<int> flush_dist;

    int ops_until_flush;

    workload_state(const test_config& cfg)
        : flush_dist(cfg.min_flush_interval, cfg.max_flush_interval)
    {
        rng.seed(cfg.random_seed);
        ops_until_flush = flush_dist(rng);
    }

    // Pick an existing partition index using the configured distribution
    int64_t pick_existing_partition(const test_config& cfg) {
        if (partition_rows.empty()) {
            return -1;
        }

        int64_t pk;
        if (cfg.distribution == "uniform") {
            std::uniform_int_distribution<int64_t> dist(0, next_partition_id - 1);
            pk = dist(rng);
        } else if (cfg.distribution == "zipfian") {
            // Simple Zipfian approximation: use inverse power distribution
            double u = uniform_01(rng);
            double rank = std::pow(next_partition_id, u);
            pk = std::min(static_cast<int64_t>(rank), next_partition_id - 1);
        } else {
            // Default: gaussian centered at mid, stddev = N/6
            double center = next_partition_id / 2.0;
            double stddev = next_partition_id / 6.0;
            std::normal_distribution<double> gauss(center, std::max(stddev, 1.0));
            double val = gauss(rng);
            pk = static_cast<int64_t>(std::round(val));
            pk = std::clamp(pk, int64_t(0), next_partition_id - 1);
        }

        // Ensure the picked pk actually exists in our map.
        // unordered_map has no ordering, so search for the exact key first,
        // then fall back to any existing partition.
        if (partition_rows.contains(pk)) {
            return pk;
        }
        // Pick a random existing partition as fallback
        auto it = partition_rows.begin();
        std::uniform_int_distribution<size_t> idx_dist(0, partition_rows.size() - 1);
        std::advance(it, idx_dist(rng));
        return it->first;
    }
};

using sstable_set_t = std::unordered_map<sstables::generation_type, uint64_t>;

sstable_set_t get_sstable_set(sharded<replica::database>& db, const schema_ptr& s) {
    return db.map_reduce0([gs = global_schema_ptr(s)] (replica::database& db) {
        sstable_set_t local;
        auto& cf = db.find_column_family(gs);
        auto sstables = cf.get_sstables();
        for (auto& sst : *sstables) {
            local[sst->generation()] = sst->bytes_on_disk();
        }
        return local;
    }, sstable_set_t{}, [] (sstable_set_t a, sstable_set_t b) {
        a.merge(std::move(b));
        return a;
    }).get();
}

// Compute sum of bytes_on_disk for SSTables in `current` that are not in `previous`
uint64_t new_sstable_bytes(const sstable_set_t& previous, const sstable_set_t& current) {
    uint64_t total = 0;
    for (auto& [gen, bytes] : current) {
        if (!previous.contains(gen)) {
            total += bytes;
        }
    }
    return total;
}

// Compute sum of bytes_on_disk for SSTables in `previous` that are not in `current`
uint64_t removed_sstable_bytes(const sstable_set_t& previous, const sstable_set_t& current) {
    uint64_t total = 0;
    for (auto& [gen, bytes] : previous) {
        if (!current.contains(gen)) {
            total += bytes;
        }
    }
    return total;
}

uint64_t get_total_bytes_on_disk(sharded<replica::database>& db, const schema_ptr& s) {
    return db.map_reduce0([gs = global_schema_ptr(s)] (replica::database& db) -> uint64_t {
        return db.find_column_family(gs).get_stats().total_disk_space_used.on_disk;
    }, uint64_t(0), std::plus<uint64_t>()).get();
}

uint64_t get_sstable_count(sharded<replica::database>& db, const schema_ptr& s) {
    return db.map_reduce0([gs = global_schema_ptr(s)] (replica::database& db) -> uint64_t {
        return db.find_column_family(gs).get_stats().live_sstable_count;
    }, uint64_t(0), std::plus<uint64_t>()).get();
}

// Measure read amplification by probing bloom filters on the owning shard
uint64_t probe_bloom_filters(sharded<replica::database>& db, const schema_ptr& s, int64_t pk) {
    auto pkey = partition_key::from_single_value(*s, long_type->decompose(pk));
    auto token = dht::get_token(*s, pkey);
    auto& local_cf = db.local().find_column_family(s);
    auto shard = local_cf.shard_for_reads(token);

    return db.invoke_on(shard, [gs = global_schema_ptr(s), pkey = std::move(pkey)] (replica::database& db) {
        auto& cf = db.find_column_family(gs);
        auto schema = gs.get();
        uint64_t hits = 0;
        auto sstables = cf.get_sstables();
        for (auto& sst : *sstables) {
            if (sst->filter_has_key(*schema, pkey)) {
                ++hits;
            }
        }
        return hits;
    }).get();
}

// Print per-shard SSTable state for the given table.
void log_sstable_state(sharded<replica::database>& db, const schema_ptr& s, sstring label) {
    db.invoke_on_all([gs = global_schema_ptr(s), label = std::move(label)] (replica::database& db) {
        auto& cf = db.find_column_family(gs);
        auto sstables = cf.get_sstables();
        if (sstables->empty()) {
            return;
        }
        std::vector<sstables::shared_sstable> sorted(sstables->begin(), sstables->end());
        std::sort(sorted.begin(), sorted.end(), [] (const auto& a, const auto& b) {
            return a->bytes_on_disk() < b->bytes_on_disk();
        });
        fmt::print("[shard {}] {} - {} sstables:\n", this_shard_id(), label, sorted.size());
        for (auto& sst : sorted) {
            fmt::print("  gen={} size={}\n", sst->generation(), sst->bytes_on_disk());
        }
        std::fflush(stdout);
    }).get();
}

// Wait for any ongoing compactions of the given table on all shards.
void await_table_compactions(sharded<replica::database>& db, const schema_ptr& s) {
    db.invoke_on_all([gs = global_schema_ptr(s)] (replica::database& db) {
        auto& cf = db.find_column_family(gs);
        auto& cm = db.get_compaction_manager();
        return cf.parallel_foreach_compaction_group_view([&cm] (compaction::compaction_group_view& v) {
            return cm.await_ongoing_compactions(&v);
        });
    }).get();
}

// Wait for any ongoing compactions on all shards and measure bytes written.
// Returns the per-compaction efficiency, or -1 if no compaction occurred.
double await_compactions(sharded<replica::database>& db, const schema_ptr& s, metrics& m,
                       const sstable_set_t& before) {
    await_table_compactions(db, s);

    auto after = get_sstable_set(db, s);
    auto output_size = new_sstable_bytes(before, after);
    m.compaction_bytes_written += output_size;

    // Compute compaction efficiency: (input_size - output_size) / input_size
    auto input_size = removed_sstable_bytes(before, after);
    if (input_size > 0) {
        m.total_compaction_input_bytes += input_size;
        m.total_compaction_output_bytes += output_size;
        double efficiency = static_cast<double>(input_size - output_size) / input_size;
        m.compaction_efficiency_samples.push_back(efficiency);
        return efficiency;
    }
    return -1;
}

void do_compaction_efficiency_test(cql_test_env& env, test_config& cfg) {
    using clk = std::chrono::steady_clock;

    // Create table with configured compaction strategy
    sstring create_table_cql = fmt::format(
        "CREATE TABLE ks.perf_compaction ("
        "  pk bigint,"
        "  ck bigint,"
        "  v bigint,"
        "  PRIMARY KEY (pk, ck)"
        ") WITH compaction = {{'class': '{}', 'min_sstable_size': '{}'}}"
        " AND tablets = {{'min_per_shard_tablet_count': '1'}}",
        cfg.compaction_strategy, cfg.min_sstable_size);

    if (cfg.default_time_to_live > 0) {
        create_table_cql += fmt::format(" AND default_time_to_live = {}",
                                         cfg.default_time_to_live);
    }

    env.execute_cql(create_table_cql).get();

    auto& cf = env.local_db().find_column_family("ks", "perf_compaction");
    auto s = cf.schema();

    // Prepare the INSERT statement
    auto insert_id = env.prepare(
        "INSERT INTO ks.perf_compaction (pk, ck, v) VALUES (?, ?, ?)").get();

    workload_state state(cfg);
    metrics m;

    auto run_phase = [&](uint64_t max_ops, unsigned max_duration, bool is_warmup) {
        auto phase_start = clk::now();
        auto deadline = max_duration > 0
            ? phase_start + std::chrono::seconds(max_duration)
            : clk::time_point::max();

        uint64_t ops = 0;

        while (true) {
            // Check termination conditions
            if (max_ops > 0 && ops >= max_ops) {
                break;
            }
            if (max_duration > 0 && clk::now() >= deadline) {
                break;
            }

            // Decide operation type: rewrite or unique write
            double r = state.partition_rows.empty() ? 1.0 : state.uniform_01(state.rng);

            int64_t pk, ck;
            if (r < cfg.rewrite_ratio) {
                // Rewrite: existing partition, existing row
                pk = state.pick_existing_partition(cfg);
                auto max_ck = state.partition_rows[pk];
                std::uniform_int_distribution<int64_t> ck_dist(0, max_ck);
                ck = ck_dist(state.rng);
                if (!is_warmup) {
                    m.rewrites++;
                }
            } else {
                // Unique write: new partition or new row in existing partition
                if (state.partition_rows.empty()
                    || (state.uniform_01(state.rng) < 0.5
                        && state.next_partition_id < cfg.partitions)) {
                    // New partition
                    pk = state.next_partition_id++;
                    ck = 0;
                    state.partition_rows[pk] = 0;
                } else {
                    // New row in existing partition
                    pk = state.pick_existing_partition(cfg);
                    ck = ++state.partition_rows[pk];
                }
                if (!is_warmup) {
                    m.unique_writes++;
                }
            }

            // Execute the write
            int64_t v = state.rng();
            env.execute_prepared(insert_id, {
                {cql3::raw_value::make_value(long_type->decompose(pk)),
                 cql3::raw_value::make_value(long_type->decompose(ck)),
                 cql3::raw_value::make_value(long_type->decompose(v))}
            }).get();

            ops++;
            if (!is_warmup) {
                m.total_operations++;
            }

            // Flush control
            if (--state.ops_until_flush <= 0) {
                // Snapshot sstable set before flush
                auto before_flush = get_sstable_set(env.db(), s);

                replica::database::flush_table_on_all_shards(env.db(), "ks", "perf_compaction").get();

                if (!is_warmup) {
                    // Measure bytes written by flush
                    auto after_flush = get_sstable_set(env.db(), s);
                    m.flush_bytes_written += new_sstable_bytes(before_flush, after_flush);
                    m.total_flushes++;

                    if (cfg.verbose) {
                        log_sstable_state(env.db(), s, fmt::format("after flush #{}", m.total_flushes));
                    }

                    // Wait for any triggered compactions and measure bytes written
                    auto ce = await_compactions(env.db(), s, m, after_flush);

                    if (cfg.verbose) {
                        log_sstable_state(env.db(), s, fmt::format("after compaction (flush #{})", m.total_flushes));
                        if (ce >= 0) {
                            fmt::print("  compaction efficiency: {:.4f}\n", ce);
                            std::fflush(stdout);
                        }
                    }

                    // Probe bloom filters after flush+compaction when all data is on disk
                    if (cfg.read_frequency > 0
                        && m.total_flushes % cfg.read_frequency == 0
                        && !state.partition_rows.empty()) {
                        auto probe_pk = state.pick_existing_partition(cfg);
                        auto hits = probe_bloom_filters(env.db(), s, probe_pk);
                        m.read_amp_samples.push_back(hits);
                    }
                } else {
                    // During warmup, just wait for compactions
                    await_table_compactions(env.db(), s);
                }

                // Reset flush counter
                state.ops_until_flush = state.flush_dist(state.rng);
            }
        }

        return;
    };

    // Warmup phase
    if (cfg.warmup_operations > 0 || cfg.warmup_duration > 0) {
        fmt::print("Running warmup...\n");
        run_phase(cfg.warmup_operations, cfg.warmup_duration, true);
        // Reset metrics after warmup but keep workload state
        m = metrics{};
        fmt::print("Warmup complete. Starting measured phase.\n");
    }

    // Measured phase
    m.start_time = clk::now();
    run_phase(cfg.operations, cfg.duration_in_seconds, false);
    m.end_time = clk::now();

    // Final metrics
    auto final_sstable_count = get_sstable_count(env.db(), s);
    auto total_bytes_on_disk = get_total_bytes_on_disk(env.db(), s);

    // Major compaction to measure fully compacted size
    env.db().invoke_on_all([gs = global_schema_ptr(s)] (replica::database& db) {
        auto& cf = db.find_column_family(gs);
        auto& cm = db.get_compaction_manager();
        return cf.parallel_foreach_compaction_group_view([&cm] (compaction::compaction_group_view& v) {
            return cm.perform_major_compaction(v, tasks::task_info{});
        });
    }).get();
    auto major_compacted_bytes = get_total_bytes_on_disk(env.db(), s);

    if (cfg.verbose) {
        log_sstable_state(env.db(), s, "after major compaction");
    }

    if (cfg.output_format == "json") {
        m.write_json(cfg, final_sstable_count, total_bytes_on_disk, major_compacted_bytes);
    } else {
        m.print_results(cfg, final_sstable_count, total_bytes_on_disk, major_compacted_bytes);
    }
}

} // anonymous namespace

namespace perf {

int scylla_compaction_efficiency_main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("partitions", bpo::value<unsigned>()->default_value(100000),
            "total number of unique partition keys")
        ("operations", bpo::value<uint64_t>()->default_value(1000000),
            "total operations to perform (0 = use duration)")
        ("duration", bpo::value<unsigned>()->default_value(0),
            "run time in seconds (0 = use operations count)")
        ("warmup-operations", bpo::value<uint64_t>()->default_value(0),
            "warmup operations before measurement")
        ("warmup-duration", bpo::value<unsigned>()->default_value(0),
            "warmup time in seconds")
        ("rewrite-ratio", bpo::value<double>()->default_value(0.5),
            "ratio of rewrites (0.0-1.0)")
        ("distribution", bpo::value<std::string>()->default_value("gaussian"),
            "key selection distribution: gaussian, uniform, zipfian")
        ("random-seed", bpo::value<unsigned>(),
            "random number generator seed")
        ("read-frequency", bpo::value<unsigned>()->default_value(1),
            "measure read amplification every N flushes")
        ("default-time-to-live", bpo::value<unsigned>()->default_value(0),
            "table TTL in seconds (0 = disabled)")
        ("compaction-strategy", bpo::value<std::string>()->default_value("IncrementalCompactionStrategy"),
            "compaction strategy class name")
        ("min-flush-interval", bpo::value<unsigned>()->default_value(100),
            "min operations between flushes")
        ("max-flush-interval", bpo::value<unsigned>()->default_value(2000),
            "max operations between flushes")
        ("min-sstable-size", bpo::value<uint64_t>()->default_value(10000),
            "min_sstable_size compaction strategy option (bytes)")
        ("output-format", bpo::value<std::string>()->default_value("text"),
            "output format: text, json")
        ("verbose", bpo::bool_switch()->default_value(false),
            "print per-shard sstable state after each flush and compaction")
        ;

    set_abort_on_internal_error(true);

    return app.run(argc, argv, [&app] {
        auto conf_seed = app.configuration()["random-seed"];
        auto seed = conf_seed.empty() ? std::random_device()() : conf_seed.as<unsigned>();
        auto output_format = app.configuration()["output-format"].as<std::string>();
        if (output_format == "text") {
            std::cout << "random-seed=" << seed << '\n';
        }

        return seastar::async([&app, seed] {
            auto db_cfg = ::make_shared<db::config>();
            // Set memtable space very high to prevent automatic flushing.
            // The test controls flush timing explicitly.
            db_cfg->memtable_total_space_in_mb(1 << 20); // ~1TB
            cql_test_config cql_cfg(db_cfg);
            cql_cfg.initial_tablets = 1; // Enable tablets for the default keyspace

            do_with_cql_env_thread([&app, seed] (auto& env) {
                test_config cfg;
                cfg.partitions = app.configuration()["partitions"].as<unsigned>();
                cfg.operations = app.configuration()["operations"].as<uint64_t>();
                cfg.duration_in_seconds = app.configuration()["duration"].as<unsigned>();
                cfg.warmup_operations = app.configuration()["warmup-operations"].as<uint64_t>();
                cfg.warmup_duration = app.configuration()["warmup-duration"].as<unsigned>();
                cfg.rewrite_ratio = app.configuration()["rewrite-ratio"].as<double>();
                cfg.distribution = app.configuration()["distribution"].as<std::string>();
                cfg.random_seed = seed;
                cfg.read_frequency = app.configuration()["read-frequency"].as<unsigned>();
                cfg.default_time_to_live = app.configuration()["default-time-to-live"].as<unsigned>();
                cfg.compaction_strategy = app.configuration()["compaction-strategy"].as<std::string>();
                cfg.min_flush_interval = app.configuration()["min-flush-interval"].as<unsigned>();
                cfg.max_flush_interval = app.configuration()["max-flush-interval"].as<unsigned>();
                cfg.min_sstable_size = app.configuration()["min-sstable-size"].as<uint64_t>();
                cfg.output_format = app.configuration()["output-format"].as<std::string>();
                cfg.verbose = app.configuration()["verbose"].as<bool>();
                if (cfg.output_format != "text" && cfg.output_format != "json") {
                    throw std::invalid_argument(fmt::format("invalid value for output-format: {}", cfg.output_format));
                }
                if (cfg.distribution != "gaussian" && cfg.distribution != "uniform" && cfg.distribution != "zipfian") {
                    throw std::invalid_argument(fmt::format("invalid value for distribution: {}", cfg.distribution));
                }

                if (cfg.output_format == "text") {
                    fmt::print("Compaction efficiency benchmark\n");
                    fmt::print("  partitions: {}\n", cfg.partitions);
                    fmt::print("  operations: {}\n", cfg.operations);
                    fmt::print("  duration: {}s\n", cfg.duration_in_seconds);
                    fmt::print("  rewrite-ratio: {}\n", cfg.rewrite_ratio);
                    fmt::print("  distribution: {}\n", cfg.distribution);
                    fmt::print("  read-frequency: {}\n", cfg.read_frequency);
                    fmt::print("  compaction-strategy: {}\n", cfg.compaction_strategy);
                    fmt::print("  min-flush-interval: {}\n", cfg.min_flush_interval);
                    fmt::print("  max-flush-interval: {}\n", cfg.max_flush_interval);
                    fmt::print("  min-sstable-size: {}\n", cfg.min_sstable_size);
                    if (cfg.default_time_to_live > 0) {
                        fmt::print("  default-time-to-live: {}s\n", cfg.default_time_to_live);
                    }
                }

                do_compaction_efficiency_test(env, cfg);
            }, std::move(cql_cfg)).get();
        });
    });
}

} // namespace perf
