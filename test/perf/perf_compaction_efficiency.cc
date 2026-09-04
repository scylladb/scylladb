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
#include <sstream>
#include <json/json.h>

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/timer.hh>

#include "test/lib/cql_test_env.hh"
#include "test/perf/perf.hh"
#include "compaction/compaction_manager.hh"
#include "compaction/compaction_group_view.hh"
#include "db/config.hh"
#include "replica/database.hh"
#include "replica/memtable.hh"
#include "sstables/sstables.hh"
#include "schema/schema_builder.hh"
#include "schema/schema_registry.hh"
#include "utils/estimated_histogram.hh"
#include "backlog_controller.hh"

namespace {

struct test_config {
    unsigned partitions = 100000;
    uint64_t operations = 1000000;
    unsigned duration_in_seconds = 0;
    uint64_t warmup_operations = 0;
    unsigned warmup_duration = 0;
    double rewrite_ratio = 0.25;
    double delete_ratio = 0.25;
    sstring distribution = "gaussian";
    unsigned random_seed = 0;
    bool random_seed_given = false;
    unsigned read_frequency = 1;
    unsigned default_time_to_live = 0;
    sstring compaction_strategy = "IncrementalCompactionStrategy";
    unsigned min_flush_interval = 100;
    unsigned max_flush_interval = 2000;
    uint64_t min_sstable_size = 10000;
    std::map<sstring, sstring> compaction_options;
    sstring output_format = "text";
    bool verbose = false;

    // Concurrent mode: writer fibers run against a periodic flush, so compaction runs
    // alongside the write load and has to compete for CPU. The serial mode (flush, then
    // wait for compaction) never lets that happen, so it cannot say anything about the
    // compaction controller.
    //
    // Both inputs the comparison depends on are rate controlled: how many operations per
    // second the writers issue, and how often a flush is triggered. Everything else about
    // compaction's behaviour is an output.
    bool concurrent_mode = false;
    unsigned concurrent_writers = 16;
    unsigned write_rate = 0;            // operations/s across all writers, 0 = unlimited
    unsigned flush_rate = 10;           // flush triggers per second
    double read_ratio = 0.1;            // fraction of operations that are reads
    unsigned tablets = 1;               // tablets per shard, i.e. compaction groups
    unsigned stats_interval_ms = 1000;
    unsigned available_memory_mb = 0;   // 0 = default (memory::stats().total_memory())
    float compaction_static_shares = 0; // >0 pins shares and disables the controller
};

// Time series of a scalar sampled at a fixed interval, reported as percentiles. Used to
// answer "how bad did it get, and for how long", which a single final value can't.
struct sample_series {
    std::vector<double> samples;

    void add(double v) { samples.push_back(v); }
    bool empty() const { return samples.empty(); }
    size_t size() const { return samples.size(); }

    double percentile(double p) const {
        if (samples.empty()) {
            return 0;
        }
        auto sorted = samples;
        std::sort(sorted.begin(), sorted.end());
        auto idx = static_cast<size_t>(std::ceil(p / 100.0 * sorted.size()));
        idx = std::min(idx ? idx - 1 : 0, sorted.size() - 1);
        return sorted[idx];
    }

    double max() const {
        return samples.empty() ? 0 : *std::ranges::max_element(samples);
    }

    double mean() const {
        return samples.empty() ? 0
            : std::accumulate(samples.begin(), samples.end(), 0.0) / samples.size();
    }

    // Percentile over the given half of the run. Comparing the two halves shows whether
    // compaction is holding steady or falling behind over time.
    double percentile_of_half(double p, bool second_half) const {
        if (samples.size() < 2) {
            return percentile(p);
        }
        auto mid = samples.begin() + samples.size() / 2;
        sample_series half;
        half.samples.assign(second_half ? mid : samples.begin(),
                            second_half ? samples.end() : mid);
        return half.percentile(p);
    }

    // Fraction of samples equal to v, within a relative tolerance. Used to tell whether
    // the controller is pinned at an endpoint rather than tracking the backlog.
    double fraction_at(double v) const {
        if (samples.empty()) {
            return 0;
        }
        auto n = std::ranges::count_if(samples, [v] (double s) {
            return std::abs(s - v) <= std::max(1.0, std::abs(v) * 1e-6);
        });
        return double(n) / samples.size();
    }

    // Number of times consecutive samples differ, i.e. how often the controller moved.
    // Zero means it never reacted over the whole run.
    uint64_t changes() const {
        uint64_t n = 0;
        for (size_t i = 1; i < samples.size(); ++i) {
            if (samples[i] != samples[i - 1]) {
                ++n;
            }
        }
        return n;
    }
};

struct metrics {
    uint64_t total_operations = 0;
    uint64_t unique_writes = 0;
    uint64_t rewrites = 0;
    uint64_t deletes = 0;
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
        fmt::print("Operations: {} ({} unique, {} rewrites, {} deletes)\n",
                   total_operations, unique_writes, rewrites, deletes);
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
                ? (static_cast<int64_t>(total_compaction_input_bytes) - static_cast<int64_t>(total_compaction_output_bytes)) / static_cast<double>(total_compaction_input_bytes)
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
                    uint64_t total_bytes_on_disk, uint64_t major_compacted_bytes,
                    const Json::Value* extra = nullptr) const {
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
        root["deletes"] = Json::Value::UInt64(deletes);
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
                ? (static_cast<int64_t>(total_compaction_input_bytes) - static_cast<int64_t>(total_compaction_output_bytes)) / static_cast<double>(total_compaction_input_bytes)
                : 0;
            root["compaction_efficiency_samples"] = Json::Value::UInt64(sorted_ce.size());
            root["compaction_efficiency_weighted_avg"] = fmt::format("{:.4f}", weighted_avg_ce);
            root["compaction_efficiency_p50"] = fmt::format("{:.4f}", percentile_double(sorted_ce, 50));
            root["compaction_efficiency_p95"] = fmt::format("{:.4f}", percentile_double(sorted_ce, 95));
            root["compaction_efficiency_p99"] = fmt::format("{:.4f}", percentile_double(sorted_ce, 99));
            root["compaction_efficiency_max"] = fmt::format("{:.4f}", sorted_ce.back());
        }

        if (extra) {
            for (const auto& key : extra->getMemberNames()) {
                root[key] = (*extra)[key];
            }
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

// When origin is given, only sstables written by that producer are collected. Flush
// accounting needs "memtable", otherwise sstables produced by a compaction that happened
// to finish inside the same window are counted as flushed bytes, which inflates flush
// bytes (and write amplification) by however busy compaction was at the time.
sstable_set_t get_sstable_set(sharded<replica::database>& db, const schema_ptr& s,
                              std::optional<sstring> origin = std::nullopt) {
    return db.map_reduce0([gs = global_schema_ptr(s), origin] (replica::database& db) {
        sstable_set_t local;
        auto& cf = db.find_column_family(gs);
        auto sstables = cf.get_sstables();
        for (auto& sst : *sstables) {
            if (origin && sst->get_origin() != *origin) {
                continue;
            }
            local[sst->generation()] = sst->bytes_on_disk();
        }
        return local;
    }, sstable_set_t{}, [] (sstable_set_t a, sstable_set_t b) {
        a.merge(std::move(b));
        return a;
    }).get();
}

// Bytes of sstables produced by memtable flushes, i.e. what the write path actually
// wrote. Used as the denominator of write amplification.
static const sstring memtable_origin = "memtable";

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
        double efficiency = (static_cast<int64_t>(input_size) - static_cast<int64_t>(output_size)) / static_cast<double>(input_size);
        m.compaction_efficiency_samples.push_back(efficiency);
        return efficiency;
    }
    return -1;
}

void do_compaction_efficiency_test(cql_test_env& env, test_config& cfg) {
    using clk = std::chrono::steady_clock;

    // Create table with configured compaction strategy
    // Create table with configured compaction strategy
    sstring compaction_opts = fmt::format("'class': '{}'", cfg.compaction_strategy);
    for (auto& [k, v] : cfg.compaction_options) {
        compaction_opts += fmt::format(", '{}': '{}'", k, v);
    }

    sstring create_table_cql = fmt::format(
        "CREATE TABLE ks.perf_compaction ("
        "  pk bigint,"
        "  ck bigint,"
        "  v bigint,"
        "  PRIMARY KEY (pk, ck)"
        ") WITH compaction = {{{}}}"
        " AND tablets = {{'min_per_shard_tablet_count': '{}'}}"
        " AND tombstone_gc = {{'mode': 'immediate'}}",
        compaction_opts, cfg.tablets);

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

    // Prepare the DELETE statement
    auto delete_id = env.prepare(
        "DELETE FROM ks.perf_compaction WHERE pk = ? AND ck = ?").get();

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

            // Decide operation type: unique write, rewrite, or delete
            double r = state.partition_rows.empty() ? 1.0 : state.uniform_01(state.rng);

            int64_t pk, ck;
            if (r < cfg.rewrite_ratio) {
                // Rewrite: existing partition, existing row
                pk = state.pick_existing_partition(cfg);
                auto max_ck = state.partition_rows[pk];
                std::uniform_int_distribution<int64_t> ck_dist(0, max_ck);
                ck = ck_dist(state.rng);

                int64_t v = state.rng();
                env.execute_prepared(insert_id, {
                    {cql3::raw_value::make_value(long_type->decompose(pk)),
                     cql3::raw_value::make_value(long_type->decompose(ck)),
                     cql3::raw_value::make_value(long_type->decompose(v))}
                }).get();

                if (!is_warmup) {
                    m.rewrites++;
                }
            } else if (r < cfg.rewrite_ratio + cfg.delete_ratio) {
                // Delete: existing partition, existing row
                pk = state.pick_existing_partition(cfg);
                auto max_ck = state.partition_rows[pk];
                std::uniform_int_distribution<int64_t> ck_dist(0, max_ck);
                ck = ck_dist(state.rng);

                env.execute_prepared(delete_id, {
                    {cql3::raw_value::make_value(long_type->decompose(pk)),
                     cql3::raw_value::make_value(long_type->decompose(ck))}
                }).get();

                if (!is_warmup) {
                    m.deletes++;
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

                int64_t v = state.rng();
                env.execute_prepared(insert_id, {
                    {cql3::raw_value::make_value(long_type->decompose(pk)),
                     cql3::raw_value::make_value(long_type->decompose(ck)),
                     cql3::raw_value::make_value(long_type->decompose(v))}
                }).get();

                if (!is_warmup) {
                    m.unique_writes++;
                }
            }

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

    // Final flush to capture any unflushed data from the measured phase
    {
        auto before_flush = get_sstable_set(env.db(), s);
        replica::database::flush_table_on_all_shards(env.db(), "ks", "perf_compaction").get();
        auto after_flush = get_sstable_set(env.db(), s);
        m.flush_bytes_written += new_sstable_bytes(before_flush, after_flush);
        m.total_flushes++;
        await_compactions(env.db(), s, m, after_flush);
    }

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

// Sum of the per-view backlog for the given table across all shards. Used only for
// observability; the value crosses trackers/shards additively, so it is representative
// of the total pressure the compaction controller sees.
double get_total_backlog(sharded<replica::database>& db, const schema_ptr& s) {
    return db.map_reduce0([gs = global_schema_ptr(s)] (replica::database& db) -> future<double> {
        auto& cf = db.find_column_family(gs);
        double sum = 0;
        co_await cf.parallel_foreach_compaction_group_view([&sum] (compaction::compaction_group_view& v) -> future<> {
            sum += v.get_backlog_tracker().backlog();
            co_return;
        });
        co_return sum;
    }, double(0), std::plus<double>()).get();
}

// Sum of the shard-local compaction_manager backlog across all shards. This includes
// every table's backlog (not just ours) but in a benchmark that runs a single table
// it is effectively equivalent to get_total_backlog and — more importantly — it is
// exactly the value the compaction controller sees when it computes shares:
//     shares = f(compaction_manager.backlog() / available_memory())
// (see backlog_controller::adjust / compaction_manager.cc).
double get_cm_backlog_sum(sharded<replica::database>& db) {
    return db.map_reduce0([] (replica::database& db) -> double {
        return db.get_compaction_manager().backlog();
    }, double(0), std::plus<double>()).get();
}

// available_memory() reported by the compaction manager on shard 0. The controller
// normalises the backlog by this value before mapping it to a shares value on the
// compaction scheduling group.
size_t get_cm_available_memory(sharded<replica::database>& db) {
    return db.invoke_on(0, [] (replica::database& db) {
        return db.get_compaction_manager().available_memory();
    }).get();
}

// Shares currently assigned to the compaction scheduling group. This is what the
// controller sets in response to the observed backlog: higher backlog -> more shares
// -> more CPU allocated to compaction relative to foreground writes. Sum across shards
// so we get a single number to display in the timeline; on a single-shard run this
// equals the per-shard value.
double get_compaction_shares_sum(sharded<replica::database>& db) {
    return db.map_reduce0([] (replica::database& db) -> double {
        return db.get_compaction_manager().compaction_sg().get_shares();
    }, double(0), std::plus<double>()).get();
}

// Number of compactions running, and queued waiting for an opportunity to run, across
// all shards. A run where compaction is given plenty of shares but few jobs are active
// is being held back by admission (weight class / fan-in gates), not by CPU.
std::pair<uint64_t, int64_t> get_compaction_task_counts(sharded<replica::database>& db) {
    return db.map_reduce0([] (replica::database& db) {
        auto& st = db.get_compaction_manager().get_stats();
        return std::make_pair(st.active_tasks, st.pending_tasks);
    }, std::make_pair(uint64_t(0), int64_t(0)), [] (auto a, auto b) {
        return std::make_pair(a.first + b.first, a.second + b.second);
    }).get();
}

// Per-compaction facts for the run, read back from system.compaction_history: how much
// each job read and wrote, how many sstables it merged, and how long it took. This is
// what tells apart "compaction got more CPU and used it well" from "compaction got more
// CPU and spent it rewriting the same data".
struct compaction_job_stats {
    uint64_t jobs = 0;
    uint64_t bytes_in = 0;
    uint64_t bytes_out = 0;
    sample_series fan_in;
    sample_series duration_ms;
};

compaction_job_stats get_compaction_job_stats(cql_test_env& env, const sstring& cf_name) {
    compaction_job_stats st;
    auto msg = env.execute_cql(
        "SELECT columnfamily_name, bytes_in, bytes_out, rows_merged, started_at, compacted_at "
        "FROM system.compaction_history").get();
    auto res = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
    if (!res) {
        return st;
    }
    auto rows = res->rs().result_set().rows();
    for (const auto& row : rows) {
        if (!row[0] || value_cast<sstring>(utf8_type->deserialize(*row[0])) != cf_name) {
            continue;
        }
        ++st.jobs;
        if (row[1]) {
            st.bytes_in += value_cast<int64_t>(long_type->deserialize(*row[1]));
        }
        if (row[2]) {
            st.bytes_out += value_cast<int64_t>(long_type->deserialize(*row[2]));
        }
        if (row[3]) {
            // rows_merged maps "number of sstables this row came from" -> row count, so
            // the largest key is the job's fan-in.
            auto mt = map_type_impl::get_instance(int32_type, long_type, false);
            auto v = value_cast<map_type_impl::native_type>(mt->deserialize(*row[3]));
            int32_t fan_in = 0;
            for (const auto& [k, _] : v) {
                fan_in = std::max(fan_in, value_cast<int32_t>(k));
            }
            if (fan_in) {
                st.fan_in.add(double(fan_in));
            }
        }
        if (row[4] && row[5]) {
            auto started = value_cast<db_clock::time_point>(timestamp_type->deserialize(*row[4]));
            auto ended = value_cast<db_clock::time_point>(timestamp_type->deserialize(*row[5]));
            st.duration_ms.add(double(std::chrono::duration_cast<std::chrono::milliseconds>(
                ended - started).count()));
        }
    }
    return st;
}

// Concurrent variant of the compaction efficiency benchmark. Unlike the sequential mode
// (which flushes and awaits compactions synchronously), this mode continuously drives
// N writer fibers per shard while a background timer forces periodic flushes without
// waiting for compaction. This creates sustained pressure on the compaction manager
// and its backlog controller: compactions run concurrently with the write load and
// have to compete for CPU shares, which is what the backlog signal is used for.
//
// This mode is the appropriate one for measuring the impact of changes to the backlog
// controller or to the backlog formula itself.
void do_concurrent_compaction_test(cql_test_env& env, test_config& cfg) {
    using clk = std::chrono::steady_clock;

    // Create table with configured compaction strategy (same shape as the serial mode).
    sstring compaction_opts = fmt::format("'class': '{}'", cfg.compaction_strategy);
    for (auto& [k, v] : cfg.compaction_options) {
        compaction_opts += fmt::format(", '{}': '{}'", k, v);
    }

    sstring create_table_cql = fmt::format(
        "CREATE TABLE ks.perf_compaction ("
        "  pk bigint,"
        "  ck bigint,"
        "  v bigint,"
        "  PRIMARY KEY (pk, ck)"
        ") WITH compaction = {{{}}}"
        " AND tablets = {{'min_per_shard_tablet_count': '{}'}}"
        " AND tombstone_gc = {{'mode': 'immediate'}}",
        compaction_opts, cfg.tablets);

    if (cfg.default_time_to_live > 0) {
        create_table_cql += fmt::format(" AND default_time_to_live = {}",
                                         cfg.default_time_to_live);
    }

    env.execute_cql(create_table_cql).get();

    auto& cf = env.local_db().find_column_family("ks", "perf_compaction");
    auto s = cf.schema();

    // Prepared statements shared by all writer fibers.
    auto insert_id = env.prepare(
        "INSERT INTO ks.perf_compaction (pk, ck, v) VALUES (?, ?, ?)").get();
    auto delete_id = env.prepare(
        "DELETE FROM ks.perf_compaction WHERE pk = ? AND ck = ?").get();
    // Tiny sstables matter because of read amplification, so the benchmark has to read.
    // The table keeps an sstables-per-read histogram; these reads are what populate it.
    auto select_id = env.prepare(
        "SELECT v FROM ks.perf_compaction WHERE pk = ? AND ck = ?").get();

    metrics m;

    // Timing histogram and counters. Everything below runs on shard 0 (via cql_test_env),
    // so plain integers and a single histogram are safe: no cross-shard sharing.
    utils::estimated_histogram write_lat_hist;
    uint64_t writes_completed = 0;
    uint64_t reads_completed = 0;
    uint64_t writes_failed = 0;
    bool stop_requested = false;
    // Fibers below sleep for their whole interval, which can be much longer than the
    // time we're willing to spend shutting down. Abort the sleeps instead of waiting
    // them out, otherwise the run keeps flushing long after the writers are done.
    seastar::abort_source stop_as;

    // What the benchmark controls is how often a flush is *triggered*, together with the
    // write rate. How many of those triggers turn into completed flushes is a dependent
    // variable: as compaction takes more CPU, flushes take longer, and the achieved flush
    // rate drops on its own. That's expected, so it's measured rather than forced.
    //
    // Flushes may overlap up to a bound. Accounting stays correct regardless, because
    // sstables are counted by generation and a generation is only counted once however
    // many snapshots observe it. The bound exists so a slow flush path cannot pile up
    // unbounded concurrency; a trigger arriving when the bound is reached is dropped and
    // counted, and a high skip rate means the creation rate was set by flush latency
    // rather than by --flush-rate.
    uint64_t flush_triggers = 0;
    uint64_t flush_skipped = 0;
    uint64_t total_flushes = 0;
    constexpr unsigned max_flushes_in_flight = 8;
    unsigned flushes_in_flight = 0;
    seastar::gate flush_gate;

    // Every memtable-origin sstable this run produced, counted once. Tracking generations
    // instead of diffing sizes per flush means an sstable can never be counted twice, and
    // it gives the real number of sstables handed to compaction — which is the input the
    // runs need to share for a comparison to be fair, and which the flush trigger count
    // is not a substitute for.
    std::unordered_set<sstables::generation_type> flushed_generations;
    uint64_t total_flush_bytes = 0;
    auto observe_flushed = [&] (const sstable_set_t& snapshot) {
        for (const auto& [gen, bytes] : snapshot) {
            if (flushed_generations.insert(gen).second) {
                total_flush_bytes += bytes;
            }
        }
    };

    auto do_flush = [&] {
        if (flush_gate.is_closed()) {
            return;
        }
        ++flush_triggers;
        if (flushes_in_flight >= max_flushes_in_flight) {
            ++flush_skipped;
            return;
        }
        ++flushes_in_flight;
        (void)with_gate(flush_gate, [&] {
            return async([&] {
                replica::database::flush_table_on_all_shards(env.db(), "ks", "perf_compaction").get();
                // Observe immediately on completion, so this flush's own output is seen
                // before compaction can remove it.
                observe_flushed(get_sstable_set(env.db(), s, memtable_origin));
                ++total_flushes;
            }).handle_exception([] (std::exception_ptr) {
            }).finally([&] {
                --flushes_in_flight;
            });
        });
    };

    timer<> flusher;
    flusher.set_callback([&] { do_flush(); });

    // Time series sampled independently of (and more finely than) the stats printout,
    // so the percentiles below describe the whole run rather than the printed rows.
    //
    // sstable_count answers whether compaction keeps up with the flush rate: if it does,
    // the count oscillates around a plateau; if it doesn't, the count keeps climbing and
    // the second half's p90 is well above the first half's.
    //
    // shares and norm_backlog answer whether the controller is reacting at all. A run
    // where shares never changes, or sits at the first control point the whole time,
    // means the backlog signal never left the lazy region.
    // Fixed sampling period: fine enough to describe a run, not worth a knob.
    constexpr auto sample_interval = std::chrono::milliseconds(200);
    sample_series sstable_count_series;
    sample_series shares_series;
    sample_series norm_backlog_series;
    sample_series active_compactions_series;
    sample_series pending_compactions_series;
    auto sampler_fiber = seastar::async([&] {
        while (!stop_requested) {
            try {
                seastar::sleep_abortable(sample_interval, stop_as).get();
            } catch (const seastar::sleep_aborted&) {
                break;
            }
            if (stop_requested) {
                break;
            }
            sstable_count_series.add(get_sstable_count(env.db(), s));
            shares_series.add(get_compaction_shares_sum(env.db()));
            auto am = get_cm_available_memory(env.db());
            norm_backlog_series.add(am > 0 ? get_cm_backlog_sum(env.db()) / am : 0.0);
            auto [active, pending] = get_compaction_task_counts(env.db());
            active_compactions_series.add(double(active));
            pending_compactions_series.add(double(pending));
        }
    });

    // Periodic stats printer, run as a seastar::thread fiber (NOT a timer callback):
    // fetching the sstable count / backlog / shares requires cross-shard invocations
    // whose futures we then .get(), and .get() from a non-thread context aborts.
    // Emits a snapshot of the interesting quantities so the controller's behaviour
    // can be inspected over time. Includes both the raw backlog (bytes) and the
    // normalized backlog (backlog / available_memory), which is what the compaction
    // controller actually uses to interpolate CPU shares. The current compaction
    // scheduling group shares are also printed so you can see the controller
    // reacting to the backlog signal in real time.
    auto prev_time = clk::now();
    uint64_t prev_writes = 0;
    uint64_t prev_flushes = 0;
    auto avail_mem = get_cm_available_memory(env.db());
    fmt::print("\n=== Concurrent mode: per-{}ms samples (available_memory={:.1f} MiB) ===\n",
               cfg.stats_interval_ms, avail_mem / (1024.0 * 1024.0));
    fmt::print("{:>8} {:>10} {:>10} {:>10} {:>11} {:>14} {:>10} {:>8} {:>10} {:>10} {:>10}\n",
               "elapsed", "wr/s", "flush/s", "sstables", "avg_flush_kb", "backlog", "norm_bl", "shares",
               "wr_p50_us", "wr_p95_us", "wr_p99_us");
    auto prev_flush_bytes = uint64_t(0);
    auto stats_fiber = seastar::async([&] {
        while (!stop_requested) {
            try {
                seastar::sleep_abortable(std::chrono::milliseconds(cfg.stats_interval_ms), stop_as).get();
            } catch (const seastar::sleep_aborted&) {
                break;
            }
            if (stop_requested) {
                break;
            }
            auto now = clk::now();
            auto elapsed_since_prev = std::chrono::duration<double>(now - prev_time).count();
            auto elapsed_total = std::chrono::duration<double>(now - m.start_time).count();
            auto wr_rate = elapsed_since_prev > 0
                ? (writes_completed - prev_writes) / elapsed_since_prev : 0.0;
            auto flushes_since_prev = total_flushes - prev_flushes;
            auto flush_rate = elapsed_since_prev > 0
                ? flushes_since_prev / elapsed_since_prev : 0.0;
            auto bytes_since_prev = total_flush_bytes - prev_flush_bytes;
            auto avg_flush_kb = flushes_since_prev > 0
                ? bytes_since_prev / double(flushes_since_prev) / 1024.0 : 0.0;

            auto sst_count = get_sstable_count(env.db(), s);
            auto backlog = get_total_backlog(env.db(), s);
            // The controller normalises backlog per-shard by that shard's
            // available_memory, then interpolates control points to pick shares.
            // Reproduce that here for display purposes (single-shard runs only;
            // multi-shard sums are an upper bound).
            auto norm_backlog = avail_mem > 0
                ? get_cm_backlog_sum(env.db()) / avail_mem : 0.0;
            auto shares = get_compaction_shares_sum(env.db());

            fmt::print("{:>8.1f} {:>10.0f} {:>10.1f} {:>10} {:>11.1f} {:>14.2f} {:>10.3f} {:>8.1f} {:>10} {:>10} {:>10}\n",
                       elapsed_total, wr_rate, flush_rate, sst_count, avg_flush_kb,
                       backlog, norm_backlog, shares,
                       write_lat_hist.percentile(0.5),
                       write_lat_hist.percentile(0.95),
                       write_lat_hist.percentile(0.99));
            std::fflush(stdout);

            prev_time = now;
            prev_writes = writes_completed;
            prev_flushes = total_flushes;
            prev_flush_bytes = total_flush_bytes;
            // Reset the histogram so each sample reflects the last interval only.
            write_lat_hist.clear();
        }
    });

    // Writer fibers: all run on shard 0 (the app-template thread) and dispatch
    // writes/deletes through env.execute_prepared, which routes them to the owning
    // shard via the storage proxy. concurrent_writers fibers concurrently issue
    // requests, so compaction has to compete with foreground work for CPU shares.
    // Each fiber loops until stop_requested is set.
    // Write rate limiter. Without it the writers run flat out and the size of each
    // flushed sstable is whatever the machine happens to manage in a flush period,
    // which makes runs incomparable and can make it impossible for compaction to ever
    // catch up. Paced as a shared deadline advanced by one slot per operation, so the
    // aggregate rate is bounded regardless of the number of fibers.
    auto write_period = cfg.write_rate > 0
        ? std::chrono::duration_cast<clk::duration>(std::chrono::duration<double>(1.0 / cfg.write_rate))
        : clk::duration::zero();
    auto next_write_slot = clk::now();
    auto pace_write = [&] {
        if (write_period == clk::duration::zero()) {
            return;
        }
        auto now = clk::now();
        if (next_write_slot < now) {
            next_write_slot = now;
        }
        auto slot = next_write_slot;
        next_write_slot += write_period;
        if (slot > now) {
            seastar::sleep(slot - now).get();
        }
    };

    auto make_writer_fiber = [&] (unsigned fiber_idx) {
        return seastar::async([&, fiber_idx] {
            auto rng = std::mt19937_64(cfg.random_seed + fiber_idx);
            std::uniform_real_distribution<double> u01(0.0, 1.0);
            std::uniform_int_distribution<int64_t> pk_dist(0, cfg.partitions - 1);
            // Rows per partition is bounded, so keys repeat: writes overwrite earlier
            // ones and deletes land on rows that exist. With an ever-increasing clustering
            // key nothing is ever superseded, sstables never overlap, and compaction has
            // nothing to reclaim -- which makes space amplification and compaction
            // efficiency meaningless.
            constexpr int64_t rows_per_partition = 64;
            std::uniform_int_distribution<int64_t> ck_dist(0, rows_per_partition - 1);
            while (!stop_requested) {
                pace_write();
                if (stop_requested) {
                    break;
                }
                double r = u01(rng);
                int64_t pk = pk_dist(rng);
                int64_t ck = ck_dist(rng);
                int64_t v = static_cast<int64_t>(rng());

                // r partitions the operation mix: reads first, then deletes, then writes.
                // A read of a random key is what exercises read amplification.
                bool is_read = r < cfg.read_ratio;
                bool is_delete = !is_read && r < cfg.read_ratio + cfg.delete_ratio;
                bool is_rewrite = !is_read && !is_delete;

                auto t0 = clk::now();
                try {
                    if (is_read) {
                        env.execute_prepared(select_id, {
                            {cql3::raw_value::make_value(long_type->decompose(pk)),
                             cql3::raw_value::make_value(long_type->decompose(ck))}
                        }).get();
                        ++reads_completed;
                    } else if (is_delete) {
                        env.execute_prepared(delete_id, {
                            {cql3::raw_value::make_value(long_type->decompose(pk)),
                             cql3::raw_value::make_value(long_type->decompose(ck))}
                        }).get();
                        ++m.deletes;
                    } else {
                        env.execute_prepared(insert_id, {
                            {cql3::raw_value::make_value(long_type->decompose(pk)),
                             cql3::raw_value::make_value(long_type->decompose(ck)),
                             cql3::raw_value::make_value(long_type->decompose(v))}
                        }).get();
                        ++m.rewrites;
                        (void)is_rewrite;
                    }
                    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                                  clk::now() - t0).count();
                    write_lat_hist.add(us);
                    ++writes_completed;
                    ++m.total_operations;
                } catch (...) {
                    ++writes_failed;
                }
            }
        });
    };

    m.start_time = clk::now();
    prev_time = m.start_time;

    flusher.arm_periodic(std::chrono::milliseconds(std::max(1u, 1000 / cfg.flush_rate)));

    // Launch concurrent writer fibers and let them run for the configured duration.
    // The main coroutine sleeps for `duration`; fibers observe stop_requested between
    // iterations to terminate promptly.
    std::vector<future<>> writer_fibers;
    writer_fibers.reserve(cfg.concurrent_writers);
    for (unsigned i = 0; i < cfg.concurrent_writers; ++i) {
        writer_fibers.push_back(make_writer_fiber(i));
    }
    seastar::sleep(std::chrono::seconds(cfg.duration_in_seconds)).get();

    stop_requested = true;
    stop_as.request_abort();
    for (auto& f : writer_fibers) {
        f.get();
    }
    // Stop producing sstables before joining anything else, so nothing is attributed to
    // the run after the writers stopped.
    flusher.cancel();
    stats_fiber.get();
    sampler_fiber.get();
    // Wait for any in-flight background flushes to finish before mutating the local
    // metrics from the drain phase below.
    flush_gate.close().get();

    m.end_time = clk::now();

    // Drain: force one final flush and wait for outstanding compactions so the final
    // metrics reflect a settled state.
    {
        replica::database::flush_table_on_all_shards(env.db(), "ks", "perf_compaction").get();
        auto after_flush = get_sstable_set(env.db(), s, memtable_origin);
        observe_flushed(after_flush);
        m.flush_bytes_written += total_flush_bytes;
        m.total_flushes = total_flushes + 1;
        await_compactions(env.db(), s, m, after_flush);
    }

    auto final_sstable_count = get_sstable_count(env.db(), s);
    auto total_bytes_on_disk = get_total_bytes_on_disk(env.db(), s);

    // Major compaction so we can report space amplification.
    env.db().invoke_on_all([gs = global_schema_ptr(s)] (replica::database& db) {
        auto& cf = db.find_column_family(gs);
        auto& cm = db.get_compaction_manager();
        return cf.parallel_foreach_compaction_group_view([&cm] (compaction::compaction_group_view& v) {
            return cm.perform_major_compaction(v, tasks::task_info{});
        });
    }).get();
    auto major_compacted_bytes = get_total_bytes_on_disk(env.db(), s);

    fmt::print("\nWrites failed: {}\n", writes_failed);

    auto observed_flush_rate = m.duration_seconds() > 0 ? total_flushes / m.duration_seconds() : 0.0;
    auto trigger_rate = m.duration_seconds() > 0 ? flush_triggers / m.duration_seconds() : 0.0;
    auto sstables_created = flushed_generations.size();
    auto creation_rate = m.duration_seconds() > 0 ? sstables_created / m.duration_seconds() : 0.0;
    auto observed_flush_kb = sstables_created > 0 ? total_flush_bytes / double(sstables_created) / 1024.0 : 0.0;
    // The first control point's output is the shares the controller assigns at zero
    // backlog, i.e. the floor it sits at when it sees nothing to do.
    auto shares_floor = compaction_controller::default_control_points.front().output;
    auto shares_ceiling = compaction_controller::default_control_points.back().output;

    fmt::print("\n=== Flush, controller reaction and read amplification ===\n");
    // Triggers are the controlled input; flushes and skips are how the system responded.
    // sstables created is the real input to compaction, and the number two runs must
    // share before their sstable counts can be compared.
    fmt::print("Flush triggers: {} ({:.1f}/s)   completed: {} ({:.1f}/s)   skipped: {} ({:.1f}%)\n",
               flush_triggers, trigger_rate, total_flushes, observed_flush_rate,
               flush_skipped, flush_triggers ? 100.0 * flush_skipped / flush_triggers : 0.0);
    fmt::print("SSTables created by flush: {} ({:.1f}/s, avg {:.1f} KB, {:.1f} MB total)\n",
               sstables_created, creation_rate, observed_flush_kb,
               total_flush_bytes / 1048576.0);
    // Flush latency grows as compaction takes CPU, so a high skip rate means the sstable
    // creation rate was decided by the treatment rather than by the trigger cadence. Two
    // runs whose creation rates differ are not comparable on sstable count, however
    // similar their configuration looks: lower the trigger rate until skips are rare.
    if (flush_triggers && flush_skipped * 100 > flush_triggers * 10) {
        fmt::print("  WARNING: {:.1f}% of flush triggers were skipped; the sstable creation\n"
                   "           rate is limited by flush latency, not by --flush-rate.\n"
                   "           Compare sstables-created across runs before trusting sstable counts.\n",
                   100.0 * flush_skipped / flush_triggers);
    }
    fmt::print("Configured: {} flush triggers/s, {} ops/s, read ratio {:.2f}, {} tablet(s)/shard\n",
               cfg.flush_rate,
               cfg.write_rate ? fmt::format("{}", cfg.write_rate) : "unlimited",
               cfg.read_ratio, cfg.tablets);
    fmt::print("Samples: {} every {}ms\n", sstable_count_series.size(), sample_interval.count());
    fmt::print("\n");
    fmt::print("SSTable count (mean/p50/p90/p99/max): {:.1f}/{:.0f}/{:.0f}/{:.0f}/{:.0f}\n",
               sstable_count_series.mean(),
               sstable_count_series.percentile(50), sstable_count_series.percentile(90),
               sstable_count_series.percentile(99), sstable_count_series.max());
    fmt::print("  90% of the time sstable count was below {:.0f}\n",
               sstable_count_series.percentile(90));
    fmt::print("  p90 first half / second half: {:.0f} / {:.0f}{}\n",
               sstable_count_series.percentile_of_half(90, false),
               sstable_count_series.percentile_of_half(90, true),
               sstable_count_series.percentile_of_half(90, true) >
                   1.2 * sstable_count_series.percentile_of_half(90, false)
                   ? "   <-- compaction is falling behind" : "");
    fmt::print("\n");
    fmt::print("Normalized backlog (mean/p50/p90/p99/max): {:.3f}/{:.3f}/{:.3f}/{:.3f}/{:.3f}\n",
               norm_backlog_series.mean(),
               norm_backlog_series.percentile(50), norm_backlog_series.percentile(90),
               norm_backlog_series.percentile(99), norm_backlog_series.max());
    fmt::print("Compaction shares (mean/p50/p90/max): {:.1f}/{:.1f}/{:.1f}/{:.1f}\n",
               shares_series.mean(),
               shares_series.percentile(50), shares_series.percentile(90),
               shares_series.max());
    fmt::print("  changed {} times over {} samples ({:.1f}% at floor {:.0f}, {:.1f}% at max {:.0f})\n",
               shares_series.changes(), shares_series.size(),
               100.0 * shares_series.fraction_at(shares_floor), shares_floor,
               100.0 * shares_series.fraction_at(shares_ceiling), shares_ceiling);
    if (shares_series.changes() == 0 && !shares_series.empty()) {
        fmt::print("  WARNING: shares never changed, the controller did not react at all\n");
    }
    fmt::print("Active compactions (mean/p50/p90/max): {:.2f}/{:.0f}/{:.0f}/{:.0f}\n",
               active_compactions_series.mean(), active_compactions_series.percentile(50),
               active_compactions_series.percentile(90), active_compactions_series.max());
    fmt::print("Pending compactions (mean/p50/p90/max): {:.2f}/{:.0f}/{:.0f}/{:.0f}\n",
               pending_compactions_series.mean(), pending_compactions_series.percentile(50),
               pending_compactions_series.percentile(90), pending_compactions_series.max());

    // Read amplification, straight from the histogram the table maintains for
    // single-partition reads. This is the quantity tiny sstables actually hurt, and
    // sstable count is only a proxy for it.
    {
        auto& ra = env.local_db().find_column_family("ks", "perf_compaction")
                        .get_stats().estimated_sstable_per_read;
        fmt::print("\nReads: {}\n", reads_completed);
        if (reads_completed) {
            fmt::print("SSTables per read (p50/p90/p95/p99/max): {}/{}/{}/{}/{}\n",
                       ra.percentile(0.5), ra.percentile(0.9), ra.percentile(0.95),
                       ra.percentile(0.99), ra.max());
        }
    }

    // Read back what each compaction actually did, so the shares the controller handed
    // out can be compared against the work they bought.
    auto jobs = get_compaction_job_stats(env, "perf_compaction");
    // The summary below derives write amplification from compaction_bytes_written, which
    // is only populated by the serial mode's await_compactions(). Feed it the real number
    // so concurrent runs don't report a write amplification of zero.
    m.compaction_bytes_written = jobs.bytes_out;
    fmt::print("\nCompaction jobs: {}\n", jobs.jobs);
    if (jobs.jobs) {
        fmt::print("  bytes in/out: {:.1f} / {:.1f} MB\n",
                   jobs.bytes_in / 1048576.0, jobs.bytes_out / 1048576.0);
        fmt::print("  write amplification (compaction out / flushed): {:.2f}x\n",
                   m.flush_bytes_written > 0 ? jobs.bytes_out / double(m.flush_bytes_written) : 0.0);
        fmt::print("  fan-in (p50/p90/max): {:.0f}/{:.0f}/{:.0f}\n",
                   jobs.fan_in.percentile(50), jobs.fan_in.percentile(90), jobs.fan_in.max());
        fmt::print("  duration ms (p50/p90/max): {:.0f}/{:.0f}/{:.0f}\n",
                   jobs.duration_ms.percentile(50), jobs.duration_ms.percentile(90),
                   jobs.duration_ms.max());
    }

    if (cfg.output_format == "json") {
        Json::Value storm;
        storm["configured_flush_rate"] = cfg.flush_rate;
        storm["tablets"] = cfg.tablets;
        storm["read_ratio"] = cfg.read_ratio;
        storm["configured_write_rate"] = cfg.write_rate;
        storm["flush_triggers"] = Json::Value::UInt64(flush_triggers);
        storm["flush_skipped"] = Json::Value::UInt64(flush_skipped);
        storm["observed_flush_rate"] = fmt::format("{:.2f}", observed_flush_rate);
        storm["sstables_created"] = Json::Value::UInt64(sstables_created);
        storm["sstable_creation_rate"] = fmt::format("{:.2f}", creation_rate);
        storm["avg_created_sstable_kb"] = fmt::format("{:.2f}", observed_flush_kb);
        storm["flushed_bytes"] = Json::Value::UInt64(total_flush_bytes);
        storm["samples"] = Json::Value::UInt64(sstable_count_series.size());
        storm["sstable_count_mean"] = fmt::format("{:.2f}", sstable_count_series.mean());
        storm["sstable_count_p50"] = fmt::format("{:.0f}", sstable_count_series.percentile(50));
        storm["sstable_count_p90"] = fmt::format("{:.0f}", sstable_count_series.percentile(90));
        storm["sstable_count_p99"] = fmt::format("{:.0f}", sstable_count_series.percentile(99));
        storm["sstable_count_max"] = fmt::format("{:.0f}", sstable_count_series.max());
        storm["sstable_count_p90_first_half"] = fmt::format("{:.0f}", sstable_count_series.percentile_of_half(90, false));
        storm["sstable_count_p90_second_half"] = fmt::format("{:.0f}", sstable_count_series.percentile_of_half(90, true));
        storm["norm_backlog_mean"] = fmt::format("{:.4f}", norm_backlog_series.mean());
        storm["norm_backlog_p50"] = fmt::format("{:.4f}", norm_backlog_series.percentile(50));
        storm["norm_backlog_p90"] = fmt::format("{:.4f}", norm_backlog_series.percentile(90));
        storm["norm_backlog_p99"] = fmt::format("{:.4f}", norm_backlog_series.percentile(99));
        storm["norm_backlog_max"] = fmt::format("{:.4f}", norm_backlog_series.max());
        storm["shares_mean"] = fmt::format("{:.2f}", shares_series.mean());
        storm["shares_p50"] = fmt::format("{:.2f}", shares_series.percentile(50));
        storm["shares_p90"] = fmt::format("{:.2f}", shares_series.percentile(90));
        storm["shares_max"] = fmt::format("{:.2f}", shares_series.max());
        storm["shares_changes"] = Json::Value::UInt64(shares_series.changes());
        storm["shares_fraction_at_floor"] = fmt::format("{:.4f}", shares_series.fraction_at(shares_floor));
        storm["shares_fraction_at_max"] = fmt::format("{:.4f}", shares_series.fraction_at(shares_ceiling));
        storm["active_compactions_mean"] = fmt::format("{:.2f}", active_compactions_series.mean());
        storm["active_compactions_max"] = fmt::format("{:.0f}", active_compactions_series.max());
        storm["pending_compactions_mean"] = fmt::format("{:.2f}", pending_compactions_series.mean());
        storm["pending_compactions_max"] = fmt::format("{:.0f}", pending_compactions_series.max());
        storm["compaction_jobs"] = Json::Value::UInt64(jobs.jobs);
        storm["compaction_bytes_in"] = Json::Value::UInt64(jobs.bytes_in);
        storm["compaction_bytes_out"] = Json::Value::UInt64(jobs.bytes_out);
        storm["compaction_fan_in_p50"] = fmt::format("{:.0f}", jobs.fan_in.percentile(50));
        storm["compaction_fan_in_p90"] = fmt::format("{:.0f}", jobs.fan_in.percentile(90));
        storm["compaction_fan_in_max"] = fmt::format("{:.0f}", jobs.fan_in.max());
        storm["compaction_duration_ms_p50"] = fmt::format("{:.0f}", jobs.duration_ms.percentile(50));
        storm["compaction_duration_ms_p90"] = fmt::format("{:.0f}", jobs.duration_ms.percentile(90));
        storm["compaction_duration_ms_max"] = fmt::format("{:.0f}", jobs.duration_ms.max());
        m.write_json(cfg, final_sstable_count, total_bytes_on_disk, major_compacted_bytes, &storm);
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
        ("rewrite-ratio", bpo::value<double>()->default_value(0.25),
            "ratio of rewrites (0.0-1.0)")
        ("delete-ratio", bpo::value<double>()->default_value(0.25),
            "ratio of deletes (0.0-1.0)")
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
        ("compaction-options", bpo::value<std::string>()->default_value(""),
            "compaction strategy options as key=value,key=value (min_sstable_size=10000 added by default)")
        ("output-format", bpo::value<std::string>()->default_value("text"),
            "output format: text, json")
        ("verbose", bpo::bool_switch()->default_value(false),
            "print per-shard sstable state after each flush and compaction")
        ("concurrent-mode", bpo::bool_switch()->default_value(false),
            "run writer fibers against a periodic flush, so compaction runs concurrently "
            "with the write load and competes for CPU. The serial mode never lets that "
            "happen, so use this to measure anything about the compaction controller")
        ("concurrent-writers", bpo::value<unsigned>()->default_value(16),
            "number of concurrent writer fibers")
        ("write-rate", bpo::value<unsigned>()->default_value(0),
            "cap on operations per second across all writers (0 = unlimited). Fixing it "
            "is what makes two runs comparable")
        ("flush-rate", bpo::value<unsigned>()->default_value(10),
            "flush triggers per second. Only one flush runs at a time; a trigger arriving "
            "while one is in flight is dropped and counted. Lower it until skipped "
            "triggers are rare, otherwise the sstable creation rate is decided by flush "
            "latency (which grows with compaction load) instead of by this option")
        ("read-ratio", bpo::value<double>()->default_value(0.1),
            "fraction of operations that are single-partition reads, which is what "
            "populates the sstables-per-read histogram reported at the end")
        ("tablets", bpo::value<unsigned>()->default_value(1),
            "tablets per shard, i.e. compaction groups sharing the shard's backlog. Tiny "
            "sstables spread over many groups is the case worth measuring")
        ("compaction-static-shares", bpo::value<float>()->default_value(0),
            "pin the compaction scheduling group to this many shares, disabling the "
            "backlog controller (0 = controller enabled). Use it to separate what shares "
            "buy from what the backlog formula does")
        ("stats-interval-ms", bpo::value<unsigned>()->default_value(1000),
            "period between per-interval stats printouts in concurrent mode")
        ("available-memory-mb", bpo::value<unsigned>()->default_value(0),
            "override the compaction manager's available_memory (0 = machine default). "
            "The controller divides backlog by this, so on a machine with lots of RAM a "
            "realistic backlog normalises to ~0 and the controller never leaves its "
            "minimum shares")
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

            // If the user asked to shrink the compaction manager's view of available
            // memory (used by the controller to normalise backlog), install a custom
            // database_config with that value. Everything else is left unset so the test
            // env keeps its defaults; scheduling groups are overwritten by cql_test_env.
            // Pin the compaction scheduling group's shares, bypassing the controller.
            // Lets the effect of shares on compaction be measured on its own, without the
            // backlog formula in the loop.
            auto static_shares = app.configuration()["compaction-static-shares"].as<float>();
            if (static_shares > 0) {
                db_cfg->compaction_static_shares(static_shares);
            }
            auto available_memory_mb = app.configuration()["available-memory-mb"].as<unsigned>();
            if (available_memory_mb > 0) {
                replica::database_config dbcfg;
                dbcfg.available_memory = size_t(available_memory_mb) * 1024 * 1024;
                cql_cfg.dbcfg = std::move(dbcfg);
            }

            do_with_cql_env_thread([&app, seed] (auto& env) {
                test_config cfg;
                cfg.partitions = app.configuration()["partitions"].as<unsigned>();
                cfg.operations = app.configuration()["operations"].as<uint64_t>();
                cfg.duration_in_seconds = app.configuration()["duration"].as<unsigned>();
                cfg.warmup_operations = app.configuration()["warmup-operations"].as<uint64_t>();
                cfg.warmup_duration = app.configuration()["warmup-duration"].as<unsigned>();
                cfg.rewrite_ratio = app.configuration()["rewrite-ratio"].as<double>();
                cfg.delete_ratio = app.configuration()["delete-ratio"].as<double>();
                cfg.distribution = app.configuration()["distribution"].as<std::string>();
                cfg.random_seed = seed;
                cfg.read_frequency = app.configuration()["read-frequency"].as<unsigned>();
                cfg.default_time_to_live = app.configuration()["default-time-to-live"].as<unsigned>();
                cfg.compaction_strategy = app.configuration()["compaction-strategy"].as<std::string>();
                cfg.min_flush_interval = app.configuration()["min-flush-interval"].as<unsigned>();
                cfg.max_flush_interval = app.configuration()["max-flush-interval"].as<unsigned>();
                // Parse compaction options from key=value,key=value format
                auto opts_str = app.configuration()["compaction-options"].as<std::string>();
                if (!opts_str.empty()) {
                    std::istringstream iss(opts_str);
                    std::string pair;
                    while (std::getline(iss, pair, ',')) {
                        auto eq = pair.find('=');
                        if (eq == std::string::npos) {
                            throw std::invalid_argument(fmt::format("invalid compaction option (expected key=value): {}", pair));
                        }
                        cfg.compaction_options[sstring(pair.substr(0, eq))] = sstring(pair.substr(eq + 1));
                    }
                }
                // Add default min_sstable_size if not explicitly provided
                if (!cfg.compaction_options.contains("min_sstable_size")) {
                    cfg.compaction_options["min_sstable_size"] = "10000";
                }
                cfg.output_format = app.configuration()["output-format"].as<std::string>();
                cfg.verbose = app.configuration()["verbose"].as<bool>();
                cfg.concurrent_mode = app.configuration()["concurrent-mode"].as<bool>();
                cfg.concurrent_writers = app.configuration()["concurrent-writers"].as<unsigned>();
                cfg.stats_interval_ms = app.configuration()["stats-interval-ms"].as<unsigned>();
                cfg.available_memory_mb = app.configuration()["available-memory-mb"].as<unsigned>();
                cfg.write_rate = app.configuration()["write-rate"].as<unsigned>();
                cfg.flush_rate = app.configuration()["flush-rate"].as<unsigned>();
                cfg.read_ratio = app.configuration()["read-ratio"].as<double>();
                cfg.tablets = app.configuration()["tablets"].as<unsigned>();
                cfg.compaction_static_shares = app.configuration()["compaction-static-shares"].as<float>();
                if (cfg.concurrent_mode) {
                    if (cfg.flush_rate == 0) {
                        throw std::invalid_argument("--flush-rate must be > 0");
                    }
                    if (cfg.tablets == 0) {
                        throw std::invalid_argument("--tablets must be > 0");
                    }
                    if (cfg.read_ratio < 0 || cfg.read_ratio >= 1.0) {
                        throw std::invalid_argument("--read-ratio must be in [0, 1)");
                    }
                    if (cfg.read_ratio + cfg.delete_ratio >= 1.0) {
                        throw std::invalid_argument("--read-ratio plus --delete-ratio must be < 1");
                    }
                }
                if (cfg.output_format != "text" && cfg.output_format != "json") {
                    throw std::invalid_argument(fmt::format("invalid value for output-format: {}", cfg.output_format));
                }
                if (cfg.distribution != "gaussian" && cfg.distribution != "uniform" && cfg.distribution != "zipfian") {
                    throw std::invalid_argument(fmt::format("invalid value for distribution: {}", cfg.distribution));
                }
                if (cfg.operations == 0 && cfg.duration_in_seconds == 0 && !cfg.concurrent_mode) {
                    throw std::invalid_argument("at least one of --operations or --duration must be non-zero");
                }
                if (cfg.concurrent_mode && cfg.duration_in_seconds == 0) {
                    throw std::invalid_argument("--concurrent-mode requires --duration");
                }
                if (cfg.concurrent_mode && cfg.concurrent_writers == 0) {
                    throw std::invalid_argument("--concurrent-writers must be > 0 in concurrent mode");
                }
                if (cfg.rewrite_ratio < 0 || cfg.rewrite_ratio > 1.0) {
                    throw std::invalid_argument(fmt::format("rewrite-ratio must be in [0, 1], got {}", cfg.rewrite_ratio));
                }
                if (cfg.delete_ratio < 0 || cfg.delete_ratio > 1.0) {
                    throw std::invalid_argument(fmt::format("delete-ratio must be in [0, 1], got {}", cfg.delete_ratio));
                }
                if (cfg.rewrite_ratio + cfg.delete_ratio > 1.0) {
                    throw std::invalid_argument(fmt::format("rewrite-ratio + delete-ratio must be <= 1.0, got {}", cfg.rewrite_ratio + cfg.delete_ratio));
                }

                if (cfg.output_format == "text") {
                    fmt::print("Compaction efficiency benchmark\n");
                    fmt::print("  partitions: {}\n", cfg.partitions);
                    fmt::print("  operations: {}\n", cfg.operations);
                    fmt::print("  duration: {}s\n", cfg.duration_in_seconds);
                    fmt::print("  rewrite-ratio: {}\n", cfg.rewrite_ratio);
                    fmt::print("  delete-ratio: {}\n", cfg.delete_ratio);
                    fmt::print("  distribution: {}\n", cfg.distribution);
                    fmt::print("  read-frequency: {}\n", cfg.read_frequency);
                    fmt::print("  compaction-strategy: {}\n", cfg.compaction_strategy);
                    fmt::print("  min-flush-interval: {}\n", cfg.min_flush_interval);
                    fmt::print("  max-flush-interval: {}\n", cfg.max_flush_interval);
                    for (auto& [k, v] : cfg.compaction_options) {
                        fmt::print("  compaction.{}={}\n", k, v);
                    }
                    if (cfg.default_time_to_live > 0) {
                        fmt::print("  default-time-to-live: {}s\n", cfg.default_time_to_live);
                    }
                    if (cfg.concurrent_mode) {
                        fmt::print("  concurrent-mode: true\n");
                        fmt::print("  concurrent-writers: {}\n", cfg.concurrent_writers);
                        fmt::print("  write-rate: {}\n", cfg.write_rate);
                        fmt::print("  flush-rate: {}\n", cfg.flush_rate);
                        fmt::print("  read-ratio: {}\n", cfg.read_ratio);
                        fmt::print("  tablets (per shard): {}\n", cfg.tablets);
                        fmt::print("  stats-interval-ms: {}\n", cfg.stats_interval_ms);
                        if (cfg.available_memory_mb > 0) {
                            fmt::print("  available-memory-mb (compaction controller): {}\n",
                                       cfg.available_memory_mb);
                        }
                        if (cfg.compaction_static_shares > 0) {
                            fmt::print("  compaction-static-shares: {} (controller disabled)\n",
                                       cfg.compaction_static_shares);
                        }
                    }
                }

                if (cfg.concurrent_mode) {
                    do_concurrent_compaction_test(env, cfg);
                } else {
                    do_compaction_efficiency_test(env, cfg);
                }
            }, std::move(cql_cfg)).get();
        });
    });
}

} // namespace perf
