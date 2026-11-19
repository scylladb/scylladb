/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Cost of repair building rows from disk (read, hash, freeze) with repair's own code; rows are
// dropped as built, not buffered. Compare builds of two commits for a change's effect.

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <cstring>
#include <limits>
#include <random>
#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/util/closeable.hh>
#include <seastar/testing/linux_perf_event.hh>
#include <fmt/core.h>

#include "test/lib/cql_test_env.hh"
#include "test/lib/data_model.hh"
#include "test/lib/random_schema.hh"
#include "replica/database.hh"
#include "db/config.hh"
#include "compaction/compaction_strategy.hh"
#include "dht/token-sharding.hh"
#include "repair/hash.hh"
#include "repair/incremental.hh"
#include "repair/reader.hh"
#include "repair/row.hh"
#include "repair/row_level.hh"
#include "types/map.hh"
#include "types/types.hh"

namespace {

constexpr uint64_t seed = 0x5eed;
constexpr size_t MB = 1024 * 1024;
// Per value beyond its bytes: key, timestamp and the test description's own struct.
constexpr uint64_t value_overhead = 128;
constexpr size_t narrow_columns = 5;
constexpr uint64_t narrow_row_bytes = narrow_columns * 16;

struct pass_result {
    std::chrono::duration<double, std::milli> elapsed;
    uint64_t instructions = 0; // user-space, whole pass
    uint64_t rows = 0;
    uint64_t bytes = 0; // repair_row size, only when building rows
    uint64_t sink = 0;  // order-dependent mix of row hashes; equal across builds of the same data
};

// read includes the partition-key hash, which repair_reader computes as it reads.
enum class stage { read, hash, row };

pass_result run_pass(linux_perf_event& instructions, cql_test_env& env, replica::column_family& cf, schema_ptr s, stage st) {
    auto& db = env.local_db();
    auto permit = db.obtain_reader_permit(cf, "perf-repair-rows", db::no_timeout, {}).get();
    const dht::static_sharder sharder;
    repair_reader rd(env.db(), cf, s, std::move(permit), dht::token_range::make_open_ended_both_sides(), sharder, this_shard_id(), seed,
            repair_reader::read_strategy::local, gc_clock::now(), incremental_repair_meta{}, 0, false);
    auto close_rd = deferred_close(rd);
    repair_hasher hasher(seed, s);
    pass_result r;
    auto start = std::chrono::steady_clock::now();
    auto start_instructions = instructions.read();
    while (auto mfo = rd.read_mutation_fragment().get()) {
        rd.track_partition(*mfo);
        if (!makes_repair_row(*mfo)) {
            continue;
        }
        ++r.rows;
        if (st == stage::hash) {
            r.sink = std::rotl(r.sink, 1) ^ hasher.do_hash_for_mf(*rd.get_current_dk(), *mfo).hash;
        } else if (st == stage::row) {
            auto row = make_repair_row_from_disk(rd.get_current_dk(), hasher, *s, *mfo);
            r.sink = std::rotl(r.sink, 1) ^ row.hash().hash;
            r.bytes += row.size();
        }
    }
    r.elapsed = std::chrono::steady_clock::now() - start;
    r.instructions = instructions.read() - start_instructions;
    return r;
}

// Fixed column types, so each shape is the workload it names.
class shape_spec : public tests::random_schema_specification {
    std::vector<data_type> _regular;
    compress_sstable _compress = compress_sstable::yes;

public:
    explicit shape_spec(std::vector<data_type> regular)
        : random_schema_specification("ks")
        , _regular(std::move(regular)) {
    }
    sstring table_name(std::mt19937&) override {
        return "t";
    }
    sstring udt_name(std::mt19937&) override {
        return "unused";
    }
    std::vector<data_type> partition_key_columns(std::mt19937&) override {
        return {int32_type};
    }
    std::vector<data_type> clustering_key_columns(std::mt19937&) override {
        return {int32_type};
    }
    std::vector<data_type> regular_columns(std::mt19937&) override {
        return _regular;
    }
    std::vector<data_type> static_columns(std::mt19937&) override {
        return {};
    }
    compress_sstable& compress() override {
        return _compress;
    }
};

schema_ptr populate(cql_test_env& env, const std::string& shape, uint64_t rows, unsigned elements, unsigned value_size) {
    shape_spec spec(shape == "narrow" ? std::vector<data_type>(narrow_columns, int32_type)
                    : shape == "wide" ? std::vector<data_type>{map_type_impl::get_instance(int32_type, bytes_type, true)}
                                      : std::vector<data_type>{bytes_type});
    tests::random_schema rs(seed, spec);
    rs.create_with_cql(env).get();
    auto s = rs.schema();
    auto& db = env.local_db();
    auto& cf = db.find_column_family(s->id());
    cf.set_compaction_strategy(compaction::compaction_strategy_type::null);

    // random_schema's value generator caps collections at 16 elements, so values are built here.
    const uint64_t rows_per_partition = shape == "bigpartition" ? rows : (shape == "wide" ? 1 : 100);
    std::mt19937_64 rng(seed);
    // Fresh bytes per value; a repeated value would compress unrealistically well.
    auto next_blob = [&] {
        bytes b(bytes::initialized_later(), value_size);
        for (size_t i = 0; i < b.size(); i += sizeof(uint64_t)) {
            const uint64_t v = rng();
            std::memcpy(b.data() + i, &v, std::min(sizeof(v), b.size() - i));
        }
        return b;
    };
    std::vector<sstring> columns;
    for (const auto& c : s->regular_columns()) {
        columns.push_back(c.name_as_text());
    }

    std::optional<tests::data_model::mutation_description> md;
    int32_t current_pk = -1;
    uint64_t unflushed = 0;
    auto apply = [&] {
        db.apply(s, freeze(md->build(s)), {}, db::commitlog_force_sync::no, db::no_timeout).get();
        md.reset();
    };
    for (uint64_t i = 0; i < rows; ++i) {
        int32_t pk = i / rows_per_partition;
        int32_t ck = i % rows_per_partition;
        if (md && pk != current_pk) {
            apply();
        }
        if (!md) {
            md.emplace(tests::data_model::mutation_description::key{int32_type->decompose(pk)});
            current_pk = pk;
        }
        const tests::data_model::mutation_description::key ckey{int32_type->decompose(ck)};
        if (shape == "narrow") {
            for (const auto& c : columns) {
                md->add_clustered_cell(ckey, c, tests::data_model::mutation_description::atomic_value(int32_type->decompose(int32_t(i))));
            }
            unflushed += narrow_row_bytes;
        } else if (shape == "wide") {
            tests::data_model::mutation_description::collection m;
            m.elements.reserve(elements);
            for (unsigned e = 0; e < elements; ++e) {
                m.elements.push_back({int32_type->decompose(int32_t(e)), {next_blob()}});
                seastar::thread::maybe_yield();
            }
            md->add_clustered_cell(ckey, columns[0], std::move(m));
            unflushed += uint64_t(elements) * (value_size + value_overhead);
        } else {
            md->add_clustered_cell(ckey, columns[0], tests::data_model::mutation_description::atomic_value(next_blob()));
            unflushed += value_size + value_overhead;
        }
        if (unflushed >= 64 * MB) {
            apply();
            cf.flush().get();
            unflushed = 0;
        }
        seastar::thread::maybe_yield();
    }
    if (md) {
        apply();
    }
    // One sstable, so the read cost does not depend on how many flushes populate did.
    cf.compact_all_sstables(tasks::make_empty_task_info()).get();
    return s;
}

} // namespace

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("shape", bpo::value<std::string>()->default_value("narrow"), "narrow | wide | bigpartition")
        ("rows", bpo::value<uint64_t>(), "total CQL rows (default 200000, 50 for wide)")
        ("elements", bpo::value<unsigned>()->default_value(50000), "map elements per row (wide)")
        ("value-size", bpo::value<unsigned>()->default_value(32), "cell value size in bytes (wide, bigpartition)")
        ("iterations", bpo::value<unsigned>()->default_value(3), "runs per pass; the best is reported")
        ;
    return app.run(argc, argv, [&app] {
        if (this_smp_shard_count() != 1) {
            throw std::runtime_error("run with --smp=1");
        }
        auto cfg_ptr = make_shared<db::config>();
        cfg_ptr->enable_commitlog(false);
        cfg_ptr->enable_cache(false);
        cfg_ptr->sstable_format("ms");
        cql_test_config cfg(cfg_ptr);
        return do_with_cql_env_thread(
                [&app](cql_test_env& env) {
                    const auto& c = app.configuration();
                    auto shape = c["shape"].as<std::string>();
                    auto rows = c.count("rows") ? c["rows"].as<uint64_t>() : (shape == "wide" ? 50 : 200000);
                    auto elements = c["elements"].as<unsigned>();
                    auto value_size = c["value-size"].as<unsigned>();
                    auto iterations = c["iterations"].as<unsigned>();
                    if (shape != "narrow" && shape != "wide" && shape != "bigpartition") {
                        throw std::runtime_error("unknown shape: " + shape);
                    }
                    if (rows == 0 || iterations == 0 || (shape == "wide" && elements == 0)) {
                        throw std::runtime_error("--rows and --iterations must be positive, and --elements too for wide");
                    }
                    if (rows > uint64_t(std::numeric_limits<int32_t>::max())) {
                        throw std::runtime_error("--rows must fit in an int32 clustering key");
                    }
                    // A row lives in several copies at once (description, mutation, frozen, memtable).
                    const uint64_t row_bytes =
                            shape == "narrow" ? narrow_row_bytes : (shape == "wide" ? uint64_t(elements) : 1) * (uint64_t(value_size) + value_overhead);
                    if (row_bytes > 256'000'000 || rows * row_bytes > 64'000'000'000) {
                        throw std::runtime_error("data set too large: keep a row under 256 MB and the total under 64 GB");
                    }

                    auto s = populate(env, shape, rows, elements, value_size);
                    auto& cf = env.local_db().find_column_family(s->id());

                    // Passes run round-robin so frequency and thermal drift hit every
                    // variant alike; each keeps its fastest run.
                    auto instructions = linux_perf_event::user_instructions_retired();
                    instructions.enable();
                    constexpr auto inf = std::chrono::duration<double, std::milli>::max();
                    std::array<pass_result, 3> best{pass_result{inf}, pass_result{inf}, pass_result{inf}};
                    // Repair reads under the streaming scheduling group, which picks its read semaphore.
                    const auto streaming_sg = get_scheduling_groups().get().streaming_scheduling_group;
                    for (unsigned i = 0; i < iterations; ++i) {
                        std::array<pass_result, 3> cur;
                        // Rotate the starting pass so no stage always runs first, against a cold cache.
                        for (size_t k = 0; k < cur.size(); ++k) {
                            const auto st = (i + k) % cur.size();
                            cur[st] = with_scheduling_group(streaming_sg, [&] {
                                return seastar::async([&] {
                                    return run_pass(instructions, env, cf, s, static_cast<stage>(st));
                                });
                            }).get();
                            if (cur[st].elapsed < best[st].elapsed) {
                                best[st] = cur[st];
                            }
                        }
                    }
                    if (best[1].sink != best[2].sink) {
                        throw std::runtime_error("row hashes differ between the hash and row passes");
                    }

                    // Print only the parameters the shape uses.
                    const auto params = shape == "wide"           ? fmt::format(" elements={} value_size={}", elements, value_size)
                                        : shape == "bigpartition" ? fmt::format(" value_size={}", value_size)
                                                                  : std::string();
                    // digest must match across builds being compared, or they hashed different data.
                    fmt::print("shape={} rows={}{} sstables={} row_bytes={:.1f}MB digest={:016x}\n", shape, best[0].rows, params, cf.sstables_count(),
                            double(best[2].bytes) / MB, best[1].sink);
                    auto line = [&](const char* name, const pass_result& r) {
                        // Zero means perf_event_open failed (e.g. perf_event_paranoid), not a free pass.
                        // Includes reactor polling while reads are in flight, so compare passes rather than read it as CPU per row.
                        const auto insns = r.instructions ? fmt::format("{:.0f}", double(r.instructions) / r.rows) : std::string("-");
                        fmt::print(
                                "{:<16} {:>10.1f} ms {:>12.0f} rows/s {:>10} insns/row\n", name, r.elapsed.count(), r.rows / (r.elapsed.count() / 1000), insns);
                    };
                    line("read", best[0]);
                    line("read+hash", best[1]);
                    line("read+hash+row", best[2]);
                },
                cfg)
                .then([] {
                    return 0;
                });
    });
}
