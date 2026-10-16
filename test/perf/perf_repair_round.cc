/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Cost of the survivor side of one repair round, split into three stages
// measured as cumulative passes over the same sstables: streaming read
// (read + parse), plus per-row hashing as repair does it, plus freezing.
// Differences between passes give the per-stage cost.
//
//   build/release/test/perf/perf_repair_round -c1 -m4G --shape wide --rows 50 --elements 50000

#include <chrono>
#include <algorithm>
#include <random>
#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/closeable.hh>
#include <fmt/core.h>

#include "test/lib/cql_test_env.hh"
#include "replica/database.hh"
#include "db/config.hh"
#include "compaction/compaction_strategy.hh"
#include "dht/i_partitioner.hh"
#include "mutation/mutation.hh"
#include "mutation/frozen_mutation.hh"
#include "mutation/collection_mutation.hh"
#include "mutation/mutation_fragment.hh"
#include "readers/mutation_fragment_v1_stream.hh"
#include "utils/hashing.hh"
#include "utils/xx_hasher.hh"
#include "types/types.hh"

namespace {

constexpr uint64_t seed = 0x5eed;
constexpr size_t MB = 1024 * 1024;

struct pass_result {
    std::chrono::duration<double, std::milli> elapsed;
    uint64_t rows = 0;
    uint64_t bytes = 0; // frozen size, only when freezing
    uint64_t sink = 0;  // xor of digests; compared across hashers
};

template <typename Hasher>
pass_result run_pass(replica::database& db, replica::column_family& cf, schema_ptr s, bool do_hash, bool do_freeze) {
    auto permit = db.obtain_reader_permit(cf, "perf-repair-round", db::no_timeout, {}).get();
    auto rd = mutation_fragment_v1_stream(cf.make_streaming_reader(s, std::move(permit), query::full_partition_range, gc_clock::now()));
    auto close_rd = deferred_close(rd);
    pass_result r;
    uint64_t pk_hash = 0;
    auto start = std::chrono::steady_clock::now();
    while (auto mfo = rd().get()) {
        auto& mf = *mfo;
        if (mf.is_partition_start()) {
            // Same as repair's decorated_key_with_hash.
            xx_hasher h(seed);
            feed_hash(h, mf.as_partition_start().key().key(), *s);
            pk_hash = h.finalize_uint64();
            if (!mf.as_partition_start().partition_tombstone()) {
                continue;
            }
        } else if (mf.is_end_of_partition()) {
            continue;
        }
        ++r.rows;
        if (do_hash) {
            Hasher h(seed);
            feed_hash(h, mf, *s);
            feed_hash(h, pk_hash);
            r.sink ^= h.finalize_uint64();
        }
        if (do_freeze) {
            r.bytes += freeze(*s, mf).representation().size();
        }
    }
    r.elapsed = std::chrono::steady_clock::now() - start;
    return r;
}

template <typename Hasher>
pass_result best_of(unsigned iterations, replica::database& db, replica::column_family& cf, schema_ptr s, bool do_hash, bool do_freeze) {
    pass_result best{.elapsed = std::chrono::duration<double, std::milli>::max()};
    for (unsigned i = 0; i < iterations; ++i) {
        best = std::min(best, run_pass<Hasher>(db, cf, s, do_hash, do_freeze), [] (const pass_result& a, const pass_result& b) { return a.elapsed < b.elapsed; });
    }
    return best;
}

void populate(cql_test_env& env, const std::string& shape, uint64_t rows, unsigned elements, unsigned value_size) {
    if (shape == "narrow") {
        env.execute_cql("CREATE TABLE ks.t (pk int, ck int, v1 int, v2 int, v3 int, v4 int, v5 int, PRIMARY KEY (pk, ck))").get();
    } else if (shape == "wide") {
        env.execute_cql("CREATE TABLE ks.t (pk int, ck int, m map<int, blob>, PRIMARY KEY (pk, ck))").get();
    } else if (shape == "bigpartition") {
        env.execute_cql("CREATE TABLE ks.t (pk int, ck int, v blob, PRIMARY KEY (pk, ck))").get();
    } else {
        throw std::runtime_error("unknown shape: " + shape);
    }
    auto& db = env.local_db();
    auto s = db.find_schema("ks", "t");
    auto& cf = db.find_column_family(s->id());
    cf.set_compaction_strategy(compaction::compaction_strategy_type::null);

    const uint64_t rows_per_partition = shape == "bigpartition" ? rows : (shape == "wide" ? 1 : 100);
    const auto ts = api::new_timestamp();
    bytes blob(bytes::initialized_later(), value_size);
    std::generate(blob.begin(), blob.end(), std::mt19937_64(seed));
    const column_definition* mdef = shape == "wide" ? s->get_column_definition(to_bytes("m")) : nullptr;
    const column_definition* vdef = shape == "bigpartition" ? s->get_column_definition(to_bytes("v")) : nullptr;

    std::optional<mutation> m;
    int32_t current_pk = -1;
    uint64_t unflushed = 0;
    auto apply = [&] {
        db.apply(s, freeze(*m), {}, db::commitlog_force_sync::no, db::no_timeout).get();
        m.reset();
    };
    for (uint64_t i = 0; i < rows; ++i) {
        int32_t pk = i / rows_per_partition;
        int32_t ck = i % rows_per_partition;
        if (!m || pk != current_pk) {
            if (m) {
                apply();
            }
            m.emplace(s, partition_key::from_single_value(*s, int32_type->decompose(pk)));
            current_pk = pk;
        }
        auto ckey = clustering_key::from_single_value(*s, int32_type->decompose(ck));
        if (shape == "narrow") {
            for (auto name : {"v1", "v2", "v3", "v4", "v5"}) {
                m->set_clustered_cell(ckey, *s->get_column_definition(to_bytes(name)), atomic_cell::make_live(*int32_type, ts, int32_type->decompose(int32_t(i))));
            }
            unflushed += 5 * 16;
        } else if (shape == "wide") {
            collection_mutation_writer w({});
            for (unsigned e = 0; e < elements; ++e) {
                w.push_back(managed_bytes_view(int32_type->decompose(int32_t(e))), atomic_cell::make_live(*bytes_type, ts, blob, atomic_cell::collection_member::yes));
            }
            m->set_clustered_cell(ckey, *mdef, atomic_cell_or_collection(std::move(w).finish()));
            unflushed += uint64_t(elements) * (value_size + 24);
        } else {
            m->set_clustered_cell(ckey, *vdef, atomic_cell::make_live(*bytes_type, ts, blob));
            unflushed += value_size + 24;
        }
        if (unflushed >= 64 * MB) {
            apply();
            cf.flush().get();
            unflushed = 0;
        }
        seastar::thread::maybe_yield();
    }
    if (m) {
        apply();
    }
    cf.flush().get();
}

} // namespace

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("shape", bpo::value<std::string>()->default_value("narrow"), "narrow | wide | bigpartition")
        ("rows", bpo::value<uint64_t>()->default_value(200000), "total CQL rows")
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
        return do_with_cql_env_thread([&app] (cql_test_env& env) {
            const auto& c = app.configuration();
            auto shape = c["shape"].as<std::string>();
            auto rows = c["rows"].as<uint64_t>();
            auto elements = c["elements"].as<unsigned>();
            auto value_size = c["value-size"].as<unsigned>();
            auto iterations = c["iterations"].as<unsigned>();

            populate(env, shape, rows, elements, value_size);
            auto& db = env.local_db();
            auto s = db.find_schema("ks", "t");
            auto& cf = db.find_column_family(s->id());

            auto read = best_of<xx_hasher>(iterations, db, cf, s, false, false);
            auto hash_plain = best_of<xx_hasher>(iterations, db, cf, s, true, false);
            auto hash_buf = best_of<buffered_xx_hasher>(iterations, db, cf, s, true, false);
            auto all_plain = best_of<xx_hasher>(iterations, db, cf, s, true, true);
            auto all_buf = best_of<buffered_xx_hasher>(iterations, db, cf, s, true, true);
            if (hash_plain.sink != hash_buf.sink || all_plain.sink != all_buf.sink) {
                throw std::runtime_error("buffered_xx_hasher digest differs from xx_hasher");
            }

            const double mb = double(all_buf.bytes) / MB;
            fmt::print("shape={} rows={} elements={} value_size={} sstables={} frozen_bytes={:.1f}MB\n",
                    shape, read.rows, elements, value_size, cf.sstables_count(), mb);
            auto line = [&] (const char* name, const pass_result& r) {
                fmt::print("{:<28} {:>10.1f} ms {:>9.1f} MB/s {:>12.0f} rows/s\n", name, r.elapsed.count(), mb / (r.elapsed.count() / 1000), r.rows / (r.elapsed.count() / 1000));
            };
            line("read", read);
            line("read+hash (xx_hasher)", hash_plain);
            line("read+hash (buffered)", hash_buf);
            line("read+hash+freeze (xx_hasher)", all_plain);
            line("read+hash+freeze (buffered)", all_buf);
            auto pct = [&] (double part, double whole) { return 100.0 * part / whole; };
            fmt::print("stage share of a round (buffered): read {:.0f}% hash {:.0f}% freeze {:.0f}%\n",
                    pct(read.elapsed.count(), all_buf.elapsed.count()),
                    pct(hash_buf.elapsed.count() - read.elapsed.count(), all_buf.elapsed.count()),
                    pct(all_buf.elapsed.count() - hash_buf.elapsed.count(), all_buf.elapsed.count()));
        }, cfg).then([] { return 0; });
    });
}
