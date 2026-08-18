/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

//
// Benchmark for sstables::sstable_set as built by make_partitioned_sstable_set().
//
// MOTIVATION
//
// A compaction group whose compaction cannot keep up with ingestion accumulates
// tens of thousands of sstables, and the set that indexes them has been observed
// to dominate the node's memory (SCYLLADB-3790). This benchmark builds such a
// set from a synthetic workload and reports its footprint and the cost of its
// operations, so that changes to the implementation can be compared against a
// recorded baseline.
//
// THE WORKLOAD
//
// The interesting property of the workload is its *overlap depth*: how many
// sstables cover a typical token. Depth is what a range index has to represent,
// and it is independent of how many sstables there are.
//
//   - Two thirds of the sstables are emitted as runs of `--fragments-per-run`
//     mutually disjoint fragments, each run spanning the compaction group's
//     whole token range. Fragment boundaries are randomized per run, so they
//     stagger across runs and depth grows with the number of runs. This models
//     a compaction group with a large backlog of runs. Fewer fragments per run
//     means wider fragments and greater depth; `--fragment-width-spread` varies
//     the fragment count between runs so that widths are not uniform.
//   - One third span nearly the whole token range, as sstables flushed from a
//     memtable do. `--wide-fraction` controls the mix.
//
// Ranges are generated in index space over a sorted pool of partition keys, so
// the overlap structure is exact and fully determined by the seed. --random-seed
// pins it; when unset a seed is drawn at random and echoed as "random-seed=N",
// which replays that run.
//
// WHAT IT MEASURES
//
//   - heap footprint and live allocation count of the populated set, measured
//     separately from the footprint of the sstable objects themselves
//   - the number of (sub-range, sstable) incidences the workload implies,
//     derived from the generated ranges alone, so that a per-incidence cost can
//     be computed without reaching into the set
//   - wall time of insert, erase, copy construction, select() for a single key
//     and for a range, positioning an incremental selector partway around the ring
//     as a range scan does, and a full incremental selector sweep
//
// Run with a single shard and enough memory for the set, e.g.
//
//     ./tools/toolchain/dbuild -- ./build/dev/test/perf/perf_sstable_set -c1 -m6G
//     ./tools/toolchain/dbuild -- ./build/dev/test/perf/perf_sstable_set -c1 -m6G --scaling
//
// --scaling repeats the scenario at count/8, count/4, count/2 and count, which
// shows how the footprint grows with the number of sstables.
//

#include <seastar/core/app-template.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>

#include <fmt/core.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <bit>
#include <cstdlib>
#include <limits>
#include <optional>
#include <random>
#include <set>
#include <unordered_set>
#include <vector>

#include "dht/i_partitioner.hh"
#include "schema/schema_builder.hh"
#include "seastarx.hh"
#include "sstables/sstable_set.hh"
#include "sstables/sstables.hh"
#include "test/lib/sstable_test_env.hh"
#include "test/lib/sstable_utils.hh"

using namespace sstables;
using namespace std::chrono;

namespace {

// Fraction of the compaction group's token range an sstable must span for this
// benchmark to call it "wide". Purely a workload classification: it decides which
// sstables the generator makes range-spanning and which of them the report counts
// as wide. The value is the threshold by which the set used to choose between two
// storage paths, kept so that measurements stay comparable with the baseline
// recorded before those paths were unified.
constexpr double wide_threshold = 0.85;

struct config {
    size_t count = 10000;
    double wide_fraction = 1.0 / 3.0;
    size_t fragments_per_run = 12;
    size_t fragment_width_spread = 1;
    size_t queries = 10000;
    double owned_range_fraction = 1.0;
    unsigned random_seed = 0;   // resolved: never 0 by the time a scenario runs
    bool scaling = false;
};

struct mem_delta {
    int64_t live_bytes = 0;
    int64_t live_objects = 0;
    int64_t churn = 0;
};

class mem_snapshot {
    int64_t _live_bytes;
    int64_t _live_objects;
    int64_t _churn;
public:
    static mem_snapshot take() {
        auto s = memory::stats();
        mem_snapshot r;
        r._live_bytes = int64_t(s.allocated_memory());
        r._live_objects = int64_t(s.live_objects());
        r._churn = int64_t(s.total_bytes_allocated());
        return r;
    }
    mem_delta since(const mem_snapshot& before) const {
        return mem_delta{
            .live_bytes = _live_bytes - before._live_bytes,
            .live_objects = _live_objects - before._live_objects,
            .churn = _churn - before._churn,
        };
    }
};

// One synthetic sstable, described as a pair of indices into the sorted key
// pool plus the run it belongs to. Working in index space keeps the overlap
// structure exact and independent of how tokens happen to be distributed.
struct sst_spec {
    size_t lo;
    size_t hi;
    run_id run;
    bool wide = false;      // classified by actual overlap ratio, not by intent
};

// Generates n distinct partition keys, ordered by ring position.
//
// Rolled by hand rather than via tests::generate_partition_keys() so that the
// whole scenario is determined by the seed, and so that this benchmark does
// not drag in the test-lib random schema machinery (which is only linked for tests
// using the seastar test framework).
std::vector<dht::decorated_key> make_key_pool(const schema_ptr& s, size_t n, std::mt19937& rnd) {
    std::vector<dht::decorated_key> keys;
    keys.reserve(n);
    std::unordered_set<int32_t> seen;
    seen.reserve(n * 2);
    std::uniform_int_distribution<int32_t> dist(
            std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());
    while (keys.size() < n) {
        auto v = dist(rnd);
        if (!seen.insert(v).second) {
            continue;
        }
        auto pk = partition_key::from_single_value(*s, int32_type->decompose(data_value(v)));
        keys.push_back(dht::decorate_key(*s, pk));
    }
    // Distinct partition keys are distinct ring positions, so the pool keeps its
    // full size; sorting just puts the indices in ring order.
    std::ranges::sort(keys, dht::decorated_key::less_comparator(s));
    return keys;
}

template <typename Func>
double timeit(Func&& f) {
    auto start = steady_clock::now();
    f();
    return duration_cast<duration<double>>(steady_clock::now() - start).count();
}

sstring fmt_bytes(int64_t b) {
    if (std::abs(b) >= 1 << 30) {
        return format("{:.2f} GiB", double(b) / (1 << 30));
    }
    if (std::abs(b) >= 1 << 20) {
        return format("{:.2f} MiB", double(b) / (1 << 20));
    }
    if (std::abs(b) >= 1 << 10) {
        return format("{:.2f} KiB", double(b) / (1 << 10));
    }
    return format("{} B", b);
}

// Counts the (sub-range, sstable) incidences the workload implies.
//
// Cutting the ring at every endpoint of every narrow sstable yields a set of
// sub-ranges; the count is how many (sub-range, sstable) pairs those cuts
// produce, i.e. the sum of the overlap depth over all sub-ranges. It is a
// property of the workload, not of any implementation, and it is the quantity a
// range index may end up materializing one entry per.
//
// Both endpoints of a range are cut points, and deliberately so. The index space
// here stands in for compatible_ring_position_or_view, which is continuous: two
// adjacent pool indices are distinct ring positions with room between them, so a
// fragment ending at hi and the next one starting at hi + 1 contribute two cuts,
// and an interval map materializes the sub-range between them for whatever wider
// sstables span it. Treating hi + 1 as the sole cut -- correct for a discrete
// domain -- halves the count and stops it predicting the interval map: measured
// against the interval-map implementation it came within 9% and 16% of the live
// allocation count in two workloads, at 57.2 bytes per element, which is what an
// unordered_set node plus its share of the bucket array costs; the halved count
// would imply 106 bytes per element.
//
// Computed from the generated ranges rather than from the set, so that it stays
// a valid reference point across implementation changes.
size_t count_overlap_incidences(const std::vector<sst_spec>& specs) {
    std::vector<size_t> bounds;
    bounds.reserve(2 * specs.size());
    for (const auto& sp : specs) {
        if (sp.wide) {
            continue;
        }
        bounds.push_back(sp.lo);
        bounds.push_back(sp.hi);
    }
    std::ranges::sort(bounds);
    bounds.erase(std::unique(bounds.begin(), bounds.end()), bounds.end());

    size_t total = 0;
    for (const auto& sp : specs) {
        if (sp.wide) {
            continue;
        }
        auto b = std::ranges::lower_bound(bounds, sp.lo);
        auto e = std::ranges::upper_bound(bounds, sp.hi);
        total += size_t(e - b);
    }
    return total;
}

// Builds the index ranges for one scenario.
//
// Wide sstables span nearly the whole pool, as a memtable flush of a busy
// compaction group does, each in its own run. Narrow sstables are emitted as
// runs of disjoint fragments that together cover the pool; the split points are
// randomized per run, so fragment boundaries are staggered across runs and every
// token ends up covered by roughly one fragment per run.
//
// With fragment_width_spread > 1 the fragment count varies per run, so narrow
// sstables no longer all have the same width. A real set mixes widths -- recently
// flushed data sits in few large fragments while older data has been split into
// many small ones -- and a structure whose query cost depends on the widest
// indexed sstable behaves differently under a mix than under a uniform width.
std::vector<sst_spec> make_specs(const config& cfg, size_t pool_size, std::mt19937& rnd) {
    const size_t n_wide = size_t(std::llround(cfg.count * cfg.wide_fraction));
    const size_t n_narrow = cfg.count - n_wide;

    std::vector<sst_spec> specs;
    specs.reserve(cfg.count);

    // Wide sstables: [0 .. jitter] to [pool-1-jitter .. pool-1].
    const size_t jitter = std::max<size_t>(1, pool_size / 50);
    std::uniform_int_distribution<size_t> jitter_dist(0, jitter);
    for (size_t i = 0; i < n_wide; ++i) {
        specs.push_back(sst_spec{
            .lo = jitter_dist(rnd),
            .hi = pool_size - 1 - jitter_dist(rnd),
            .run = run_id::create_random_id(),
        });
    }

    // Fragmented runs.
    std::uniform_int_distribution<size_t> split_dist(1, pool_size - 2);
    const size_t spread = std::max<size_t>(1, cfg.fragment_width_spread);
    std::uniform_int_distribution<size_t> frags_dist(
            std::max<size_t>(2, cfg.fragments_per_run / spread), cfg.fragments_per_run * spread);
    std::vector<size_t> splits;
    size_t emitted = 0;
    while (emitted < n_narrow) {
        // Splits are drawn from [1, pool_size - 2] and deduplicated below, so a run
        // cannot end up with more fragments than the pool has keys however many are
        // asked for. Capping here keeps an extravagant --fragments-per-run from
        // drawing a correspondingly extravagant number of values to throw away; it
        // does not change the workload for any count the pool can express.
        const size_t frags = std::min(spread > 1 ? frags_dist(rnd) : cfg.fragments_per_run, pool_size - 1);
        splits.clear();
        splits.push_back(0);
        while (splits.size() < frags) {
            splits.push_back(split_dist(rnd));
        }
        std::ranges::sort(splits);
        splits.erase(std::unique(splits.begin(), splits.end()), splits.end());
        splits.push_back(pool_size);

        auto run = run_id::create_random_id();
        for (size_t i = 0; i + 1 < splits.size() && emitted < n_narrow; ++i) {
            // Fragment i covers [splits[i], splits[i+1]-1]; consecutive
            // fragments never share a key, which keeps the run disjoint and
            // stops partitioned_sstable_set::insert() from re-assigning run ids.
            if (splits[i + 1] == splits[i]) {
                continue;
            }
            specs.push_back(sst_spec{
                .lo = splits[i],
                .hi = splits[i + 1] - 1,
                .run = run,
            });
            ++emitted;
        }
    }

    return specs;
}

struct scenario_result {
    size_t count = 0;
    size_t narrow = 0;
    size_t wide = 0;
    size_t runs = 0;
    size_t overlap_incidences = 0;
    // Distinct width tiers the workload spans, i.e. distinct values of
    // bit_width() over the sstables' token widths. An index that partitions by
    // width materializes one tier per distinct value.
    size_t width_tiers = 0;
    uint8_t width_tier_min = 0;
    uint8_t width_tier_max = 0;
    // Width of the narrow sstables as a fraction of the whole token range.
    double narrow_width_min = 0;
    double narrow_width_mean = 0;
    double narrow_width_max = 0;
    mem_delta sstables_mem;
    mem_delta set_mem;
    double insert_s = 0;
    double select_key_s = 0;
    double select_mid_s = 0;
    double select_range_s = 0;
    double sweep_s = 0;
    double seek_mid_s = 0;
    size_t seek_mid_iters = 0;
    size_t seek_mid_hits = 0;
    size_t sweep_steps = 0;
    size_t sweep_selected = 0;
    double clone_s = 0;
    mem_delta clone_mem;
    double erase_s = 0;
    size_t select_key_hits = 0;
    size_t select_mid_hits = 0;
    size_t select_range_hits = 0;
};

scenario_result run_scenario(test_env& env, schema_ptr s, const config& cfg, bool verbose) {
    scenario_result res;
    res.count = cfg.count;

    std::mt19937 rnd(cfg.random_seed);

    // Four pool entries per sstable leaves room for 2N distinct endpoints with
    // slack, so distinct fragments get distinct boundary keys. The pool is enlarged
    // by 1/--owned-range-fraction so that the sstables still get four entries each
    // within the part of it they are confined to, and the workload's geometry in
    // index space is the same whatever the fraction is.
    const size_t pool_size = std::max<size_t>(size_t(4 * cfg.count / cfg.owned_range_fraction), 1024);
    if (verbose) {
        fmt::print("generating {} partition keys...\n", pool_size);
    }
    auto pool = make_key_pool(s, pool_size, rnd);
    const auto token_range = dht::token_range::make(
            {pool.front().token(), true}, {pool.back().token(), true});

    // The sstables are confined to the leading owned_range_fraction of the pool, while
    // the set is still told it covers all of it. That is what a compaction group
    // splitting a tablet sees: it is handed the whole tablet range, but the memtables
    // it flushes only hold the side of the split point it took.
    const size_t owned_pool_size = std::max<size_t>(size_t(pool_size * cfg.owned_range_fraction), 8);
    auto specs = make_specs(cfg, owned_pool_size, rnd);

    // Classify each spec by the width it actually came out at, so the report
    // states the mix that was achieved rather than the one that was asked for.
    for (auto& sp : specs) {
        auto sst_tr = dht::token_range(pool[sp.lo].token(), pool[sp.hi].token());
        sp.wide = dht::overlap_ratio(token_range, sst_tr) >= wide_threshold;
    }
    res.wide = std::ranges::count_if(specs, [] (const sst_spec& sp) { return sp.wide; });
    res.narrow = specs.size() - res.wide;
    res.overlap_incidences = count_overlap_incidences(specs);
    {
        std::set<uint8_t> tiers;
        for (const auto& sp : specs) {
            auto f = pool[sp.lo].token().unbias();
            auto l = pool[sp.hi].token().unbias();
            tiers.insert(uint8_t(std::bit_width(l > f ? l - f : uint64_t(0))));
        }
        res.width_tiers = tiers.size();
        res.width_tier_min = tiers.empty() ? 0 : *tiers.begin();
        res.width_tier_max = tiers.empty() ? 0 : *tiers.rbegin();
    }
    if (res.narrow) {
        double sum = 0;
        res.narrow_width_min = 1.0;
        for (const auto& sp : specs) {
            if (sp.wide) {
                continue;
            }
            // Index-space width is a faithful proxy for token-space width, since
            // the pool is a sorted sample of the ring.
            const double w = double(sp.hi - sp.lo + 1) / double(pool_size);
            sum += w;
            res.narrow_width_min = std::min(res.narrow_width_min, w);
            res.narrow_width_max = std::max(res.narrow_width_max, w);
        }
        res.narrow_width_mean = sum / double(res.narrow);
    }
    {
        std::unordered_set<run_id> runs;
        for (const auto& sp : specs) {
            runs.insert(sp.run);
        }
        res.runs = runs.size();
    }

    // Materialize the sstables. Measured separately so that the set's own
    // footprint can be reported without the sstable objects mixed in.
    std::vector<shared_sstable> ssts;
    ssts.reserve(specs.size());
    {
        auto before = mem_snapshot::take();
        for (const auto& sp : specs) {
            auto sst = env.make_sstable(s);
            stats_metadata stats{};
            sstables::test(sst).set_values(pool[sp.lo].key(), pool[sp.hi].key(), std::move(stats));
            // set_values() assigns a fresh random run id; override it so that
            // fragments of the same run are grouped as they would be in a real
            // compaction group.
            sstables::test(sst).set_run_identifier(sp.run);
            ssts.push_back(std::move(sst));
        }
        res.sstables_mem = mem_snapshot::take().since(before);
    }

    // Sstables reach a real set in arbitrary order; don't hand it a sorted
    // sequence, which no caller would produce.
    std::shuffle(ssts.begin(), ssts.end(), rnd);

    auto set = sstables::make_partitioned_sstable_set(s, token_range);
    {
        auto before = mem_snapshot::take();
        res.insert_s = timeit([&] {
            for (auto& sst : ssts) {
                set.insert(sst);
            }
        });
        res.set_mem = mem_snapshot::take().since(before);
    }

    if (set.size() != specs.size()) {
        throw std::runtime_error(format("expected {} sstables in the set, got {}", specs.size(), set.size()));
    }

    // select() for a single key -- the read path's partition lookup.
    {
        std::uniform_int_distribution<size_t> key_dist(0, pool_size - 1);
        std::vector<dht::partition_range> ranges;
        ranges.reserve(cfg.queries);
        for (size_t i = 0; i < cfg.queries; ++i) {
            ranges.push_back(dht::partition_range::make_singular(pool[key_dist(rnd)]));
        }
        res.select_key_s = timeit([&] {
            for (const auto& r : ranges) {
                res.select_key_hits += set.select(r).size();
            }
        });
    }

    // select() for a single key at the middle of the ring, which is the worst
    // case for the walk: the index is bounded on one end only, and at the middle
    // both ends are about half the set, so choosing the shorter side does not
    // help. Reported separately from the uniformly random keys above, because a
    // uniform average hides it.
    {
        const size_t band = std::max<size_t>(1, pool_size / 100);
        const size_t mid = pool_size / 2;
        std::uniform_int_distribution<size_t> mid_dist(mid - band / 2, mid + band / 2);
        std::vector<dht::partition_range> ranges;
        ranges.reserve(cfg.queries);
        for (size_t i = 0; i < cfg.queries; ++i) {
            ranges.push_back(dht::partition_range::make_singular(pool[mid_dist(rnd)]));
        }
        res.select_mid_s = timeit([&] {
            for (const auto& r : ranges) {
                res.select_mid_hits += set.select(r).size();
            }
        });
    }

    // select() for a range covering ~1% of the pool.
    {
        const size_t span = std::max<size_t>(1, pool_size / 100);
        std::uniform_int_distribution<size_t> start_dist(0, pool_size - span - 1);
        std::vector<dht::partition_range> ranges;
        ranges.reserve(cfg.queries);
        for (size_t i = 0; i < cfg.queries; ++i) {
            auto start = start_dist(rnd);
            ranges.push_back(dht::partition_range::make(
                    {dht::ring_position(pool[start]), true},
                    {dht::ring_position(pool[start + span]), true}));
        }
        res.select_range_s = timeit([&] {
            for (const auto& r : ranges) {
                res.select_range_hits += set.select(r).size();
            }
        });
    }

    // Cost of positioning a selector in the middle of the ring, which is what a
    // range scan over a sub-range does: incremental_reader_selector builds a
    // selector and seeks it to the start of the range, once per scan and again on
    // every fast_forward_to() rewind. Measured separately from the sweep below,
    // which starts at the beginning and so never pays for a seek.
    {
        res.seek_mid_iters = std::min<size_t>(cfg.queries, 2000);
        auto pos = dht::ring_position_view(pool[pool_size / 2]);
        res.seek_mid_s = timeit([&] {
            for (size_t i = 0; i < res.seek_mid_iters; ++i) {
                auto sel = set.make_incremental_selector();
                res.seek_mid_hits += sel.select(pos).sstables.size();
            }
        });
    }

    // Full incremental selector sweep, in ring order from the start of the
    // range to its end -- the access pattern compaction uses to walk the set.
    res.sweep_s = timeit([&] {
        auto sel = set.make_incremental_selector();
        dht::ring_position_view pos = dht::ring_position_view::min();
        do {
            auto ret = sel.select(pos);
            pos = ret.next_position;
            res.sweep_selected += ret.sstables.size();
            ++res.sweep_steps;
        } while (!pos.is_max());
    });

    // Copy construction. Callers clone a set rather than mutate a shared one,
    // so this is on the path of adding or removing sstables.
    {
        auto before = mem_snapshot::take();
        std::optional<sstable_set> copy;
        res.clone_s = timeit([&] {
            copy.emplace(set);
        });
        res.clone_mem = mem_snapshot::take().since(before);
        copy.reset();
    }

    res.erase_s = timeit([&] {
        for (auto& sst : ssts) {
            set.erase(sst);
        }
    });

    return res;
}

void print_report(const scenario_result& r, const config& cfg) {
    auto per_op_ns = [] (double secs, size_t ops) {
        return ops ? secs * 1e9 / double(ops) : 0.0;
    };

    fmt::print("\n=== composition ===\n");
    fmt::print("  sstables total          {}\n", r.count);
    fmt::print("  narrow (< {:.0f}% of range) {} ({:.1f}%)\n", 100.0 * wide_threshold, r.narrow, 100.0 * double(r.narrow) / double(r.count));
    fmt::print("  wide  (>= {:.0f}% of range) {} ({:.1f}%)\n", 100.0 * wide_threshold, r.wide, 100.0 * double(r.wide) / double(r.count));
    fmt::print("  sstable runs            {}\n", r.runs);
    fmt::print("  fragments per run       {}{}\n", cfg.fragments_per_run,
            cfg.fragment_width_spread > 1
                    ? format(" (varied per run, spread {}x)", cfg.fragment_width_spread) : sstring(""));
    fmt::print("  distinct width tiers    {} (bit_width {}..{})\n",
            r.width_tiers, r.width_tier_min, r.width_tier_max);
    fmt::print("  narrow width of range   min {:.3f}%  mean {:.3f}%  max {:.3f}%\n",
            100.0 * r.narrow_width_min, 100.0 * r.narrow_width_mean, 100.0 * r.narrow_width_max);

    fmt::print("\n=== workload overlap ===\n");
    fmt::print("  (sub-range, sstable) incidences  {}\n", r.overlap_incidences);
    if (r.narrow) {
        fmt::print("  incidences per narrow sstable    {:.1f}\n",
                double(r.overlap_incidences) / double(r.narrow));
    }

    fmt::print("\n=== memory ===\n");
    fmt::print("  sstable objects   {:>12}  ({} live allocations)\n",
            fmt_bytes(r.sstables_mem.live_bytes), r.sstables_mem.live_objects);
    fmt::print("  populated set     {:>12}  ({} live allocations)\n",
            fmt_bytes(r.set_mem.live_bytes), r.set_mem.live_objects);
    fmt::print("  set churn         {:>12}  (bytes allocated while inserting)\n",
            fmt_bytes(r.set_mem.churn));
    if (r.count) {
        fmt::print("  set bytes/sstable {:>12}\n", fmt_bytes(r.set_mem.live_bytes / int64_t(r.count)));
    }
    if (r.overlap_incidences) {
        fmt::print("  set bytes/incid.  {:>12.1f}\n",
                double(r.set_mem.live_bytes) / double(r.overlap_incidences));
    }
    fmt::print("  copy ctor         {:>12}  ({} live allocations)\n",
            fmt_bytes(r.clone_mem.live_bytes), r.clone_mem.live_objects);

    fmt::print("\n=== cpu ===\n");
    fmt::print("  insert         {:>9.3f} s  {:>12.1f} ns/sstable\n",
            r.insert_s, per_op_ns(r.insert_s, r.count));
    fmt::print("  erase          {:>9.3f} s  {:>12.1f} ns/sstable\n",
            r.erase_s, per_op_ns(r.erase_s, r.count));
    fmt::print("  copy ctor      {:>9.3f} s  {:>12.1f} ns/sstable copied\n",
            r.clone_s, per_op_ns(r.clone_s, r.count));
    fmt::print("  select(key)    {:>9.3f} s  {:>12.1f} ns/query   {:.1f} sstables/query\n",
            r.select_key_s, per_op_ns(r.select_key_s, cfg.queries),
            double(r.select_key_hits) / double(cfg.queries));
    fmt::print("  select(key)mid {:>9.3f} s  {:>12.1f} ns/query   {:.1f} sstables/query\n",
            r.select_mid_s, per_op_ns(r.select_mid_s, cfg.queries),
            double(r.select_mid_hits) / double(cfg.queries));
    fmt::print("  select(range)  {:>9.3f} s  {:>12.1f} ns/query   {:.1f} sstables/query\n",
            r.select_range_s, per_op_ns(r.select_range_s, cfg.queries),
            double(r.select_range_hits) / double(cfg.queries));
    fmt::print("  selector seek  {:>9.3f} s  {:>12.1f} ns/seek   {:.1f} sstables/seek\n",
            r.seek_mid_s, per_op_ns(r.seek_mid_s, r.seek_mid_iters),
            r.seek_mid_iters ? double(r.seek_mid_hits) / double(r.seek_mid_iters) : 0.0);
    fmt::print("  selector sweep {:>9.3f} s  {:>12.1f} ns/step    {} steps, {:.1f} sstables/step\n",
            r.sweep_s, per_op_ns(r.sweep_s, r.sweep_steps), r.sweep_steps,
            r.sweep_steps ? double(r.sweep_selected) / double(r.sweep_steps) : 0.0);
    fmt::print("\n");
}

void print_scaling_row_header() {
    fmt::print("\n=== scaling ===\n");
    fmt::print("{:>8}  {:>8}  {:>14}  {:>10}  {:>14}  {:>12}  {:>12}  {:>12}\n",
            "count", "narrow", "incidences", "incid./sst", "set bytes", "bytes/sst", "insert s", "sweep s");
}

void print_scaling_row(const scenario_result& r) {
    fmt::print("{:>8}  {:>8}  {:>14}  {:>10.1f}  {:>14}  {:>12}  {:>12.3f}  {:>12.3f}\n",
            r.count, r.narrow, r.overlap_incidences,
            r.narrow ? double(r.overlap_incidences) / double(r.narrow) : 0.0,
            fmt_bytes(r.set_mem.live_bytes),
            r.count ? fmt_bytes(r.set_mem.live_bytes / int64_t(r.count)) : sstring("-"),
            r.insert_s, r.sweep_s);
}

schema_ptr make_test_schema() {
    return schema_builder(this_smp_shard_count(), "ks", "sstable_set_perf")
        .with_column("pk", int32_type, column_kind::partition_key)
        .with_column("v", int32_type)
        .build();
}

} // anonymous namespace

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("count", bpo::value<size_t>()->default_value(10000), "number of sstables to insert")
        ("wide-fraction", bpo::value<double>()->default_value(1.0 / 3.0),
                "fraction of sstables that span most of the token range, as a memtable flush does")
        ("fragments-per-run", bpo::value<size_t>()->default_value(12),
                "disjoint fragments per narrow run; each fragment spans ~1/N of the token range")
        ("fragment-width-spread", bpo::value<size_t>()->default_value(1),
                "vary the fragment count per run within this factor of --fragments-per-run, "
                "so that narrow sstables do not all have the same width; 1 keeps them uniform")
        ("queries", bpo::value<size_t>()->default_value(10000), "number of select() calls per benchmark")
        ("owned-range-fraction", bpo::value<double>()->default_value(1.0),
                "fraction of the range the set is told it covers that the sstables actually span; "
                "0.5 models a compaction group splitting a tablet, which is handed the whole "
                "tablet range but holds only one side of the split point")
        ("random-seed", bpo::value<unsigned>(), "random number generator seed; "
                "picked at random and echoed if unset")
        ("scaling", "also run the scenario at count/8, count/4 and count/2 to show the growth rate")
        ("verbose", "enable info-level logging");

    return app.run(argc, argv, [&] {
        if (this_smp_shard_count() != 1) {
            throw std::runtime_error("This test has to be run with -c1");
        }
        if (!app.configuration().contains("verbose")) {
            logging::logger_registry().set_all_loggers_level(seastar::log_level::warn);
        }

        config cfg;
        cfg.count = app.configuration()["count"].as<size_t>();
        cfg.wide_fraction = app.configuration()["wide-fraction"].as<double>();
        cfg.fragments_per_run = app.configuration()["fragments-per-run"].as<size_t>();
        cfg.fragment_width_spread = app.configuration()["fragment-width-spread"].as<size_t>();
        cfg.queries = app.configuration()["queries"].as<size_t>();
        cfg.owned_range_fraction = app.configuration()["owned-range-fraction"].as<double>();
        auto conf_seed = app.configuration()["random-seed"];
        cfg.random_seed = conf_seed.empty() ? std::random_device()() : conf_seed.as<unsigned>();
        cfg.scaling = app.configuration().contains("scaling");
        const bool verbose = app.configuration().contains("verbose");

        if (cfg.count < 8) {
            throw std::runtime_error("--count must be at least 8");
        }
        // Negated so that a NaN, which compares false against everything, is rejected too.
        if (!(cfg.wide_fraction >= 0.0 && cfg.wide_fraction < 1.0)) {
            throw std::runtime_error("--wide-fraction must be in [0, 1)");
        }
        if (!(cfg.owned_range_fraction > 0.0 && cfg.owned_range_fraction <= 1.0)) {
            throw std::runtime_error("--owned-range-fraction must be in (0, 1]");
        }
        // run_scenario() sizes the key pool as 4 * --count / --owned-range-fraction. The
        // multiplication has to be checked before the division, since it is done in size_t
        // and would wrap first, and the quotient has to fit in size_t for the conversion
        // back from double to be defined.
        if (cfg.count > std::numeric_limits<size_t>::max() / 4 ||
                double(4 * cfg.count) / cfg.owned_range_fraction >= double(std::numeric_limits<size_t>::max())) {
            throw std::runtime_error("--count divided by --owned-range-fraction must fit in size_t, "
                    "it is the size of the key pool");
        }
        if (cfg.queries < 1) {
            throw std::runtime_error("--queries must be at least 1");
        }
        if (cfg.fragment_width_spread < 1) {
            throw std::runtime_error("--fragment-width-spread must be at least 1");
        }
        if (cfg.fragments_per_run < 2) {
            throw std::runtime_error("--fragments-per-run must be at least 2, "
                    "otherwise a run is a single fragment spanning the whole range, "
                    "which is the wide workload rather than the narrow one");
        }
        // make_specs() draws the per-run fragment count from
        // [--fragments-per-run / spread, --fragments-per-run * spread].
        if (cfg.fragments_per_run > std::numeric_limits<size_t>::max() / cfg.fragment_width_spread) {
            throw std::runtime_error("--fragments-per-run multiplied by --fragment-width-spread must "
                    "fit in size_t");
        }

        // Echo it so that a run can always be replayed with --random-seed.
        fmt::print("random-seed={}\n", cfg.random_seed);

        return test_env::do_with_async([&] (test_env& env) {
            auto s = make_test_schema();

            if (cfg.scaling) {
                print_scaling_row_header();
                for (size_t div : {8u, 4u, 2u, 1u}) {
                    auto sub = cfg;
                    sub.count = cfg.count / div;
                    sub.queries = std::min(cfg.queries, sub.count);
                    print_scaling_row(run_scenario(env, s, sub, verbose));
                }
                fmt::print("\n");
            }

            fmt::print("partitioned_sstable_set baseline: {} sstables, {} fragments/run, "
                    "{:.0f}% wide sstables, sstables over {:.0f}% of the set's range\n",
                    cfg.count, cfg.fragments_per_run, 100.0 * cfg.wide_fraction,
                    100.0 * cfg.owned_range_fraction);
            print_report(run_scenario(env, s, cfg, verbose), cfg);
        });
    });
}
