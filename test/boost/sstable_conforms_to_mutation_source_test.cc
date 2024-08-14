/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include "utils/assert.hh"
#include <boost/test/unit_test.hpp>
#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include <seastar/core/thread.hh>
#include "sstables/sstables.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/sstable_utils.hh"
#include "row_cache.hh"
#include "test/lib/simple_schema.hh"
#include "partition_slice_builder.hh"
#include "test/lib/mutation_reader_assertions.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/random_schema.hh"

using namespace sstables;
using namespace std::chrono_literals;

static
mutation_source make_sstable_mutation_source(sstables::test_env& env, schema_ptr s, sstring dir, std::vector<mutation> mutations,
        sstable_writer_config cfg, sstables::sstable::version_types version, gc_clock::time_point query_time = gc_clock::now()) {
    auto sst = env.make_sstable(s, dir, env.new_generation(), version, sstable_format_types::big, default_sstable_buffer_size, query_time);
    auto mt = make_memtable(s, mutations);
    auto mr = mt->make_flat_reader(s, env.make_reader_permit());
    sst->write_components(std::move(mr), mutations.size(), s, cfg, mt->get_encoding_stats()).get();
    sst->load(s->get_sharder()).get();
    return sst->as_mutation_source();
}

static void consume_all(mutation_reader& rd) {
    while (auto mfopt = rd().get()) {}
}

// It is assumed that src won't change.
static snapshot_source snapshot_source_from_snapshot(mutation_source src) {
    return snapshot_source([src = std::move(src)] {
        return src;
    });
}

static
void test_cache_population_with_range_tombstone_adjacent_to_population_range(populate_fn_ex populate) {
    simple_schema s;
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto cache_mt = make_lw_shared<replica::memtable>(s.schema());

    auto pkey = s.make_pkey();

    // underlying should not be empty, otherwise cache will make the whole range continuous
    mutation m1(s.schema(), pkey);
    s.add_row(m1, s.make_ckey(0), "v1");
    s.add_row(m1, s.make_ckey(1), "v2");
    s.add_row(m1, s.make_ckey(2), "v3");
    s.delete_range(m1, s.make_ckey_range(2, 100));
    cache_mt->apply(m1);

    cache_tracker tracker;
    auto ms = populate(s.schema(), std::vector<mutation>({m1}), gc_clock::now());
    row_cache cache(s.schema(), snapshot_source_from_snapshot(std::move(ms)), tracker);

    auto pr = dht::partition_range::make_singular(pkey);

    auto populate_range = [&] (int start) {
        auto slice = partition_slice_builder(*s.schema())
                .with_range(query::clustering_range::make_singular(s.make_ckey(start)))
                .build();
        auto rd = cache.make_reader(s.schema(), semaphore.make_permit(), pr, slice);
        auto close_rd = deferred_close(rd);
        consume_all(rd);
    };

    populate_range(2);

    // The cache now has only row with ckey 2 populated and the rest is discontinuous.
    // Populating reader which stops populating at entry with ckey 2 should not forget
    // to emit range_tombstone which starts at before(2).

    assert_that(cache.make_reader(s.schema(), semaphore.make_permit()))
            .produces(m1)
            .produces_end_of_stream();
}

static future<> test_sstable_conforms_to_mutation_source(sstable_version_types version, int index_block_size) {
    return sstables::test_env::do_with_async([version, index_block_size] (sstables::test_env& env) {
        sstable_writer_config cfg = env.manager().configure_writer();
        cfg.promoted_index_block_size = index_block_size;

        std::vector<tmpdir> dirs;
        auto populate = [&env, &dirs, &cfg, version] (schema_ptr s, const std::vector<mutation>& partitions,
                                                      gc_clock::time_point query_time) -> mutation_source {
            dirs.emplace_back();
            return make_sstable_mutation_source(env, s, dirs.back().path().string(), partitions, cfg, version, query_time);
        };

        run_mutation_source_tests(populate);

        if (index_block_size == 1) {
            // The tests below are not sensitive to index bock size so run once.
            test_cache_population_with_range_tombstone_adjacent_to_population_range(populate);
        }
    });
}

static constexpr std::array<int, 3> block_sizes = { 1, 128, 64 * 1024 };

// Split for better parallelizm

SEASTAR_TEST_CASE(test_sstable_conforms_to_mutation_source_mc_tiny) {
    return test_sstable_conforms_to_mutation_source(writable_sstable_versions[0], block_sizes[0]);
}

SEASTAR_TEST_CASE(test_sstable_conforms_to_mutation_source_mc_medium) {
    return test_sstable_conforms_to_mutation_source(writable_sstable_versions[0], block_sizes[1]);
}

SEASTAR_TEST_CASE(test_sstable_conforms_to_mutation_source_mc_large) {
    return test_sstable_conforms_to_mutation_source(writable_sstable_versions[0], block_sizes[2]);
}

SEASTAR_TEST_CASE(test_sstable_conforms_to_mutation_source_md_tiny) {
    return test_sstable_conforms_to_mutation_source(writable_sstable_versions[1], block_sizes[0]);
}

SEASTAR_TEST_CASE(test_sstable_conforms_to_mutation_source_md_medium) {
    return test_sstable_conforms_to_mutation_source(writable_sstable_versions[1], block_sizes[1]);
}

SEASTAR_TEST_CASE(test_sstable_conforms_to_mutation_source_md_large) {
    return test_sstable_conforms_to_mutation_source(writable_sstable_versions[1], block_sizes[2]);
}

// This SCYLLA_ASSERT makes sure we don't miss writable vertions
static_assert(writable_sstable_versions.size() == 3);

// `keys` may contain repetitions.
// The generated position ranges are non-empty. The start of each range in the vector is greater than the end of the previous range.
//
// TODO: could probably be placed in random_utils/random_schema after some tuning
static std::vector<position_range> random_ranges(const std::vector<clustering_key>& keys, const schema& s, std::mt19937& engine) {
    std::vector<position_in_partition> positions;
    if (tests::random::get_bool(engine)) {
        positions.push_back(position_in_partition::before_all_clustered_rows());
    }

    for (auto& k: keys) {
        auto exploded = k.explode(s);
        auto tag = tests::random::get_int<size_t>(1, 3);
        auto prefix_size = tag == 2 ? exploded.size() : tests::random::get_int<size_t>(1, exploded.size(), engine);
        auto ckp = clustering_key_prefix::from_exploded(s, std::vector<bytes>{exploded.begin(), exploded.begin() + prefix_size});
        switch (tag) {
            case 1:
                positions.emplace_back(position_in_partition::before_clustering_row_tag_t{}, std::move(ckp));
                break;
            case 2:
                positions.emplace_back(position_in_partition::clustering_row_tag_t{}, std::move(ckp));
                break;
            default:
                positions.emplace_back(position_in_partition_view::after_all_prefixed(std::move(ckp)));
                break;
        }
    }

    auto subset_size = tests::random::get_int<size_t>(0, positions.size(), engine);
    positions = tests::random::random_subset<position_in_partition>(std::move(positions), subset_size, engine);
    std::sort(positions.begin(), positions.end(), position_in_partition::less_compare(s));
    positions.erase(std::unique(positions.begin(), positions.end(), position_in_partition::equal_compare(s)), positions.end());

    if (tests::random::get_bool(engine)) {
        positions.push_back(position_in_partition::after_all_clustered_rows());
    }

    std::vector<position_range> ranges;
    for (unsigned i = 0; i + 1 < positions.size(); i += 2) {
        ranges.emplace_back(std::move(positions[i]), std::move(positions[i+1]));
    }

    if (ranges.empty()) {
        ranges.emplace_back(position_in_partition::before_all_clustered_rows(), position_in_partition::after_all_clustered_rows());
    }

    return ranges;
}

SEASTAR_THREAD_TEST_CASE(test_sstable_reversing_reader_random_schema) {
    // Create two sources: one by creating an sstable from a set of mutations,
    // and one by creating an sstable from the same set of mutations but reversed.
    // We query the first source in forward mode and the second in reversed mode.
    // The two queries should return the same result.

    auto random_spec = tests::make_random_schema_specification(get_name());
    auto random_schema = tests::random_schema{tests::random::get_int<uint32_t>(), *random_spec};
    auto query_schema = random_schema.schema();
    testlog.debug("Random schema:\n{}", random_schema.cql());

    auto muts = tests::generate_random_mutations(random_schema).get();

    std::vector<mutation> reversed_muts;
    for (auto& m : muts) {
        reversed_muts.push_back(reverse(m));
    }

    std::vector<clustering_key> keys;
    for (const auto& m: muts) {
        for (auto& r: m.partition().clustered_rows()) {
            keys.push_back(r.key());
        }
    }

    std::mt19937 engine{tests::random::get_int<uint32_t>()};
    auto fwd_ranges = random_ranges(keys, *query_schema, engine);

    std::vector<query::clustering_range> ranges;
    for (auto& r: fwd_ranges) {
        SCYLLA_ASSERT(position_in_partition::less_compare(*query_schema)(r.start(), r.end()));
        auto cr_opt = position_range_to_clustering_range(r, *query_schema);
        if (!cr_opt) {
            continue;
        }
        ranges.push_back(std::move(*cr_opt));
    }

    auto slice = partition_slice_builder(*query_schema)
        .with_ranges(ranges)
        .build();

    // Clustering ranges of the slice are already reversed in relation to the reversed
    // query schema. No need to reverse it. Just toggle the reverse option.
    auto rev_slice = partition_slice_builder(*query_schema, slice)
            .with_option<query::partition_slice::option::reversed>()
            .build();
    auto rev_full_slice = partition_slice_builder(*query_schema, query_schema->full_slice())
            .with_option<query::partition_slice::option::reversed>()
            .build();

    sstables::test_env::do_with_async([&, version = writable_sstable_versions[1]] (sstables::test_env& env) {

        std::vector<tmpdir> dirs;
        sstable_writer_config cfg = env.manager().configure_writer();

        for (auto index_block_size: block_sizes) {
            cfg.promoted_index_block_size = index_block_size;

            dirs.emplace_back();
            auto source = make_sstable_mutation_source(env, query_schema, dirs.back().path().string(), muts, cfg, version);

            dirs.emplace_back();
            auto rev_source = make_sstable_mutation_source(env, query_schema->make_reversed(), dirs.back().path().string(), reversed_muts, cfg, version);

            tests::reader_concurrency_semaphore_wrapper semaphore;

            testlog.trace("Slice: {}", slice);

            for (const auto& m: muts) {
                auto prange = dht::partition_range::make_singular(m.decorated_key());

                {
                    auto r1 = source.make_reader_v2(query_schema, semaphore.make_permit(), prange,
                            slice, nullptr,
                            streamed_mutation::forwarding::no, mutation_reader::forwarding::no);
                    auto close_r1 = deferred_action([&r1] { r1.close().get(); });

                    auto r2 = rev_source.make_reader_v2(query_schema, semaphore.make_permit(), prange,
                            rev_slice, nullptr,
                            streamed_mutation::forwarding::no, mutation_reader::forwarding::no);
                    close_r1.cancel();

                    compare_readers(*query_schema, std::move(r1), std::move(r2), true);
                }

                auto r1 = source.make_reader_v2(query_schema, semaphore.make_permit(), prange,
                        query_schema->full_slice(), nullptr,
                        streamed_mutation::forwarding::yes, mutation_reader::forwarding::no);
                auto close_r1 = deferred_action([&r1] { r1.close().get(); });

                auto r2 = rev_source.make_reader_v2(query_schema, semaphore.make_permit(), prange,
                        rev_full_slice, nullptr,
                        streamed_mutation::forwarding::yes, mutation_reader::forwarding::no);
                close_r1.cancel();

                // We don't use `compare_readers` because in forwarding mode the readers
                // may return different, however equivalent (and both correct), fragment streams.
                // See #9472.
                auto m1 = forwardable_reader_to_mutation(std::move(r1), fwd_ranges);
                auto m2 = forwardable_reader_to_mutation(std::move(r2), fwd_ranges);

                assert_that(m1).is_equal_to(m2, slice.row_ranges(*m.schema(), m.key()));
            }
        }
    }).get();
}
