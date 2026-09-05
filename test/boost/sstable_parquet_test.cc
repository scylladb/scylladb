/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// End-to-end tests for the `pq` sstable format: mutations in through the normal
// sstable_writer, a real Parquet file on disk, and the same mutations back out
// through sstable::make_reader.
//
// The suites under sstables/parquet/ cover the format codec and the shredder in
// isolation and without Seastar. This file covers what those cannot: that the
// sstable layer dispatches to the pq writer and reader at all, that the
// components a loadable sstable needs are written, and that the round trip
// survives the real read path rather than a test harness.
//
// The whole mutation model is covered here: row markers, row and partition
// tombstones, static rows, range tombstones, non-frozen collections and counters.
// sstable_conforms_to_mutation_source_test holds pq to the same contract as every
// other writable version and is the broader net; these cases are the targeted
// ones, each aimed at a specific way the encoding can go wrong, and they exist
// because iterating on the conformance suite means a three-minute build per guess.

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include "test/lib/simple_schema.hh"
#include "test/lib/sstable_test_env.hh"
#include "sstables/parquet/gain_estimator.hh"
#include "compaction/size_tiered_compaction_strategy.hh"
#include "sstables/parquet/tiering_context.hh"
#include "sstables/compressor.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/mutation_assertions.hh"
#include "test/lib/mutation_reader_assertions.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "test/lib/eventually.hh"

#include "schema/schema_builder.hh"
#include "readers/from_mutations.hh"
#include "readers/combined.hh"
#include "readers/mutation_fragment_v1_stream.hh"
#include "sstables/sstables.hh"
#include "sstables/parquet/format/parquet_metadata.hh"
#include "sstables/parquet/format/parquet_reader.hh"
#include "sstables/parquet/format/encryption.hh"
#include "sstables/parquet/encryption_keys.hh"
#include "sstables/parquet/batch_reader.hh"
#include "sstables/parquet/footer_cache.hh"
#include "partition_slice_builder.hh"
#include "mutation/mutation.hh"
#include "mutation/collection_mutation.hh"
#include "mutation/counters.hh"
#include "types/map.hh"
#include "types/set.hh"
#include "types/list.hh"
#include "types/user.hh"
#include "utils/UUID_gen.hh"

#include <cstdio>
#include <filesystem>
#include <cstdlib>

using namespace sstables;

namespace {

// A schema made only of types the shredder maps to native Parquet columns, so a
// failure here is a failure of the pq path rather than of the blob fallback.
schema_ptr pq_schema() {
    return schema_builder(1, "ks", "pq_tbl")
        .with_column("pk", utf8_type, column_kind::partition_key)
        .with_column("ck", int32_type, column_kind::clustering_key)
        .with_column("v_int", int32_type)
        .with_column("v_big", long_type)
        .with_column("v_dbl", double_type)
        .with_column("v_txt", utf8_type)
        .build();
}

// `n_part` partitions of `n_rows` rows each, with a scattering of absent values
// so definition levels are exercised, and per-cell timestamps that mostly agree
// (which is what L1 row-folding is built for) but sometimes do not.
utils::chunked_vector<mutation> make_muts(schema_ptr s, int n_part, int n_rows) {
    utils::chunked_vector<mutation> muts;
    for (int p = 0; p < n_part; ++p) {
        auto pk = partition_key::from_single_value(
                *s, utf8_type->decompose(sstring(format("key{:04d}", p))));
        mutation m(s, pk);
        for (int r = 0; r < n_rows; ++r) {
            auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
            const api::timestamp_type row_ts = 1000 + p;
            auto put = [&] (const char* name, bytes val, api::timestamp_type ts) {
                m.set_clustered_cell(ck, *s->get_column_definition(to_bytes(name)),
                                     atomic_cell::make_live(*s->get_column_definition(
                                             to_bytes(name))->type, ts, val));
            };
            put("v_int", int32_type->decompose(r * 7), row_ts);
            if (r % 3) { put("v_big", long_type->decompose(int64_t(r) * 1'000'003), row_ts); }
            put("v_dbl", double_type->decompose(double(r) * 1.5), row_ts);
            // Every fifth row disagrees with its row timestamp in one column,
            // which is exactly what the sparse exception channel encodes.
            if (r % 5 == 0) {
                put("v_txt", utf8_type->decompose(sstring(format("v{}", r))), row_ts + 1);
            } else {
                put("v_txt", utf8_type->decompose(sstring(format("v{}", r))), row_ts);
            }
        }
        muts.push_back(std::move(m));
    }
    // The writer requires token order, which is not key order.
    std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
        return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
    });
    return muts;
}

// The raw fragment stream, printed. Reassembling into mutations normalises
// range-tombstone bounds, which hides a lost bound weight; the fragments do not.
std::vector<sstring> fragments_in(shared_sstable sst, schema_ptr s, reader_permit permit,
                                  const dht::partition_range& pr,
                                  const query::partition_slice& slice) {
    auto rd = sst->make_reader(s, permit, pr, slice);
    auto close = deferred_close(rd);
    std::vector<sstring> out;
    while (auto mf = rd().get()) {
        out.push_back(seastar::format("{}", mutation_fragment_v2::printer(*s, *mf)));
    }
    return out;
}

std::vector<sstring> fragments_of(shared_sstable sst, schema_ptr s, reader_permit permit) {
    return fragments_in(sst, s, permit, query::full_partition_range, s->full_slice());
}

utils::chunked_vector<mutation> read_all(shared_sstable sst, schema_ptr s,
                                         reader_permit permit) {
    auto rd = sst->make_reader(s, permit, query::full_partition_range,
                               s->full_slice());
    auto close = deferred_close(rd);
    utils::chunked_vector<mutation> out;
    while (auto m = read_mutation_from_mutation_reader(rd).get()) {
        out.push_back(std::move(*m));
    }
    return out;
}

} // namespace

SEASTAR_THREAD_TEST_CASE(test_pq_sstable_is_written_and_read_back) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto muts = make_muts(s, 12, 40);
        auto expected = muts;

        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        BOOST_REQUIRE(sst->get_version() == sstable_version_types::pq);

        auto got = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(got.size(), expected.size());
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(expected[i]);
        }
    }).get();
}

// The Data component must be a Parquet file any other implementation can open:
// the whole premise of the format is that it is not a Scylla-private container.
SEASTAR_THREAD_TEST_CASE(test_pq_data_component_is_a_parquet_file) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), make_muts(s, 4, 25)).get();

        const uint64_t len = sst->ondisk_data_size();
        auto buf = sst->data_read(0, len, env.make_reader_permit()).get();
        BOOST_REQUIRE_GE(buf.size(), 12u);
        BOOST_REQUIRE_EQUAL(std::string_view(buf.get(), 4), "PAR1");
        BOOST_REQUIRE_EQUAL(std::string_view(buf.get() + buf.size() - 4, 4), "PAR1");

        // Magic bytes only prove the envelope. Setting SCYLLA_PQ_DUMP writes the
        // component out so an external Parquet implementation can be pointed at
        // it; sstables/parquet/run_tests.sh does exactly that with pyarrow.
        if (const char* dst = std::getenv("SCYLLA_PQ_DUMP")) {
            auto f = std::fopen(dst, "wb");
            BOOST_REQUIRE(f);
            BOOST_REQUIRE_EQUAL(std::fwrite(buf.get(), 1, buf.size(), f), buf.size());
            std::fclose(f);
            auto idx = seastar::format("{}", sst->index_filename());
            std::filesystem::copy_file(std::filesystem::path(idx.c_str()),
                                       std::filesystem::path(std::string(dst) + ".index"),
                                       std::filesystem::copy_options::overwrite_existing);
        }
    }).get();
}

// The index, summary and filter are what make an sstable loadable at all. A pq
// sstable that reads back correctly but has no partition index would still be
// useless to everything above the sstable layer.
SEASTAR_THREAD_TEST_CASE(test_pq_sstable_has_index_summary_and_filter) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto muts = make_muts(s, 30, 5);
        const size_t n_part = muts.size();
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        BOOST_REQUIRE(sst->has_component(component_type::Index));
        BOOST_REQUIRE(sst->has_component(component_type::Summary));
        BOOST_REQUIRE(sst->has_component(component_type::Filter));
        BOOST_REQUIRE(sst->has_component(component_type::Statistics));
        BOOST_REQUIRE(sst->has_component(component_type::TOC));

        // Every key written must be found by the filter. A filter that says no
        // to a key that is present is a lost read, not a false positive.
        auto full = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(full.size(), n_part);
        for (const auto& m : full) {
            auto k = key::from_partition_key(*s, m.key());
            BOOST_REQUIRE(sst->filter_has_key(k));
        }

        BOOST_REQUIRE(sst->get_first_decorated_key().less_compare(
                *s, sst->get_last_decorated_key()));
    }).get();
}

// Single-partition reads go down a different path than a full scan: they use
// the partition range to seek rather than streaming everything.
SEASTAR_THREAD_TEST_CASE(test_pq_single_partition_read) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto muts = make_muts(s, 16, 10);
        auto expected = muts;
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        for (const auto& want : expected) {
            auto pr = dht::partition_range::make_singular(want.decorated_key());
            auto rd = sst->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
            auto close = deferred_close(rd);
            auto got = read_mutation_from_mutation_reader(rd).get();
            BOOST_REQUIRE(got);
            assert_that(*got).is_equal_to(want);
            BOOST_REQUIRE(!read_mutation_from_mutation_reader(rd).get());
        }
    }).get();
}

// A *bounded* range wide enough that the reader streams whole row groups instead of paging
// 512 rows at a time. That choice used to be made from the presence of a bound rather than the
// width of the range (design doc 10.26), so this is the case whose read path changed, and the
// thing to prove about it is that the rows did not: fragment for fragment against the row
// format, over the whole span, over an interior sub-range that starts and ends mid-row-group,
// and over a single partition -- the three shapes the per-row-group decision has to get right.
//
// 40 x 200 = 8 000 rows cuts more than one row group at the 5 000-row default, so the span case
// really does contain interior groups wanted whole, which is what the cheap half of the
// predicate resolves; the sub-range case exercises the other half, where a partial group is
// compared against its own page index.
SEASTAR_THREAD_TEST_CASE(test_pq_bounded_range_streams_and_agrees_with_row_format) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto muts = make_muts(s, 40, 200);
        auto ref = make_sstable_containing(
                env.make_sstable(s, sstables::get_highest_sstable_version()), muts).get();
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), muts).get();

        auto check = [&] (const char* what, const dht::partition_range& pr, bool expect_rows = true) {
            auto fw = fragments_in(ref, s, env.make_reader_permit(), pr, s->full_slice());
            auto fg = fragments_in(sst, s, env.make_reader_permit(), pr, s->full_slice());
            BOOST_TEST_CONTEXT(what) {
                BOOST_REQUIRE_EQUAL(fg.size(), fw.size());
                for (size_t i = 0; i < fg.size(); ++i) {
                    BOOST_REQUIRE_EQUAL(fg[i], fw[i]);
                }
                // Guards the guard: a range that silently returned nothing would pass every
                // comparison above.
                BOOST_REQUIRE_EQUAL(!fg.empty(), expect_rows);
            }
        };

        // The whole ring segment the sstable holds, bounded at both ends -- what a range scan
        // looks like once the coordinator has split it at a tablet boundary.
        check("full span, both bounds closed",
              dht::partition_range::make({muts.front().decorated_key(), true},
                                         {muts.back().decorated_key(), true}));
        // Open start, closed end: the first piece of a split, and the shape that made
        // `start() || end()` true for every scan in the first place.
        check("open start, closed end",
              dht::partition_range::make_ending_with({muts.back().decorated_key(), true}));
        check("closed start, open end",
              dht::partition_range::make_starting_with({muts.front().decorated_key(), true}));
        // An interior slab, and the same slab with its bounds excluded, so the boundary row
        // groups are partial at both ends.
        check("interior, inclusive",
              dht::partition_range::make({muts[7].decorated_key(), true},
                                         {muts[31].decorated_key(), true}));
        check("interior, exclusive",
              dht::partition_range::make({muts[7].decorated_key(), false},
                                         {muts[31].decorated_key(), false}));
        // One partition, which must keep taking the narrow path it was measured on.
        check("singular", dht::partition_range::make_singular(muts[19].decorated_key()));
        // A range that selects nothing between two adjacent keys.
        check("empty interior",
              dht::partition_range::make({muts[5].decorated_key(), false},
                                         {muts[6].decorated_key(), false}),
              false);
    }).get();
}

// Why the reader does *not* push column projection down, pinned as a test rather than left as a
// comment, because the natural "optimisation" is a data-loss bug and it would pass every other
// case in this file.
//
// A CQL row is live if its marker is live or any of its cells is. make_muts() writes cells with no
// row marker -- which is what an UPDATE produces -- so every row here is live only by virtue of
// its cells. A reader that honoured `with_no_regular_columns()` by not reading them would return
// rows with nothing in them, the compacting reader above it would judge those rows dead, and a
// `SELECT count(*)` or a `SELECT other_column` would quietly lose them. mx returns every cell
// whatever the slice says, and Cassandra reads every regular column from storage for this exact
// reason, so agreement with the row format is the contract.
SEASTAR_THREAD_TEST_CASE(test_pq_restricted_slice_still_returns_every_cell) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto muts = make_muts(s, 8, 30);
        auto ref = make_sstable_containing(
                env.make_sstable(s, sstables::get_highest_sstable_version()), muts).get();
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), muts).get();

        for (auto&& [what, slice] : std::vector<std::pair<const char*, query::partition_slice>>{
                {"no regular columns",
                 partition_slice_builder(*s).with_no_regular_columns().build()},
                {"one regular column",
                 partition_slice_builder(*s).with_regular_column(to_bytes("v_int")).build()}}) {
            auto fw = fragments_in(ref, s, env.make_reader_permit(),
                                   query::full_partition_range, slice);
            auto fg = fragments_in(sst, s, env.make_reader_permit(),
                                   query::full_partition_range, slice);
            BOOST_TEST_CONTEXT(what) {
                BOOST_REQUIRE(!fg.empty());
                BOOST_REQUIRE_EQUAL(fg.size(), fw.size());
                for (size_t i = 0; i < fg.size(); ++i) {
                    BOOST_REQUIRE_EQUAL(fg[i], fw[i]);
                }
                // And the cells really are there: a stream that had dropped them would still
                // match a row format that had dropped them too, if one ever did.
                BOOST_REQUIRE(std::ranges::any_of(fg, [] (const sstring& f) {
                    return f.find("v_txt") != sstring::npos;
                }));
            }
        }
    }).get();
}

// A full scan is what compaction and scrub use, and it goes through a separate
// entry point that had to be taught about pq independently of make_reader.
SEASTAR_THREAD_TEST_CASE(test_pq_full_scan_reader) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto muts = make_muts(s, 10, 12);
        auto expected = muts;
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        auto rd = sst->make_full_scan_reader(s, env.make_reader_permit(), nullptr,
                                             default_read_monitor());
        auto close = deferred_close(rd);
        size_t n = 0;
        while (auto m = read_mutation_from_mutation_reader(rd).get()) {
            assert_that(*m).is_equal_to(expected[n]);
            ++n;
        }
        BOOST_REQUIRE_EQUAL(n, expected.size());
    }).get();
}

namespace {

// A key source that resolves a key the way the real providers that issue ids do: by reading it.
//
// This is not a convenience of the stub, it is the property under test. The replicated provider
// keeps its keys in system_replicated_keys and answers key() with a CQL SELECT, and the KMIP and
// cloud providers answer it with a network round trip. Either way the pq read path suspends inside
// key_for_read() while still holding the reader permit it was admitted with -- and in the
// replicated provider's case what it suspends on is *another read admitted by the same
// reader_concurrency_semaphore*. So the stub takes a permit from the same semaphore, which is the
// smallest faithful model of that, and needs no encryption context, no system key and no keyspace.
//
// The nested permit is taken with a timeout on purpose. Without it a regression here does not fail
// the test, it wedges it: the nested read waits for admission that can never come, the outer read
// waits for the nested one, and nothing in the test would ever complete. A timeout turns the
// deadlock into a bounded, named failure -- see the test below for what it looks like.
class nested_read_key_source : public sstables::parquet::key_source {
    reader_concurrency_semaphore& _sem;
    schema_ptr _schema;
    db::timeout_clock::duration _nested_timeout;
    unsigned _resolutions = 0;

    static sstables::parquet::format::encryption_key test_key() {
        sstables::parquet::format::encryption_key k;
        k.bytes.assign(16, uint8_t(0x5a));
        return k;
    }

public:
    nested_read_key_source(reader_concurrency_semaphore& sem, schema_ptr s,
                           db::timeout_clock::duration nested_timeout)
        : _sem(sem), _schema(std::move(s)), _nested_timeout(nested_timeout) {}

    // How many times the read path asked for a key. Guards against a test that passes because
    // something warmed a cache rather than because the deadlock is gone.
    unsigned resolutions() const { return _resolutions; }

    // The write path does not run under a user read permit -- a flush runs in the memtable
    // scheduling group and a compaction in the compaction one, both of which route an internal
    // read to a *different* semaphore -- so there is nothing to model here.
    seastar::future<sstables::parquet::resolved_key> key_for_write(
            const sstables::parquet::key_options&) override {
        co_return sstables::parquet::resolved_key{test_key(), "pq-test-key"};
    }

    seastar::future<sstables::parquet::format::encryption_key> key_for_read(
            const sstables::parquet::key_options&, const seastar::sstring&) override {
        ++_resolutions;
        auto permit = co_await _sem.obtain_permit(_schema, "data-query", 1024,
                                                 db::timeout_clock::now() + _nested_timeout, {});
        co_return test_key();
    }

    seastar::future<> validate(const sstables::parquet::key_options&) override {
        return make_ready_future<>();
    }
};

// A key source that gives a *different* key to every distinct provider option set, which is the
// property per-column keys rest on: `encryption_key.<col>` names another option set, and if the
// source ignored the options and returned one key the whole feature would look like it worked while
// encrypting everything under the same key.
//
// It stands in for the provider only in this one respect. The real-provider integration is covered
// in encryption_at_rest_test (test_parquet_per_column_keys_through_local_file_provider), which goes
// through ent/encryption/parquet_key_source.cc and the local-file provider; this one exists to
// assert the *file structure*, which needs keys a test can recompute.
class per_options_key_source : public sstables::parquet::key_source {
    std::vector<sstables::parquet::key_options> _write_opts;
    std::vector<sstables::parquet::key_options> _read_opts;

public:
    // Deterministic in the options, so a test can derive the same key and parse the footer itself.
    static sstables::parquet::format::encryption_key key_of(
            const sstables::parquet::key_options& kopts) {
        size_t h = 0x9e3779b9;
        for (const auto& [k, v] : kopts) {
            for (char c : k) { h = h * 1099511628211u + uint8_t(c); }
            h = h * 1099511628211u + '=';
            for (char c : v) { h = h * 1099511628211u + uint8_t(c); }
            h = h * 1099511628211u + ',';
        }
        sstables::parquet::format::encryption_key k;
        k.bytes.resize(16);
        for (size_t i = 0; i < 16; ++i) { k.bytes[i] = uint8_t(h >> (8 * (i % 8))) ^ uint8_t(i); }
        return k;
    }

    const std::vector<sstables::parquet::key_options>& write_opts() const { return _write_opts; }
    const std::vector<sstables::parquet::key_options>& read_opts() const { return _read_opts; }

    seastar::future<sstables::parquet::resolved_key> key_for_write(
            const sstables::parquet::key_options& kopts) override {
        _write_opts.push_back(kopts);
        // An id derived from the options, so the reader's per-column id round trip is exercised
        // rather than defaulted to empty as the local-file provider would leave it.
        auto it = kopts.find("secret_key_file");
        co_return sstables::parquet::resolved_key{
                key_of(kopts), it == kopts.end() ? seastar::sstring("base") : it->second};
    }

    seastar::future<sstables::parquet::format::encryption_key> key_for_read(
            const sstables::parquet::key_options& kopts, const seastar::sstring&) override {
        _read_opts.push_back(kopts);
        co_return key_of(kopts);
    }

    seastar::future<> validate(const sstables::parquet::key_options&) override {
        return make_ready_future<>();
    }
};

schema_ptr pq_encrypted_schema() {
    return schema_builder(1, "ks", "pq_enc_tbl")
        .with_column("pk", utf8_type, column_kind::partition_key)
        .with_column("ck", int32_type, column_kind::clustering_key)
        .with_column("v_int", int32_type)
        .with_column("v_big", long_type)
        .with_column("v_dbl", double_type)
        .with_column("v_txt", utf8_type)
        .set_parquet_options({{"encryption", "aes_gcm_v1"},
                              {"cipher_algorithm", "AES/GCM/NoPadding"},
                              {"secret_key_strength", "128"}})
        .build();
}

// The same table, with two of its columns keyed separately -- `v_txt` and `v_big` under one key,
// `v_dbl` under another. Three distinct option sets in all, which is what makes the deduplication
// and the multi-key read paths both visible.
schema_ptr pq_percolumn_encrypted_schema() {
    return schema_builder(1, "ks", "pq_enc_pc_tbl")
        .with_column("pk", utf8_type, column_kind::partition_key)
        .with_column("ck", int32_type, column_kind::clustering_key)
        .with_column("v_int", int32_type)
        .with_column("v_big", long_type)
        .with_column("v_dbl", double_type)
        .with_column("v_txt", utf8_type)
        .set_parquet_options({{"encryption", "aes_gcm_v1"},
                              {"cipher_algorithm", "AES/GCM/NoPadding"},
                              {"secret_key_strength", "128"},
                              {"secret_key_file", "/keys/table.key"},
                              {"encryption_key.v_txt", "secret_key_file=/keys/pii.key"},
                              {"encryption_key.v_big", "secret_key_file=/keys/pii.key"},
                              {"encryption_key.v_dbl", "secret_key_file=/keys/dbl.key"}})
        .build();
}

} // namespace

// Reading an encrypted pq sstable whose key is not already resolved must not deadlock against the
// permit the read itself holds.
//
// The bug this pins down made *every* encrypted pq table unscannable. The read path resolved the
// key from inside load_footer(), while holding a reader permit that database::query() has marked
// need_cpu for the duration of the read; resolving the key is itself a read, and the semaphore
// will not admit a new read once `reader_concurrency_semaphore_cpu_concurrency` permits are
// need_cpu and not awaiting. So the key lookup queued behind the read that was waiting for it.
// The node's own diagnostics, which only appear when the permit's TTL finally expires, said it
// exactly:
//
//     Identified bottleneck(s): CPU
//     permits count memory table/operation/state
//     2       2     34K    pqdl.t/data-query/active/need_cpu
//     2       0     0B     system_replicated_keys.encrypted_keys/data-query/waiting_for_admission
//     reads_queued_because_need_cpu_permits: 2
//     need_cpu_permits: 2
//     awaits_permits: 0
//
// Two things about that shape are worth keeping in the test. First, awaits_permits: 0 is the
// defect -- the reader was not using the CPU, it was waiting for a key, and it never said so.
// Second, the count is what hid the bug: a point read is one need_cpu permit against a default
// cpu_concurrency of 2, so it always had a slot left for its own key lookup and always worked. A
// range scan runs two reads at a time and closed that slot. That asymmetry looked like a property
// of the scan path and is not one -- it is arithmetic -- which is why this test scans, and why it
// runs against the test env's semaphore whose cpu_concurrency is 1 and so needs only one reader.
//
// Without the fix this fails within `nested_timeout` rather than hanging, with the semaphore
// naming itself in the message:
//
//     fatal error: in "test_pq_encrypted_read_does_not_deadlock_on_its_own_key_lookup":
//     std::_Nested_exception<std::runtime_error>: pq: <...>-big-Data.db is encrypted, but its key
//     could not be obtained from the key provider (id 'pq-test-key'); Caused by
//     seastar::named_semaphore_timed_out: Semaphore timed out: sstables::test_env
//
// preceded by the semaphore's own dump, which reports reads_queued_because_need_cpu_permits: 1,
// need_cpu_permits: 1 and awaits_permits: 0 -- the single-reader form of the production shape
// above. `nested_timeout` is what distinguishes a
// deadlock from a slow provider: it is orders of magnitude longer than the microseconds this
// resolution needs when it is admitted at all, so a failure here means "never admitted", not
// "admitted late".
SEASTAR_THREAD_TEST_CASE(test_pq_encrypted_read_does_not_deadlock_on_its_own_key_lookup) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        using namespace std::chrono_literals;
        auto s = pq_encrypted_schema();

        nested_read_key_source ks(env.semaphore(), s, 10s);
        auto* const prev_ks = sstables::parquet::key_source_ptr();
        sstables::parquet::set_key_source(&ks);
        auto restore = defer([prev_ks] () noexcept {
            sstables::parquet::set_key_source(prev_ks);
        });

        auto muts = make_muts(s, 8, 20);
        auto expected = muts;
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        // The file has to actually be encrypted, or the rest of this proves nothing: a plaintext
        // footer is "PAR1" and never asks for a key at all.
        const uint64_t len = sst->ondisk_data_size();
        auto tail = sst->data_read(len - 8, 8, env.make_reader_permit()).get();
        BOOST_REQUIRE_EQUAL(std::string_view(tail.get() + 4, 4),
                            std::string_view(sstables::parquet::format::magic_encrypted, 4));

        // Nothing has resolved the read key yet: writing used key_for_write, and the footer cache
        // deliberately does not retain the key. This is the state a fresh node is in for every
        // encrypted table it has not read yet.
        const auto resolutions_before = ks.resolutions();
        const auto queued_before =
                env.semaphore().get_stats().reads_queued_because_need_cpu_permits;

        auto permit = env.make_reader_permit();
        // Exactly what database::query()'s read_func does around every user read, and the reason
        // the deadlock exists at all.
        reader_permit::need_cpu_guard ncpu{permit};

        auto got = read_all(sst, s, permit);

        BOOST_REQUIRE_EQUAL(got.size(), expected.size());
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(expected[i]);
        }
        // The scan really did resolve the key, rather than finding it already resolved.
        BOOST_REQUIRE_GT(ks.resolutions(), resolutions_before);
        // And it was admitted straight away instead of queueing behind its own reader.
        BOOST_REQUIRE_EQUAL(env.semaphore().get_stats().reads_queued_because_need_cpu_permits,
                            queued_before);
    }).get();
}

// Per-column encryption keys through the Scylla layers: the `parquet` property names a separate key
// for some columns, the writer resolves each through the key source, the file records them as
// column keys, and the reader resolves all of them and reads the table back.
//
// Three things are asserted that no format-level test can, because they are about the Scylla layers
// rather than the codec:
//
//  1. The *file* really is per-column encrypted. A reader holding only the footer key sees the
//     keyed columns' chunks with `meta` absent and their metadata sitting in
//     encrypted_column_metadata, and every other column inline as usual. This is the partial-access
//     property, and asserting it here is what makes the test fail if `wopt.encryption.column_keys`
//     is never populated -- which is exactly the state the tree was in.
//  2. Keys are deduplicated by option set. Two columns naming the same key must cost one provider
//     round trip on write and one on read, not two. This matters beyond tidiness: every read-side
//     lookup happens under the reader permit (§11.1 B3).
//  3. The round trip is byte-exact through the real read path, with three keys in play.
SEASTAR_THREAD_TEST_CASE(test_pq_per_column_encryption_keys_round_trip) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        namespace pf = sstables::parquet::format;
        auto s = pq_percolumn_encrypted_schema();

        per_options_key_source ks;
        auto* const prev_ks = sstables::parquet::key_source_ptr();
        sstables::parquet::set_key_source(&ks);
        auto restore = defer([prev_ks] () noexcept {
            sstables::parquet::set_key_source(prev_ks);
        });

        auto muts = make_muts(s, 6, 12);
        auto expected = muts;
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        // Three distinct option sets: the table's, the PII one shared by v_txt and v_big, and
        // v_dbl's. Four columns are keyed-or-footer'd but only three keys exist, so anything that
        // resolved per column rather than per option set would report four.
        BOOST_REQUIRE_EQUAL(ks.write_opts().size(), 3u);

        // The option sets the writer asked for must differ in the one option the property
        // overrode and agree on everything else -- that is what "overlaid on the table's options"
        // means, and a column landing on the default provider would show up here.
        for (const auto& o : ks.write_opts()) {
            BOOST_REQUIRE_EQUAL(o.at("cipher_algorithm"), "AES/GCM/NoPadding");
            BOOST_REQUIRE_EQUAL(o.at("secret_key_strength"), "128");
        }

        // ---- (1) the file's own structure, read with the footer key alone.
        const auto footer_key = per_options_key_source::key_of(
                sstables::parquet::parquet_parameters(s->parquet_options()).key_opts());
        const uint64_t len = sst->ondisk_data_size();
        auto image = sst->data_read(0, len, env.make_reader_permit()).get();
        auto img = std::span<const uint8_t>(
                reinterpret_cast<const uint8_t*>(image.get()), image.size());

        const std::set<std::string> keyed{"v_txt", "v_big", "v_dbl"};
        {
            auto ef = pf::parse_encrypted_footer(img, footer_key);
            BOOST_REQUIRE(!ef.md.row_groups.empty());
            size_t n_keyed = 0, n_footer = 0;
            for (const auto& ch : ef.md.row_groups[0].columns) {
                BOOST_REQUIRE(ch.crypto_metadata);
                if (ch.crypto_metadata->with_footer_key) {
                    // Encrypted under the footer key, so its metadata is inline as always.
                    BOOST_REQUIRE(ch.meta);
                    ++n_footer;
                    continue;
                }
                // Its own key: the footer names the column and nothing else about it.
                BOOST_REQUIRE(!ch.meta);
                BOOST_REQUIRE(ch.encrypted_column_metadata);
                BOOST_REQUIRE(!ch.crypto_metadata->path_in_schema.empty());
                const auto leaf = ch.crypto_metadata->path_in_schema.back();
                BOOST_REQUIRE_MESSAGE(keyed.contains(leaf),
                                      seastar::format("unexpected column key on leaf '{}'", leaf));
                ++n_keyed;
            }
            BOOST_REQUIRE_EQUAL(n_keyed, keyed.size());
            BOOST_REQUIRE_GT(n_footer, 0u);
        }

        // Handed the column keys as well, the same footer yields the metadata the previous parse
        // could not see. Without this the check above would also pass on a file whose keyed
        // columns were simply corrupt.
        {
            std::map<std::string, pf::encryption_key> cks;
            for (const auto& [col, kopts] :
                     sstables::parquet::parquet_parameters(s->parquet_options())
                             .column_key_opts()) {
                cks[std::string(col)] = per_options_key_source::key_of(kopts);
            }
            BOOST_REQUIRE_EQUAL(cks.size(), keyed.size());
            auto ef = pf::parse_encrypted_footer(img, footer_key, {}, cks);
            for (const auto& ch : ef.md.row_groups[0].columns) {
                BOOST_REQUIRE(ch.meta);
            }
        }

        // ---- (3) the round trip through the real read path, and (2) on the read side.
        const auto reads_before = ks.read_opts().size();
        auto got = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(got.size(), expected.size());
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(expected[i]);
        }
        // One footer key plus two distinct column keys. Not "at least": a per-column lookup rather
        // than a per-option-set one would make this four, and that difference is the whole of the
        // B3 amplification this feature adds, so it is asserted exactly.
        BOOST_REQUIRE_EQUAL(ks.read_opts().size() - reads_before, 3u);
    }).get();
}

// A column key the schema cannot locate must fail the read, loudly.
//
// This is the failure mode the feature must never have. format::parse_encrypted_footer tolerates a
// chunk whose key it lacks by leaving `meta` empty -- correct for a general reader that legitimately
// holds only some keys -- but inside Scylla an empty `meta` is a column with no pages, which
// reassembles as an all-null column. "No access" rendering as "no data" is silent data loss, so the
// read path refuses instead.
//
// The unavailability is arranged the way it will actually happen: an ALTER drops
// `encryption_key.<col>` from the property while files written under it still exist. The provider
// options live in the schema, not in the file (see read_crypto_for), so this is the one edit that
// takes a key away.
SEASTAR_THREAD_TEST_CASE(test_pq_missing_column_key_fails_loudly) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_percolumn_encrypted_schema();

        per_options_key_source ks;
        auto* const prev_ks = sstables::parquet::key_source_ptr();
        sstables::parquet::set_key_source(&ks);
        auto restore = defer([prev_ks] () noexcept {
            sstables::parquet::set_key_source(prev_ks);
        });

        auto muts = make_muts(s, 4, 8);
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        // The same table with v_txt's key no longer named. Everything else is identical, so the
        // sstable is still this schema's own file -- which is the point: the data is there, the
        // key is not.
        auto altered = schema_builder(s)
                .set_parquet_options({{"encryption", "aes_gcm_v1"},
                                      {"cipher_algorithm", "AES/GCM/NoPadding"},
                                      {"secret_key_strength", "128"},
                                      {"secret_key_file", "/keys/table.key"},
                                      {"encryption_key.v_big", "secret_key_file=/keys/pii.key"},
                                      {"encryption_key.v_dbl", "secret_key_file=/keys/dbl.key"}})
                .build();

        bool threw = false;
        try {
            auto got = read_all(sst, altered, env.make_reader_permit());
            // If this is ever reached the failure is not "an exception was missing", it is that
            // the read returned rows for a column it cannot decrypt. Say which.
            BOOST_FAIL(seastar::format(
                    "read of a file with an unavailable column key returned {} partitions instead "
                    "of failing", got.size()));
        } catch (const std::exception& e) {
            threw = true;
            const std::string what = e.what();
            // The message has to name the column and say where to put the option back; a bare
            // decode error would leave an operator with no way to act.
            BOOST_REQUIRE_MESSAGE(what.find("v_txt") != std::string::npos,
                                  "message does not name the column: " + what);
            BOOST_REQUIRE_MESSAGE(what.find("encryption_key.v_txt") != std::string::npos,
                                  "message does not name the missing sub-option: " + what);
        }
        BOOST_REQUIRE(threw);
    }).get();
}

// Row markers and tombstones through the real sstable path. Each of these was a
// silent-data-loss bug before: a lost marker deletes a row that exists, and a
// lost tombstone resurrects one that does not.
SEASTAR_THREAD_TEST_CASE(test_pq_markers_and_tombstones_round_trip) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        utils::chunked_vector<mutation> muts;

        for (int p = 0; p < 24; ++p) {
            auto pk = partition_key::from_single_value(
                    *s, utf8_type->decompose(sstring(format("key{:04d}", p))));
            mutation m(s, pk);
            const api::timestamp_type ts = 2000 + p;

            // Every fourth partition is deleted outright.
            if (p % 4 == 0) {
                m.partition().apply(tombstone(ts - 1, gc_clock::time_point(gc_clock::duration(p + 1))));
            }
            for (int r = 0; r < 6; ++r) {
                auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                auto& dr = m.partition().clustered_row(*s, ck);

                if (r % 3 == 0) {
                    // A row that exists purely because of its marker: no cells at
                    // all. This is the case that vanished entirely before.
                    dr.apply(row_marker(ts));
                } else if (r % 3 == 1) {
                    dr.apply(row_marker(ts, gc_clock::duration(3600),
                                        gc_clock::time_point(gc_clock::duration(ts + 3600))));
                    dr.cells().apply(*s->get_column_definition(to_bytes("v_int")),
                            atomic_cell::make_live(*int32_type, ts, int32_type->decompose(r)));
                } else {
                    dr.apply(row_tombstone(tombstone(ts,
                            gc_clock::time_point(gc_clock::duration(p * 10 + r)))));
                }
            }
            muts.push_back(std::move(m));
        }
        std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
            return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
        });
        auto expected = muts;

        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();
        auto got = read_all(sst, s, env.make_reader_permit());

        BOOST_REQUIRE_EQUAL(got.size(), expected.size());
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(expected[i]);
        }
    }).get();
}

// Static rows: held by the shredder and replayed onto every row of the
// partition, then split back out on read. The interesting cases are a partition
// with a static row and no clustering rows at all, which has no row to attach it
// to, and a static-only partition that is also deleted.
SEASTAR_THREAD_TEST_CASE(test_pq_static_rows_round_trip) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = schema_builder(1, "ks", "pq_static")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("st", int32_type, column_kind::static_column)
            .with_column("st2", utf8_type, column_kind::static_column)
            .with_column("v", int32_type)
            .build();

        utils::chunked_vector<mutation> muts;
        for (int p = 0; p < 20; ++p) {
            auto pk = partition_key::from_single_value(
                    *s, utf8_type->decompose(sstring(format("k{:04d}", p))));
            mutation m(s, pk);
            const api::timestamp_type ts = 500 + p;

            if (p % 4 != 3) {
                m.set_static_cell(*s->get_column_definition(to_bytes("st")),
                        atomic_cell::make_live(*int32_type, ts, int32_type->decompose(p)));
                m.set_static_cell(*s->get_column_definition(to_bytes("st2")),
                        atomic_cell::make_live(*utf8_type, ts,
                                utf8_type->decompose(sstring(format("s{}", p % 5)))));
            }
            // p % 4 == 1 is static-only: no clustering rows at all.
            if (p % 4 != 1) {
                for (int r = 0; r < 3; ++r) {
                    auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                    m.set_clustered_cell(ck, *s->get_column_definition(to_bytes("v")),
                            atomic_cell::make_live(*int32_type, ts, int32_type->decompose(r * 3)));
                }
            }
            // p % 4 == 3 has neither statics nor rows; skip it entirely.
            if (p % 4 != 3) {
                muts.push_back(std::move(m));
            }
        }
        std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
            return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
        });
        auto expected = muts;

        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();
        auto got = read_all(sst, s, env.make_reader_permit());

        BOOST_REQUIRE_EQUAL(got.size(), expected.size());
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(expected[i]);
        }
    }).get();
}

// Range tombstones. They are fragments *between* rows, not attributes of one, so
// they are carried as marked rows that keep their place in the clustering order:
// the clustering columns hold the bound's prefix, and __rtc_len says how much of
// that prefix is real.
SEASTAR_THREAD_TEST_CASE(test_pq_range_tombstones_round_trip) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        utils::chunked_vector<mutation> muts;
        for (int p = 0; p < 12; ++p) {
            auto pk = partition_key::from_single_value(
                    *s, utf8_type->decompose(sstring(format("key{:04d}", p))));
            mutation m(s, pk);
            const api::timestamp_type ts = 3000 + p;
            for (int r = 0; r < 10; ++r) {
                auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                m.set_clustered_cell(ck, *s->get_column_definition(to_bytes("v_int")),
                        atomic_cell::make_live(*int32_type, ts, int32_type->decompose(r)));
            }
            // A deleted band in the middle, with the bound kinds varied so both
            // inclusive and exclusive bounds are exercised.
            auto lo = clustering_key_prefix::from_single_value(*s, int32_type->decompose(3));
            auto hi = clustering_key_prefix::from_single_value(*s, int32_type->decompose(6));
            m.partition().apply_delete(*s, range_tombstone(
                    std::move(lo), p % 2 ? bound_kind::incl_start : bound_kind::excl_start,
                    std::move(hi), p % 2 ? bound_kind::excl_end : bound_kind::incl_end,
                    tombstone(ts + 1, gc_clock::time_point(gc_clock::duration(p + 1)))));
            muts.push_back(std::move(m));
        }
        std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
            return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
        });
        // Compare pq against the default format rather than against the in-memory
        // mutation: the write path legitimately drops rows a newer range tombstone
        // shadows, and the question here is whether pq behaves like mc, not
        // whether either matches an unwritten mutation.
        auto ref = make_sstable_containing(
                env.make_sstable(s, sstables::get_highest_sstable_version()), muts).get();
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        auto want = read_all(ref, s, env.make_reader_permit());
        auto got  = read_all(sst, s, env.make_reader_permit());

        BOOST_REQUIRE_EQUAL(got.size(), want.size());
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(want[i]);
        }

        // And the raw streams, which keep the bound weights that reassembly
        // normalises away.
        auto fw = fragments_of(ref, s, env.make_reader_permit());
        auto fg = fragments_of(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(fg.size(), fw.size());
        for (size_t i = 0; i < fg.size(); ++i) {
            BOOST_REQUIRE_EQUAL(fg[i], fw[i]);
        }
    }).get();
}

// sstable::validate() deliberately excludes pq from mx::validate -- mx walks the
// mx data format, which a Parquet Data component is not -- so it falls through
// to the generic validator, which reads through make_full_scan_reader and hence
// through the pq reader. That fall-through is easy to get wrong and silent when
// it is: a validator that cannot parse the format would either crash or report
// phantom errors.
SEASTAR_THREAD_TEST_CASE(test_pq_sstable_validates_clean) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), make_muts(s, 20, 8)).get();

        abort_source abort;
        uint64_t reported = 0;
        auto errors = sst->validate(env.make_reader_permit(), abort,
                                    [&reported] (sstring) { ++reported; },
                                    default_read_monitor()).get();
        BOOST_REQUIRE_EQUAL(errors, 0);
        BOOST_REQUIRE_EQUAL(reported, 0);
    }).get();
}

// Compaction reads through make_full_scan_reader and writes through the normal
// writer, so a pq-to-pq compaction exercises both halves at once. It is also the
// path that a hybrid LSM would use to converge a table onto Parquet.
SEASTAR_THREAD_TEST_CASE(test_pq_sstables_compact_into_one) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();

        // Two disjoint halves, so the merged output should be their union.
        auto all = make_muts(s, 24, 6);
        utils::chunked_vector<mutation> a, b;
        for (size_t i = 0; i < all.size(); ++i) {
            (i % 2 ? b : a).push_back(all[i]);
        }
        auto sst_a = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(a)).get();
        auto sst_b = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(b)).get();

        // Merge the two full-scan readers and write the result as a third pq
        // sstable, which is what compaction does.
        auto out = env.make_sstable(s, sstable_version_types::pq);
        {
            std::vector<mutation_reader> rds;
            rds.push_back(sst_a->make_full_scan_reader(s, env.make_reader_permit(), nullptr,
                                                       default_read_monitor()));
            rds.push_back(sst_b->make_full_scan_reader(s, env.make_reader_permit(), nullptr,
                                                       default_read_monitor()));
            auto merged = make_combined_reader(s, env.make_reader_permit(), std::move(rds));
            auto cfg = env.manager().configure_writer("test");
            out->write_components(std::move(merged), all.size(), s, cfg, encoding_stats{}).get();
            out->open_data().get();
        }

        auto got = read_all(out, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(got.size(), all.size());
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(all[i]);
        }
    }).get();
}

// ---------------------------------------------------------------------------
// Mixed-format (hybrid) tables.
//
// `storage_format = 'hybrid'` means native and pq sstables coexist in one table, so every read
// merges across both and every compaction can take one of each as input. Until now nothing tested
// that. The pq suite built a native sstable beside a pq one in eight places, but always to compare
// two *separate* readers -- never to merge them -- and the one place a mixed compaction actually
// happens (cql_ddl_test's test_storage_format_converts_on_compaction) neither asserts that the
// input set was mixed nor reads back the rows that came through the native input.
//
// That gap matters more than the single-format round trip, because merging is where a wrong
// encoding turns into wrong data rather than a wrong file. A tombstone that pq stores as an absent
// cell reads back correctly from the pq sstable alone -- there is nothing there to contradict it --
// and only resurrects the value it was meant to shadow when an older sstable supplies one. The
// merge is the first place the difference between `dead` and `absent` is observable at all.
//
// The reference in all of these is the same merge with both inputs in the native format: the
// question is whether pq behaves like the row format under merge, not whether either matches some
// hand-written expectation.

namespace {

// A static column is included deliberately: statics are shredded as regular `__s_<name>` columns
// replayed onto every row and split back out on read, so a static cell deleted in the overlay is
// the case where a lost `__dmask` bit resurrects a value on *every* row of the partition.
schema_ptr hybrid_schema() {
    return schema_builder(1, "ks", "hyb_tbl")
        .with_column("pk", utf8_type, column_kind::partition_key)
        .with_column("ck", int32_type, column_kind::clustering_key)
        .with_column("a", int32_type)
        .with_column("b", int32_type)
        .with_column("c", utf8_type)
        .with_column("st", int32_type, column_kind::static_column)
        .build();
}

constexpr int hyb_parts = 8;
constexpr int hyb_rows = 10;

partition_key hyb_pk(const schema& s, int p) {
    return partition_key::from_single_value(s, utf8_type->decompose(sstring(format("hk{:04d}", p))));
}

clustering_key hyb_ck(const schema& s, int r) {
    return clustering_key::from_single_value(s, int32_type->decompose(r));
}

void hyb_sort(utils::chunked_vector<mutation>& muts) {
    std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
        return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
    });
}

// The older generation: every row fully populated, one low timestamp band. Partition 6 is absent,
// so the merge has a partition that exists only in the overlay.
utils::chunked_vector<mutation> hybrid_base(schema_ptr s) {
    const auto& adef = *s->get_column_definition(to_bytes("a"));
    const auto& bdef = *s->get_column_definition(to_bytes("b"));
    const auto& cdef = *s->get_column_definition(to_bytes("c"));
    const auto& stdef = *s->get_column_definition(to_bytes("st"));

    utils::chunked_vector<mutation> muts;
    for (int p = 0; p < hyb_parts; ++p) {
        if (p == 6) { continue; }
        mutation m(s, hyb_pk(*s, p));
        const api::timestamp_type ts = 1000 + p;
        m.set_static_cell(stdef, atomic_cell::make_live(*int32_type, ts,
                                                        int32_type->decompose(p * 11)));
        for (int r = 0; r < hyb_rows; ++r) {
            if (r == 9) { continue; }   // row 9 exists only in the overlay
            auto ck = hyb_ck(*s, r);
            // Row 8's marker is deliberately *newer* than its cells, and newer than the
            // shadowable tombstone the overlay puts on it. That is the only arrangement under
            // which the shadowable and regular halves of a row tombstone are distinguishable:
            // the marker cancels a shadowable tombstone but not a regular one, so a pq reader
            // that collapses the two halves deletes cells that must survive. It is observable
            // only against an older generation -- from the overlay alone there are no cells for
            // the difference to act on -- which is why it lives here and not in the
            // single-sstable round-trip tests.
            m.partition().clustered_row(*s, ck).apply(row_marker(r == 8 ? ts + 2000 : ts));
            m.set_clustered_cell(ck, adef, atomic_cell::make_live(
                    *int32_type, ts, int32_type->decompose(r)));
            m.set_clustered_cell(ck, bdef, atomic_cell::make_live(
                    *int32_type, ts, int32_type->decompose(r * 100)));
            m.set_clustered_cell(ck, cdef, atomic_cell::make_live(
                    *utf8_type, ts, utf8_type->decompose(sstring(format("base{}", r)))));
        }
        muts.push_back(std::move(m));
    }
    hyb_sort(muts);
    return muts;
}

// The newer generation: one of every tombstone and update shape, at a strictly higher timestamp
// band so it always wins the merge. Partition 5 is absent, so the merge also has a partition that
// exists only in the base.
utils::chunked_vector<mutation> hybrid_overlay(schema_ptr s) {
    const auto& adef = *s->get_column_definition(to_bytes("a"));
    const auto& cdef = *s->get_column_definition(to_bytes("c"));
    const auto& stdef = *s->get_column_definition(to_bytes("st"));

    utils::chunked_vector<mutation> muts;
    for (int p = 0; p < hyb_parts; ++p) {
        if (p == 5) { continue; }
        mutation m(s, hyb_pk(*s, p));
        const api::timestamp_type ts = 2000 + p;
        const auto ldt = gc_clock::time_point(gc_clock::duration(70000 + p));

        // Partition 7 is deleted outright: everything the base holds for it must disappear.
        if (p == 7) {
            m.partition().apply(tombstone(ts, ldt));
            muts.push_back(std::move(m));
            continue;
        }

        // A static cell updated, deleted, or left alone -- the deleted arm is the one that must
        // not come back as absent.
        if (p % 3 == 0) {
            m.set_static_cell(stdef, atomic_cell::make_live(*int32_type, ts,
                                                           int32_type->decompose(p * 11 + 1)));
        } else if (p % 3 == 1) {
            m.set_static_cell(stdef, atomic_cell::make_dead(ts, ldt));
        }

        // Row 0: a cell updated, with `b` untouched, so the merge must take `a` from the overlay
        // and `b` from the base. A cell wrongly written as absent is indistinguishable from
        // "untouched" here, which is exactly the confusion being tested.
        m.set_clustered_cell(hyb_ck(*s, 0), adef, atomic_cell::make_live(
                *int32_type, ts, int32_type->decompose(999)));

        // Row 1: a cell deleted. `b` and `c` must survive from the base, `a` must not.
        m.set_clustered_cell(hyb_ck(*s, 1), adef, atomic_cell::make_dead(ts, ldt));

        // Row 2: a row tombstone.
        m.partition().clustered_row(*s, hyb_ck(*s, 2)).apply(row_tombstone(tombstone(ts, ldt)));

        // Row 3: a shadowable tombstone plus a marker -- what an UPDATE produces. The regular half
        // must stay empty; collapsing the two would delete cells that should survive.
        {
            auto& dr = m.partition().clustered_row(*s, hyb_ck(*s, 3));
            dr.apply(row_marker(ts));
            dr.apply(shadowable_tombstone(ts, ldt));
        }

        // Rows 4-6: a range tombstone over a band the base populated. If pq loses the bound weight
        // or the tombstone itself, three rows come back from the dead.
        m.partition().apply_delete(*s, range_tombstone(
                clustering_key_prefix::from_single_value(*s, int32_type->decompose(4)),
                bound_kind::incl_start,
                clustering_key_prefix::from_single_value(*s, int32_type->decompose(6)),
                bound_kind::incl_end,
                tombstone(ts, ldt)));

        // Row 7: an expiring cell. A live cell with a TTL carries a deletion time too, so it is
        // the case that must *not* be read as dead.
        m.set_clustered_cell(hyb_ck(*s, 7), cdef, atomic_cell::make_live(
                *utf8_type, ts, utf8_type->decompose(sstring("ttl")),
                gc_clock::time_point(gc_clock::duration(90000 + p)), gc_clock::duration(600)));

        // Row 8: a shadowable tombstone with no regular half, sitting *below* the base row's
        // marker. The marker cancels it, so every base cell on row 8 must survive. If pq rebuilds
        // the row tombstone with its regular half as strong as its shadowable one, those cells are
        // deleted instead -- a silent data loss that no single-format round trip can see.
        m.partition().clustered_row(*s, hyb_ck(*s, 8)).apply(shadowable_tombstone(ts, ldt));

        // Row 9 exists only in the overlay.
        m.partition().clustered_row(*s, hyb_ck(*s, 9)).apply(row_marker(ts));
        m.set_clustered_cell(hyb_ck(*s, 9), adef, atomic_cell::make_live(
                *int32_type, ts, int32_type->decompose(9)));

        muts.push_back(std::move(m));
    }
    hyb_sort(muts);
    return muts;
}

// Dead atomic cells, regular and static, in a reassembled result. The merge assertions compare
// against a native reference, so they would pass just as well if *both* sides lost every
// tombstone; this is what stops that.
size_t count_dead_cells(const schema& s, const utils::chunked_vector<mutation>& ms) {
    size_t n = 0;
    auto scan = [&] (const row& cells, column_kind kind) {
        cells.for_each_cell([&] (column_id id, const atomic_cell_or_collection& acoc) {
            const auto& def = s.column_at(kind, id);
            if (def.is_atomic() && !acoc.as_atomic_cell(def).is_live()) { ++n; }
        });
    };
    for (const auto& m : ms) {
        scan(m.partition().static_row().get(), column_kind::static_column);
        for (const rows_entry& re : m.partition().clustered_rows()) {
            scan(re.row().cells(), column_kind::regular_column);
        }
    }
    return n;
}

size_t count_rows(const utils::chunked_vector<mutation>& ms) {
    size_t n = 0;
    for (const auto& m : ms) {
        n += std::distance(m.partition().clustered_rows().begin(),
                           m.partition().clustered_rows().end());
    }
    return n;
}

} // namespace

// A read that spans a native and a pq sstable in the same table, in both orders.
//
// The three arms are the two mixed orders plus an all-pq control. The control matters: if pq lost a
// tombstone, the all-pq arm would lose it on both sides of the merge and could still agree with
// itself -- it is the *mixed* arms that catch it, and having the control in the same test makes it
// obvious which one failed.
SEASTAR_THREAD_TEST_CASE(test_hybrid_read_merges_native_and_parquet) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = hybrid_schema();
        auto base = hybrid_base(s);
        auto over = hybrid_overlay(s);
        const auto nat = sstables::get_highest_sstable_version();

        auto mk = [&] (sstable_version_types v, const utils::chunked_vector<mutation>& ms) {
            return make_sstable_containing(env.make_sstable(s, v), ms).get();
        };
        auto nat_base = mk(nat, base), nat_over = mk(nat, over);
        auto pq_base = mk(sstable_version_types::pq, base);
        auto pq_over = mk(sstable_version_types::pq, over);

        // The query path: two make_reader()s over the full range, combined. This is what a
        // coordinator read of a hybrid table does.
        auto merged = [&] (shared_sstable x, shared_sstable y) {
            std::vector<mutation_reader> rds;
            rds.push_back(x->make_reader(s, env.make_reader_permit(),
                                         query::full_partition_range, s->full_slice()));
            rds.push_back(y->make_reader(s, env.make_reader_permit(),
                                         query::full_partition_range, s->full_slice()));
            return make_combined_reader(s, env.make_reader_permit(), std::move(rds));
        };
        auto merged_frags = [&] (shared_sstable x, shared_sstable y) {
            auto rd = merged(x, y);
            auto close = deferred_close(rd);
            std::vector<sstring> out;
            while (auto mf = rd().get()) {
                out.push_back(seastar::format("{}", mutation_fragment_v2::printer(*s, *mf)));
            }
            return out;
        };
        auto merged_muts = [&] (shared_sstable x, shared_sstable y) {
            auto rd = merged(x, y);
            auto close = deferred_close(rd);
            utils::chunked_vector<mutation> out;
            while (auto m = read_mutation_from_mutation_reader(rd).get()) {
                out.push_back(std::move(*m));
            }
            return out;
        };

        const auto want_f = merged_frags(nat_base, nat_over);
        const auto want_m = merged_muts(nat_base, nat_over);

        // Preconditions on the reference, so none of the comparisons below can pass by being
        // uniformly empty. The row count is the range-tombstone check: the base wrote 9 rows for
        // each of 7 partitions and the overlay adds row 9, but rows 4-6 are deleted in every
        // partition the overlay touches and partition 7 is deleted whole.
        const size_t dead_ref = count_dead_cells(*s, want_m);
        BOOST_REQUIRE_GT(dead_ref, 0u);
        BOOST_REQUIRE_GT(want_m.size(), 0u);
        const size_t rows_ref = count_rows(want_m);
        BOOST_REQUIRE_GT(rows_ref, 0u);
        BOOST_REQUIRE_LT(rows_ref, size_t(hyb_parts * hyb_rows));

        struct arm { const char* what; shared_sstable lo; shared_sstable hi; };
        for (auto a : {arm{"native base + parquet overlay", nat_base, pq_over},
                       arm{"parquet base + native overlay", pq_base, nat_over},
                       arm{"parquet base + parquet overlay (control)", pq_base, pq_over}}) {
            BOOST_TEST_CONTEXT("mixed set: " << a.what) {
                // Fragments first: reassembling into mutations normalises range-tombstone bounds,
                // which is precisely what a lost bound weight would hide.
                auto got_f = merged_frags(a.lo, a.hi);
                BOOST_REQUIRE_EQUAL(got_f.size(), want_f.size());
                for (size_t i = 0; i < got_f.size(); ++i) {
                    BOOST_REQUIRE_EQUAL(got_f[i], want_f[i]);
                }

                auto got_m = merged_muts(a.lo, a.hi);
                BOOST_REQUIRE_EQUAL(got_m.size(), want_m.size());
                for (size_t i = 0; i < got_m.size(); ++i) {
                    assert_that(got_m[i]).is_equal_to(want_m[i]);
                }
                BOOST_REQUIRE_EQUAL(count_dead_cells(*s, got_m), dead_ref);
                BOOST_REQUIRE_EQUAL(count_rows(got_m), rows_ref);
            }
        }
    }).get();
}

// A compaction whose input set is mixed, writing each output format in turn.
//
// This is the ICS-under-hybrid steady state and the duration of any ALTER between formats: the
// merge happens through make_full_scan_reader rather than make_reader, and its result is written
// back out, so a tombstone lost here is lost *permanently* -- the next compaction has no older
// sstable left to contradict. Both output formats are covered because the hybrid decision can go
// either way for the same input set depending on tiering.
SEASTAR_THREAD_TEST_CASE(test_hybrid_compaction_merges_native_and_parquet) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = hybrid_schema();
        auto base = hybrid_base(s);
        auto over = hybrid_overlay(s);
        const auto nat = sstables::get_highest_sstable_version();
        const size_t n_part = std::max(base.size(), over.size());

        auto mk = [&] (sstable_version_types v, const utils::chunked_vector<mutation>& ms) {
            return make_sstable_containing(env.make_sstable(s, v), ms).get();
        };
        auto nat_base = mk(nat, base), nat_over = mk(nat, over);
        auto pq_base = mk(sstable_version_types::pq, base);
        auto pq_over = mk(sstable_version_types::pq, over);

        // Compact `x` and `y` into one sstable of version `out_v`, the way compaction does.
        auto compact = [&] (shared_sstable x, shared_sstable y, sstable_version_types out_v) {
            auto out = env.make_sstable(s, out_v);
            std::vector<mutation_reader> rds;
            rds.push_back(x->make_full_scan_reader(s, env.make_reader_permit(), nullptr,
                                                  default_read_monitor()));
            rds.push_back(y->make_full_scan_reader(s, env.make_reader_permit(), nullptr,
                                                  default_read_monitor()));
            auto merged = make_combined_reader(s, env.make_reader_permit(), std::move(rds));
            auto cfg = env.manager().configure_writer("test");
            out->write_components(std::move(merged), n_part, s, cfg, encoding_stats{}).get();
            out->open_data().get();
            return out;
        };

        // The reference: native inputs, native output -- the pre-existing behaviour.
        auto want_sst = compact(nat_base, nat_over, nat);
        const auto want_m = read_all(want_sst, s, env.make_reader_permit());
        const auto want_f = fragments_of(want_sst, s, env.make_reader_permit());
        const size_t dead_ref = count_dead_cells(*s, want_m);
        const size_t rows_ref = count_rows(want_m);
        BOOST_REQUIRE_GT(dead_ref, 0u);
        BOOST_REQUIRE_GT(rows_ref, 0u);
        BOOST_REQUIRE_LT(rows_ref, size_t(hyb_parts * hyb_rows));

        struct arm { const char* what; shared_sstable lo; shared_sstable hi;
                     sstable_version_types out; };
        for (auto a : {arm{"mixed in, parquet out", nat_base, pq_over,
                           sstable_version_types::pq},
                       arm{"mixed in, native out", nat_base, pq_over, nat},
                       arm{"mixed in reversed, parquet out", pq_base, nat_over,
                           sstable_version_types::pq},
                       arm{"mixed in reversed, native out", pq_base, nat_over, nat},
                       arm{"parquet in, parquet out (control)", pq_base, pq_over,
                           sstable_version_types::pq}}) {
            BOOST_TEST_CONTEXT("compaction: " << a.what) {
                auto out = compact(a.lo, a.hi, a.out);
                BOOST_REQUIRE(out->get_version() == a.out);

                auto got_f = fragments_of(out, s, env.make_reader_permit());
                BOOST_REQUIRE_EQUAL(got_f.size(), want_f.size());
                for (size_t i = 0; i < got_f.size(); ++i) {
                    BOOST_REQUIRE_EQUAL(got_f[i], want_f[i]);
                }

                auto got_m = read_all(out, s, env.make_reader_permit());
                BOOST_REQUIRE_EQUAL(got_m.size(), want_m.size());
                for (size_t i = 0; i < got_m.size(); ++i) {
                    assert_that(got_m[i]).is_equal_to(want_m[i]);
                }
                BOOST_REQUIRE_EQUAL(count_dead_cells(*s, got_m), dead_ref);
                BOOST_REQUIRE_EQUAL(count_rows(got_m), rows_ref);
            }
        }
    }).get();
}

// Counters and non-frozen collections, merged across formats.
//
// The two hybrid cases above carry every tombstone shape, but every cell in them is an atomic
// scalar. Counters and multi-cell collections are the two kinds of cell whose merge is *not* "the
// newer one wins", so those cases cannot speak for them, and they fail differently:
//
//   * A counter cell is a set of per-replica shards, and merging two of them means taking, per
//     shard id, the shard with the higher **logical clock** -- not the one from the newer cell
//     (`counter_cell_view::apply`, `mutation/counters.cc:128`). A shard from the *older* sstable
//     therefore survives into the result whenever its clock is higher. Counters are not
//     idempotent, so dropping or duplicating a shard yields a wrong *total* rather than a missing
//     row: the failure is silent and arithmetic, and the sum is the only thing that shows it.
//   * A collection cell is a collection-wide tombstone plus a list of per-element cells, merged
//     element by element. "The tombstone is in one format and the elements it must not delete are
//     in the other" is therefore a real ordering question, and it has no analogue among atomic
//     cells, where a tombstone and the value it shadows can never both survive.
//
// Both cases below assert twice, and the second assertion is the point. The first is the one the
// rest of this family uses: the same merge with native sstables on both sides. The second is
// against an expectation computed from the fixture itself -- the merged shard totals for counters,
// the surviving element sets for collections. A merge rule broken for *both* formats at once
// passes the first and fails the second, which is what makes these tests of counter and collection
// semantics rather than of pq-equals-native.

namespace {

// The query path over a format-mixed sstable set: one make_reader() per sstable, combined. The two
// hybrid cases above predate these and keep their own local copies; new cases share these.
mutation_reader merge_readers(sstables::test_env& env, schema_ptr s,
                              std::vector<shared_sstable> ssts) {
    std::vector<mutation_reader> rds;
    for (auto& sst : ssts) {
        rds.push_back(sst->make_reader(s, env.make_reader_permit(),
                                       query::full_partition_range, s->full_slice()));
    }
    return make_combined_reader(s, env.make_reader_permit(), std::move(rds));
}

utils::chunked_vector<mutation> merged_mutations(sstables::test_env& env, schema_ptr s,
                                                std::vector<shared_sstable> ssts) {
    auto rd = merge_readers(env, s, std::move(ssts));
    auto close = deferred_close(rd);
    utils::chunked_vector<mutation> out;
    while (auto m = read_mutation_from_mutation_reader(rd).get()) {
        out.push_back(std::move(*m));
    }
    return out;
}

std::vector<sstring> merged_fragments(sstables::test_env& env, schema_ptr s,
                                     std::vector<shared_sstable> ssts) {
    auto rd = merge_readers(env, s, std::move(ssts));
    auto close = deferred_close(rd);
    std::vector<sstring> out;
    while (auto mf = rd().get()) {
        out.push_back(seastar::format("{}", mutation_fragment_v2::printer(*s, *mf)));
    }
    return out;
}

// A compaction of a format-mixed input set, writing `out_v`: the merge happens through
// make_full_scan_reader and its result goes back out through the writer, so anything lost here is
// lost permanently.
shared_sstable compact_into(sstables::test_env& env, schema_ptr s,
                            std::vector<shared_sstable> in, sstable_version_types out_v,
                            size_t n_part) {
    auto out = env.make_sstable(s, out_v);
    std::vector<mutation_reader> rds;
    for (auto& sst : in) {
        rds.push_back(sst->make_full_scan_reader(s, env.make_reader_permit(), nullptr,
                                                default_read_monitor()));
    }
    auto merged = make_combined_reader(s, env.make_reader_permit(), std::move(rds));
    auto cfg = env.manager().configure_writer("test");
    out->write_components(std::move(merged), n_part, s, cfg, encoding_stats{}).get();
    out->open_data().get();
    return out;
}

// ---------------------------------------------------------------------------------------------
// Counters

constexpr int ctr_parts = 7;
constexpr int ctr_deleted_part = 4;     // the overlay deletes this partition outright
constexpr int ctr_base_only_part = 5;   // absent from the overlay
constexpr int ctr_over_only_part = 6;   // absent from the base

// Counter and non-counter regular columns cannot coexist in one table -- Scylla rejects it -- so
// every column here is a counter and there is no scalar to fall back on.
schema_ptr counter_hybrid_schema() {
    return schema_builder(1, "ks", "hyb_ctr")
        .with_column("pk", utf8_type, column_kind::partition_key)
        .with_column("ck", int32_type, column_kind::clustering_key)
        .with_column("c", counter_type)
        .with_column("c2", counter_type)
        .with_column("sc", counter_type, column_kind::static_column)
        .build();
}

partition_key ctr_pk(const schema& s, int p) {
    return partition_key::from_single_value(s, utf8_type->decompose(sstring(format("cp{:04d}", p))));
}

clustering_key ctr_ck(const schema& s, int r) {
    return clustering_key::from_single_value(s, int32_type->decompose(r));
}

// Deterministic shard ids, and invertible: the assertions name shards by the fixture's small
// index, so a failure says "shard 2" rather than printing a UUID.
counter_id ctr_shard_id(int n) {
    return counter_id(utils::UUID(0x1000000000000000LL + n, 0x2000000000000000LL + n * 7));
}

int ctr_shard_index(counter_id id) {
    return int(id.uuid().get_most_significant_bits() - 0x1000000000000000LL);
}

struct ctr_shard { int id; int64_t value; int64_t clock; };

// What the two generations hold for one clustering row. `base`/`over` are column `c`'s shard sets
// and `base2`/`over2` are `c2`'s; the flags cover the shapes a shard list cannot express.
struct ctr_case {
    int row;
    std::vector<ctr_shard> base;
    std::vector<ctr_shard> over;
    std::vector<ctr_shard> base2;
    std::vector<ctr_shard> over2;
    bool base_dead = false;
    bool over_dead = false;
    bool over_row_tombstone = false;
    const char* why;
};

std::vector<ctr_case> counter_cases() {
    return {
        // Disjoint and overlapping shard ids in one cell: s0 comes from the base alone, s1 keeps
        // the base's shard because its clock is higher, s2 takes the overlay's, s3 is new. The
        // merged total (102) is neither generation's (60 and 93), so taking either side wholesale
        // is arithmetically visible.
        {0, {{0,10,3},{1,20,9},{2,30,4}}, {{1,21,6},{2,31,8},{3,41,1}}, {}, {},
         false, false, false, "shard union, one winner from each side"},
        // Every shard superseded: the overlay wins outright, which is the case a last-write-wins
        // merge also gets right. It is here so the fixture is not made only of the hard shapes.
        {1, {{0,10,7},{1,20,7}}, {{0,11,9},{1,21,9}}, {}, {},
         false, false, false, "overlay wins every shard"},
        // The case that separates a counter merge from last-write-wins. The base's cell is older
        // by timestamp, but its s0 shard has the higher *logical clock*, so s0's value must come
        // from the older sstable. Merged 31 against 30 in the base and 32 in the overlay: a merge
        // that takes the newer cell whole is off by one and nothing else notices.
        {2, {{0,10,10},{1,20,1}}, {{0,11,2},{1,21,5}}, {{5,500,1}}, {{5,501,9},{6,600,1}},
         false, false, false, "an older shard with a higher clock wins"},
        // A counter cell deleted by the overlay: nothing may survive from the base.
        {3, {{0,10,2},{1,20,3}}, {}, {}, {},
         false, true, false, "cell deleted by the overlay"},
        // The reverse -- dead in the *base*, live in the overlay -- and the surprise. A counter
        // tombstone is not timestamp-ordered: it wins whichever side it is on (see ctr_merge).
        // So the overlay's newer live cell does not resurrect the column, and a merge that
        // resolved this by timestamp like any other atomic cell would silently bring a deleted
        // counter back.
        {4, {}, {{0,11,1},{1,21,2},{2,31,3}}, {}, {},
         true, false, false, "dead in the base, live in the overlay"},
        // A row tombstone over a row full of counters.
        {5, {{0,10,4},{1,20,5},{2,30,6},{3,40,7}}, {}, {{5,500,2}}, {},
         false, false, true, "row tombstone over a counter row"},
        // Untouched by the overlay: the base's cell must come through unchanged, shards and all.
        {6, {{0,10,3},{1,20,4}}, {}, {}, {},
         false, false, false, "base only"},
        // Present only in the overlay.
        {7, {}, {{0,11,5},{1,21,6}}, {}, {{5,501,7}},
         false, false, false, "overlay only"},
    };
}

// The static counter's two generations. The overlay's arm rotates with the partition so that
// updated, deleted and untouched all occur, and partition 2 is absent from the base -- which,
// since 2 % 3 == 2 leaves the overlay silent too, is the never-written case.
std::vector<ctr_shard> ctr_static_base(int p) {
    if (p == 2) { return {}; }
    return {{0, 700 + p, 5}, {1, 800 + p, 5}};
}

std::vector<ctr_shard> ctr_static_over(int p) {
    if (p % 3 == 0) { return {{1, 801 + p, 4}, {2, 900 + p, 6}}; }
    return {};
}

bool ctr_static_over_dead(int p) { return p % 3 == 1; }

atomic_cell make_ctr_cell(api::timestamp_type ts, const std::vector<ctr_shard>& shards) {
    counter_cell_builder b{shards.size()};
    for (const auto& sh : shards) {
        b.add_maybe_unsorted_shard(counter_shard(ctr_shard_id(sh.id), sh.value, sh.clock));
    }
    b.sort_and_remove_duplicates();
    return b.build(ts);
}

void ctr_sort(utils::chunked_vector<mutation>& muts) {
    std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
        return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
    });
}

utils::chunked_vector<mutation> counter_base(schema_ptr s) {
    const auto& cdef  = *s->get_column_definition(to_bytes("c"));
    const auto& c2def = *s->get_column_definition(to_bytes("c2"));
    const auto& scdef = *s->get_column_definition(to_bytes("sc"));
    const auto cases = counter_cases();

    utils::chunked_vector<mutation> muts;
    for (int p = 0; p < ctr_parts; ++p) {
        if (p == ctr_over_only_part) { continue; }
        mutation m(s, ctr_pk(*s, p));
        const api::timestamp_type ts = 1000 + p;
        if (auto sh = ctr_static_base(p); !sh.empty()) {
            m.set_static_cell(scdef, make_ctr_cell(ts, sh));
        }
        for (const auto& c : cases) {
            auto ck = ctr_ck(*s, c.row);
            if (c.base_dead) {
                m.set_clustered_cell(ck, cdef, atomic_cell::make_dead(
                        ts, gc_clock::time_point(gc_clock::duration(60000 + p))));
            } else if (!c.base.empty()) {
                m.set_clustered_cell(ck, cdef, make_ctr_cell(ts, c.base));
            }
            if (!c.base2.empty()) {
                m.set_clustered_cell(ck, c2def, make_ctr_cell(ts, c.base2));
            }
        }
        muts.push_back(std::move(m));
    }
    ctr_sort(muts);
    return muts;
}

utils::chunked_vector<mutation> counter_overlay(schema_ptr s) {
    const auto& cdef  = *s->get_column_definition(to_bytes("c"));
    const auto& c2def = *s->get_column_definition(to_bytes("c2"));
    const auto& scdef = *s->get_column_definition(to_bytes("sc"));
    const auto cases = counter_cases();

    utils::chunked_vector<mutation> muts;
    for (int p = 0; p < ctr_parts; ++p) {
        if (p == ctr_base_only_part) { continue; }
        mutation m(s, ctr_pk(*s, p));
        const api::timestamp_type ts = 2000 + p;
        const auto ldt = gc_clock::time_point(gc_clock::duration(70000 + p));

        if (p == ctr_deleted_part) {
            m.partition().apply(tombstone(ts, ldt));
            muts.push_back(std::move(m));
            continue;
        }

        if (ctr_static_over_dead(p)) {
            m.set_static_cell(scdef, atomic_cell::make_dead(ts, ldt));
        } else if (auto sh = ctr_static_over(p); !sh.empty()) {
            m.set_static_cell(scdef, make_ctr_cell(ts, sh));
        }

        for (const auto& c : cases) {
            auto ck = ctr_ck(*s, c.row);
            if (c.over_row_tombstone) {
                m.partition().clustered_row(*s, ck).apply(row_tombstone(tombstone(ts, ldt)));
                continue;
            }
            if (c.over_dead) {
                m.set_clustered_cell(ck, cdef, atomic_cell::make_dead(ts, ldt));
            } else if (!c.over.empty()) {
                m.set_clustered_cell(ck, cdef, make_ctr_cell(ts, c.over));
            }
            if (!c.over2.empty()) {
                m.set_clustered_cell(ck, c2def, make_ctr_cell(ts, c.over2));
            }
        }
        muts.push_back(std::move(m));
    }
    ctr_sort(muts);
    return muts;
}

// A counter cell's shard set, either as the fixture predicts it or as the merge produced it.
struct ctr_expect {
    bool present = false;      // some cell is there, live or dead
    bool live = false;
    int64_t total = 0;         // sum of the winning shards' values
    std::vector<int> ids;      // shard indices, ascending
};

// The merge rule applied to the fixture rather than read off the product: per shard id, the shard
// with the higher logical clock, across both generations.
//
// Deadness, though, is *not* resolved by timestamp -- which is the one thing about counters this
// test did not expect and had to be corrected on. `counter_cell_view::apply`
// (`mutation/counters.cc:94`) keeps the dead cell whenever either side is dead, without consulting
// timestamps at all: a live cell merged with a tombstone yields the tombstone even when the live
// cell is strictly newer. That is deliberate -- a counter's shards cannot be safely resurrected,
// because the shards a delete removed would be re-added by the next increment and double-count --
// but it means a counter tombstone behaves unlike every other atomic cell tombstone in the system,
// and the design doc does not say so anywhere.
ctr_expect ctr_merge(const std::vector<ctr_shard>& base, bool base_dead,
                     const std::vector<ctr_shard>& over, bool over_dead) {
    ctr_expect e;
    const bool has_base = base_dead || !base.empty();
    const bool has_over = over_dead || !over.empty();
    if (!has_base && !has_over) { return e; }
    e.present = true;
    if (base_dead || over_dead) { return e; }
    std::map<int, ctr_shard> won;
    for (const auto& sh : base) { won.emplace(sh.id, sh); }
    for (const auto& sh : over) {
        auto it = won.find(sh.id);
        if (it == won.end()) {
            won.emplace(sh.id, sh);
        } else if (it->second.clock < sh.clock) {
            it->second = sh;
        }
    }
    if (won.empty()) { return e; }
    e.live = true;
    for (const auto& [id, sh] : won) {
        e.total += sh.value;
        e.ids.push_back(id);
    }
    return e;
}

ctr_expect ctr_read(const column_definition& def, const row& cells) {
    ctr_expect got;
    const auto* c = cells.find_cell(def.id);
    if (!c) { return got; }
    got.present = true;
    auto av = c->as_atomic_cell(def);
    if (!av.is_live()) { return got; }
    got.live = true;
    counter_cell_view ccv(av);
    for (auto&& cs : ccv.shards()) {
        got.total += cs.value();
        got.ids.push_back(ctr_shard_index(cs.id()));
    }
    std::sort(got.ids.begin(), got.ids.end());
    return got;
}

} // namespace

SEASTAR_THREAD_TEST_CASE(test_hybrid_merge_of_counters_across_formats) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = counter_hybrid_schema();
        const auto& cdef  = *s->get_column_definition(to_bytes("c"));
        const auto& c2def = *s->get_column_definition(to_bytes("c2"));
        const auto& scdef = *s->get_column_definition(to_bytes("sc"));
        const auto cases = counter_cases();

        auto base = counter_base(s);
        auto over = counter_overlay(s);
        const auto nat = sstables::get_highest_sstable_version();
        const size_t n_part = std::max(base.size(), over.size());

        auto mk = [&] (sstable_version_types v, const utils::chunked_vector<mutation>& ms) {
            return make_sstable_containing(env.make_sstable(s, v), ms).get();
        };
        auto nat_base = mk(nat, base), nat_over = mk(nat, over);
        auto pq_base = mk(sstable_version_types::pq, base);
        auto pq_over = mk(sstable_version_types::pq, over);

        // Keys back to the fixture's indices, compared as the serialised bytes rather than parsed
        // out of a string, so a mis-encoded key fails the lookup instead of being accepted.
        std::map<bytes, int> p_of;
        for (int p = 0; p < ctr_parts; ++p) {
            p_of[utf8_type->decompose(sstring(format("cp{:04d}", p)))] = p;
        }
        std::map<bytes, const ctr_case*> case_of;
        for (const auto& c : cases) {
            case_of[int32_type->decompose(c.row)] = &c;
        }

        struct stats {
            size_t live = 0, dead = 0, multi_shard = 0, statics = 0;
            size_t neither_side = 0;   // rows whose total is neither generation's
            size_t row_tombs = 0;
        };

        // Every counter cell in a merged result, against ctr_merge() -- the fixture's own rule,
        // not the reference format's behaviour. The deleted partition is excluded because a merge
        // does not garbage-collect: its rows survive shadowed by the partition tombstone, and
        // modelling what a shadowed row still holds is not what this test is for. Its contents are
        // covered by the reference comparison below.
        auto check = [&] (const utils::chunked_vector<mutation>& ms) {
            stats st;
            for (const auto& m : ms) {
                auto pit = p_of.find(m.key().explode(*s).at(0));
                BOOST_REQUIRE(pit != p_of.end());
                const int p = pit->second;
                if (p == ctr_deleted_part) {
                    BOOST_REQUIRE(m.partition().partition_tombstone());
                    continue;
                }
                const bool in_base = p != ctr_over_only_part;
                const bool in_over = p != ctr_base_only_part;
                const std::vector<ctr_shard> none;

                BOOST_TEST_CONTEXT("partition " << p) {
                    auto want_st = ctr_merge(in_base ? ctr_static_base(p) : none, false,
                                             in_over ? ctr_static_over(p) : none,
                                             in_over && ctr_static_over_dead(p));
                    auto got_st = ctr_read(scdef, m.partition().static_row().get());
                    BOOST_TEST_CONTEXT("static counter") {
                        BOOST_REQUIRE_EQUAL(got_st.present, want_st.present);
                        BOOST_REQUIRE_EQUAL(got_st.live, want_st.live);
                        if (want_st.live) {
                            BOOST_REQUIRE_EQUAL(got_st.total, want_st.total);
                            BOOST_REQUIRE(got_st.ids == want_st.ids);
                            ++st.statics;
                        }
                    }

                    for (const rows_entry& re : m.partition().clustered_rows()) {
                        auto cit = case_of.find(re.key().explode(*s).at(0));
                        BOOST_REQUIRE(cit != case_of.end());
                        const ctr_case& c = *cit->second;
                        BOOST_TEST_CONTEXT("row " << c.row << ": " << c.why) {
                            if (c.over_row_tombstone && in_over) {
                                // Same reason as the deleted partition: the row survives the merge
                                // shadowed rather than removed, so what is asserted is that the
                                // tombstone arrived at all.
                                BOOST_REQUIRE(bool(re.row().deleted_at()));
                                ++st.row_tombs;
                                continue;
                            }
                            struct arm { const char* name; const column_definition& def;
                                         const std::vector<ctr_shard>& b;
                                         const std::vector<ctr_shard>& o; bool bd; bool od; };
                            for (const auto& a : {arm{"c", cdef, c.base, c.over, c.base_dead,
                                                      c.over_dead},
                                                  arm{"c2", c2def, c.base2, c.over2, false,
                                                      false}}) {
                                BOOST_TEST_CONTEXT("column " << a.name) {
                                    auto want = ctr_merge(in_base ? a.b : none,
                                                          in_base && a.bd,
                                                          in_over ? a.o : none,
                                                          in_over && a.od);
                                    auto got = ctr_read(a.def, re.row().cells());
                                    BOOST_REQUIRE_EQUAL(got.present, want.present);
                                    BOOST_REQUIRE_EQUAL(got.live, want.live);
                                    if (!want.live) {
                                        if (want.present) { ++st.dead; }
                                        continue;
                                    }
                                    BOOST_REQUIRE_EQUAL(got.total, want.total);
                                    BOOST_REQUIRE(got.ids == want.ids);
                                    ++st.live;
                                    if (want.ids.size() > 1) { ++st.multi_shard; }
                                    // The non-vacuity guard that is specific to counters: a total
                                    // that matches neither generation on its own cannot have been
                                    // produced by taking one side wholesale.
                                    auto b_only = ctr_merge(in_base ? a.b : none,
                                                            in_base && a.bd, none, false);
                                    auto o_only = ctr_merge(none, false,
                                                            in_over ? a.o : none,
                                                            in_over && a.od);
                                    if ((!b_only.live || b_only.total != want.total) &&
                                        (!o_only.live || o_only.total != want.total)) {
                                        ++st.neither_side;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return st;
        };

        // The reference: native on both sides. Checking the fixture's rule against it first is
        // what establishes that the rule is Scylla's and not this test's invention -- if these
        // fail, the expectation is wrong rather than pq.
        const auto want_f = merged_fragments(env, s, {nat_base, nat_over});
        const auto want_m = merged_mutations(env, s, {nat_base, nat_over});
        const auto ref_st = check(want_m);
        BOOST_REQUIRE_GT(ref_st.live, 0u);
        BOOST_REQUIRE_GT(ref_st.dead, 0u);
        BOOST_REQUIRE_GT(ref_st.multi_shard, 0u);
        BOOST_REQUIRE_GT(ref_st.statics, 0u);
        BOOST_REQUIRE_GT(ref_st.row_tombs, 0u);
        // Two rows per partition have a total that is neither generation's on its own (rows 0 and
        // 2, plus row 2's second counter column), over the partitions that have both generations.
        BOOST_REQUIRE_GE(ref_st.neither_side, 6u);

        struct arm { const char* what; shared_sstable lo; shared_sstable hi; };
        for (auto a : {arm{"native base + parquet overlay", nat_base, pq_over},
                       arm{"parquet base + native overlay", pq_base, nat_over},
                       arm{"parquet base + parquet overlay (control)", pq_base, pq_over},
                       arm{"native base + native overlay (control)", nat_base, nat_over}}) {
            BOOST_TEST_CONTEXT("counter read merge: " << a.what) {
                auto got_f = merged_fragments(env, s, {a.lo, a.hi});
                BOOST_REQUIRE_EQUAL(got_f.size(), want_f.size());
                for (size_t i = 0; i < got_f.size(); ++i) {
                    BOOST_REQUIRE_EQUAL(got_f[i], want_f[i]);
                }
                auto got_m = merged_mutations(env, s, {a.lo, a.hi});
                BOOST_REQUIRE_EQUAL(got_m.size(), want_m.size());
                for (size_t i = 0; i < got_m.size(); ++i) {
                    assert_that(got_m[i]).is_equal_to(want_m[i]);
                }
                const auto st = check(got_m);
                BOOST_REQUIRE_EQUAL(st.live, ref_st.live);
                BOOST_REQUIRE_EQUAL(st.dead, ref_st.dead);
                BOOST_REQUIRE_EQUAL(st.multi_shard, ref_st.multi_shard);
                BOOST_REQUIRE_EQUAL(st.statics, ref_st.statics);
                BOOST_REQUIRE_EQUAL(st.neither_side, ref_st.neither_side);
            }
        }

        // And through compaction, where the merged counter cells are written back out. A shard set
        // that survives a read but cannot be re-shredded loses data permanently here, because the
        // older sstable is gone afterwards.
        struct carm { const char* what; shared_sstable lo; shared_sstable hi;
                      sstable_version_types out; };
        for (auto a : {carm{"mixed in, parquet out", nat_base, pq_over,
                            sstable_version_types::pq},
                       carm{"mixed in reversed, parquet out", pq_base, nat_over,
                            sstable_version_types::pq},
                       carm{"mixed in, native out", nat_base, pq_over, nat},
                       carm{"parquet in, parquet out (control)", pq_base, pq_over,
                            sstable_version_types::pq}}) {
            BOOST_TEST_CONTEXT("counter compaction: " << a.what) {
                auto out = compact_into(env, s, {a.lo, a.hi}, a.out, n_part);
                BOOST_REQUIRE(out->get_version() == a.out);
                auto got_m = read_all(out, s, env.make_reader_permit());
                BOOST_REQUIRE_EQUAL(got_m.size(), want_m.size());
                for (size_t i = 0; i < got_m.size(); ++i) {
                    assert_that(got_m[i]).is_equal_to(want_m[i]);
                }
                const auto st = check(got_m);
                BOOST_REQUIRE_EQUAL(st.live, ref_st.live);
                BOOST_REQUIRE_EQUAL(st.dead, ref_st.dead);
                BOOST_REQUIRE_EQUAL(st.neither_side, ref_st.neither_side);
            }
        }
    }).get();
}

namespace {

// ---------------------------------------------------------------------------------------------
// Collections

constexpr int coll_parts = 7;
constexpr int coll_deleted_part = 4;    // the overlay deletes this partition outright
constexpr int coll_base_only_part = 5;  // absent from the overlay
constexpr int coll_over_only_part = 6;  // absent from the base

data_type coll_map_type()    { return map_type_impl::get_instance(utf8_type, int32_type, true); }
data_type coll_set_type()    { return set_type_impl::get_instance(int32_type, true); }
data_type coll_list_type()   { return list_type_impl::get_instance(int32_type, true); }
data_type coll_frozen_type() { return map_type_impl::get_instance(utf8_type, int32_type, false); }

// One non-frozen collection of each kind, one frozen one, and a static non-frozen one. The three
// non-frozen kinds differ in their key space -- text, the element itself, and an opaque time UUID
// -- which is the part of the encoding that the shredder has to carry verbatim.
schema_ptr collection_hybrid_schema() {
    return schema_builder(1, "ks", "hyb_coll")
        .with_column("pk", utf8_type, column_kind::partition_key)
        .with_column("ck", int32_type, column_kind::clustering_key)
        .with_column("v", int32_type)
        .with_column("m", coll_map_type())
        .with_column("t", coll_set_type())
        .with_column("l", coll_list_type())
        .with_column("fm", coll_frozen_type())
        .with_column("sm", coll_map_type(), column_kind::static_column)
        .build();
}

partition_key coll_pk(const schema& s, int p) {
    return partition_key::from_single_value(s, utf8_type->decompose(sstring(format("lp{:04d}", p))));
}

clustering_key coll_ck(const schema& s, int r) {
    return clustering_key::from_single_value(s, int32_type->decompose(r));
}

// One clustering row's arrangement, applied identically to the map, the set and the list, so one
// fixture covers a keyed, a keyless and an opaquely-keyed multi-cell collection.
struct coll_case {
    int row;
    std::vector<int> base;         // live elements in the base
    bool over_tomb = false;        // the overlay carries a collection-wide tombstone
    std::vector<int> over;         // live elements the overlay writes, above its tombstone
    std::vector<int> over_dead;    // per-element deletes in the overlay
    bool over_row_tombstone = false;
    const char* why;
};

std::vector<coll_case> collection_cases() {
    return {
        {0, {0,1,2}, false, {3},   {1},   false, "per-element delete plus an addition"},
        {1, {0,1},   true,  {},    {},    false, "whole-collection delete"},
        // The ordering case, and the reason this test exists: the collection-wide tombstone is in
        // one format and the elements that must survive it are in the other. Everything the base
        // holds is below the tombstone and must go; everything the overlay adds is above it and
        // must stay. Getting the two the wrong way round either resurrects a cleared collection or
        // swallows the elements that replaced it.
        {2, {0,1},   true,  {2,3}, {},    false, "tombstone below the surviving elements"},
        {3, {0,1,2}, false, {},    {0,2}, false, "two per-element deletes, one element left"},
        {4, {},      true,  {},    {},    false, "a tombstone with nothing to delete"},
        {5, {0,1,2}, false, {},    {},    true,  "row tombstone over a row of collections"},
        {6, {0,1},   false, {},    {},    false, "base only -- untouched by the overlay"},
        {7, {},      false, {2,3}, {},    false, "overlay only"},
    };
}

// The static collection's arm rotates with the partition, and partition 2 -- whose arm leaves the
// overlay silent -- is also absent from the base, so `sm` is never written there at all.
coll_case coll_static_case(int p) {
    if (p % 3 == 0) { return {0, {0,1}, true,  {2}, {},  false, "static: cleared, then refilled"}; }
    if (p % 3 == 1) { return {0, {0,1}, false, {},  {0}, false, "static: per-element delete"}; }
    return                   {0, {0,1}, false, {},  {},  false, "static: untouched"};
}

bool coll_static_in_base(int p) { return p != coll_over_only_part && p != 2; }

// Timestamp bands. The overlay's elements sit strictly *above* its collection tombstone, which is
// what makes case 2 mean anything; its per-element deletes sit above the base's elements and below
// its own additions.
api::timestamp_type coll_base_ts(int p)      { return 1000 + p; }
api::timestamp_type coll_tomb_ts(int p)      { return 2000 + p; }
api::timestamp_type coll_over_dead_ts(int p) { return 2050 + p; }
api::timestamp_type coll_over_ts(int p)      { return 2100 + p; }

bytes coll_map_key(int i)  { return utf8_type->decompose(sstring(format("k{}", i))); }
bytes coll_set_key(int i)  { return int32_type->decompose(i); }
bytes coll_list_key(int i) {
    // Deterministic and monotone in `i`: min_time_UUID pins the clock-sequence half, so ordering
    // is the timestamp's, which is the order the elements are pushed in below.
    return timeuuid_type->decompose(data_value(
            utils::UUID_gen::min_time_UUID(utils::UUID_gen::decimicroseconds(10'000'000 + i))));
}

int coll_map_key_index(bytes_view b) {
    return std::stoi(std::string(reinterpret_cast<const char*>(b.data()), b.size()).substr(1));
}
int coll_set_key_index(bytes_view b) {
    return value_cast<int32_t>(int32_type->deserialize(b));
}
int coll_list_key_index(bytes_view b) {
    for (int i = 0; i < 16; ++i) {
        if (coll_list_key(i) == bytes(b)) { return i; }
    }
    return -1;
}

bytes coll_frozen_value(int salt) {
    return make_map_value(coll_frozen_type(),
            map_type_impl::native_type({{sstring(format("f{}", salt)), int32_t(salt * 3)}}))
            .serialize_nonnull();
}

// One multi-cell collection cell: the named elements in ascending index order, which is the key
// comparator's order for all three key kinds here, under an optional collection-wide tombstone.
atomic_cell_or_collection coll_cell(const std::function<bytes(int)>& key_of,
                                    const abstract_type& value_type,
                                    const std::function<bytes(int)>& value_of,
                                    api::timestamp_type live_ts, api::timestamp_type dead_ts,
                                    std::optional<api::timestamp_type> tomb_ts,
                                    const std::vector<int>& live, const std::vector<int>& dead) {
    collection_mutation_writer w(tomb_ts
            ? tombstone(*tomb_ts, gc_clock::time_point(gc_clock::duration(70000)))
            : tombstone());
    std::set<int> live_set(live.begin(), live.end());
    std::set<int> all(live.begin(), live.end());
    all.insert(dead.begin(), dead.end());
    for (int i : all) {
        auto k = key_of(i);
        auto kb = managed_bytes(reinterpret_cast<const int8_t*>(k.data()), k.size());
        if (live_set.contains(i)) {
            auto v = value_of(i);
            w.push_back(managed_bytes_view(kb),
                        atomic_cell::make_live(value_type, live_ts, bytes_view(v)));
        } else {
            w.push_back(managed_bytes_view(kb), atomic_cell::make_dead(
                    dead_ts, gc_clock::time_point(gc_clock::duration(60000))));
        }
    }
    return atomic_cell_or_collection(std::move(w).finish());
}

// The three non-frozen collections, written from one case. `which` picks the column.
struct coll_col { const char* name; std::function<bytes(int)> key_of;
                  const abstract_type* value_type; std::function<bytes(int)> value_of;
                  std::function<int(bytes_view)> key_index; };

std::vector<coll_col> coll_cols() {
    return {
        {"m", coll_map_key, int32_type.get(),
         [] (int i) { return int32_type->decompose(i * 10); }, coll_map_key_index},
        // A set element carries liveness and nothing else, so its value is empty -- the shape that
        // makes "absent" and "present but valueless" easy to conflate.
        {"t", coll_set_key, bytes_type.get(),
         [] (int) { return bytes(); }, coll_set_key_index},
        {"l", coll_list_key, int32_type.get(),
         [] (int i) { return int32_type->decompose(i * 100); }, coll_list_key_index},
    };
}

void coll_sort(utils::chunked_vector<mutation>& muts) {
    std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
        return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
    });
}

utils::chunked_vector<mutation> collection_base(schema_ptr s) {
    const auto& vdef  = *s->get_column_definition(to_bytes("v"));
    const auto& fmdef = *s->get_column_definition(to_bytes("fm"));
    const auto& smdef = *s->get_column_definition(to_bytes("sm"));
    const auto cases = collection_cases();
    const auto cols = coll_cols();

    utils::chunked_vector<mutation> muts;
    for (int p = 0; p < coll_parts; ++p) {
        if (p == coll_over_only_part) { continue; }
        mutation m(s, coll_pk(*s, p));
        const auto ts = coll_base_ts(p);
        if (coll_static_in_base(p)) {
            const auto sc = coll_static_case(p);
            m.set_static_cell(smdef, coll_cell(coll_map_key, *int32_type,
                    [] (int i) { return int32_type->decompose(i * 10); },
                    ts, ts, std::nullopt, sc.base, {}));
        }
        for (const auto& c : cases) {
            if (c.row == 7) { continue; }       // exists only in the overlay
            auto ck = coll_ck(*s, c.row);
            m.set_clustered_cell(ck, vdef, atomic_cell::make_live(
                    *int32_type, ts, int32_type->decompose(c.row)));
            m.set_clustered_cell(ck, fmdef, atomic_cell::make_live(
                    *fmdef.type, ts, bytes_view(coll_frozen_value(c.row))));
            if (c.base.empty()) { continue; }
            for (const auto& col : cols) {
                m.set_clustered_cell(ck, *s->get_column_definition(to_bytes(col.name)),
                        coll_cell(col.key_of, *col.value_type, col.value_of,
                                  ts, ts, std::nullopt, c.base, {}));
            }
        }
        muts.push_back(std::move(m));
    }
    coll_sort(muts);
    return muts;
}

utils::chunked_vector<mutation> collection_overlay(schema_ptr s) {
    const auto& vdef  = *s->get_column_definition(to_bytes("v"));
    const auto& fmdef = *s->get_column_definition(to_bytes("fm"));
    const auto& smdef = *s->get_column_definition(to_bytes("sm"));
    const auto cases = collection_cases();
    const auto cols = coll_cols();

    utils::chunked_vector<mutation> muts;
    for (int p = 0; p < coll_parts; ++p) {
        if (p == coll_base_only_part) { continue; }
        mutation m(s, coll_pk(*s, p));
        const auto ldt = gc_clock::time_point(gc_clock::duration(70000 + p));

        if (p == coll_deleted_part) {
            m.partition().apply(tombstone(coll_tomb_ts(p), ldt));
            muts.push_back(std::move(m));
            continue;
        }

        {
            const auto sc = coll_static_case(p);
            if (sc.over_tomb || !sc.over.empty() || !sc.over_dead.empty()) {
                m.set_static_cell(smdef, coll_cell(coll_map_key, *int32_type,
                        [] (int i) { return int32_type->decompose(i * 10); },
                        coll_over_ts(p), coll_over_dead_ts(p),
                        sc.over_tomb ? std::optional(coll_tomb_ts(p)) : std::nullopt,
                        sc.over, sc.over_dead));
            }
        }

        for (const auto& c : cases) {
            auto ck = coll_ck(*s, c.row);
            if (c.over_row_tombstone) {
                m.partition().clustered_row(*s, ck).apply(
                        row_tombstone(tombstone(coll_tomb_ts(p), ldt)));
                continue;
            }
            if (c.row != 6) {       // row 6 is untouched by the overlay
                m.set_clustered_cell(ck, vdef, atomic_cell::make_live(
                        *int32_type, coll_over_ts(p), int32_type->decompose(c.row + 1000)));
                // A frozen collection is a single atomic cell, so its whole-collection delete is a
                // *cell* tombstone -- the shape a pq file stores in `__dmask` rather than in a
                // collection tombstone slot, and the one that must not come back as absent.
                if (c.over_tomb) {
                    m.set_clustered_cell(ck, fmdef, atomic_cell::make_dead(
                            coll_tomb_ts(p), ldt));
                } else if (!c.over_dead.empty()) {
                    m.set_clustered_cell(ck, fmdef, atomic_cell::make_live(
                            *fmdef.type, coll_over_ts(p),
                            bytes_view(coll_frozen_value(c.row + 50))));
                }
            }
            if (!c.over_tomb && c.over.empty() && c.over_dead.empty()) { continue; }
            for (const auto& col : cols) {
                m.set_clustered_cell(ck, *s->get_column_definition(to_bytes(col.name)),
                        coll_cell(col.key_of, *col.value_type, col.value_of,
                                  coll_over_ts(p), coll_over_dead_ts(p),
                                  c.over_tomb ? std::optional(coll_tomb_ts(p)) : std::nullopt,
                                  c.over, c.over_dead));
            }
        }
        muts.push_back(std::move(m));
    }
    coll_sort(muts);
    return muts;
}

// The surviving live element set, computed from the fixture. The base's elements are all below the
// overlay's tombstone, and the overlay's own additions are all above it.
std::vector<int> coll_expect(const coll_case& c, bool in_base, bool in_over) {
    std::set<int> live;
    if (in_base) { live.insert(c.base.begin(), c.base.end()); }
    if (in_over) {
        if (c.over_tomb) { live.clear(); }
        for (int k : c.over_dead) { live.erase(k); }
        live.insert(c.over.begin(), c.over.end());
    }
    return {live.begin(), live.end()};
}

struct coll_readback {
    bool present = false;
    bool tomb = false;
    std::vector<int> live;
    size_t dead = 0;
    // Live cells the collection-wide tombstone already covers. Counted separately and asserted to
    // be zero rather than quietly filtered out of `live`: a merge that stopped applying the
    // tombstone to the *other* side's elements (`filter_cells`, mutation/collection_mutation.cc)
    // would leave them here, and a reader that hid them would be blind to it -- and so would the
    // reference comparison, because a broken merge rule breaks both formats identically.
    size_t shadowed = 0;
};

// A multi-cell collection as the merge left it.
coll_readback coll_read(const column_definition& def, const row& cells,
                        const std::function<int(bytes_view)>& key_index) {
    coll_readback out;
    const auto* c = cells.find_cell(def.id);
    if (!c) { return out; }
    out.present = true;
    auto cmv = c->as_collection_mutation();
    const auto t = cmv.tomb();
    out.tomb = bool(t);
    for (auto&& kv : cmv) {
        auto kb = linearized(kv.first);
        if (!kv.second.is_live()) { ++out.dead; continue; }
        if (t && kv.second.timestamp() <= t.timestamp) { ++out.shadowed; continue; }
        const int i = key_index(bytes_view(kb));
        BOOST_REQUIRE_GE(i, 0);
        out.live.push_back(i);
    }
    std::sort(out.live.begin(), out.live.end());
    return out;
}

} // namespace

SEASTAR_THREAD_TEST_CASE(test_hybrid_merge_of_collections_across_formats) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = collection_hybrid_schema();
        const auto& fmdef = *s->get_column_definition(to_bytes("fm"));
        const auto& smdef = *s->get_column_definition(to_bytes("sm"));
        const auto cases = collection_cases();
        const auto cols = coll_cols();

        auto base = collection_base(s);
        auto over = collection_overlay(s);
        const auto nat = sstables::get_highest_sstable_version();
        const size_t n_part = std::max(base.size(), over.size());

        auto mk = [&] (sstable_version_types v, const utils::chunked_vector<mutation>& ms) {
            return make_sstable_containing(env.make_sstable(s, v), ms).get();
        };
        auto nat_base = mk(nat, base), nat_over = mk(nat, over);
        auto pq_base = mk(sstable_version_types::pq, base);
        auto pq_over = mk(sstable_version_types::pq, over);

        std::map<bytes, int> p_of;
        for (int p = 0; p < coll_parts; ++p) {
            p_of[utf8_type->decompose(sstring(format("lp{:04d}", p)))] = p;
        }
        std::map<bytes, const coll_case*> case_of;
        for (const auto& c : cases) {
            case_of[int32_type->decompose(c.row)] = &c;
        }

        struct stats {
            size_t live_elems = 0, dead_elems = 0, tombs = 0, shadowed = 0;
            size_t shrunk = 0;        // collections the merge made strictly smaller than the union
            size_t frozen_live = 0, frozen_dead = 0, frozen_absent = 0;
            size_t statics = 0, row_tombs = 0;
        };

        auto check = [&] (const utils::chunked_vector<mutation>& ms) {
            stats st;
            for (const auto& m : ms) {
                auto pit = p_of.find(m.key().explode(*s).at(0));
                BOOST_REQUIRE(pit != p_of.end());
                const int p = pit->second;
                if (p == coll_deleted_part) {
                    BOOST_REQUIRE(m.partition().partition_tombstone());
                    continue;
                }
                const bool in_base = p != coll_over_only_part;
                const bool in_over = p != coll_base_only_part;

                BOOST_TEST_CONTEXT("partition " << p) {
                    {
                        const auto sc = coll_static_case(p);
                        const auto want = coll_expect(sc, in_base && coll_static_in_base(p),
                                                      in_over);
                        auto got = coll_read(smdef, m.partition().static_row().get(),
                                             coll_map_key_index);
                        BOOST_TEST_CONTEXT("static collection: " << sc.why) {
                            BOOST_REQUIRE(got.live == want);
                            st.shadowed += got.shadowed;
                            if (!want.empty()) { ++st.statics; }
                        }
                    }

                    for (const rows_entry& re : m.partition().clustered_rows()) {
                        auto cit = case_of.find(re.key().explode(*s).at(0));
                        BOOST_REQUIRE(cit != case_of.end());
                        const coll_case& c = *cit->second;
                        BOOST_TEST_CONTEXT("row " << c.row << ": " << c.why) {
                            if (c.over_row_tombstone && in_over) {
                                BOOST_REQUIRE(bool(re.row().deleted_at()));
                                ++st.row_tombs;
                                continue;
                            }
                            const auto want = coll_expect(c, in_base, in_over);
                            std::set<int> union_of(c.base.begin(), c.base.end());
                            if (in_over) { union_of.insert(c.over.begin(), c.over.end()); }

                            for (const auto& col : cols) {
                                const auto& def = *s->get_column_definition(to_bytes(col.name));
                                BOOST_TEST_CONTEXT("column " << col.name) {
                                    auto got = coll_read(def, re.row().cells(), col.key_index);
                                    BOOST_REQUIRE(got.live == want);
                                    st.live_elems += got.live.size();
                                    st.dead_elems += got.dead;
                                    st.shadowed += got.shadowed;
                                    if (got.tomb) { ++st.tombs; }
                                    if (want.size() < union_of.size()) { ++st.shrunk; }
                                }
                            }

                            // The frozen collection, whose merge is a plain atomic-cell merge --
                            // included because its *delete* is a cell tombstone rather than a
                            // collection tombstone, and that is a different channel on disk.
                            BOOST_TEST_CONTEXT("column fm") {
                                const auto* fc = re.row().cells().find_cell(fmdef.id);
                                const bool want_dead = in_over && c.over_tomb;
                                const bool want_new  = in_over && !c.over_tomb
                                                       && !c.over_dead.empty();
                                const bool want_base = in_base && c.row != 7;
                                if (!want_dead && !want_new && !want_base) {
                                    BOOST_REQUIRE(!fc);
                                    ++st.frozen_absent;
                                } else {
                                    BOOST_REQUIRE(fc);
                                    auto av = fc->as_atomic_cell(fmdef);
                                    if (want_dead) {
                                        BOOST_REQUIRE(!av.is_live());
                                        ++st.frozen_dead;
                                    } else {
                                        BOOST_REQUIRE(av.is_live());
                                        const auto expect = want_new
                                                ? coll_frozen_value(c.row + 50)
                                                : coll_frozen_value(c.row);
                                        BOOST_REQUIRE(av.value().linearize() == expect);
                                        ++st.frozen_live;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return st;
        };

        const auto want_f = merged_fragments(env, s, {nat_base, nat_over});
        const auto want_m = merged_mutations(env, s, {nat_base, nat_over});
        const auto ref_st = check(want_m);
        BOOST_REQUIRE_GT(ref_st.live_elems, 0u);
        // Per-element tombstones survive a merge that does not garbage-collect, so this is not a
        // tautology: it is what stops the element-delete arm of the fixture from quietly becoming
        // a no-op.
        BOOST_REQUIRE_GT(ref_st.dead_elems, 0u);
        BOOST_REQUIRE_GT(ref_st.tombs, 0u);
        BOOST_REQUIRE_GT(ref_st.shrunk, 0u);
        BOOST_REQUIRE_GT(ref_st.statics, 0u);
        BOOST_REQUIRE_GT(ref_st.row_tombs, 0u);
        BOOST_REQUIRE_GT(ref_st.frozen_live, 0u);
        BOOST_REQUIRE_GT(ref_st.frozen_dead, 0u);
        BOOST_REQUIRE_GT(ref_st.frozen_absent, 0u);
        // A collection-wide tombstone removes the elements it covers rather than merely hiding
        // them, on both sides of the merge. Asserted rather than assumed, because it is the fact
        // that lets `live` above be the whole truth about what survived.
        BOOST_REQUIRE_EQUAL(ref_st.shadowed, 0u);

        struct arm { const char* what; shared_sstable lo; shared_sstable hi; };
        for (auto a : {arm{"native base + parquet overlay", nat_base, pq_over},
                       arm{"parquet base + native overlay", pq_base, nat_over},
                       arm{"parquet base + parquet overlay (control)", pq_base, pq_over},
                       arm{"native base + native overlay (control)", nat_base, nat_over}}) {
            BOOST_TEST_CONTEXT("collection read merge: " << a.what) {
                auto got_f = merged_fragments(env, s, {a.lo, a.hi});
                BOOST_REQUIRE_EQUAL(got_f.size(), want_f.size());
                for (size_t i = 0; i < got_f.size(); ++i) {
                    BOOST_REQUIRE_EQUAL(got_f[i], want_f[i]);
                }
                auto got_m = merged_mutations(env, s, {a.lo, a.hi});
                BOOST_REQUIRE_EQUAL(got_m.size(), want_m.size());
                for (size_t i = 0; i < got_m.size(); ++i) {
                    assert_that(got_m[i]).is_equal_to(want_m[i]);
                }
                const auto st = check(got_m);
                BOOST_REQUIRE_EQUAL(st.live_elems, ref_st.live_elems);
                BOOST_REQUIRE_EQUAL(st.dead_elems, ref_st.dead_elems);
                BOOST_REQUIRE_EQUAL(st.tombs, ref_st.tombs);
                BOOST_REQUIRE_EQUAL(st.shadowed, 0u);
                BOOST_REQUIRE_EQUAL(st.frozen_live, ref_st.frozen_live);
                BOOST_REQUIRE_EQUAL(st.frozen_dead, ref_st.frozen_dead);
            }
        }

        struct carm { const char* what; shared_sstable lo; shared_sstable hi;
                      sstable_version_types out; };
        for (auto a : {carm{"mixed in, parquet out", nat_base, pq_over,
                            sstable_version_types::pq},
                       carm{"mixed in reversed, parquet out", pq_base, nat_over,
                            sstable_version_types::pq},
                       carm{"mixed in, native out", nat_base, pq_over, nat},
                       carm{"parquet in, parquet out (control)", pq_base, pq_over,
                            sstable_version_types::pq}}) {
            BOOST_TEST_CONTEXT("collection compaction: " << a.what) {
                auto out = compact_into(env, s, {a.lo, a.hi}, a.out, n_part);
                BOOST_REQUIRE(out->get_version() == a.out);
                auto got_m = read_all(out, s, env.make_reader_permit());
                BOOST_REQUIRE_EQUAL(got_m.size(), want_m.size());
                for (size_t i = 0; i < got_m.size(); ++i) {
                    assert_that(got_m[i]).is_equal_to(want_m[i]);
                }
                const auto st = check(got_m);
                BOOST_REQUIRE_EQUAL(st.live_elems, ref_st.live_elems);
                BOOST_REQUIRE_EQUAL(st.tombs, ref_st.tombs);
                BOOST_REQUIRE_EQUAL(st.shadowed, 0u);
                BOOST_REQUIRE_EQUAL(st.frozen_dead, ref_st.frozen_dead);
            }
        }
    }).get();
}

// A non-frozen user-defined type, which is multi-cell and therefore travels the collection path --
// but is *not* a collection type.
//
// `columns_of()` marks a column multi_cell as `!is_atomic() || is_counter()`, so a non-frozen UDT is
// shredded like a map, with the field index as the element key. That half works. The read back did
// not: `build_collection()` opened with
//
//     const auto& ctype = dynamic_cast<const collection_type_impl&>(*cdef.type);
//
// and `user_type_impl` derives from `tuple_type_impl`, not from `collection_type_impl`, so that
// reference cast throws `std::bad_cast`. Every read of a `pq` sstable holding a non-frozen UDT
// column therefore failed outright -- not wrong data, an exception out of the read path.
//
// It stayed hidden because every fixed-schema pq test in the tree uses collections and counters and
// no non-frozen UDT, and the one suite that generates UDT columns at random
// (`test_sstable_bytes_on_disk_correctness`, via `make_random_schema_specification`) took the
// version-less `env.make_sstable(schema)` overload and so never ran as `pq`.
SEASTAR_THREAD_TEST_CASE(test_pq_non_frozen_udt_round_trips) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        // (a int, b text, c bigint), multi-cell -- the `false` arm is the frozen one, which is an
        // ordinary atomic cell and was never affected.
        auto udt = user_type_impl::get_instance("ks", to_bytes("pq_ut"),
                {to_bytes("a"), to_bytes("b"), to_bytes("c")},
                {int32_type, utf8_type, long_type}, true);
        auto frozen_udt = user_type_impl::get_instance("ks", to_bytes("pq_fut"),
                {to_bytes("a"), to_bytes("b")}, {int32_type, utf8_type}, false);

        auto s = schema_builder(1, "ks", "pq_udt")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("u", udt)
            .with_column("fu", frozen_udt)
            .with_column("su", udt, column_kind::static_column)
            .build();

        const auto& udef  = *s->get_column_definition(to_bytes("u"));
        const auto& fudef = *s->get_column_definition(to_bytes("fu"));
        const auto& sudef = *s->get_column_definition(to_bytes("su"));
        BOOST_REQUIRE(!udef.is_atomic());       // the property that routes it here
        BOOST_REQUIRE(fudef.is_atomic());

        // One UDT cell. `live` names the fields that are set, `dead` those with a field-level
        // tombstone; field-index keys are pushed ascending, which is their comparator's order.
        auto udt_cell = [&] (api::timestamp_type ts, std::optional<api::timestamp_type> tomb_ts,
                             const std::vector<int>& live, const std::vector<int>& dead) {
            collection_mutation_writer w(tomb_ts
                    ? tombstone(*tomb_ts, gc_clock::time_point(gc_clock::duration(70000)))
                    : tombstone());
            std::set<int> live_set(live.begin(), live.end());
            std::set<int> all(live.begin(), live.end());
            all.insert(dead.begin(), dead.end());
            for (int f : all) {
                auto k = serialize_field_index(size_t(f));
                auto kb = managed_bytes(reinterpret_cast<const int8_t*>(k.data()), k.size());
                if (!live_set.contains(f)) {
                    w.push_back(managed_bytes_view(kb), atomic_cell::make_dead(
                            ts, gc_clock::time_point(gc_clock::duration(60000))));
                    continue;
                }
                bytes v = f == 0 ? int32_type->decompose(f * 7 + 1)
                        : f == 1 ? utf8_type->decompose(sstring(format("f{}", f)))
                                 : long_type->decompose(int64_t(f) * 1000);
                w.push_back(managed_bytes_view(kb),
                            atomic_cell::make_live(*udt->type(size_t(f)), ts, bytes_view(v)));
            }
            return atomic_cell_or_collection(std::move(w).finish());
        };

        utils::chunked_vector<mutation> muts;
        for (int p = 0; p < 6; ++p) {
            auto pk = partition_key::from_single_value(
                    *s, utf8_type->decompose(sstring(format("ukey{:04d}", p))));
            mutation m(s, pk);
            const api::timestamp_type ts = 3000 + p;
            if (p % 3 != 2) {
                m.set_static_cell(sudef, udt_cell(ts, std::nullopt, {0, 2}, {}));
            }
            for (int r = 0; r < 4; ++r) {
                auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                m.partition().clustered_row(*s, ck).apply(row_marker(ts));
                switch ((p + r) % 4) {
                case 0: m.set_clustered_cell(ck, udef, udt_cell(ts, std::nullopt, {0, 1, 2}, {}));
                        break;
                // A field-level tombstone: the shape that must not read back as an unset field.
                case 1: m.set_clustered_cell(ck, udef, udt_cell(ts, std::nullopt, {0}, {1}));
                        break;
                // A whole-UDT delete, which is a collection-wide tombstone, plus one field
                // written above it.
                case 2: m.set_clustered_cell(ck, udef, udt_cell(ts + 1, ts, {2}, {}));
                        break;
                // Nothing at all for `u`; the frozen twin carries the row instead.
                default: m.set_clustered_cell(ck, fudef, atomic_cell::make_live(
                                *frozen_udt, ts, bytes_view(
                                        make_user_value(frozen_udt, user_type_impl::native_type({
                                                data_value(int32_t(r)),
                                                data_value(sstring(format("z{}", r)))}))
                                        .serialize_nonnull())));
                        break;
                }
            }
            muts.push_back(std::move(m));
        }
        std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
            return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
        });

        auto ref = make_sstable_containing(
                env.make_sstable(s, sstables::get_highest_sstable_version()), muts).get();
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        auto want = read_all(ref, s, env.make_reader_permit());
        auto got  = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(got.size(), want.size());
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(want[i]);
        }

        // And the fields are really there, so this cannot pass on two uniformly empty UDT cells.
        size_t live_fields = 0, dead_fields = 0, tombs = 0;
        for (const auto& m : got) {
            for (const rows_entry& re : m.partition().clustered_rows()) {
                const auto* c = re.row().cells().find_cell(udef.id);
                if (!c) { continue; }
                auto cmv = c->as_collection_mutation();
                if (cmv.tomb()) { ++tombs; }
                for (auto&& kv : cmv) {
                    BOOST_REQUIRE_LT(deserialize_field_index(kv.first), 3u);
                    kv.second.is_live() ? ++live_fields : ++dead_fields;
                }
            }
        }
        BOOST_REQUIRE_GT(live_fields, 0u);
        BOOST_REQUIRE_GT(dead_fields, 0u);
        BOOST_REQUIRE_GT(tombs, 0u);
    }).get();
}

// `bytes_on_disk()`, checked against the sizes the storage layer actually reports.
//
// The accounting matters beyond bookkeeping: `ondisk_data_size()` is what mixed-format candidate
// sets are bucketed on (design doc 10.3i), so a `pq` sstable that mis-reports its footprint
// mis-buckets. And `pq`'s component set is unlike any mx version's -- the Index component carries
// no promoted index, and the whole Parquet image including its footer lives inside Data.db -- so it
// is summing a different set of files from the one the existing check was written against.
//
// That existing check is `test_sstable_bytes_on_disk_correctness`
// (`sstable_datafile_test.cc:3199`), the only place in the tree that compares the reported number
// against real file sizes. It takes the version-less `env.make_sstable(schema)` overload, which
// resolves to `get_highest_sstable_version()` -- and that deliberately steps back past `pq`
// (`version.hh:87`), so it has never run this format. Pointing it at `pq` is a two-line change and
// was tried; see the note below for why it is not the change made here.
SEASTAR_THREAD_TEST_CASE(test_pq_bytes_on_disk_matches_the_storage_layer) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto muts = make_muts(s, 10, 30);

        // Both formats, because the assertion is only interesting if it is capable of *not* being
        // trivially true, and the native arm is the control that says the summation itself works.
        for (auto v : {sstables::get_highest_sstable_version(), sstable_version_types::pq}) {
            BOOST_TEST_CONTEXT("version " << fmt::to_string(v)) {
                auto sst = make_sstable_containing(env.make_sstable(s, v), muts).get();

                uint64_t summed = 0;
                size_t components = 0;
                auto& storage = const_cast<sstables::storage&>(sst->get_storage());
                for (auto& ct : sstables::test(sst).get_components()) {
                    auto f = storage.open_component(*sst, ct, open_flags::ro, file_open_options{},
                                                    true).get();
                    summed += f.size().get();
                    ++components;
                }
                // A pq sstable still carries TOC, Index, Summary, Filter, Statistics and Digest
                // beside Data, so a count this low would mean the loop above summed almost nothing.
                BOOST_REQUIRE_GT(components, 3u);
                BOOST_REQUIRE_EQUAL(sst->bytes_on_disk(), summed);

                // ondisk_data_size() is the Data component alone -- the number tiering buckets on --
                // so it must be a strict part of the total rather than the total itself.
                BOOST_REQUIRE_GT(sst->ondisk_data_size(), 0u);
                BOOST_REQUIRE_LT(sst->ondisk_data_size(), sst->bytes_on_disk());
            }
        }
    }).get();
}

// Intra-partition forwarding. The reader itself cannot seek by clustering
// position, so make_reader() wraps it in the forwardable adapter rather than
// accepting forwarding::yes and ignoring the position range -- which is what it
// used to do, and which silently returned rows the caller had not asked for.
SEASTAR_THREAD_TEST_CASE(test_pq_forwarding_reader_honours_position_range) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto muts = make_muts(s, 6, 10);
        auto expected = muts;
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        const auto& want = expected[0];
        auto pr = dht::partition_range::make_singular(want.decorated_key());
        auto rd = sst->make_reader(s, env.make_reader_permit(), pr, s->full_slice(),
                                   nullptr, streamed_mutation::forwarding::yes);
        auto ck = [&] (int i) {
            return clustering_key::from_single_value(*s, int32_type->decompose(i));
        };
        assert_that(std::move(rd))
            .produces_partition_start(want.decorated_key())
            // Nothing until forwarded: that is what forwarding means.
            .produces_end_of_stream()
            .fast_forward_to(ck(3), ck(6))
            .produces_row_with_key(ck(3))
            .produces_row_with_key(ck(4))
            .produces_row_with_key(ck(5))
            .produces_end_of_stream()
            .fast_forward_to(ck(8), ck(9))
            .produces_row_with_key(ck(8))
            .produces_end_of_stream();
    }).get();
}

// A read with a clustering slice must not return rows outside it. The reader
// used to take the slice and drop it, which the mutation-source conformance
// suite catches in test_range_tombstones_v2 -- and which also left a dangling
// reference, because the reversed path builds its slice with reverse_slice()
// and a reader outlives the call that made it.
SEASTAR_THREAD_TEST_CASE(test_pq_read_honours_clustering_slice) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto muts = make_muts(s, 8, 12);
        auto expected = muts;
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        auto ck = [&] (int i) {
            return clustering_key::from_single_value(*s, int32_type->decompose(i));
        };
        auto slice = partition_slice_builder(*s)
                .with_range(query::clustering_range::make(ck(4), ck(7)))
                .build();

        const auto& want = expected[0];
        auto pr = dht::partition_range::make_singular(want.decorated_key());
        auto rd = sst->make_reader(s, env.make_reader_permit(), pr, slice);
        auto close = deferred_close(rd);
        auto got = read_mutation_from_mutation_reader(rd).get();
        BOOST_REQUIRE(got);

        sstring keys;
        for (const auto& re : got->partition().clustered_rows()) {
            keys += format("{}{}", keys.empty() ? "" : ",",
                           value_cast<int32_t>(int32_type->deserialize(
                                   re.key().explode(*s)[0])));
        }
        BOOST_REQUIRE_EQUAL(keys, sstring("4,5,6,7"));
    }).get();
}

// Non-frozen collections, through the real sstable path. They are the one thing
// in the mutation stream that needs Dremel nesting rather than another leaf, and
// the states that matter are absent, present-but-empty, populated, and deleted --
// conflating absent with empty resurrects a collection the user cleared.
SEASTAR_THREAD_TEST_CASE(test_pq_collections_round_trip) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto map_si = map_type_impl::get_instance(utf8_type, int32_type, true);
        auto set_i  = set_type_impl::get_instance(int32_type, true);
        auto s = schema_builder(1, "ks", "pq_coll")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("v", int32_type)
            .with_column("m", map_si)
            .with_column("t", set_i)
            .with_column("sm", map_si, column_kind::static_column)
            .build();

        const auto& mdef = *s->get_column_definition(to_bytes("m"));
        const auto& tdef = *s->get_column_definition(to_bytes("t"));
        const auto& smdef = *s->get_column_definition(to_bytes("sm"));

        auto make_map = [&] (api::timestamp_type ts, int n, bool tomb, bool dead_first) {
            collection_mutation_writer w(tomb
                    ? tombstone(ts - 1, gc_clock::time_point(gc_clock::duration(7)))
                    : tombstone());
            for (int i = 0; i < n; ++i) {
                auto k = utf8_type->decompose(sstring(format("k{}", i)));
                auto kb = managed_bytes(reinterpret_cast<const int8_t*>(k.data()), k.size());
                if (dead_first && i == 0) {
                    w.push_back(managed_bytes_view(kb), atomic_cell::make_dead(ts,
                            gc_clock::time_point(gc_clock::duration(3))));
                } else {
                    w.push_back(managed_bytes_view(kb), atomic_cell::make_live(
                            *int32_type, ts, int32_type->decompose(i * 10)));
                }
            }
            return atomic_cell_or_collection(std::move(w).finish());
        };
        auto make_set = [&] (api::timestamp_type ts, int n) {
            collection_mutation_writer w{tombstone{}};
            for (int i = 0; i < n; ++i) {
                auto k = int32_type->decompose(i);
                auto kb = managed_bytes(reinterpret_cast<const int8_t*>(k.data()), k.size());
                w.push_back(managed_bytes_view(kb),
                            atomic_cell::make_live(*bytes_type, ts, bytes_view()));
            }
            return atomic_cell_or_collection(std::move(w).finish());
        };

        utils::chunked_vector<mutation> muts;
        for (int p = 0; p < 18; ++p) {
            auto pk = partition_key::from_single_value(
                    *s, utf8_type->decompose(sstring(format("key{:04d}", p))));
            mutation m(s, pk);
            const api::timestamp_type ts = 4000 + p;
            // A static collection on most partitions: it belongs to the partition,
            // so it must come back on the static row rather than on any row.
            if (p % 3 != 2) {
                m.set_static_cell(smdef, make_map(ts, 2, p % 6 == 1, false));
            }
            for (int r = 0; r < 3; ++r) {
                auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                m.set_clustered_cell(ck, *s->get_column_definition(to_bytes("v")),
                        atomic_cell::make_live(*int32_type, ts, int32_type->decompose(r)));
                const int kind = (p + r) % 5;
                if (kind == 1) {
                    m.set_clustered_cell(ck, mdef, make_map(ts, 3, false, false));
                } else if (kind == 2) {
                    m.set_clustered_cell(ck, mdef, make_map(ts, 2, true, false));
                } else if (kind == 3) {
                    m.set_clustered_cell(ck, mdef, make_map(ts, 2, false, true));
                } else if (kind == 4) {
                    m.set_clustered_cell(ck, tdef, make_set(ts, 4));
                }
                // kind == 0: neither collection present at all
            }
            muts.push_back(std::move(m));
        }
        std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
            return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
        });

        // Against the reference format rather than the in-memory mutation: the
        // write path may legitimately normalise a collection, and the question is
        // whether pq behaves like mc.
        auto ref = make_sstable_containing(
                env.make_sstable(s, sstables::get_highest_sstable_version()), muts).get();
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        auto want = read_all(ref, s, env.make_reader_permit());
        auto got  = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(got.size(), want.size());
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(want[i]);
        }
    }).get();
}

// A deleted cell must not come back as a cell that was never written.
//
// L0 folding stores a per-column `__live_` flag, so it can tell "dead" from "absent".
// L1 and L2 -- and L1 is the default -- do not: they carry the value, a `__ttl_` and a
// `__ldt_`. Deadness therefore has to be read off `__ldt_`, and the reassembler used to
// bail on a missing value before looking at it:
//
//     if (!present) { continue; }
//
// so every dead cell in an L1 file was silently dropped on the way back. That is the
// worst shape of bug this format can have: the file is valid, the read succeeds, and a
// cell the user deleted returns as though the delete never happened -- resurrecting
// whatever it shadowed on merge. It was invisible to a round-trip test that only wrote
// live cells, and it is why this case gets its own test.
SEASTAR_THREAD_TEST_CASE(test_pq_dead_cells_are_not_lost) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = schema_builder(1, "ks", "pq_dead")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("a", int32_type)
            .with_column("b", int32_type)
            .with_column("c", utf8_type)
            .build();
        const auto& adef = *s->get_column_definition(to_bytes("a"));
        const auto& bdef = *s->get_column_definition(to_bytes("b"));
        const auto& cdef = *s->get_column_definition(to_bytes("c"));

        utils::chunked_vector<mutation> muts;
        for (int p = 0; p < 12; ++p) {
            auto pk = partition_key::from_single_value(
                    *s, utf8_type->decompose(sstring(format("key{:04d}", p))));
            mutation m(s, pk);
            const api::timestamp_type ts = 5000 + p;
            for (int r = 0; r < 5; ++r) {
                auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                const auto ldt = gc_clock::time_point(gc_clock::duration(3600 + r));
                switch ((p + r) % 5) {
                case 0:
                    // The case that was lost: the row's *only* content is a dead cell.
                    m.set_clustered_cell(ck, adef, atomic_cell::make_dead(ts, ldt));
                    break;
                case 1:
                    // Dead beside live, so the row survives either way and only the
                    // deletion itself goes missing.
                    m.set_clustered_cell(ck, adef, atomic_cell::make_dead(ts, ldt));
                    m.set_clustered_cell(ck, bdef, atomic_cell::make_live(
                            *int32_type, ts, int32_type->decompose(r)));
                    break;
                case 2:
                    // Every column dead.
                    m.set_clustered_cell(ck, adef, atomic_cell::make_dead(ts, ldt));
                    m.set_clustered_cell(ck, bdef, atomic_cell::make_dead(ts, ldt));
                    m.set_clustered_cell(ck, cdef, atomic_cell::make_dead(ts, ldt));
                    break;
                case 3:
                    // A live cell with a TTL also carries an ldt (its expiry), so it
                    // must still read back as live -- the discriminator is the value,
                    // not the presence of an ldt.
                    m.set_clustered_cell(ck, adef, atomic_cell::make_live(
                            *int32_type, ts, int32_type->decompose(r),
                            gc_clock::time_point(gc_clock::duration(9000 + r)),
                            gc_clock::duration(600)));
                    m.set_clustered_cell(ck, bdef, atomic_cell::make_dead(ts, ldt));
                    break;
                default:
                    // Absent must stay absent: `b` and `c` are never written here.
                    m.set_clustered_cell(ck, adef, atomic_cell::make_live(
                            *int32_type, ts, int32_type->decompose(r)));
                    break;
                }
            }
            muts.push_back(std::move(m));
        }
        std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
            return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
        });

        auto ref = make_sstable_containing(
                env.make_sstable(s, sstables::get_highest_sstable_version()), muts).get();
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        auto want = read_all(ref, s, env.make_reader_permit());
        auto got  = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(got.size(), want.size());
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(want[i]);
        }

        // Count the dead cells explicitly, so this cannot pass by both sides being
        // equally empty -- which is exactly how the bug hid.
        auto count_dead = [&] (const utils::chunked_vector<mutation>& ms) {
            size_t n = 0;
            for (const auto& m : ms) {
                for (const rows_entry& re : m.partition().clustered_rows()) {
                    re.row().cells().for_each_cell([&] (column_id id,
                                                       const atomic_cell_or_collection& acoc) {
                        const auto& def = s->regular_column_at(id);
                        if (def.is_atomic() && !acoc.as_atomic_cell(def).is_live()) { ++n; }
                    });
                }
            }
            return n;
        };
        const size_t dead_ref = count_dead(want), dead_pq = count_dead(got);
        BOOST_REQUIRE_GT(dead_ref, 0u);
        BOOST_REQUIRE_EQUAL(dead_pq, dead_ref);
    }).get();
}

// The folded deletion channel, on both write paths, from one body of data.
//
// A dead cell's deletion time used to go into its own column's `__ldt_<col>` leaf: one leaf per
// column, where the same rows' *write* times were folded into a single `__ts` for the whole row.
// On the corpus's 197-column Backblaze slice that was 195 leaves and 60.1 MB against 1.9 MB for
// the write times -- the same information shape at ~32x the cost, and the reason `pq` paid ~63 MB
// where the row format paid ~10 MB for the same tombstones. `__ldt` + `__dmask` + the `__ldtx`
// pair replace it with four leaves regardless of table width.
//
// Asserted on both paths in one test, deliberately. An sstable is written either by
// cut_row_group() once it outgrows the row-group budget or by write_rows() in one shot when it
// fits, the choice is a function of data size and invisible to the operator, and that divergence
// has already produced two separate bugs here (design doc 8.2b: per-column encodings applied only
// on the cutting path; 10.15: the L2 footer key written only on the other). The two paths also
// differ in leaf set -- the cutting path must fix its leaves before it has seen all the rows, so
// it uses the conservative set -- which is exactly the kind of asymmetry that hides a bug, so the
// same rows go down both and the expectations differ only where they must.
//
// No TTLs anywhere here, on purpose: a live cell with a TTL legitimately keeps a per-column
// `__ldt_<col>` for its expiry, and leaving that case out is what lets this test assert the strong
// thing -- that on the derived path the per-column deletion leaves are *gone*, not merely empty.
// The mixed case is covered by test_pq_dead_cells_are_not_lost and by parquet_shred_test's matrix.
SEASTAR_THREAD_TEST_CASE(test_pq_folded_deletion_channel_on_both_write_paths) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        constexpr int NCOL = 8;
        auto build = [] (const char* name, std::optional<int> rows_per_rg) {
            auto sb = schema_builder(1, "ks", name)
                .with_column("pk", utf8_type, column_kind::partition_key)
                .with_column("ck", int32_type, column_kind::clustering_key);
            for (int i = 0; i < NCOL; ++i) {
                sb.with_column(to_bytes(format("v{}", i)), int32_type);
            }
            if (rows_per_rg) {
                sb.set_parquet_options({{"rows_per_row_group", format("{}", *rows_per_rg)}});
            }
            return sb.build();
        };
        // Same shape, same data; the only difference is the row-group budget, which is what
        // selects the write path. 1 000 is the minimum the option accepts.
        auto s_cut   = build("pq_fold_cut", 1000);
        auto s_whole = build("pq_fold_whole", std::nullopt);

        // 3 000 rows: three row groups under a 1 000-row budget, one under the 5 000 default.
        constexpr int PARTS = 150, ROWS = 20;
        auto make_muts = [&] (const schema_ptr& s) {
            std::vector<const column_definition*> defs;
            for (int i = 0; i < NCOL; ++i) {
                defs.push_back(s->get_column_definition(to_bytes(format("v{}", i))));
            }
            utils::chunked_vector<mutation> muts;
            muts.reserve(PARTS);
            for (int p = 0; p < PARTS; ++p) {
                auto pk = partition_key::from_single_value(
                        *s, utf8_type->decompose(sstring(format("key{:06d}", p))));
                mutation m(s, pk);
                for (int r = 0; r < ROWS; ++r) {
                    auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                    const api::timestamp_type ts = 1700000000000000 + p * 100 + r;
                    // The row's deletion time, shared by its dead cells -- which is what a
                    // bind-NULL INSERT produces, since one statement carries one timestamp.
                    const auto row_ldt = gc_clock::time_point(gc_clock::duration(40000 + p));
                    for (int i = 0; i < NCOL; ++i) {
                        switch ((p + r + i) % 5) {
                        case 0:
                        case 1:
                            // Dead, sharing the row's deletion time. The common case, and the
                            // one the fold collapses.
                            m.set_clustered_cell(ck, *defs[i],
                                                 atomic_cell::make_dead(ts, row_ldt));
                            break;
                        case 2:
                            // Dead, but with its own deletion time -- an exception entry, as a
                            // cell deleted by a separate statement would be.
                            m.set_clustered_cell(ck, *defs[i], atomic_cell::make_dead(
                                    ts, gc_clock::time_point(
                                            gc_clock::duration(40000 + p + i * 13 + 1))));
                            break;
                        case 3:
                            m.set_clustered_cell(ck, *defs[i], atomic_cell::make_live(
                                    *int32_type, ts, int32_type->decompose(r * 7 + i)));
                            break;
                        default:
                            break;   // absent: never written, and must stay that way
                        }
                    }
                }
                muts.push_back(std::move(m));
            }
            std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
                return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
            });
            return muts;
        };

        auto count_dead = [] (const schema_ptr& s,
                              const utils::chunked_vector<mutation>& ms) {
            size_t n = 0;
            for (const auto& m : ms) {
                for (const rows_entry& re : m.partition().clustered_rows()) {
                    re.row().cells().for_each_cell([&] (column_id id,
                                                       const atomic_cell_or_collection& acoc) {
                        const auto& def = s->regular_column_at(id);
                        if (def.is_atomic() && !acoc.as_atomic_cell(def).is_live()) { ++n; }
                    });
                }
            }
            return n;
        };

        // The reference is a native-format sstable over the same mutations: it is what the rows
        // must still be after a pq round trip, deletion times included.
        auto ref_muts = make_muts(s_whole);
        auto ref = make_sstable_containing(
                env.make_sstable(s_whole, sstables::get_highest_sstable_version()),
                ref_muts).get();
        auto want = read_all(ref, s_whole, env.make_reader_permit());
        const size_t dead_ref = count_dead(s_whole, ref_muts);
        BOOST_REQUIRE_GT(dead_ref, 0u);

        struct arm { const char* what; schema_ptr s; bool expect_cut; };
        for (auto a : {arm{"cut_row_group", s_cut, true},
                       arm{"write_rows", s_whole, false}}) {
            BOOST_TEST_CONTEXT("write path: " << a.what) {
                auto muts = make_muts(a.s);
                auto sst = make_sstable_containing(
                        env.make_sstable(a.s, sstable_version_types::pq), std::move(muts)).get();

                const uint64_t len = sst->ondisk_data_size();
                auto buf = sst->data_read(0, len, env.make_reader_permit()).get();
                std::vector<uint8_t> img(buf.get(), buf.get() + buf.size());
                auto md = sstables::parquet::format::parse_footer(img);

                // The arm is only meaningful if it actually took the path it names.
                if (a.expect_cut) {
                    BOOST_REQUIRE_GT(md.row_groups.size(), 1u);
                } else {
                    BOOST_REQUIRE_EQUAL(md.row_groups.size(), 1u);
                }

                std::vector<std::string> leaves;
                for (size_t i = 1; i < md.schema.size(); ++i) {
                    if (md.schema[i].is_leaf()) { leaves.push_back(md.schema[i].name); }
                }
                auto has = [&] (const std::string& n) {
                    return std::find(leaves.begin(), leaves.end(), n) != leaves.end();
                };
                // The folded channel, on both paths. This is the assertion the two prior
                // divergence bugs would have failed.
                BOOST_REQUIRE(has("__ldt"));
                BOOST_REQUIRE(has("__dmask"));
                BOOST_REQUIRE(has("__ldtx_mask"));
                BOOST_REQUIRE(has("__ldtx_vals"));

                // And the per-column deletion leaves must carry nothing. On the derived leaf
                // set they are not emitted at all -- with no TTLs there is no live expiry to
                // hold -- which is the leaf-count collapse the fold is for. On the
                // conservative set they exist because the writer had to fix its leaves before
                // seeing every row, but every chunk of them must be all-null: a value there
                // would mean a dead cell's deletion time still went per column.
                size_t percol = 0, percol_values = 0;
                for (int i = 0; i < NCOL; ++i) {
                    if (has(format("__ldt_v{}", i))) { ++percol; }
                }
                for (const auto& rg : md.row_groups) {
                    for (const auto& cc : rg.columns) {
                        if (!cc.meta) { continue; }
                        const std::string p = cc.meta->path();
                        if (p.rfind("__ldt_", 0) != 0) { continue; }
                        const int64_t nulls = cc.meta->stats && cc.meta->stats->null_count
                                ? *cc.meta->stats->null_count : -1;
                        BOOST_REQUIRE_EQUAL(nulls, cc.meta->num_values);
                        percol_values += size_t(cc.meta->num_values) - size_t(nulls);
                    }
                }
                BOOST_REQUIRE_EQUAL(percol_values, 0u);
                if (a.expect_cut) {
                    BOOST_REQUIRE_EQUAL(percol, size_t(NCOL));   // conservative: present, empty
                } else {
                    BOOST_REQUIRE_EQUAL(percol, 0u);             // derived: gone entirely
                }

                // Losslessness is the point of all of it: the rows, and every deletion time in
                // them, must match the native-format reference exactly.
                auto got = read_all(sst, a.s, env.make_reader_permit());
                BOOST_REQUIRE_EQUAL(got.size(), want.size());
                for (size_t i = 0; i < got.size(); ++i) {
                    assert_that(got[i]).is_equal_to(want[i]);
                }
                BOOST_REQUIRE_EQUAL(count_dead(a.s, got), dead_ref);
            }
        }
    }).get();
}

// `dead` and `absent` told apart on disk, per row group, with statistics-based leaf elision in
// play.
//
// Everything else in this file establishes that a deletion survives a round trip. That is necessary
// and not sufficient: a deletion stored as an absent cell round-trips perfectly through the pq
// reader, because there is nothing in the file to contradict it. It only becomes wrong when an
// older sstable supplies the value it should have shadowed -- which the hybrid merge tests now
// cover from above, and which this test covers from below, by asserting what is actually in the
// file rather than what comes back out of it.
//
// `__dmask` is the only thing in the format that distinguishes the two states: an absent cell
// contributes no def-level to it, a dead one contributes a set bit. So the count of non-null
// `__dmask` entries, summed over row groups, must equal the number of rows carrying at least one
// dead cell -- a quantity this test's generator knows exactly. Comparing against a total rather
// than per row group is deliberate: cut_row_group() cuts on partition boundaries and overshoots
// its budget, so which rows land in which group is the writer's business, and pinning it would
// make this a change-detector.
//
// It also pins the interaction between the fold and leaf elision, which is the one coupling in the
// read path where a lost tombstone would be silent. The reader elides a leaf whose statistics say
// it is null in every row of the group (`null_count == num_values`) and substitutes "no value for
// any row", so an all-null `__dmask` row group is skipped entirely and every column in it resolves
// to absent. That is correct exactly when the group really holds no dead cell. Partitions 20-59
// carry none, which at 50 rows each is 2 000 rows against a 1 000-row budget and therefore
// guarantees at least one wholly elidable group; partitions 0-19 and 60-79 do carry them, so at
// least one group is not elidable. Both states have to occur in the same file for the pairing of
// the two to be exercised at all.
SEASTAR_THREAD_TEST_CASE(test_pq_dead_and_absent_are_distinguishable_on_disk) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = schema_builder(1, "ks", "pq_deadmask")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("a", int32_type)
            .with_column("b", int32_type)
            .with_column("c", utf8_type)
            .with_column("st", int32_type, column_kind::static_column)
            .set_parquet_options({{"rows_per_row_group", "1000"}})
            .build();
        const auto& adef = *s->get_column_definition(to_bytes("a"));
        const auto& bdef = *s->get_column_definition(to_bytes("b"));
        const auto& cdef = *s->get_column_definition(to_bytes("c"));
        const auto& stdef = *s->get_column_definition(to_bytes("st"));

        constexpr int parts = 80, rows = 50;
        // The quiet band, wide enough to contain a whole row group with nothing dead in it.
        //
        // Banded by position in *token* order, not by key. The writer lays partitions out in token
        // order and cuts row groups on partition boundaries, so a band that is contiguous in the
        // key is scattered through the file and no row group comes out wholly quiet -- which is
        // exactly what the first run of this test found. So the empty partitions are built and
        // sorted first, and the band is a contiguous run of the sorted sequence.
        auto quiet = [] (size_t i) { return i >= 20 && i < 60; };
        // One partition in the noisy band has its static cell deleted. A static is shredded as a
        // regular column replayed onto every row, so this sets a __dmask bit on all 50 of them --
        // and if that bit is lost the value comes back on every row, not just one.
        constexpr size_t dead_static_part = 65;

        utils::chunked_vector<mutation> muts;
        for (int p = 0; p < parts; ++p) {
            muts.emplace_back(s, partition_key::from_single_value(
                    *s, utf8_type->decompose(sstring(format("dk{:04d}", p)))));
        }
        std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
            return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
        });

        // Rows carrying at least one dead cell, counted the way the shredder sees them: a deleted
        // static counts for every row of its partition.
        size_t expect_rows_with_dead = 0;
        for (size_t p = 0; p < muts.size(); ++p) {
            mutation& m = muts[p];
            const api::timestamp_type ts = 8000 + p;
            const auto ldt = gc_clock::time_point(gc_clock::duration(60000 + p));

            const bool dead_static = (p == dead_static_part);
            if (dead_static) {
                m.set_static_cell(stdef, atomic_cell::make_dead(ts, ldt));
            } else if (p % 7 == 0) {
                m.set_static_cell(stdef, atomic_cell::make_live(
                        *int32_type, ts, int32_type->decompose(int32_t(p))));
            }

            for (int r = 0; r < rows; ++r) {
                auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                bool row_has_dead = dead_static;

                // `b` is always live, so no row is ever empty and the row count is not itself
                // what carries the signal.
                m.set_clustered_cell(ck, bdef, atomic_cell::make_live(
                        *int32_type, ts, int32_type->decompose(r)));

                if (quiet(p)) {
                    // Live and absent only. `a` present, `c` absent on odd rows -- so the quiet
                    // band still exercises absence, which is the state a lost tombstone decays
                    // into and must therefore be distinguishable from.
                    m.set_clustered_cell(ck, adef, atomic_cell::make_live(
                            *int32_type, ts, int32_type->decompose(r * 3)));
                    if (r % 2 == 0) {
                        m.set_clustered_cell(ck, cdef, atomic_cell::make_live(
                                *utf8_type, ts, utf8_type->decompose(sstring(format("q{}", r)))));
                    }
                } else {
                    switch (r % 4) {
                    case 0:
                        // `a` dead, `c` absent: the pair that must not be conflated.
                        m.set_clustered_cell(ck, adef, atomic_cell::make_dead(ts, ldt));
                        row_has_dead = true;
                        break;
                    case 1:
                        // `c` dead with its own deletion time, so the exception channel is used.
                        m.set_clustered_cell(ck, cdef, atomic_cell::make_dead(
                                ts, gc_clock::time_point(gc_clock::duration(60000 + p + r))));
                        row_has_dead = true;
                        break;
                    case 2:
                        // Both dead, sharing the row's deletion time.
                        m.set_clustered_cell(ck, adef, atomic_cell::make_dead(ts, ldt));
                        m.set_clustered_cell(ck, cdef, atomic_cell::make_dead(ts, ldt));
                        row_has_dead = true;
                        break;
                    default:
                        // Nothing dead: `a` live, `c` absent. Interleaving these among the dead
                        // rows means __dmask's def-levels are genuinely sparse rather than a
                        // constant, which is what makes the null count meaningful.
                        m.set_clustered_cell(ck, adef, atomic_cell::make_live(
                                *int32_type, ts, int32_type->decompose(r * 5)));
                        break;
                    }
                }
                if (row_has_dead) { ++expect_rows_with_dead; }
            }
        }
        BOOST_REQUIRE_GT(expect_rows_with_dead, 0u);

        auto ref = make_sstable_containing(
                env.make_sstable(s, sstables::get_highest_sstable_version()), muts).get();
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), muts).get();

        // ---- on disk ----
        const uint64_t len = sst->ondisk_data_size();
        auto buf = sst->data_read(0, len, env.make_reader_permit()).get();
        std::vector<uint8_t> img(buf.get(), buf.get() + buf.size());
        auto md = sstables::parquet::format::parse_footer(img);

        // The premise of the whole test: more than one row group, so "elidable in one group and
        // not in another" is a state this file can actually be in.
        BOOST_REQUIRE_GT(md.row_groups.size(), 1u);

        size_t dmask_live = 0, dmask_groups = 0, all_null_groups = 0, partial_groups = 0;
        for (const auto& rg : md.row_groups) {
            for (const auto& cc : rg.columns) {
                if (!cc.meta || cc.meta->path() != "__dmask") { continue; }
                ++dmask_groups;
                // Statistics must be present, or elision is off and this test proves nothing
                // about it.
                BOOST_REQUIRE(cc.meta->stats && cc.meta->stats->null_count);
                const int64_t nulls = *cc.meta->stats->null_count;
                const int64_t n = cc.meta->num_values;
                BOOST_REQUIRE_GE(nulls, 0);
                BOOST_REQUIRE_LE(nulls, n);
                dmask_live += size_t(n - nulls);
                if (n > 0 && nulls == n) { ++all_null_groups; } else { ++partial_groups; }
            }
        }
        BOOST_REQUIRE_EQUAL(dmask_groups, md.row_groups.size());

        // The assertion this test exists for: exactly one non-null __dmask entry per row carrying
        // a dead cell, and none for a row whose columns are merely absent. A tombstone written as
        // an absent cell lowers this; nothing else does.
        BOOST_REQUIRE_EQUAL(dmask_live, expect_rows_with_dead);

        // Both elision states occur, so the read-back below exercises the elided path and the
        // decoded path in one file.
        BOOST_REQUIRE_GT(all_null_groups, 0u);
        BOOST_REQUIRE_GT(partial_groups, 0u);

        // The fold's own invariant, cheap to re-check here: no dead cell's deletion time went to a
        // per-column leaf. There are no TTLs in this schema's data, so any value in a
        // `__ldt_<col>` leaf would have to be a dead cell's.
        for (const auto& rg : md.row_groups) {
            for (const auto& cc : rg.columns) {
                if (!cc.meta || cc.meta->path().rfind("__ldt_", 0) != 0) { continue; }
                BOOST_REQUIRE(cc.meta->stats && cc.meta->stats->null_count);
                BOOST_REQUIRE_EQUAL(*cc.meta->stats->null_count, cc.meta->num_values);
            }
        }

        // ---- through the reader ----
        auto want = read_all(ref, s, env.make_reader_permit());
        auto got  = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(got.size(), want.size());
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(want[i]);
        }

        // And the counts, so this cannot pass by both sides losing the same thing. `absent` is
        // counted as well as `dead`: a dead cell wrongly stored as absent lowers the dead count,
        // while an absent cell wrongly materialised as dead raises it, and only checking both
        // pins the distinction in the direction that matters.
        auto tally = [&] (const utils::chunked_vector<mutation>& ms) {
            size_t dead = 0, live = 0;
            for (const auto& m : ms) {
                auto scan = [&] (const row& cells, column_kind kind) {
                    cells.for_each_cell([&] (column_id id,
                                            const atomic_cell_or_collection& acoc) {
                        const auto& def = s->column_at(kind, id);
                        if (!def.is_atomic()) { return; }
                        if (acoc.as_atomic_cell(def).is_live()) { ++live; } else { ++dead; }
                    });
                };
                scan(m.partition().static_row().get(), column_kind::static_column);
                for (const rows_entry& re : m.partition().clustered_rows()) {
                    scan(re.row().cells(), column_kind::regular_column);
                }
            }
            return std::pair<size_t, size_t>{dead, live};
        };
        const auto [dead_w, live_w] = tally(want);
        const auto [dead_g, live_g] = tally(got);
        BOOST_REQUIRE_GT(dead_w, 0u);
        BOOST_REQUIRE_GT(live_w, 0u);
        BOOST_REQUIRE_EQUAL(dead_g, dead_w);
        BOOST_REQUIRE_EQUAL(live_g, live_w);
    }).get();
}

// A row group wider than the scan window, so a read has to cross the seam inside one group.
//
// next_window() picks streaming or paging per window from two comparisons: `lo == rg_first` and
// `grp_hi == rg_end`. When both hold it streams the group without touching the page index, and a
// streamed window is capped at scan_window_rows (16 384). So the *second* window of a row group
// wider than that starts at 16 384 with `lo != rg_first`, which drops into the other branch: it
// loads the OffsetIndex, computes the elidable leaves, and either pages the rest in 512-row windows
// or streams it with a non-zero start offset. Both are code the first window never reaches.
//
// What is new here is specifically the *cap-driven* second window: a group that needs more than one
// window because the 16 384 cap binds, rather than because a range bound falls inside it. To be
// precise about the difference, since it is easy to overstate:
// test_pq_bounded_range_streams_and_agrees_with_row_format does reach `lo != rg_first`, when a
// bounded range starts partway into a group -- verified by mutation, see the commit message. What
// it cannot reach is a group needing a second window at all, because at 8 000 rows in 5 000-row
// groups no group is wider than the cap, so `lo + win` never binds and no read ever decodes one
// group in two batches. Before this test, no data-writing case anywhere in the tree configured
// rows_per_row_group above 16 384, so that geometry had never been written, let alone read.
//
// rows_per_row_group is set to 20 000, above the cap, and 25 000 rows are written. Cuts happen on
// partition boundaries once the budget is reached, so the first group comes out at 20 090 rows --
// the overshoot is expected -- with the seam at 16 384 inside it. The row-group width is asserted
// on disk rather than assumed, so the test degrades loudly rather than silently if the writer's
// budget or the cap ever changes. Dead cells and a range tombstone are spread across the seam as
// well, because the window boundary is also where the folded deletion channel and the per-row
// metadata have to line up across two separately decoded batches.
SEASTAR_THREAD_TEST_CASE(test_pq_reads_cross_a_window_seam_inside_a_row_group) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = schema_builder(1, "ks", "pq_seam")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("a", int32_type)
            .with_column("b", utf8_type)
            .set_parquet_options({{"rows_per_row_group", "20000"}})
            .build();
        const auto& adef = *s->get_column_definition(to_bytes("a"));
        const auto& bdef = *s->get_column_definition(to_bytes("b"));

        // 250 x 100 = 25 000 rows: one 20 000-row group holding the seam, then a 5 000-row tail.
        constexpr int parts = 250, rows = 100;
        constexpr int64_t window = 16384;

        utils::chunked_vector<mutation> muts;
        for (int p = 0; p < parts; ++p) {
            muts.emplace_back(s, partition_key::from_single_value(
                    *s, utf8_type->decompose(sstring(format("sk{:05d}", p)))));
        }
        // Token order is the file's order, so the seam's position is only predictable once the
        // partitions are sorted -- and the interesting reads below are picked by file position.
        std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
            return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
        });

        for (size_t p = 0; p < muts.size(); ++p) {
            mutation& m = muts[p];
            const api::timestamp_type ts = 900000 + p;
            const auto ldt = gc_clock::time_point(gc_clock::duration(50000 + p));
            for (int r = 0; r < rows; ++r) {
                auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                m.partition().clustered_row(*s, ck).apply(row_marker(ts));
                if (r % 5 == 0) {
                    // Dead, so the folded channel is populated on both sides of the seam.
                    m.set_clustered_cell(ck, adef, atomic_cell::make_dead(ts, ldt));
                } else if (r % 5 != 4) {
                    m.set_clustered_cell(ck, adef, atomic_cell::make_live(
                            *int32_type, ts, int32_type->decompose(int32_t(p) * 100 + r)));
                }   // r % 5 == 4: absent
                m.set_clustered_cell(ck, bdef, atomic_cell::make_live(
                        *utf8_type, ts, utf8_type->decompose(sstring(format("s{}-{}", p, r)))));
            }
            // A range tombstone in every partition, so one crosses the seam wherever it falls.
            m.partition().apply_delete(*s, range_tombstone(
                    clustering_key_prefix::from_single_value(*s, int32_type->decompose(40)),
                    bound_kind::incl_start,
                    clustering_key_prefix::from_single_value(*s, int32_type->decompose(44)),
                    bound_kind::excl_end,
                    tombstone(ts + 1, ldt)));
        }

        auto ref = make_sstable_containing(
                env.make_sstable(s, sstables::get_highest_sstable_version()), muts).get();
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), muts).get();

        // The premise: a row group actually wider than the window. Without this the test would
        // silently degrade into another full-scan round trip if the writer's budget or the window
        // constant ever changed.
        const uint64_t len = sst->ondisk_data_size();
        auto buf = sst->data_read(0, len, env.make_reader_permit()).get();
        std::vector<uint8_t> img(buf.get(), buf.get() + buf.size());
        auto md = sstables::parquet::format::parse_footer(img);
        BOOST_REQUIRE_GT(md.row_groups.size(), 1u);
        bool wide = false;
        for (const auto& rg : md.row_groups) {
            if (rg.num_rows > window) { wide = true; }
        }
        BOOST_REQUIRE(wide);

        // A full scan, which crosses the seam at row 16 384 inside the first group.
        {
            auto want = fragments_of(ref, s, env.make_reader_permit());
            auto got  = fragments_of(sst, s, env.make_reader_permit());
            BOOST_REQUIRE_EQUAL(got.size(), want.size());
            for (size_t i = 0; i < got.size(); ++i) {
                BOOST_REQUIRE_EQUAL(got[i], want[i]);
            }
        }

        // Ranges and single partitions picked by file position rather than by key, so each one
        // lands where it is meant to relative to the seam. At 100 rows per partition, the
        // partition at sorted index i starts at row 100*i, so the seam at 16 384 falls inside
        // sorted partition 163.
        const size_t seam_part = size_t(window) / rows;          // 163
        auto dk = [&] (size_t i) { return muts[i].decorated_key(); };
        auto range_of = [&] (size_t a, size_t b) {
            return dht::partition_range::make({dk(a), true}, {dk(b), true});
        };

        struct probe { const char* what; dht::partition_range pr; };
        std::vector<probe> probes;
        // Straddling the seam: begins before it, ends after it.
        probes.push_back({"range straddling the seam", range_of(seam_part - 3, seam_part + 3)});
        // Entirely past the seam but still inside the wide group, so every window it produces has
        // lo != rg_first.
        probes.push_back({"range past the seam, same group", range_of(seam_part + 5,
                                                                     seam_part + 20)});
        // The single partition the seam runs through.
        probes.push_back({"single partition on the seam",
                          dht::partition_range::make_singular(dk(seam_part))});
        // A single partition past the seam: a point read whose window starts mid-group.
        probes.push_back({"single partition past the seam",
                          dht::partition_range::make_singular(dk(seam_part + 7))});
        // Spanning the row-group boundary at 20 000 as well as the seam.
        probes.push_back({"range spanning the group boundary", range_of(seam_part, parts - 20)});

        for (auto& pb : probes) {
            BOOST_TEST_CONTEXT("probe: " << pb.what) {
                auto want = fragments_in(ref, s, env.make_reader_permit(), pb.pr, s->full_slice());
                auto got  = fragments_in(sst, s, env.make_reader_permit(), pb.pr, s->full_slice());
                BOOST_REQUIRE_GT(want.size(), 0u);
                BOOST_REQUIRE_EQUAL(got.size(), want.size());
                for (size_t i = 0; i < got.size(); ++i) {
                    BOOST_REQUIRE_EQUAL(got[i], want[i]);
                }
            }
        }
    }).get();
}

// `metadata_folding = 'uniform'` is silently not honoured once an sstable cuts a row group.
//
// This started as a hunt for a bug that turned out not to be reachable, and the real finding is
// the one worth pinning. L2 keeps a single timestamp in the footer instead of a per-row column,
// and the reader requires that key. write_rows() -- the path taken when the whole sstable fits one
// row group -- emits it; cut_row_group() did not. That looked like "any L2 table large enough to
// cut is unreadable", which would have been serious.
//
// It is not, because the two paths differ in a second way that happens to cover the first: the
// cutting path fixes its leaf set before it has seen all the rows, so it uses the *conservative*
// set, which sets all_same_ts = false and turns on every optional metadata leaf. That breaks L2's
// precondition, build_mapped_schema() falls the level back to L1, and no uniform timestamp is ever
// needed.
//
// So the operator-visible behaviour is: the same table is L2 while it is small and L1 once it is
// large, with nothing logged. That is not data loss -- L1 is lossless and the rows read back
// exactly -- but it is a setting that stops applying at scale, and it belongs in a test rather
// than in someone's afternoon. If a later change makes the cutting path able to reach L2, this
// test fails and the footer-key guard in cut_row_group() is what keeps the file readable.
SEASTAR_THREAD_TEST_CASE(test_pq_uniform_folding_falls_back_when_row_groups_are_cut) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto sb = schema_builder(1, "ks", "pq_l2rg")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("v_txt", utf8_type);
        for (int i = 0; i < 7; ++i) {
            sb.with_column(to_bytes(format("v{}", i)), int32_type);
        }
        sb.set_parquet_options({{"metadata_folding", "uniform"}});
        auto s = sb.build();
        const auto& vt = *s->get_column_definition(to_bytes("v_txt"));

        // One timestamp for every cell and no markers, TTLs or deletions -- L2's precondition is
        // satisfied by the *data*, so anything that stops it being used is the writer's choice
        // rather than the input's.
        constexpr int PARTS = 2500, ROWS = 24;
        constexpr api::timestamp_type TS = 1700000000000000;
        utils::chunked_vector<mutation> muts;
        muts.reserve(PARTS);
        for (int p = 0; p < PARTS; ++p) {
            auto pk = partition_key::from_single_value(
                    *s, utf8_type->decompose(sstring(format("key{:06d}", p))));
            mutation m(s, pk);
            for (int r = 0; r < ROWS; ++r) {
                auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                m.set_clustered_cell(ck, vt, atomic_cell::make_live(
                        *utf8_type, TS, utf8_type->decompose(sstring(format("v{}", r % 40)))));
                for (int i = 0; i < 7; ++i) {
                    m.set_clustered_cell(ck, *s->get_column_definition(to_bytes(format("v{}", i))),
                            atomic_cell::make_live(*int32_type, TS,
                                                   int32_type->decompose(r * 3 + i)));
                }
            }
            muts.push_back(std::move(m));
        }
        std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
            return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
        });
        auto expected = muts;

        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        const uint64_t len = sst->ondisk_data_size();
        auto buf = sst->data_read(0, len, env.make_reader_permit()).get();
        std::vector<uint8_t> img(buf.get(), buf.get() + buf.size());
        auto md = sstables::parquet::format::parse_footer(img);
        // A single row group would make the whole test vacuous.
        BOOST_REQUIRE_GT(md.row_groups.size(), 1u);

        const std::string* lvl = md.kv("scylla.folding_level");
        BOOST_REQUIRE(lvl);
        BOOST_REQUIRE_EQUAL(*lvl, "L1");
        // And the invariant that made this look dangerous: an L2 footer must carry its timestamp,
        // an L1 footer has no business carrying one.
        BOOST_REQUIRE(!md.kv("scylla.uniform_timestamp"));

        // Whatever level it landed on, the data has to come back exactly -- the fallback is a
        // size choice, not a correctness one.
        auto got = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(got.size(), expected.size());
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(expected[i]);
        }
    }).get();
}

// The same schema and data, small enough that no cut happens, does reach L2 -- which is what makes
// the fallback above a difference between the two write paths rather than a property of the data.
SEASTAR_THREAD_TEST_CASE(test_pq_uniform_folding_applies_without_a_cut) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto sb = schema_builder(1, "ks", "pq_l2small")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("v_txt", utf8_type);
        sb.set_parquet_options({{"metadata_folding", "uniform"}});
        auto s = sb.build();
        const auto& vt = *s->get_column_definition(to_bytes("v_txt"));

        constexpr api::timestamp_type TS = 1700000000000000;
        utils::chunked_vector<mutation> muts;
        for (int p = 0; p < 20; ++p) {
            auto pk = partition_key::from_single_value(
                    *s, utf8_type->decompose(sstring(format("key{:06d}", p))));
            mutation m(s, pk);
            for (int r = 0; r < 10; ++r) {
                auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                m.set_clustered_cell(ck, vt, atomic_cell::make_live(
                        *utf8_type, TS, utf8_type->decompose(sstring(format("v{}", r)))));
            }
            muts.push_back(std::move(m));
        }
        std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
            return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
        });
        auto expected = muts;

        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();
        const uint64_t len = sst->ondisk_data_size();
        auto buf = sst->data_read(0, len, env.make_reader_permit()).get();
        std::vector<uint8_t> img(buf.get(), buf.get() + buf.size());
        auto md = sstables::parquet::format::parse_footer(img);
        BOOST_REQUIRE_EQUAL(md.row_groups.size(), 1u);

        const std::string* lvl = md.kv("scylla.folding_level");
        BOOST_REQUIRE(lvl);
        BOOST_REQUIRE_EQUAL(*lvl, "L2");
        const std::string* u = md.kv("scylla.uniform_timestamp");
        BOOST_REQUIRE(u);
        BOOST_REQUIRE_EQUAL(*u, std::to_string(TS));

        auto got = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(got.size(), expected.size());
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(expected[i]);
        }
    }).get();
}

// Row groups are cut when the write buffer exceeds its budget, and the result still
// round-trips.
//
// Until this existed the writer emitted exactly one row group per sstable, so
// `fragment_shredder` buffered every row before encoding anything -- about 1.8 kB per row,
// which is 17 GiB at ten million rows (R-13, design doc 5.5a). It also meant the reader's
// multi-row-group path, which has existed all along, was never once exercised through the
// sstable layer. Turning on code that has never run is exactly how the delta-encoding
// bug got in, so this test drives the real thing rather than a unit of it.
//
// It uses the *shipping* budget rather than a test-only override, so the row count has to
// be large enough to trip it: 64 MiB at ~1.9 kB/row is about 35 600 rows.
SEASTAR_THREAD_TEST_CASE(test_pq_row_groups_are_cut_by_the_memory_budget) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        // Eight value columns rather than two: the budget is on *buffered bytes*, and a
        // wider row reaches it with far fewer rows, which keeps the test's runtime down.
        // A row here costs roughly 1.7 kB buffered (a std::map entry plus a cell per
        // column), so ~40 000 rows is one cut.
        auto sb = schema_builder(1, "ks", "pq_rg")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("v_txt", utf8_type);
        for (int i = 0; i < 7; ++i) {
            sb.with_column(to_bytes(format("v{}", i)), int32_type);
        }
        auto s = sb.build();
        const auto& vt = *s->get_column_definition(to_bytes("v_txt"));

        constexpr int PARTS = 2500, ROWS = 24;      // 60 000 rows -> at least two cuts
        utils::chunked_vector<mutation> muts;
        muts.reserve(PARTS);
        for (int p = 0; p < PARTS; ++p) {
            auto pk = partition_key::from_single_value(
                    *s, utf8_type->decompose(sstring(format("key{:06d}", p))));
            mutation m(s, pk);
            const api::timestamp_type ts = 1000 + p;
            for (int r = 0; r < ROWS; ++r) {
                auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                m.set_clustered_cell(ck, vt, atomic_cell::make_live(
                        *utf8_type, ts, utf8_type->decompose(sstring(format("v{}", r % 40)))));
                for (int i = 0; i < 7; ++i) {
                    m.set_clustered_cell(ck, *s->get_column_definition(to_bytes(format("v{}", i))),
                            atomic_cell::make_live(*int32_type, ts,
                                                   int32_type->decompose(r * 3 + i)));
                }
            }
            muts.push_back(std::move(m));
        }
        std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
            return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
        });
        auto expected = muts;

        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        // The point of the test: more than one row group actually happened. Without this
        // the round-trip below would pass on a single-row-group file and prove nothing.
        const uint64_t len = sst->ondisk_data_size();
        auto buf = sst->data_read(0, len, env.make_reader_permit()).get();
        std::vector<uint8_t> img(buf.get(), buf.get() + buf.size());
        auto md = sstables::parquet::format::parse_footer(img);
        BOOST_TEST_MESSAGE(fmt::format("row groups: {}, rows: {}",
                                       md.row_groups.size(), md.num_rows));
        BOOST_REQUIRE_GT(md.row_groups.size(), 1u);
        BOOST_REQUIRE_EQUAL(md.num_rows, int64_t(PARTS) * ROWS);

        // Row-group row counts must sum to the total, or the reader's cumulative
        // ordinal table would point at the wrong group.
        int64_t sum = 0;
        for (const auto& g : md.row_groups) {
            BOOST_REQUIRE_GT(g.num_rows, 0);
            sum += g.num_rows;
        }
        BOOST_REQUIRE_EQUAL(sum, md.num_rows);

        auto got = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(got.size(), expected.size());
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(expected[i]);
        }

        // And a single-partition read, which is the path that turns an index row ordinal
        // into a page and is where a row group boundary is most likely to be mishandled.
        for (int p : {0, PARTS / 2, PARTS - 1}) {
            auto& want = expected[size_t(p)];
            auto pr = dht::partition_range::make_singular(want.decorated_key());
            auto rd = sst->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
            auto close = deferred_close(rd);
            auto m = read_mutation_from_mutation_reader(rd).get();
            BOOST_REQUIRE(m);
            assert_that(*m).is_equal_to(want);
        }
    }).get();
}

// Counter cells, which are atomic but not scalar: their value is a set of
// per-replica shards, and merging two counter cells means merging shards by id
// rather than taking the newer value. Stored as an opaque blob they would still
// read back byte-identical from a single sstable while being wrong the moment
// anything merged them, so this checks the shards individually as well as
// comparing whole mutations against the reference format.
SEASTAR_THREAD_TEST_CASE(test_pq_counters_round_trip) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = schema_builder(1, "ks", "pq_counters")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("c", counter_type)
            .with_column("c2", counter_type)
            .with_column("sc", counter_type, column_kind::static_column)
            .build();

        const auto& cdef   = *s->get_column_definition(to_bytes("c"));
        const auto& c2def  = *s->get_column_definition(to_bytes("c2"));
        const auto& scdef  = *s->get_column_definition(to_bytes("sc"));
        BOOST_REQUIRE(cdef.is_counter());

        // Deterministic shard ids, so the fixture is reproducible.
        auto shard_id = [] (int n) {
            return counter_id(utils::UUID(0x1000000000000000LL + n, 0x2000000000000000LL + n * 7));
        };
        auto make_counter = [&] (api::timestamp_type ts, int nshards, int salt) {
            counter_cell_builder b{size_t(nshards)};
            for (int i = 0; i < nshards; ++i) {
                b.add_maybe_unsorted_shard(counter_shard(
                        shard_id(i), int64_t(salt) * 1000 + i, int64_t(i) + 1));
            }
            b.sort_and_remove_duplicates();
            return b.build(ts);
        };

        utils::chunked_vector<mutation> muts;
        for (int p = 0; p < 14; ++p) {
            auto pk = partition_key::from_single_value(
                    *s, utf8_type->decompose(sstring(format("key{:04d}", p))));
            mutation m(s, pk);
            const api::timestamp_type ts = 9000 + p;
            if (p % 5 != 4) {
                m.set_static_cell(scdef, make_counter(ts, 1 + p % 3, p + 50));
            }
            for (int r = 0; r < 4; ++r) {
                auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                const int kind = (p + r) % 4;
                if (kind == 0) {
                    m.set_clustered_cell(ck, cdef, make_counter(ts, 1, p));
                } else if (kind == 1) {
                    // Several shards: the case an opaque blob would fail to merge.
                    m.set_clustered_cell(ck, cdef, make_counter(ts, 5, p));
                } else if (kind == 2) {
                    // A deleted counter cell, which has no shards at all.
                    m.set_clustered_cell(ck, cdef, atomic_cell::make_dead(
                            ts, gc_clock::time_point(gc_clock::duration(p + 3))));
                } else {
                    m.set_clustered_cell(ck, cdef, make_counter(ts, 2, p));
                    m.set_clustered_cell(ck, c2def, make_counter(ts, 3, p + 20));
                }
            }
            muts.push_back(std::move(m));
        }
        std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
            return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
        });

        auto ref = make_sstable_containing(
                env.make_sstable(s, sstables::get_highest_sstable_version()), muts).get();
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        auto want = read_all(ref, s, env.make_reader_permit());
        auto got  = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(got.size(), want.size());
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(want[i]);
        }

        // Shard-level check, so this cannot pass on blobs that merely compare
        // equal: every live counter cell must come back with its shard ids,
        // values and logical clocks intact, and multi-shard cells must exist.
        size_t live_cells = 0, multi_shard_cells = 0;
        for (size_t i = 0; i < got.size(); ++i) {
            for (const rows_entry& re : got[i].partition().clustered_rows()) {
                const auto* cell = re.row().cells().find_cell(cdef.id);
                if (!cell) { continue; }
                auto av = cell->as_atomic_cell(cdef);
                if (!av.is_live()) { continue; }
                ++live_cells;
                counter_cell_view ccv(av);
                size_t n = 0;
                for (auto&& cs : ccv.shards()) {
                    BOOST_REQUIRE_EQUAL(cs.id(), shard_id(int(n)));
                    BOOST_REQUIRE_EQUAL(cs.logical_clock(), int64_t(n) + 1);
                    ++n;
                }
                BOOST_REQUIRE_GT(n, 0u);
                if (n > 1) { ++multi_shard_cells; }
            }
        }
        BOOST_REQUIRE_GT(live_cells, 0u);
        BOOST_REQUIRE_GT(multi_shard_cells, 0u);
    }).get();
}

// Static content when the partition's *first* row is not a clustering row.
//
// The writer replays static cells onto every row of the partition and the reader
// rebuilds the static row from whichever row it sees first. That makes the first
// row's identity load-bearing, and two shapes make it something other than a
// clustering row: a range tombstone change that opens before all rows, and the
// placeholder emitted for a partition that has no rows at all. Both used to
// replay only the atomic static cells, so every static collection was silently
// dropped -- and only in those shapes, which is why the ordinary collections
// round-trip test never saw it.
//
// The partition-wide range tombstone matters for a second reason: its bounds are
// before/after_all_clustered_rows, whose clustering prefix is *empty but present*.
// Rebuilding those as an absent prefix yields a position that compares as
// nonsense rather than failing, which sent the walker past every range and
// dropped the partition's rows wholesale.
SEASTAR_THREAD_TEST_CASE(test_pq_statics_survive_a_leading_range_tombstone) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto map_si = map_type_impl::get_instance(utf8_type, int32_type, true);
        auto s = schema_builder(1, "ks", "pq_static_rtc")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("v", int32_type)
            .with_column("sa", int32_type, column_kind::static_column)
            .with_column("sm", map_si, column_kind::static_column)
            .with_column("sm2", map_si, column_kind::static_column)
            .build();

        const auto& sadef  = *s->get_column_definition(to_bytes("sa"));
        const auto& smdef  = *s->get_column_definition(to_bytes("sm"));
        const auto& sm2def = *s->get_column_definition(to_bytes("sm2"));

        auto make_map = [&] (api::timestamp_type ts, int n, int salt) {
            collection_mutation_writer w{tombstone{}};
            for (int i = 0; i < n; ++i) {
                auto k = utf8_type->decompose(sstring(format("k{}_{}", salt, i)));
                auto kb = managed_bytes(reinterpret_cast<const int8_t*>(k.data()), k.size());
                w.push_back(managed_bytes_view(kb), atomic_cell::make_live(
                        *int32_type, ts, int32_type->decompose(i * 10 + salt)));
            }
            return atomic_cell_or_collection(std::move(w).finish());
        };

        utils::chunked_vector<mutation> muts;
        for (int p = 0; p < 16; ++p) {
            auto pk = partition_key::from_single_value(
                    *s, utf8_type->decompose(sstring(format("key{:04d}", p))));
            mutation m(s, pk);
            const api::timestamp_type ts = 7000 + p;

            // Every partition carries an atomic static cell and two static
            // collections. The atomic one is the control: it survived the bug, so
            // if only it comes back the collections were dropped.
            m.set_static_cell(sadef, atomic_cell::make_live(
                    *int32_type, ts, int32_type->decompose(p)));
            m.set_static_cell(smdef, make_map(ts, 3, p));
            if (p % 4 != 3) {
                m.set_static_cell(sm2def, make_map(ts, 2, p + 100));
            }

            const int shape = p % 4;
            if (shape != 2) {                       // shape 2: no rows at all
                for (int r = 0; r < 6; ++r) {
                    auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                    m.set_clustered_cell(ck, *s->get_column_definition(to_bytes("v")),
                            atomic_cell::make_live(*int32_type, ts, int32_type->decompose(r)));
                }
            }

            if (shape == 0) {
                // Covers the whole partition: both bounds are an empty prefix.
                m.partition().apply_delete(*s, range_tombstone(
                        bound_view::bottom(), bound_view::top(),
                        tombstone(ts - 1, gc_clock::time_point(gc_clock::duration(p + 1)))));
            } else if (shape == 1) {
                // Opens before every row and closes in the middle, so the first
                // fragment after the static row is still a range tombstone change
                // but the partition keeps some live rows.
                auto hi = clustering_key_prefix::from_single_value(*s, int32_type->decompose(3));
                m.partition().apply_delete(*s, range_tombstone(
                        bound_view::bottom(),
                        bound_view(hi, bound_kind::incl_end),
                        tombstone(ts - 1, gc_clock::time_point(gc_clock::duration(p + 1)))));
            }
            // shape 3: ordinary partition, and sm2 absent -- the control case.
            muts.push_back(std::move(m));
        }
        std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
            return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
        });

        auto ref = make_sstable_containing(
                env.make_sstable(s, sstables::get_highest_sstable_version()), muts).get();
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        auto want = read_all(ref, s, env.make_reader_permit());
        auto got  = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(got.size(), want.size());
        // Assert the static collections are actually there, so this cannot pass by
        // both sides being equally empty.
        size_t with_static_collection = 0;
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(want[i]);
            const auto& sr = got[i].partition().static_row().get();
            if (sr.find_cell(smdef.id)) { ++with_static_collection; }
        }
        BOOST_REQUIRE_EQUAL(with_static_collection, got.size());

        auto fw = fragments_of(ref, s, env.make_reader_permit());
        auto fg = fragments_of(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(fg.size(), fw.size());
        for (size_t i = 0; i < fg.size(); ++i) {
            BOOST_REQUIRE_EQUAL(fg[i], fw[i]);
        }
    }).get();
}

// A local stand-in for the conformance corpus's schema: two bytes clustering
// columns, and every column doubled as regular and static with a mix of scalars
// and multi-cell lists. That combination is what test_reader_conversions builds,
// and it is worth having locally because iterating on the conformance suite means
// a three-minute build for every guess.
SEASTAR_THREAD_TEST_CASE(test_pq_corpus_shaped_schema) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto list_b = list_type_impl::get_instance(bytes_type, true);
        auto sb = schema_builder(1, "ks", "pq_corpus")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("ck1", bytes_type, column_kind::clustering_key)
            .with_column("ck2", bytes_type, column_kind::clustering_key);
        // Match the corpus more closely: it is 64 + 64 columns with 33 rows and
        // range tombstones, and it was the range tombstones that this test was
        // missing.
        for (int i = 0; i < 64; ++i) {
            sb.with_column(to_bytes(format("v{}", i)),
                           i % 2 ? data_type(list_b) : bytes_type);
            sb.with_column(to_bytes(format("s{}", i)),
                           i % 2 ? data_type(list_b) : bytes_type,
                           column_kind::static_column);
        }
        auto s = sb.build();

        auto blob = [] (int n) { return bytes(bytes::initialized_later(), size_t(2 + n % 3)); };
        auto make_list = [&] (api::timestamp_type ts, int n) {
            collection_mutation_writer w{tombstone{}};
            for (int i = 0; i < n; ++i) {
                auto k = timeuuid_type->decompose(
                        utils::UUID_gen::get_time_UUID(std::chrono::system_clock::now()));
                auto kb = managed_bytes(reinterpret_cast<const int8_t*>(k.data()), k.size());
                w.push_back(managed_bytes_view(kb),
                            atomic_cell::make_live(*bytes_type, ts + i, bytes_view(blob(i))));
            }
            return atomic_cell_or_collection(std::move(w).finish());
        };

        utils::chunked_vector<mutation> muts;
        for (int p = 0; p < 6; ++p) {
            auto pk = partition_key::from_single_value(*s, blob(p));
            mutation m(s, pk);
            // The corpus uses timestamps near the extremes of int64 -- values like
            // -9223372036854775737 appear in its output. The folding scheme stores
            // per-cell and row-marker timestamps as *deltas* against the row's
            // timestamp, and those subtractions overflow for spans that wide.
            static const api::timestamp_type extremes[] = {
                std::numeric_limits<api::timestamp_type>::min() + 70,
                std::numeric_limits<api::timestamp_type>::max() - 70,
                -9223372036854775737LL,
                7000,
            };
            const api::timestamp_type ts = extremes[p % 4] + p;
            for (int i = 0; i < 64; ++i) {
                const auto& cdef = *s->get_column_definition(to_bytes(format("s{}", i)));
                if (i % 2) { m.set_static_cell(cdef, make_list(ts, 2)); }
                else       { m.set_static_cell(cdef, atomic_cell::make_live(
                                     *bytes_type, ts, bytes_view(blob(i)))); }
            }
            for (int r = 0; r < 33; ++r) {
                auto ck = clustering_key::from_exploded(*s, {blob(r), blob(r + 1)});
                for (int i = 0; i < 64; ++i) {
                    const auto& cdef = *s->get_column_definition(to_bytes(format("v{}", i)));
                    if (i % 2) { m.set_clustered_cell(ck, cdef, make_list(ts, 1 + r % 2)); }
                    else       { m.set_clustered_cell(ck, cdef, atomic_cell::make_live(
                                        *bytes_type, ts, bytes_view(blob(r + i)))); }
                }
            }
            // Two range tombstones per partition, which is what the corpus has and
            // what this test was missing.
            for (int t = 0; t < 2; ++t) {
                auto lo = clustering_key_prefix::from_exploded(*s, {blob(1 + t * 3)});
                auto hi = clustering_key_prefix::from_exploded(*s, {blob(3 + t * 3)});
                m.partition().apply_delete(*s, range_tombstone(
                        std::move(lo), t ? bound_kind::incl_start : bound_kind::excl_start,
                        std::move(hi), t ? bound_kind::excl_end : bound_kind::incl_end,
                        tombstone(ts + 1 + t,
                                  gc_clock::time_point(gc_clock::duration(p + t + 1)))));
            }
            muts.push_back(std::move(m));
        }
        std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
            return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
        });

        auto ref = make_sstable_containing(
                env.make_sstable(s, sstables::get_highest_sstable_version()), muts).get();
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        auto want = read_all(ref, s, env.make_reader_permit());
        auto got  = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(got.size(), want.size());
        for (size_t i = 0; i < got.size(); ++i) {
            // Row count first: an empty `rows: []` against a populated one is the
            // failure mode the conformance suite showed, and comparing whole
            // mutations buries it in 400 lines of diff.
            BOOST_REQUIRE_EQUAL(got[i].partition().clustered_rows().calculate_size(),
                                want[i].partition().clustered_rows().calculate_size());
            assert_that(got[i]).is_equal_to(want[i]);
        }

        // And through the v1 fragment stream, which is what
        // test_reader_conversions exercises: the v2->v1 conversion reassembles
        // range tombstones and is the one reader path nothing else here covers.
        auto read_v1 = [&] (shared_sstable t) {
            mutation_fragment_v1_stream st(
                    t->make_reader(s, env.make_reader_permit(),
                                   query::full_partition_range, s->full_slice()));
            auto close = deferred_close(st);
            utils::chunked_vector<mutation> out;
            while (auto m = read_mutation_from_mutation_reader(st).get()) {
                out.push_back(std::move(*m));
            }
            return out;
        };
        auto want_v1 = read_v1(ref);
        auto got_v1  = read_v1(sst);
        BOOST_REQUIRE_EQUAL(got_v1.size(), want_v1.size());
        for (size_t i = 0; i < got_v1.size(); ++i) {
            BOOST_REQUIRE_EQUAL(got_v1[i].partition().clustered_rows().calculate_size(),
                                want_v1[i].partition().clustered_rows().calculate_size());
            assert_that(got_v1[i]).is_equal_to(want_v1[i]);
        }
    }).get();
}

// C6 of the tiering decision: the gain must be *measured* with the real writer over
// real data, and it must fail closed. Both halves are asserted here, because the
// failure mode that matters is a bad estimate silently converting a table.
SEASTAR_THREAD_TEST_CASE(test_c6_parquet_gain_is_measured_over_real_data) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        // A native sstable, which is what the estimator sees in a hybrid table:
        // the question it answers is "what would this become as Parquet".
        auto sst = make_sstable_containing(env.make_sstable(s), make_muts(s, 40, 250)).get();

        auto gain = sstables::parquet::estimate_parquet_gain(
                s, env.make_reader_permit(), {sst}, sstables::parquet::pq_writer_config{}).get();
        BOOST_REQUIRE(gain.has_value());
        BOOST_TEST_MESSAGE(seastar::format("measured gain: {:.3f} on {} on-disk bytes",
                                           *gain, sst->ondisk_data_size()));
        // A ratio, so bounded; and repetitive test data should not come out larger.
        BOOST_REQUIRE_GT(*gain, 0.0);
        BOOST_REQUIRE_LT(*gain, 1.0);

        // Deterministic: the same sample must yield the same answer, or the tiering
        // decision would flap between compactions.
        auto again = sstables::parquet::estimate_parquet_gain(
                s, env.make_reader_permit(), {sst}, sstables::parquet::pq_writer_config{}).get();
        BOOST_REQUIRE(again.has_value());
        BOOST_REQUIRE_EQUAL(*gain, *again);

        // Nothing to measure must read as "unknown", never as a gain. The policy
        // turns an unset gain into a rejection, so this is what keeps an
        // unmeasurable table in the native format.
        auto none = sstables::parquet::estimate_parquet_gain(
                s, env.make_reader_permit(), {}, sstables::parquet::pq_writer_config{}).get();
        BOOST_REQUIRE(!none.has_value());
    }).get();
}

// Every codec the `compression` sub-option accepts, round-tripped, plus the two things that make
// a codec more than a speed/size dial: the footer has to *declare* it per column chunk (that is
// what lets another implementation open the file) and the setting has to survive DESCRIBE.
//
// lz4 exists because the comparison that matters is against the row format, and the row format's
// default is LZ4WithDictsCompressor while pq's is zstd -- so a CPU comparison between them that
// does not vary the codec is measuring the codec as much as the format. Quantifying that was the
// point (design doc 10.28): on the perf harness zstd is 70 % of a point read at the shipping page
// geometry and lz4 recovers a third of the read, but it costs +76 % on disk here, and once the
// page geometry is fixed the codec is 28 % of the read and the swap stops paying. So lz4 is an
// option, and zstd stays the default -- but an option nobody can read back is not an option.
//
// On the wire it is LZ4_RAW (codec 7), the bare block every current Parquet implementation writes,
// not the deprecated Hadoop-framed codec 5.
SEASTAR_THREAD_TEST_CASE(test_pq_compression_codecs_round_trip) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        struct expect { const char* opt; sstables::parquet::format::codec want; };
        const expect cases[] = {
            {"zstd", sstables::parquet::format::codec::zstd},
            {"lz4",  sstables::parquet::format::codec::lz4_raw},
            {"none", sstables::parquet::format::codec::uncompressed},
        };
        // The reference arm carries no `parquet` option at all, so it also pins that the default
        // is still zstd rather than whatever the loop happens to leave behind.
        auto ref_schema = pq_schema();
        auto ref = read_all(make_sstable_containing(
                        env.make_sstable(ref_schema, sstable_version_types::pq),
                        make_muts(ref_schema, 8, 60)).get(),
                   ref_schema, env.make_reader_permit());

        for (const auto& c : cases) {
            BOOST_TEST_CONTEXT("compression=" << c.opt) {
                auto sb = schema_builder(1, "ks", "pq_tbl");
                sb.with_column("pk", utf8_type, column_kind::partition_key)
                  .with_column("ck", int32_type, column_kind::clustering_key)
                  .with_column("v_int", int32_type)
                  .with_column("v_big", long_type)
                  .with_column("v_dbl", double_type)
                  .with_column("v_txt", utf8_type);
                sb.set_parquet_options({{"compression", sstring(c.opt)}});
                auto s = sb.build();
                auto sst = make_sstable_containing(
                        env.make_sstable(s, sstable_version_types::pq),
                        make_muts(s, 8, 60)).get();

                // Same rows back, whichever codec carried them.
                auto got = read_all(sst, s, env.make_reader_permit());
                BOOST_REQUIRE_EQUAL(got.size(), ref.size());
                for (size_t i = 0; i < got.size(); ++i) {
                    assert_that(got[i]).is_equal_to(ref[i]);
                }

                // And a point read, which is a different code path in the reader: it decodes one
                // page per leaf rather than a whole row group, and a codec that only worked on
                // the scan path would pass everything above.
                {
                    auto pr = dht::partition_range::make_singular(ref[3].decorated_key());
                    auto rd = sst->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
                    auto close = deferred_close(rd);
                    auto m = read_mutation_from_mutation_reader(rd).get();
                    BOOST_REQUIRE(m);
                    assert_that(*m).is_equal_to(ref[3]);
                }

                // The footer must name the codec, per chunk, or no other reader can open this.
                const uint64_t len = sst->ondisk_data_size();
                auto buf = sst->data_read(0, len, env.make_reader_permit()).get();
                auto img = std::span<const uint8_t>(
                        reinterpret_cast<const uint8_t*>(buf.get()), buf.size());
                auto md = sstables::parquet::format::parse_footer(img);
                BOOST_REQUIRE(!md.row_groups.empty());
                size_t chunks = 0;
                for (const auto& rg : md.row_groups) {
                    for (const auto& ch : rg.columns) {
                        BOOST_REQUIRE(ch.meta);
                        BOOST_REQUIRE(ch.meta->compression == c.want);
                        ++chunks;
                    }
                }
                BOOST_REQUIRE_GT(chunks, 0u);

                // DESCRIBE: the option has to come back out under the name it went in as.
                // `none` is the interesting one in reverse -- it is not the default, so it must
                // be emitted -- and `zstd` is, so a writer that emitted nothing for it would be
                // right for the wrong reason. Assert the value either way.
                sstables::parquet::parquet_parameters params(s->parquet_options());
                auto back = params.to_map();
                auto it = back.find("compression");
                if (c.want == sstables::parquet::format::codec::zstd) {
                    // The default: absent is correct, present must say zstd.
                    if (it != back.end()) { BOOST_REQUIRE_EQUAL(it->second, sstring("zstd")); }
                } else {
                    BOOST_REQUIRE(it != back.end());
                    BOOST_REQUIRE_EQUAL(it->second, sstring(c.opt));
                }
            }
        }
    }).get();
}

// A Parquet sstable has no CompressionInfo component -- it compresses inside the file -- so
// sstable::get_compression_ratio() reported NO_COMPRESSION_RATIO (-1.0) for every Parquet table,
// i.e. nodetool and the REST API showed no ratio for a table that has a perfectly good one. The
// writer now records it in the statistics and the accessor falls back to that.
//
// Lives here rather than in cql_ddl_test because it is a property of the writer, and building the
// sstable directly avoids depending on whether a test-env major compaction chooses to rewrite.
SEASTAR_THREAD_TEST_CASE(test_pq_records_a_compression_ratio) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), make_muts(s, 30, 200)).get();

        const auto ratio = sst->get_compression_ratio();
        BOOST_TEST_MESSAGE(seastar::format("compression ratio: {}", ratio));
        // -1.0 is the "not recorded" sentinel, so a positive value is exactly the regression
        // being pinned.
        BOOST_REQUIRE_GT(ratio, 0.0);
        // And it must be a ratio, not a byte count: the test data repeats, so a real codec has
        // to come in under 1.0.
        BOOST_REQUIRE_LT(ratio, 1.0);

        // Cross-check against the file itself, so this cannot pass on a plausible-looking number
        // that bears no relation to the sstable: the numerator is the data component's size.
        const auto on_disk = double(sst->ondisk_data_size());
        BOOST_REQUIRE_GT(on_disk, 0.0);
        BOOST_REQUIRE_GT(on_disk / ratio, on_disk);   // implied uncompressed size is larger
    }).get();
}

namespace {
// Records nothing (the system-table write is not what is under test) but keeps the
// real threshold logic and the above-threshold counters of the base class.
class ld_threshold_handler : public db::large_data_handler {
public:
    ld_threshold_handler(uint64_t partition_bytes, uint64_t row_bytes, uint64_t cell_bytes,
                         uint64_t rows_count, uint64_t collection_elements)
        : large_data_handler(partition_bytes, row_bytes, cell_bytes, rows_count,
                             collection_elements) {
        start();
    }
protected:
    future<> record_large_cells(const sstables::sstable&, const sstables::key&,
            const clustering_key_prefix*, const column_definition&, uint64_t, uint64_t) const override {
        return make_ready_future<>();
    }
    future<> record_large_rows(const sstables::sstable&, const sstables::key&,
            const clustering_key_prefix*, uint64_t) const override {
        return make_ready_future<>();
    }
    future<> record_large_partitions(const sstables::sstable&, const sstables::key&,
            uint64_t, uint64_t, uint64_t, uint64_t) const override {
        return make_ready_future<>();
    }
    future<> delete_large_data_entries(const schema&, sstring, std::string_view) const override {
        return make_ready_future<>();
    }
    future<> update_large_data_entries_sstable_name(const schema&, sstring, sstring,
            std::string_view) const override {
        return make_ready_future<>();
    }
};
}

// Large-data metadata (GA blocker B1). The pq writer used to pass std::nullopt for all three
// large-data arguments of write_scylla_metadata(), so system.large_partitions / large_rows /
// large_cells were silently empty for every pq table.
//
// Two things are pinned here that the tests in sstable_3_x_test.cc do not reach.
//
// First, both write paths. An sstable is written either by cut_row_group() once it outgrows the
// row-group budget or by write_rows() in one shot when it fits; the choice is a function of data
// size and invisible to the operator, and that divergence has produced separate bugs here before
// (design doc 8.2b, 10.15). The accounting deliberately lives in the fragment consumers, upstream
// of both encoders, so identical data must yield *byte-identical* records on the two paths -- which
// is what this asserts, rather than merely asserting each path produces something.
//
// Second, ext_timestamp_stats. Nothing else in the suite reads it back for pq, and its absence
// degrades safely (compaction/compaction.cc falls back to min_timestamp, so purging stays
// conservative) -- which is exactly why a silent regression there would go unnoticed.
SEASTAR_THREAD_TEST_CASE(test_pq_large_data_metadata_on_both_write_paths) {
    // Row threshold low enough that the padded rows trip it, cell threshold likewise;
    // partition threshold low enough that every partition is recorded.
    ld_threshold_handler handler(1024, 512, 256,
            std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max());

    sstables::test_env::do_with_async([&] (sstables::test_env& env) {
        auto build = [] (const char* name, std::optional<int> rows_per_rg) {
            auto sb = schema_builder(1, "ks", name)
                .with_column("pk", utf8_type, column_kind::partition_key)
                .with_column("ck", int32_type, column_kind::clustering_key)
                .with_column("v_txt", utf8_type);
            if (rows_per_rg) {
                sb.set_parquet_options({{"rows_per_row_group", format("{}", *rows_per_rg)}});
            }
            return sb.build();
        };
        // Same shape and same data; only the row-group budget differs, and that is what selects
        // the write path. 1 000 is the minimum the option accepts.
        auto s_cut   = build("pq_ld_cut", 1000);
        auto s_whole = build("pq_ld_whole", std::nullopt);

        // 3 000 rows: three row groups under a 1 000-row budget, one under the 5 000 default.
        constexpr int PARTS = 150, ROWS = 20;
        // Fixed timestamps, so min_live_timestamp is a value this test can name.
        constexpr api::timestamp_type TS_BASE = 1700000000000000;
        const auto expected_min_live_ts = TS_BASE;

        auto make_muts = [&] (const schema_ptr& s) {
            const auto& vt = *s->get_column_definition(to_bytes("v_txt"));
            utils::chunked_vector<mutation> muts;
            muts.reserve(PARTS);
            for (int p = 0; p < PARTS; ++p) {
                auto pk = partition_key::from_single_value(
                        *s, utf8_type->decompose(sstring(format("key{:06d}", p))));
                mutation m(s, pk);
                for (int r = 0; r < ROWS; ++r) {
                    auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                    // Row 0 of every partition carries a large value, the rest are small, so the
                    // top-N heaps have something to rank and the row/cell thresholds are crossed
                    // by a knowable subset.
                    sstring val = r == 0 ? sstring(600 + p, 'x') : sstring(format("v{}", r));
                    m.set_clustered_cell(ck, vt, atomic_cell::make_live(
                            *utf8_type, TS_BASE + p * 100 + r, utf8_type->decompose(val)));
                }
                muts.push_back(std::move(m));
            }
            std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
                return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
            });
            return muts;
        };

        auto sst_cut = make_sstable_containing(
                env.make_sstable(s_cut, sstable_version_types::pq), make_muts(s_cut)).get();
        auto sst_whole = make_sstable_containing(
                env.make_sstable(s_whole, sstable_version_types::pq), make_muts(s_whole)).get();

        // Confirm the two really did take different paths. Without this the comparison below
        // could be two runs of the same encoder and would prove nothing.
        auto row_groups = [&] (const shared_sstable& sst) {
            const uint64_t len = sst->ondisk_data_size();
            auto buf = sst->data_read(0, len, env.make_reader_permit()).get();
            std::vector<uint8_t> img(buf.get(), buf.get() + buf.size());
            return sstables::parquet::format::parse_footer(img).row_groups.size();
        };
        BOOST_REQUIRE_GT(row_groups(sst_cut), 1u);
        BOOST_REQUIRE_EQUAL(row_groups(sst_whole), 1u);

        for (const auto& [what, sst] : {std::pair<const char*, shared_sstable>{"cut", sst_cut},
                                        std::pair<const char*, shared_sstable>{"whole", sst_whole}}) {
            BOOST_TEST_CONTEXT(what) {
                // Already opened by make_sstable_containing(), so the metadata below is loaded.
                // large_data_records: what backs the three virtual tables.
                auto& records_opt = sst->get_large_data_records();
                BOOST_REQUIRE(records_opt.has_value());
                std::map<sstables::large_data_type, unsigned> by_type;
                for (const auto& rec : records_opt->elements) {
                    ++by_type[rec.type];
                    BOOST_REQUIRE_GT(rec.value, 0u);
                    BOOST_REQUIRE(!rec.partition_key.value.empty());
                }
                BOOST_REQUIRE_GT(by_type[sstables::large_data_type::partition_size], 0u);
                BOOST_REQUIRE_GT(by_type[sstables::large_data_type::row_size], 0u);
                BOOST_REQUIRE_GT(by_type[sstables::large_data_type::cell_size], 0u);

                // large_data_stats: the aggregate, and the legacy fallback the virtual tables
                // use for an sstable without records.
                for (auto t : {sstables::large_data_type::partition_size,
                               sstables::large_data_type::row_size,
                               sstables::large_data_type::cell_size}) {
                    auto stat = sst->get_large_data_stat(t);
                    BOOST_REQUIRE(stat.has_value());
                    BOOST_REQUIRE_GT(stat->max_value, 0u);
                    BOOST_REQUIRE_GT(stat->above_threshold, 0u);
                }

                // ext_timestamp_stats: read by compaction to bound what a tombstone may purge.
                auto ts_stats = sst->get_ext_timestamp_stats();
                auto it = ts_stats.find(sstables::ext_timestamp_stats_type::min_live_timestamp);
                BOOST_REQUIRE(it != ts_stats.end());
                BOOST_REQUIRE_EQUAL(it->second, expected_min_live_ts);
            }
        }

        // The point of doing both: identical input must produce identical accounting, because the
        // accounting happens before either encoder sees the rows.
        auto canonical = [] (const shared_sstable& sst) {
            std::vector<std::tuple<uint32_t, bytes, bytes, bytes, uint64_t, uint64_t>> v;
            for (const auto& rec : sst->get_large_data_records()->elements) {
                v.emplace_back(static_cast<uint32_t>(rec.type), rec.partition_key.value,
                               rec.clustering_key.value, rec.column_name.value,
                               rec.value, rec.elements_count);
            }
            std::sort(v.begin(), v.end());
            return v;
        };
        BOOST_REQUIRE(canonical(sst_cut) == canonical(sst_whole));

        for (auto t : {sstables::large_data_type::partition_size,
                       sstables::large_data_type::row_size,
                       sstables::large_data_type::cell_size}) {
            BOOST_REQUIRE_EQUAL(sst_cut->get_large_data_stat(t)->max_value,
                                sst_whole->get_large_data_stat(t)->max_value);
            BOOST_REQUIRE_EQUAL(sst_cut->get_large_data_stat(t)->above_threshold,
                                sst_whole->get_large_data_stat(t)->above_threshold);
        }
    }, { &handler }).get();
}

// A counter column's map values are two big-endian int64s, not an opaque blob, and the Parquet
// schema cannot say so without a group inside the MAP value -- a third level of Dremel nesting,
// which is a schema change and not yet done. Until then the footer declares the convention, so a
// reader is not required to know it in advance. This pins that declaration.
SEASTAR_THREAD_TEST_CASE(test_pq_declares_the_counter_convention) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        // Counter and non-counter columns cannot coexist in one table -- Scylla rejects it -- so
        // the positive and negative cases need separate schemas.
        auto ctr = schema_builder(1, "ks", "ctr")
                .with_column("pk", utf8_type, column_kind::partition_key)
                .with_column("ck", int32_type, column_kind::clustering_key)
                .with_column("hits", counter_type)
                .build();

        const auto cols = sstables::parquet::columns_of(*ctr);
        auto hits = std::ranges::find_if(cols, [] (const auto& c) { return c.name == "hits"; });
        BOOST_REQUIRE(hits != cols.end());
        // The flag the declaration depends on. A counter is multi_cell like a collection, and
        // before this nothing downstream could tell the two apart.
        BOOST_REQUIRE(hits->counter);
        BOOST_REQUIRE(hits->multi_cell);

        // A written file carries the declaration.
        utils::chunked_vector<mutation> muts;
        auto pk = partition_key::from_single_value(*ctr, utf8_type->decompose(sstring("p")));
        mutation m(ctr, pk);
        // A row marker, not an empty partition: make_sstable_containing has nothing to write for
        // a partition with no content. The declaration under test is a property of the schema, so
        // the row does not need a counter cell in it -- the counter round-trip is covered by the
        // conformance cases.
        auto ck = clustering_key::from_single_value(*ctr, int32_type->decompose(1));
        m.partition().apply_insert(*ctr, ck, api::timestamp_type(1000));
        muts.push_back(std::move(m));
        auto sst = make_sstable_containing(
                env.make_sstable(ctr, sstable_version_types::pq), std::move(muts)).get();

        const auto len = sst->ondisk_data_size();
        auto buf = sst->data_read(0, len, env.make_reader_permit()).get();
        auto md = sstables::parquet::format::parse_footer(
                std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(buf.get()), buf.size()));
        auto kv = [&] (const char* k) -> std::optional<std::string> {
            for (const auto& p : md.key_value_metadata) { if (p.key == k) { return p.value; } }
            return std::nullopt;
        };
        auto names = kv("scylla.counter_columns");
        BOOST_REQUIRE(names.has_value());
        BOOST_REQUIRE_EQUAL(*names, "hits");
        auto enc = kv("scylla.counter_encoding");
        BOOST_REQUIRE(enc.has_value());
        BOOST_REQUIRE(enc->find("logical_clock") != std::string::npos);

        // The declaration is the fallback; the schema itself is now typed, which is the part that
        // makes the column interpretable without knowing Scylla. Both `value` and `clock` must be
        // INT64 leaves of the counter's group -- not a packed BYTE_ARRAY.
        auto leaves = sstables::parquet::format::walk_leaves(md);
        int typed = 0;
        for (const auto& l : leaves) {
            const auto& path = l.path;
            if (path.size() < 2 || path.front() != "hits") { continue; }
            const auto& name = path.back();
            if (name == "value" || name == "clock") {
                // leaf_info carries the schema index rather than the type, so read the type from
                // the schema element it points at.
                BOOST_REQUIRE(md.schema.at(l.index).type
                              == sstables::parquet::format::phys_type::int64);
                ++typed;
            }
        }
        BOOST_REQUIRE_EQUAL(typed, 2);

        // And a table with no counters says nothing, rather than emitting an empty key.
        const auto plain_cols = sstables::parquet::columns_of(*pq_schema());
        BOOST_REQUIRE(std::ranges::none_of(plain_cols, [] (const auto& c) { return c.counter; }));
    }).get();
}

// Size-tiered bucketing has to compare like with like across formats.
//
// `data_size()` is two different quantities: for a compressed native sstable it is the data
// component's *uncompressed* length, and for a `pq` sstable -- which has no CompressionInfo,
// because Parquet compresses internally -- it is the file size. Bucketing is built on ratios, so
// inside an all-native set that inconsistency cancels out and is invisible. In a hybrid table it
// does not: the converted file reports several times smaller than the native sstable holding the
// same rows, buckets several tiers below its true peer, and compaction settles into repeatedly
// rewriting the one format that is most expensive to rewrite.
//
// Nothing about that failure is loud -- no error, no log line, just compaction declining to group
// the converted files -- so it needs a test. Both supported strategies are affected: ICS buckets
// this way directly and TWCS falls back to it within a window.
SEASTAR_THREAD_TEST_CASE(test_size_tiered_buckets_compare_one_unit_across_formats) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        // Compression has to be on for the two units to differ at all, and the data has to be
        // genuinely compressible for them to differ by enough to matter.
        //
        // make_muts() will not do for this: its mixed ints, doubles and short strings are
        // incompressible at a 4 KiB chunk length -- measured at 59 440 uncompressed against 67 172
        // on disk, i.e. LZ4 *expanded* it by 13 %. So a payload built to compress instead, which is
        // also the realistic case: the tables Parquet is aimed at are ones whose values repeat.
        auto s = schema_builder(1, "ks", "hybrid_buckets")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("v_txt", utf8_type)
            .set_compressor_params(compression_parameters(compression_parameters::algorithm::lz4))
            .build();

        const sstring filler(400, 'a');
        auto make = [&] (sstable_version_types v, int n_part, int n_rows) {
            utils::chunked_vector<mutation> muts;
            for (int part = 0; part < n_part; ++part) {
                auto pk = partition_key::from_single_value(
                        *s, utf8_type->decompose(sstring(format("key{:05d}", part))));
                mutation m(s, pk);
                for (int r = 0; r < n_rows; ++r) {
                    auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                    auto& cdef = *s->get_column_definition(to_bytes("v_txt"));
                    m.set_clustered_cell(ck, cdef, atomic_cell::make_live(
                            *cdef.type, 1000 + part, utf8_type->decompose(filler)));
                }
                muts.push_back(std::move(m));
            }
            std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
                return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
            });
            return make_sstable_containing(env.make_sstable(s, v), std::move(muts)).get();
        };

        constexpr int n_part = 20;
        constexpr int native_rows = 50;
        std::vector<shared_sstable> native;
        for (int i = 0; i < 3; ++i) {
            native.push_back(make(sstable_version_types::me, n_part, native_rows));
        }
        const auto native_ondisk = native[0]->ondisk_data_size();

        // The point of the fixture is a `pq` sstable that occupies *the same disk space* as the
        // native ones. Two sstables of equal on-disk size belong in one bucket whatever wrote them,
        // so if they split, the only possible cause is the unit -- which is precisely the defect.
        //
        // The row count has to be derived rather than hardcoded, because the two formats are not
        // remotely comparable per row here: LZ4 gets ~39x on this payload and Parquet, which
        // dictionary-encodes a column of one repeated value, gets over 400x. So probe for Parquet's
        // bytes-per-row and scale up to match. Deriving it also means the fixture survives a change
        // in either codec's effectiveness, which a hardcoded multiplier would not.
        auto probe = make(sstable_version_types::pq, n_part, native_rows);
        const double pq_bytes_per_row = double(probe->ondisk_data_size()) / (n_part * native_rows);
        const int pq_rows = std::max(1, int(double(native_ondisk) / pq_bytes_per_row / n_part));
        auto pq = make(sstable_version_types::pq, n_part, pq_rows);

        // Fixture preconditions. Both must hold for the assertions below to mean anything, so they
        // are checked rather than assumed: the units must genuinely diverge on the native side, must
        // coincide on the pq side, and the two files must land within a bucket-width of each other
        // on disk.
        BOOST_REQUIRE_GT(native[0]->data_size(), native_ondisk * 2);
        BOOST_REQUIRE_EQUAL(pq->data_size(), pq->ondisk_data_size());
        const double ondisk_ratio = double(pq->ondisk_data_size()) / double(native_ondisk);
        BOOST_REQUIRE_MESSAGE(ondisk_ratio > 0.6 && ondisk_ratio < 1.4,
                seastar::format("fixture failed to match on-disk sizes: pq {} vs native {} ({:.2f}x)",
                                pq->ondisk_data_size(), native_ondisk, ondisk_ratio));
        // ... and on `data_size()` they must be far enough apart that the old behaviour splits
        // them, or this would pass either way.
        const double data_ratio = double(pq->data_size()) / double(native[0]->data_size());
        BOOST_REQUIRE_MESSAGE(data_ratio < 0.5,
                seastar::format("fixture would not discriminate: data_size ratio {:.3f}", data_ratio));

        compaction::size_tiered_compaction_strategy_options opts;

        // All-native: one bucket, and this path must keep using `data_size()` -- every existing
        // cluster's bucketing depends on it and none of them has a `pq` sstable to trip over.
        auto native_buckets = compaction::size_tiered_compaction_strategy::get_buckets(native, opts);
        BOOST_REQUIRE_EQUAL(native_buckets.size(), 1u);

        // Mixed: equal on disk, so one bucket.
        auto mixed = native;
        mixed.push_back(pq);
        auto mixed_buckets = compaction::size_tiered_compaction_strategy::get_buckets(mixed, opts);
        BOOST_REQUIRE_EQUAL(mixed_buckets.size(), 1u);
        BOOST_REQUIRE_EQUAL(mixed_buckets[0].size(), mixed.size());
    }).get();
}

// Under TWCS, 'hybrid' means the same thing as 'parquet': the whole table.
//
// Hybrid tiering exists to keep Parquet out of the levels that get rewritten, since re-encoding and
// recompressing a Parquet run is the expensive thing this format does. TWCS has no such levels -- a
// window is compacted once and then closed -- so there is nothing for the criteria to protect and no
// reason to leave part of a TWCS table in the row format.
//
// This is asserted on the rule rather than on an end-to-end conversion because the rule has three
// callers -- compaction, memtable flush and streaming -- and the failure it guards against is them
// disagreeing. A table whose flushes and compactions answer differently never converges: one keeps
// adding files in the format the other keeps converting away from.
SEASTAR_THREAD_TEST_CASE(test_twcs_hybrid_is_parquet_for_the_whole_table) {
    auto build = [] (storage_format_type fmt, compaction::compaction_strategy_type cs) {
        auto b = schema_builder(1, "ks", "fmt_tbl")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", timestamp_type, column_kind::clustering_key)
            .with_column("v", int32_type);
        b.set_storage_format(fmt);
        b.set_compaction_strategy(cs);
        return b.build();
    };
    using ct = compaction::compaction_strategy_type;
    const auto unconditional = [] (schema_ptr s) {
        return sstables::parquet::writes_parquet_unconditionally(*s);
    };

    // The change: hybrid + TWCS is now unconditional, where it used to go through C1/C5/C6.
    BOOST_REQUIRE(unconditional(build(storage_format_type::hybrid, ct::time_window)));

    // Hybrid under a size-tiered strategy still decides per compaction -- that is what hybrid is
    // for, and ICS does have levels that get rewritten.
    BOOST_REQUIRE(!unconditional(build(storage_format_type::hybrid, ct::incremental)));

    // The explicit settings are unchanged by all of this, under either strategy.
    BOOST_REQUIRE(unconditional(build(storage_format_type::parquet, ct::time_window)));
    BOOST_REQUIRE(unconditional(build(storage_format_type::parquet, ct::incremental)));
    BOOST_REQUIRE(!unconditional(build(storage_format_type::sstable, ct::time_window)));
    BOOST_REQUIRE(!unconditional(build(storage_format_type::sstable, ct::incremental)));
}

// Reshape and reshard **on load** must write the same format as every other write path.
//
// This was the one write path that did not. `table_populator::process_subdir()` gated on
// `storage_format() == parquet` while flush (`table::make_sstable`), streaming
// (`table::make_streaming_sstable_for_write`) and compaction (`compaction_manager.cc`) all asked
// `writes_parquet_unconditionally()`. A **hybrid + TWCS** table therefore wrote `pq` on every path
// except boot-time reshape/reshard, which wrote native -- and since reshape on boot rewrites files
// that are already there, that path can undo what the other three just converged on.
//
// The defence for the old gate was that reshaping happens on load, "where nothing is known about
// tiering yet". That is true and irrelevant: `writes_parquet_unconditionally()` reads nothing but
// the schema -- `storage_format` and the compaction strategy are both table properties, both known
// at load -- and the two cases it covers are exactly the ones defined to skip the tiering criteria.
// There was never any tiering context to be missing.
//
// Asserted on `version_for_rewrite_on_load()` because that is the only seam there is: the decision
// lives in `table_populator`, a class local to `distributed_loader.cc`, whose reshard/reshape
// creators are the only in-tree callers. `distributed_loader_for_tests::reshard()` takes the
// creator as a parameter, so a test driving it supplies the version itself and would assert its
// own argument back. Extracting the function is what made the choice observable at all.
SEASTAR_THREAD_TEST_CASE(test_reshape_on_load_writes_parquet_for_hybrid_twcs) {
    auto build = [] (storage_format_type fmt, compaction::compaction_strategy_type cs) {
        auto b = schema_builder(1, "ks", "reshape_fmt_tbl")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", timestamp_type, column_kind::clustering_key)
            .with_column("v", int32_type);
        b.set_storage_format(fmt);
        b.set_compaction_strategy(cs);
        return b.build();
    };
    using ct = compaction::compaction_strategy_type;
    using v = sstables::sstable_version_types;

    // What get_safe_sstable_version_for_rewrites() would have returned. Two different values, so
    // that "passed the native choice through" is distinguishable from "happened to return mt".
    for (auto native : {v::me, v::mt}) {
        // Formatted rather than compared as the raw enum: sstable_version_types has no
        // operator<<, so BOOST_REQUIRE_EQUAL on it does not compile, and a failure that printed
        // two integers would not say which versions they were.
        const auto version_of = [native, &build] (storage_format_type fmt, ct cs) {
            return fmt::to_string(sstables::parquet::version_for_rewrite_on_load(*build(fmt, cs), native));
        };
        const auto expect_pq = fmt::to_string(v::pq);
        const auto expect_native = fmt::to_string(native);

        // The bug: hybrid + TWCS is unconditionally Parquet, so reshape on load writes `pq` too.
        // Under the old gate this returned `native` and this line is what fails.
        BOOST_REQUIRE_EQUAL(version_of(storage_format_type::hybrid, ct::time_window), expect_pq);

        // Hybrid under a size-tiered strategy is still decided per compaction, so on load -- where
        // there is no compaction to decide about -- it keeps the native choice. This is the case
        // the old comment was really describing, and it is unchanged.
        BOOST_REQUIRE_EQUAL(version_of(storage_format_type::hybrid, ct::incremental), expect_native);

        // Explicit opt-in is `pq` under either strategy; explicit opt-out never is.
        BOOST_REQUIRE_EQUAL(version_of(storage_format_type::parquet, ct::time_window), expect_pq);
        BOOST_REQUIRE_EQUAL(version_of(storage_format_type::parquet, ct::incremental), expect_pq);
        BOOST_REQUIRE_EQUAL(version_of(storage_format_type::sstable, ct::time_window), expect_native);
        BOOST_REQUIRE_EQUAL(version_of(storage_format_type::sstable, ct::incremental), expect_native);
    }
}

// An unknown sstable version must be an error, not a silently skipped file.
//
// This is the primitive that two different downgrade-safety paths rest on, which is why it is worth a
// test of its own rather than being left implicit:
//
//   * **Local storage** puts the version in the filename, so `parse_path()` fails on an unrecognised
//     prefix and `sstable_directory` turns that into a malformed-sstable exception that aborts boot.
//     Observed directly: a file with an unknown version prefix planted in a live table directory made
//     the node exit with "malformed sstable error (aborting): invalid version" (design doc 10.9).
//   * **Object storage** has no filename -- the key is `sstables/<uuid>/Data.db` -- so the version
//     comes from the `version` column of `system.sstables_registry`, and
//     `system_keyspace::sstables_registry_list()` calls the same `version_from_string()` while
//     scanning it. That throw propagates through `table_populator::start()`, which logs and rethrows
//     everything except `compaction_stopped_exception`, so population fails.
//
// The end-to-end object-storage case has *not* been observed: the registry is not queryable through
// CQL ("unconfigured table sstables_registry"), so a bogus version cannot be planted the way the
// local file could be. What is asserted here is the shared primitive -- if this ever started
// returning a default instead of throwing, both paths would degrade from "refuses to start" to
// "silently mis-reads", and that is the difference between a safe downgrade and data loss.
SEASTAR_THREAD_TEST_CASE(test_unknown_sstable_version_is_rejected) {
    // Every version this build knows must round-trip, `pq` included -- otherwise the negative case
    // below could pass simply because the map is empty.
    for (const char* v : {"ka", "la", "mc", "md", "me", "ms", "mt", "pq"}) {
        auto parsed = sstables::version_from_string(v);
        BOOST_REQUIRE_EQUAL(fmt::to_string(parsed), std::string(v));
    }
    // And anything else throws. "zz" stands in for what `pq` looks like to a binary that predates it.
    for (const char* bad : {"zz", "pqq", "p", "", "PQ"}) {
        BOOST_REQUIRE_THROW(sstables::version_from_string(bad), std::out_of_range);
    }
}

// The parsed-footer cache (design doc 10.4l) must be invisible to a reader except in the metrics.
// Two things have to hold, and the second is the one that can silently break: a dropped entry has
// to be re-parsed rather than read as an empty footer, and the entry has to stay immutable after
// publication -- a reader that materialised a row group *into* the shared entry would both grow it
// without bound over a scan and hand another reader half-decoded state.
SEASTAR_THREAD_TEST_CASE(test_pq_footer_cache_is_transparent_across_reclaim) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto muts = make_muts(s, 16, 10);
        auto expected = muts;
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        auto stats_of = [] { return sstables::parquet::footer_cache_stats_local(); };

        // make_sstable_containing() validates by reading, which has already populated the entry.
        // Drop it so that the first counted read below is a miss.
        sstables::test(sst).reclaim_memory_from_components();
        BOOST_REQUIRE(!sst->pq_footer_cache());

        const auto before_first = stats_of();
        auto first = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(first.size(), expected.size());
        BOOST_REQUIRE_EQUAL(stats_of().misses - before_first.misses, 1u);
        BOOST_REQUIRE_EQUAL(stats_of().populations - before_first.populations, 1u);

        auto entry = sst->pq_footer_cache();
        BOOST_REQUIRE(entry);
        const size_t entry_bytes = entry->memory_size();
        // Measured, not assumed: an entry that reported zero would be reclaim-invisible, which is
        // the failure mode that makes an evictable cache un-evictable.
        BOOST_REQUIRE_GT(entry_bytes, 0u);
        BOOST_REQUIRE_EQUAL(stats_of().bytes, entry_bytes);

        // A second read hits, and returns the same data.
        const auto before_second = stats_of();
        auto second = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(stats_of().hits - before_second.hits, 1u);
        BOOST_REQUIRE_EQUAL(stats_of().misses - before_second.misses, 0u);
        BOOST_REQUIRE_EQUAL(second.size(), expected.size());
        for (size_t i = 0; i < second.size(); ++i) {
            assert_that(second[i]).is_equal_to(expected[i]);
        }
        // The hit did not mutate the entry: same object, same size.
        BOOST_REQUIRE(sst->pq_footer_cache().get() == entry.get());
        BOOST_REQUIRE_EQUAL(entry->memory_size(), entry_bytes);

        // Now what the reclaimer does. This is the same call sstables_manager makes when
        // _total_reclaimable_memory crosses components_memory_reclaim_threshold.
        const auto before_evict = stats_of();
        const size_t reclaimed = sstables::test(sst).reclaim_memory_from_components();
        BOOST_REQUIRE_GE(reclaimed, entry_bytes);
        BOOST_REQUIRE(!sst->pq_footer_cache());
        BOOST_REQUIRE_EQUAL(stats_of().evictions - before_evict.evictions, 1u);
        BOOST_REQUIRE_EQUAL(stats_of().bytes, before_evict.bytes - entry_bytes);
        // Nothing is owed to the reload fiber for it: the next read re-parses it. Reclaiming the
        // bloom filter alongside is what the remaining balance is.
        BOOST_REQUIRE_LT(sstables::test(sst).total_reclaimable_memory_size(), entry_bytes);

        // And the read after the eviction is a miss that returns identical data.
        const auto before_third = stats_of();
        auto third = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(stats_of().misses - before_third.misses, 1u);
        BOOST_REQUIRE_EQUAL(third.size(), expected.size());
        for (size_t i = 0; i < third.size(); ++i) {
            assert_that(third[i]).is_equal_to(expected[i]);
        }
        // Re-parsing an immutable file must produce a byte-identical entry.
        BOOST_REQUIRE(sst->pq_footer_cache());
        BOOST_REQUIRE_EQUAL(sst->pq_footer_cache()->memory_size(), entry_bytes);

        // Single-partition reads go through the same footer, so evicting between them must not
        // change an answer either. This is the path a point read takes.
        for (const auto& want : expected) {
            sstables::test(sst).reclaim_memory_from_components();
            auto pr = dht::partition_range::make_singular(want.decorated_key());
            auto rd = sst->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
            auto close = deferred_close(rd);
            auto got = read_mutation_from_mutation_reader(rd).get();
            BOOST_REQUIRE(got);
            assert_that(*got).is_equal_to(want);
        }
    }).get();
}

// The reclaimer has to be able to reach the footer cache through the manager's own policy, not
// only through a direct call: the whole point of registering with the existing machinery is that
// components_memory_reclaim_threshold governs it. available_memory = 0 makes the threshold zero,
// so anything the read publishes is immediately over it.
SEASTAR_THREAD_TEST_CASE(test_pq_footer_cache_is_reclaimed_by_the_manager) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto muts = make_muts(s, 8, 10);
        auto expected = muts;
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        auto before = sstables::parquet::footer_cache_stats_local();
        auto got = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(got.size(), expected.size());

        // The reclaim fiber runs on the maintenance scheduling group, so this is eventual.
        REQUIRE_EVENTUALLY_EQUAL<bool>([&] { return bool(sst->pq_footer_cache()); }, false);
        BOOST_REQUIRE_GT(sstables::parquet::footer_cache_stats_local().evictions, before.evictions);

        // And the sstable still reads correctly with the reclaimer racing every read.
        for (int i = 0; i < 3; ++i) {
            auto again = read_all(sst, s, env.make_reader_permit());
            BOOST_REQUIRE_EQUAL(again.size(), expected.size());
            for (size_t j = 0; j < again.size(); ++j) {
                assert_that(again[j]).is_equal_to(expected[j]);
            }
        }
    }, {
        // Zero available memory means the reclaim threshold is zero: the cache is dropped as soon
        // as it is published.
        .available_memory = 0
    }).get();
}

// The page index is cached per row group, and the accounting survives it growing.
//
// The two footer-cache tests above both read through read_all(), which is a full scan -- and a
// scan streams whole row groups, so it never calls load_offset_indexes() and never grows the
// entry. The growth path is only reachable from a *point* read, and it is the one part of the
// entry that is filled in after publication, so it is the one part whose bytes could go
// unaccounted or be double-subtracted on eviction.
//
// What this pins:
//   * a second point read into an already-visited row group does no page-index I/O at all,
//     which is the whole point of the cache -- it removes a device round trip that every page
//     fetch is otherwise serialised behind;
//   * the manager's reclaimable total grows when the entry does, so a page index cached for a
//     file with thousands of row groups is subject to the same pressure as the footer rather
//     than being invisible to it;
//   * and the answers do not change, with the cache warm or cold.
SEASTAR_THREAD_TEST_CASE(test_pq_page_index_is_cached_per_row_group) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto muts = make_muts(s, 24, 4);
        auto expected = muts;
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        auto read_one = [&] (const mutation& want) {
            auto pr = dht::partition_range::make_singular(want.decorated_key());
            auto rd = sst->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
            auto close = deferred_close(rd);
            auto got = read_mutation_from_mutation_reader(rd).get();
            BOOST_REQUIRE(got);
            assert_that(*got).is_equal_to(want);
        };

        // Start cold, explicitly. make_sstable_containing() validates what it writes
        // (test/lib/sstable_utils.cc:83), and that validation is itself a read -- so by the time
        // the sstable exists the page index for the groups it touched is already cached, and a
        // test that assumed otherwise would assert a miss and see a hit.
        sst->drop_pq_footer_cache(false);
        const auto base = sstables::parquet::offset_index_cache_stats_local();
        read_one(expected[0]);
        const auto after_first = sstables::parquet::offset_index_cache_stats_local();
        BOOST_REQUIRE_GT(after_first.misses, base.misses);
        BOOST_REQUIRE_GT(after_first.populations, base.populations);
        BOOST_REQUIRE(sst->pq_footer_cache());

        // The entry is now larger than the footer alone, and the manager was told.
        const size_t grown = sst->pq_footer_cache()->memory_size();
        BOOST_REQUIRE_GT(grown, 0u);

        // Re-reading the same partition must hit. Every partition of this file lands in one row
        // group at the shipping defaults, so re-reading any of them hits -- but the first
        // partition is the one certain to.
        read_one(expected[0]);
        const auto after_second = sstables::parquet::offset_index_cache_stats_local();
        BOOST_REQUIRE_GT(after_second.hits, after_first.hits);
        BOOST_REQUIRE_EQUAL(after_second.misses, after_first.misses);

        // Every other partition still reads correctly against a warm cache.
        for (const auto& m : expected) {
            read_one(m);
        }

        // And a cache that is dropped is transparent: the reader falls back to fetching the page
        // index itself and the answers are unchanged.
        sst->drop_pq_footer_cache(false);
        BOOST_REQUIRE(!sst->pq_footer_cache());
        for (const auto& m : expected) {
            read_one(m);
        }
    }).get();
}

// A decompressed data page is inflated once per page, not once per read.
//
// This is the largest single win found in the read-path investigation (design doc 10.41): at
// shipping defaults a page is the whole column chunk, so answering a 5-row point read inflates
// ~5 000 rows per column, and zstd cannot decompress part of a frame. The bytes are identical every
// time, so the second read of a page should do no codec work at all.
//
// What this pins is the *count*, not a duration: a timing assertion on a shared machine is a flake
// generator, while "the number of decompressions stopped tracking the number of reads" is exactly
// the property being claimed and is exact.
SEASTAR_THREAD_TEST_CASE(test_pq_decompressed_pages_are_cached) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        // Big enough that pq_reader takes the *paged* path. The cache is deliberately only on that
        // path -- a scan streams each page once and would retain the whole file to serve nothing --
        // and paged_fetch_is_not_cheaper() sends a small file down the streaming path instead,
        // where nothing is cached. At 24 partitions the first version of this test populated
        // nothing and looked like a broken cache.
        auto muts = make_muts(s, 6000, 2);
        auto expected = muts;
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        auto read_one = [&] (const mutation& want) {
            auto pr = dht::partition_range::make_singular(want.decorated_key());
            auto rd = sst->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
            auto close = deferred_close(rd);
            auto got = read_mutation_from_mutation_reader(rd).get();
            BOOST_REQUIRE(got);
            assert_that(*got).is_equal_to(want);
        };

        // Cold, explicitly: make_sstable_containing() validates what it writes, and that read has
        // already warmed whatever it touched.
        sst->drop_pq_footer_cache(false);

        // A spread of partitions, twice. The second pass must add hits and must not inflate a
        // single new page: the first pass has already touched every page these rows live in.
        //
        // The warm-up is the whole first pass rather than one read, for two reasons that both
        // produced a test asserting nothing. next_window() streams unconditionally when the wanted
        // window covers the whole row group, so a point read on the *first* partition of a
        // single-group file never pages; and a file small enough that the codec declines to
        // compress its pages has no decode work to cache at all, which is why this needs 6 000
        // partitions rather than a couple of dozen.
        std::vector<size_t> sample;
        for (size_t i = 0; i < expected.size(); i += 37) { sample.push_back(i); }
        for (size_t i : sample) { read_one(expected[i]); }
        const auto after_first_pass = sstables::parquet::page_cache_stats_local();
        BOOST_REQUIRE_GT(after_first_pass.populations, 0u);
        for (size_t i : sample) { read_one(expected[i]); }
        const auto after_second_pass = sstables::parquet::page_cache_stats_local();

        BOOST_REQUIRE_GT(after_second_pass.hits, after_first_pass.hits);
        BOOST_REQUIRE_EQUAL(after_second_pass.populations, after_first_pass.populations);

        // Dropping the entry takes the pages with it, and the reader falls back to inflating them
        // again -- same answers.
        sst->drop_pq_footer_cache(false);
        const auto before_cold = sstables::parquet::page_cache_stats_local();
        for (size_t i : sample) { read_one(expected[i]); }
        BOOST_REQUIRE_GT(sstables::parquet::page_cache_stats_local().populations,
                         before_cold.populations);
    }).get();
}

// A paged read issues no I/O for an extent this sstable has already read.
//
// Distinct from test_pq_decompressed_pages_are_cached, and the pair is the point: that one stops the
// *codec* running twice over the same bytes, this one stops the *read* happening twice. After the
// first, page_fetch was 73 % of a point read -- the reader was still fetching compressed bytes so
// that the decode could ignore them in favour of the decompressed form already in memory.
//
// Pins the count of fetches rather than a duration, for the same reason as its sibling: on a shared
// machine a timing assertion is a flake generator, and "the fetches stopped tracking the reads" is
// the claim.
SEASTAR_THREAD_TEST_CASE(test_pq_page_extents_are_not_refetched) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        // Same sizing as its sibling, and for the same two reasons: a small file streams instead of
        // paging, and the paged path is the only one that fetches extents.
        auto muts = make_muts(s, 6000, 2);
        auto expected = muts;
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        auto read_one = [&] (const mutation& want) {
            auto pr = dht::partition_range::make_singular(want.decorated_key());
            auto rd = sst->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
            auto close = deferred_close(rd);
            auto got = read_mutation_from_mutation_reader(rd).get();
            BOOST_REQUIRE(got);
            assert_that(*got).is_equal_to(want);
        };

        sst->drop_pq_footer_cache(false);

        std::vector<size_t> sample;
        for (size_t i = 0; i < expected.size(); i += 37) { sample.push_back(i); }

        for (size_t i : sample) { read_one(expected[i]); }
        const auto first = sstables::parquet::extent_cache_stats_local();
        BOOST_REQUIRE_GT(first.populations, 0u);

        // Second pass over the same partitions: every extent is already held, so this must add
        // hits and must not populate anything new.
        for (size_t i : sample) { read_one(expected[i]); }
        const auto second = sstables::parquet::extent_cache_stats_local();
        BOOST_REQUIRE_GT(second.hits, first.hits);
        BOOST_REQUIRE_EQUAL(second.populations, first.populations);

        // The shard-wide budget is holding something, and dropping the entry gives it back.
        //
        // This is what stops the budget being a slow leak: entries are dropped by the reclaimer and
        // by sstables going away, and if either path forgot to release, a node would stop caching
        // after a while and never say why. Exactly zero is assertable here because this test env
        // has one sstable with a page index.
        BOOST_REQUIRE_GT(sstables::parquet::read_cache_bytes_local().total(), 0u);
        sst->drop_pq_footer_cache(false);
        BOOST_REQUIRE_EQUAL(sstables::parquet::read_cache_bytes_local().total(), 0u);

        // Transparent when dropped: the reader fetches again and the answers are unchanged.
        const auto before_cold = sstables::parquet::extent_cache_stats_local();
        for (size_t i : sample) { read_one(expected[i]); }
        BOOST_REQUIRE_GT(sstables::parquet::extent_cache_stats_local().populations,
                         before_cold.populations);
    }).get();
}

// The batch reader sees exactly what the mutation reader sees.
//
// This is the whole reason the batch interface can be built before its consumers exist (design doc
// 10.44): a columnar scan and a mutation scan of the same file must agree, and that is checkable
// without touching the query path. `reassemble()` is the bridge -- it is what pq_reader itself uses
// to turn columns into rows -- so running it over the batches must reproduce as many rows as the
// mutation path produced.
//
// Order is asserted too, via first_row: batches are contiguous and in file order, which for a pq
// file is partition order, so a consumer can rely on the sequence rather than merely on the set.
SEASTAR_THREAD_TEST_CASE(test_pq_batch_reader_agrees_with_the_mutation_reader) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        // Several row groups, so batching is exercised rather than degenerating to one.
        auto muts = make_muts(s, 6000, 2);
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        size_t mutation_rows = 0;
        {
            auto rd = sst->make_reader(s, env.make_reader_permit(), query::full_partition_range,
                                       s->full_slice());
            auto close = deferred_close(rd);
            while (auto m = read_mutation_from_mutation_reader(rd).get()) {
                mutation_rows += m->partition().clustered_rows().calculate_size();
            }
        }
        BOOST_REQUIRE_GT(mutation_rows, 0u);

        auto br = sstables::parquet::make_batch_reader(sst, s, env.make_reader_permit());
        size_t batches = 0, batch_rows = 0, reassembled = 0;
        int64_t expect_first_row = 0;
        while (auto b = br->next().get()) {
            ++batches;
            BOOST_REQUIRE_EQUAL(b->first_row, expect_first_row);
            expect_first_row += b->rows;
            batch_rows += size_t(b->rows);
            auto rows = sstables::parquet::reassemble(br->schema_mapping(), br->columns(),
                                                     b->columns, size_t(b->rows));
            BOOST_REQUIRE_EQUAL(rows.size(), size_t(b->rows));
            reassembled += rows.size();
        }
        br->close().get();
        BOOST_REQUIRE_GT(batches, 1u);
        BOOST_REQUIRE_EQUAL(batch_rows, mutation_rows);
        BOOST_REQUIRE_EQUAL(reassembled, mutation_rows);
    }).get();
}

// Projection: a narrow scan reads less, and the columns it keeps are unchanged.
//
// This is the property design doc 10.30 found missing on both paths -- selecting one column of five
// moved what a pq scan read by 1.6 %, in the wrong direction. A columnar format that cannot skip
// unread columns is giving up its second largest advantage, so the two halves are asserted
// separately: the bytes must actually fall, and the surviving values must be bit-identical to what
// a full read produced.
//
// The second half is the one that could go wrong quietly. projection_skip_mask() keeps every key
// leaf and every shared metadata channel precisely because `__dmask` is what tells a dead cell from
// an absent one, and dropping it would resurrect deleted data rather than merely returning less.
SEASTAR_THREAD_TEST_CASE(test_pq_batch_reader_projection_reads_less_and_changes_nothing) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto muts = make_muts(s, 4000, 2);
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        // Everything, as the reference.
        auto full = sstables::parquet::make_batch_reader(sst, s, env.make_reader_permit());
        full->init().get();
        const size_t n_regular = full->schema_mapping().n_regular;
        BOOST_REQUIRE_GT(n_regular, 1u);

        std::vector<std::vector<sstables::parquet::row>> full_rows;
        while (auto b = full->next().get()) {
            full_rows.push_back(sstables::parquet::reassemble(
                    full->schema_mapping(), full->columns(), b->columns, size_t(b->rows)));
        }
        const uint64_t full_bytes = full->bytes_read();
        full->close().get();
        BOOST_REQUIRE_GT(full_bytes, 0u);
        BOOST_REQUIRE(!full_rows.empty());

        // Just the first regular column.
        sstables::parquet::projection proj;
        proj.want_regular.assign(n_regular, false);
        proj.want_regular[0] = true;
        auto narrow = sstables::parquet::make_batch_reader(sst, s, env.make_reader_permit(), proj);

        size_t batch = 0, compared = 0;
        while (auto b = narrow->next().get()) {
            BOOST_REQUIRE_LT(batch, full_rows.size());
            auto rows = sstables::parquet::reassemble(
                    narrow->schema_mapping(), narrow->columns(), b->columns, size_t(b->rows));
            const auto& ref = full_rows[batch];
            BOOST_REQUIRE_EQUAL(rows.size(), ref.size());
            for (size_t i = 0; i < rows.size(); ++i) {
                // The key, and the one projected column's cell, must be exactly what the full read
                // gave. Everything else is expected to differ -- that is what projecting means.
                BOOST_REQUIRE(rows[i].key == ref[i].key);
                auto a = rows[i].cells.find(0);
                auto b2 = ref[i].cells.find(0);
                BOOST_REQUIRE_EQUAL(a != rows[i].cells.end(), b2 != ref[i].cells.end());
                if (a != rows[i].cells.end() && b2 != ref[i].cells.end()) {
                    BOOST_REQUIRE_EQUAL(a->second.live, b2->second.live);
                    BOOST_REQUIRE_EQUAL(a->second.timestamp, b2->second.timestamp);
                    BOOST_REQUIRE(a->second.v.has_value() == b2->second.v.has_value());
                    if (a->second.v && b2->second.v) {
                        BOOST_REQUIRE(*a->second.v == *b2->second.v);
                    }
                    ++compared;
                }
            }
            ++batch;
        }
        const uint64_t narrow_bytes = narrow->bytes_read();
        narrow->close().get();

        BOOST_REQUIRE_EQUAL(batch, full_rows.size());
        BOOST_REQUIRE_GT(compared, 0u);
        // The point of the whole exercise.
        BOOST_REQUIRE_LT(narrow_bytes, full_bytes);
        testlog.info("projection: {} bytes for 1 of {} regular columns against {} for all ({:.1f}%)",
                     narrow_bytes, n_regular, full_bytes,
                     100.0 * double(narrow_bytes) / double(full_bytes));
    }).get();
}

// Is reader-level projection semantically safe? Asked before plumbing it anywhere -- and answered
// wrongly by this test, which is why the comment now leads with that.
//
// It reads through the batch reader and reassemble(), which returns one row per key. So it can only
// ever conclude that the row *set* survives projection, which it does at that layer. The layer a
// client goes through is the mutation path, where a clustering row with no marker and no cells is
// not a row -- and there, projecting away the only live cell of a marker-less row does lose it.
// test_parquet_bypass_cache_projection_matches_row_format (cql_query_large_test.cc) is the test that
// found this, at CQL level: 5 rows against the row format's 6.
//
// Kept, because what it asserts is true and worth pinning: reassemble() over a projected batch does
// not drop keys. Renamed in spirit rather than in name: read it as "projection preserves the key
// set", not "projection is safe".
//
// The tempting change is to have pq_reader honour the query slice's column list and skip the rest.
// reader.cc's own comment refuses that, because the row format "reads every regular column from
// storage and projects afterwards", and test_pq_restricted_slice_still_returns_every_cell pins the
// agreement. This test asks the sharper question the comment does not answer: *would* projecting
// change the answer, and where?
//
// The hazard is row existence, not values. A row written by an UPDATE that sets only one column has
// no row marker, so its existence is carried entirely by that column being present. Project that
// column away and the row may vanish -- and `SELECT other_col` is supposed to return it with a null,
// not omit it. Deletions are the same shape: a dead cell has to keep shadowing older data, so losing
// it is worse than losing a value.
//
// Written as an experiment with an assertion, so whichever way it comes out is recorded rather than
// argued.
SEASTAR_THREAD_TEST_CASE(test_pq_projection_and_row_existence) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();

        utils::chunked_vector<mutation> muts;
        auto pk = partition_key::from_single_value(*s, utf8_type->decompose(sstring("k")));
        mutation m(s, pk);
        auto ck = [&] (int i) {
            return clustering_key::from_single_value(*s, int32_type->decompose(i));
        };
        const auto& v_int = *s->get_column_definition(to_bytes("v_int"));
        const auto& v_dbl = *s->get_column_definition(to_bytes("v_dbl"));

        // Row 0: a marker and v_int. Exists whatever is projected.
        m.partition().clustered_row(*s, ck(0)).apply(row_marker(1000));
        m.set_clustered_cell(ck(0), v_int, atomic_cell::make_live(*v_int.type, 1000,
                                                                 int32_type->decompose(7)));
        // Row 1: NO marker, and only v_dbl live. Its existence rests on v_dbl alone -- this is the
        // row an UPDATE produces, and the one projection could lose.
        m.set_clustered_cell(ck(1), v_dbl, atomic_cell::make_live(*v_dbl.type, 1000,
                                                                  double_type->decompose(2.5)));
        // Row 2: NO marker, and v_dbl *dead*. A dead cell must keep shadowing older data.
        m.set_clustered_cell(ck(2), v_dbl, atomic_cell::make_dead(1000, gc_clock::now()));
        muts.push_back(std::move(m));

        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), muts).get();

        // Read everything, then read again projecting v_dbl away, and compare which rows survive.
        auto rows_for = [&] (bool project_away_v_dbl) {
            auto br = sstables::parquet::make_batch_reader(sst, s, env.make_reader_permit());
            br->init().get();
            const auto& ms = br->schema_mapping();
            std::optional<sstables::parquet::projection> proj;
            if (project_away_v_dbl) {
                proj.emplace();
                proj->want_regular.assign(ms.n_regular, true);
                // v_dbl is the third regular column of pq_schema; find it by leaf name instead of
                // trusting the order.
                for (size_t k = 0; k < ms.n_regular && k < ms.value_leaf.size(); ++k) {
                    const auto& spec_name = ms.columns[ms.value_leaf[k]].name;
                    if (spec_name.find("v_dbl") != std::string::npos) {
                        proj->want_regular[k] = false;
                    }
                }
            }
            auto r = sstables::parquet::make_batch_reader(sst, s, env.make_reader_permit(),
                                                         std::move(proj));
            std::vector<sstables::parquet::row> out;
            while (auto b = r->next().get()) {
                auto rows = sstables::parquet::reassemble(r->schema_mapping(), r->columns(),
                                                         b->columns, size_t(b->rows));
                for (auto&& rw : rows) { out.push_back(std::move(rw)); }
            }
            r->close().get();
            br->close().get();
            return out;
        };

        auto all = rows_for(false);
        auto projected = rows_for(true);

        // The row *count* is what the hazard is about. If projection can drop a row, these differ.
        testlog.info("projection and row existence: {} rows unprojected, {} projected",
                     all.size(), projected.size());
        BOOST_REQUIRE_EQUAL(projected.size(), all.size());

        // And the rows that carry no marker must still be there, in the same order.
        for (size_t i = 0; i < all.size(); ++i) {
            BOOST_REQUIRE(projected[i].key == all[i].key);
        }
    }).get();
}

// With may_project_columns, pq skips the columns the query did not ask for -- and the columns it
// did ask for are unchanged.
//
// The contract this operates under is narrow and the test says so. Without the option pq must agree
// with the row format cell for cell (test_pq_restricted_slice_still_returns_every_cell); with it,
// pq is permitted to return fewer *columns*, because the only setter is a client SELECT ... BYPASS
// CACHE, whose result is projected to those columns anyway and which does not populate the row
// cache. So the assertion is not "identical fragments" -- it is "identical answers for the columns
// the query asked for, and no extra or missing rows".
SEASTAR_THREAD_TEST_CASE(test_pq_may_project_columns_skips_unwanted_and_keeps_the_rest) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto muts = make_muts(s, 40, 8);
        auto ref = make_sstable_containing(
                env.make_sstable(s, sstables::get_highest_sstable_version()), muts).get();
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), muts).get();

        auto slice_for = [&] (bool allow_projection) {
            auto sl = partition_slice_builder(*s)
                    .with_regular_column(to_bytes("v_int"))
                    .build();
            if (allow_projection) {
                sl.options.set<query::partition_slice::option::may_project_columns>();
            }
            return sl;
        };

        // Control: without the option, pq must still match the row format exactly. This is the
        // existing contract and it must not have moved.
        {
            auto sl = slice_for(false);
            auto fw = fragments_in(ref, s, env.make_reader_permit(),
                                   query::full_partition_range, sl);
            auto fg = fragments_in(sst, s, env.make_reader_permit(),
                                   query::full_partition_range, sl);
            BOOST_REQUIRE(!fg.empty());
            BOOST_REQUIRE_EQUAL(fg.size(), fw.size());
            for (size_t i = 0; i < fg.size(); ++i) {
                BOOST_REQUIRE_EQUAL(fg[i], fw[i]);
            }
        }

        // With the option: the same rows, and the requested column identical. v_int is what was
        // asked for; the others are free to be absent, which is the whole point.
        {
            auto read_all_muts = [&] (shared_sstable from, const query::partition_slice& sl) {
                std::vector<mutation> out;
                auto rd = from->make_reader(s, env.make_reader_permit(),
                                            query::full_partition_range, sl);
                auto close = deferred_close(rd);
                while (auto m = read_mutation_from_mutation_reader(rd).get()) {
                    out.push_back(std::move(*m));
                }
                return out;
            };

            auto sl_proj = slice_for(true);
            auto sl_plain = slice_for(false);
            auto want = read_all_muts(ref, sl_plain);
            auto got = read_all_muts(sst, sl_proj);
            BOOST_REQUIRE_EQUAL(got.size(), want.size());

            const auto& v_int = *s->get_column_definition(to_bytes("v_int"));
            size_t compared = 0;
            for (size_t i = 0; i < got.size(); ++i) {
                BOOST_REQUIRE(got[i].decorated_key().equal(*s, want[i].decorated_key()));
                const auto& ga = got[i].partition().clustered_rows();
                const auto& wa = want[i].partition().clustered_rows();
                BOOST_REQUIRE_EQUAL(ga.calculate_size(), wa.calculate_size());
                auto gi = ga.begin();
                auto wi = wa.begin();
                for (; gi != ga.end() && wi != wa.end(); ++gi, ++wi) {
                    BOOST_REQUIRE(gi->key().equal(*s, wi->key()));
                    const auto* ca = gi->row().cells().find_cell(v_int.id);
                    const auto* cb = wi->row().cells().find_cell(v_int.id);
                    BOOST_REQUIRE_EQUAL(bool(ca), bool(cb));
                    if (ca && cb) {
                        BOOST_REQUIRE_EQUAL(ca->as_atomic_cell(v_int).timestamp(),
                                            cb->as_atomic_cell(v_int).timestamp());
                        BOOST_REQUIRE(ca->as_atomic_cell(v_int).value()
                                      == cb->as_atomic_cell(v_int).value());
                        ++compared;
                    }
                }
            }
            BOOST_REQUIRE_GT(compared, 0u);
        }

        // A static column the query asked for must survive projection. columns_of() puts statics
        // among the value columns, so a mask built from `regular_columns` alone would drop them --
        // which is a wrong answer to a query that selected one, not merely a slower one.
        {
            auto ss = schema_builder(1, "ks", "pq_proj_static")
                .with_column("pk", utf8_type, column_kind::partition_key)
                .with_column("ck", int32_type, column_kind::clustering_key)
                .with_column("st", int32_type, column_kind::static_column)
                .with_column("a", int32_type)
                .with_column("b", int32_type)
                .set_storage_format(storage_format_type::parquet)
                .build();

            auto pk = partition_key::from_single_value(*ss, utf8_type->decompose(sstring("p")));
            mutation m(ss, pk);
            const auto& st = *ss->get_column_definition(to_bytes("st"));
            const auto& a = *ss->get_column_definition(to_bytes("a"));
            m.set_static_cell(st, atomic_cell::make_live(*st.type, 1000,
                                                         int32_type->decompose(42)));
            auto ck0 = clustering_key::from_single_value(*ss, int32_type->decompose(0));
            m.set_clustered_cell(ck0, a, atomic_cell::make_live(*a.type, 1000,
                                                                int32_type->decompose(9)));
            utils::chunked_vector<mutation> sm;
            sm.push_back(std::move(m));
            auto ssst = make_sstable_containing(
                    env.make_sstable(ss, sstable_version_types::pq), std::move(sm)).get();

            auto sl = partition_slice_builder(*ss)
                    .with_static_column(to_bytes("st"))
                    .with_regular_column(to_bytes("a"))
                    .build();
            // Differential rather than absolute: whether a static arrives at all depends on the
            // slice options, so the question asked here is only whether *projection* changes it.
            auto read_static = [&] (bool project) {
                auto slice = sl;
                if (project) {
                    slice.options.template set<
                            query::partition_slice::option::may_project_columns>();
                }
                auto rd = ssst->make_reader(ss, env.make_reader_permit(),
                                            query::full_partition_range, slice);
                auto close = deferred_close(rd);
                auto got = read_mutation_from_mutation_reader(rd).get();
                BOOST_REQUIRE(got);
                const auto* c = got->partition().static_row().get().find_cell(st.id);
                return c ? std::optional<api::timestamp_type>(c->as_atomic_cell(st).timestamp())
                         : std::nullopt;
            };
            const auto plain = read_static(false);
            const auto projected = read_static(true);
            BOOST_REQUIRE_EQUAL(plain.has_value(), projected.has_value());
            if (plain && projected) {
                BOOST_REQUIRE_EQUAL(*plain, *projected);
            }
        }
    }).get();
}

// A bigint partition key, end to end: the partition sequence must survive the round trip.
//
// The bug this pins is described in full at `test_delta_binary_packed_wide_residual_widths`
// (parquet_writer_test.cc) -- a 64-bit bit-packing accumulator that could not hold a value of more
// than 57 bits plus the 0..7 bits of the previous one still in flight. This case is the *product*
// symptom rather than the codec one, and it is here because the two look nothing alike. A key
// column that decodes to a different value on every row does not read back as "wrong data": the
// reader groups consecutive rows into partitions by comparing the decoded key, so one partition
// comes back as a run of partitions, at tokens nobody wrote. Under a random schema that presented
// as `Mutations differ` with the partition sequence misaligned (§9.6b).
//
// The schema is what makes it reachable, and none of it is incidental:
//
//   * the key columns are `bigint`, which is what schema_mapping.cc gives DELTA_BINARY_PACKED. Every
//     other pq test in this file has a `text` partition key and an `int` clustering key, so none of
//     them encodes a single value through the packer.
//   * the partition-key values are spread across the int64 range, so the jump at each partition
//     boundary needs ~61 bits. Inside a partition the value repeats, delta 0. A near-full-width
//     residual next to a run of zeros in one miniblock is the precondition; an *ascending*
//     clustering key -- the case the encoding was chosen for -- never produces it.
//   * the clustering key alternates a small step with a huge one for the same reason, so the
//     failure is pinned for a clustering key as well as a partition key. With a constant stride
//     min_delta absorbs the whole thing and the residual width is zero.
//
// make_sstable_containing validates what it writes (test/lib/sstable_utils.cc:83), so on a broken
// build this fails at *construction*, before the assertions below. Both are kept: the assertions
// say what the test is actually about, and they are also what would catch a regression that
// validation's compacted comparison normalises away.
SEASTAR_THREAD_TEST_CASE(test_pq_bigint_key_partition_sequence_round_trips) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = schema_builder(1, "ks", "pq_bigint_key")
            .with_column("pk", long_type, column_kind::partition_key)
            .with_column("ck", long_type, column_kind::clustering_key)
            .with_column("v", int32_type)
            .build();

        // Spread across the int64 range: every pairwise difference needs at least 59 bits, so
        // whatever order the token hash puts them in, each partition boundary is a wide delta.
        const std::vector<int64_t> pks = {
            1LL, 1LL << 59, 1LL << 60, 3LL << 59, 1LL << 61, 5LL << 59, 3LL << 60, 7LL << 59,
        };
        // A small step, then a wide one, repeatedly: min_delta stays 1 and the residual is ~2^60.
        auto ck_at = [] (int r) -> int64_t {
            return int64_t(r / 2) * (int64_t(1) << 60) + int64_t(r % 2);
        };
        constexpr int rows_per_partition = 12;

        utils::chunked_vector<mutation> muts;
        for (int64_t pk : pks) {
            mutation m(s, partition_key::from_single_value(*s, long_type->decompose(pk)));
            for (int r = 0; r < rows_per_partition; ++r) {
                auto ck = clustering_key::from_single_value(*s, long_type->decompose(ck_at(r)));
                m.set_clustered_cell(ck, *s->get_column_definition(to_bytes("v")),
                                     atomic_cell::make_live(*int32_type, 1000 + r,
                                                            int32_type->decompose(r)));
            }
            muts.push_back(std::move(m));
        }
        std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
            return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
        });

        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), muts).get();

        // One partition per input partition, at the token that was written, in ring order, with
        // every row present. Pre-fix, the first partition read back holding 2 of its 12 rows
        // (`Mutations differ` out of the validation above) because the clustering key stopped
        // decoding correctly after the first wide delta; the rest of its rows landed under
        // mis-decoded keys.
        auto got = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(got.size(), muts.size());
        for (size_t i = 0; i < got.size(); ++i) {
            BOOST_TEST_CONTEXT("partition " << i) {
                BOOST_REQUIRE(got[i].decorated_key().equal(*s, muts[i].decorated_key()));
                assert_that(got[i]).is_equal_to(muts[i]);
            }
        }

        // The sstable's own first/last-key metadata, which is the second symptom of the same bug:
        // load() resolves the first and last key against the data it can read, and when the key
        // column decodes to something else that lookup finds nothing and logs "Unable to retrieve
        // metadata for first and last keys". Asserting the positions were populated catches it
        // without depending on a log line.
        BOOST_REQUIRE(sst->get_first_decorated_key().equal(*s, muts.front().decorated_key()));
        BOOST_REQUIRE(sst->get_last_decorated_key().equal(*s, muts.back().decorated_key()));
        const auto first_ck = clustering_key::from_single_value(*s, long_type->decompose(ck_at(0)));
        const auto last_ck = clustering_key::from_single_value(
                *s, long_type->decompose(ck_at(rows_per_partition - 1)));
        BOOST_REQUIRE(position_in_partition::equal_compare(*s)(
                sst->first_partition_first_position(),
                position_in_partition::for_key(first_ck)));
        BOOST_REQUIRE(position_in_partition::equal_compare(*s)(
                sst->last_partition_last_position(),
                position_in_partition::for_key(last_ck)));

        // And a point read of each partition, because the index path resolves the key
        // independently of the sequential scan above.
        for (const auto& m : muts) {
            auto pr = dht::partition_range::make_singular(m.decorated_key());
            auto rd = sst->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
            auto close = deferred_close(rd);
            auto one = read_mutation_from_mutation_reader(rd).get();
            BOOST_REQUIRE(one);
            assert_that(*one).is_equal_to(m);
            BOOST_REQUIRE(!read_mutation_from_mutation_reader(rd).get());
        }
    }).get();
}

// The read monitor is how compaction learns how far through an sstable it has got:
// compaction_read_monitor::compacted() reads the reader_position_tracker that on_read_started()
// hands it, and the backlog tracker uses that to discount work already done.
//
// pq_reader got both halves wrong. It never called on_read_started() at all, so the compaction
// monitor's tracker stayed null and compacted() returned 0 for every pq sstable for the whole of
// every compaction -- the backlog was overestimated throughout. And it called on_read_completed()
// at the end of init(), i.e. when the read was just *beginning*, which with a null tracker was a
// silent no-op. Both are invisible to a correctness test: the rows come back either way.
//
// This asserts the ordering, not just the counts, because the counts alone passed before the fix.
SEASTAR_THREAD_TEST_CASE(test_pq_full_scan_reader_reports_progress_to_the_read_monitor) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto sst = make_sstable_containing(env.make_sstable(s, sstable_version_types::pq),
                                          make_muts(s, 40, 16)).get();

        struct counting_monitor final : public sstables::read_monitor {
            unsigned started = 0;
            unsigned completed = 0;
            const sstables::reader_position_tracker* tracker = nullptr;
            uint64_t total_at_start = 0;
            uint64_t position_at_completion = 0;
            void on_read_started(const sstables::reader_position_tracker& t) override {
                ++started;
                tracker = &t;
                total_at_start = t.total_read_size;
            }
            void on_read_completed() override {
                ++completed;
                if (tracker) {
                    position_at_completion = tracker->position;
                }
            }
        };
        counting_monitor mon;

        // make_full_scan_reader is the path compaction takes.
        auto rd = sst->make_full_scan_reader(s, env.make_reader_permit(), nullptr, mon);
        auto close_rd = deferred_close(rd);
        unsigned frags = 0;
        while (auto mf = rd().get()) {
            ++frags;
        }
        BOOST_REQUIRE_GT(frags, 0u);

        // The stream is drained but the reader is still open. Progress must have been reported and
        // the read must NOT yet be marked complete -- completion used to be announced from init(),
        // before a single fragment had been emitted.
        BOOST_REQUIRE_EQUAL(mon.started, 1u);
        BOOST_REQUIRE_EQUAL(mon.completed, 0u);
        BOOST_REQUIRE(mon.tracker != nullptr);
        BOOST_REQUIRE_GT(mon.tracker->position, 0u);
        // The monitor is told how big the thing being read is, which is what turns a byte count
        // into a fraction for the backlog estimate.
        BOOST_REQUIRE_EQUAL(mon.total_at_start, sst->ondisk_data_size());

        close_rd.close_now();
        BOOST_REQUIRE_EQUAL(mon.completed, 1u);
        BOOST_REQUIRE_GT(mon.position_at_completion, 0u);
    }).get();
}

// Task #18: cancelling an in-flight pq compaction. On the AWS cluster this segfaulted the shard
// eleven times over nine crash-restart cycles -- si_addr 0x10, near-null, inside
// compacting_reader::fill_buffer, always immediately after the compaction_manager logged that it
// was stopping ongoing compactions "due to truncate".
//
// Twelve real truncate-during-compaction attempts on a 4-shard node did not reproduce it, for both
// parquet AND native, and the limiting factor was scale: those compactions were 92-156 keys, while
// the AWS ones ran over a billion-row table. So rather than race a TRUNCATE and hope, this drives
// the abort path directly -- enough input sstables that the compacting reader has a real merge to
// do, and the abort requested once it is demonstrably reading.
//
// What it asserts is deliberately modest: the compaction must fail rather than complete, and the
// process must still be able to read and write pq sstables afterwards. It does NOT assert a
// particular exception type -- the point is the absence of a fault, not the shape of the error.
SEASTAR_THREAD_TEST_CASE(test_pq_compaction_aborted_while_reading_does_not_fault) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto cf = env.make_table_for_tests(s);
        auto stop_cf = deferred_stop(cf);
        auto sst_gen = env.make_sst_factory(s, sstable_version_types::pq);

        // Big enough that a timed abort lands inside the merge. The first version used 6 x 400 x 25
        // and the compaction finished before the abort was requested -- BOOST_REQUIRE(threw) caught
        // that rather than letting a vacuous run report success, which is the whole reason the
        // assertion is there.
        //
        // Scale and abort delay are overridable so the committed default stays CI-fast while the
        // same test can be driven hard by hand. The AWS fault was on a compaction 3-4 orders of
        // magnitude bigger than this, and "the race needs a deeper in-flight pipeline" is the
        // leading hypothesis for why it does not reproduce here, so being able to turn the dial
        // without editing the test is the point.
        auto env_int = [] (const char* k, int dflt) {
            const char* v = std::getenv(k);
            return v ? std::max(1, std::atoi(v)) : dflt;
        };
        const int n_ssts  = env_int("PQ_ABORT_SSTS", 10);
        const int n_parts = env_int("PQ_ABORT_PARTS", 3000);
        const int n_rows  = env_int("PQ_ABORT_ROWS", 40);
        const int delay_ms = env_int("PQ_ABORT_DELAY_MS", 30);
        std::vector<shared_sstable> in;
        for (int i = 0; i < n_ssts; ++i) {
            in.push_back(make_sstable_containing(sst_gen(), make_muts(s, n_parts, n_rows)).get());
        }
        testlog.info("built {} pq sstables of {} x {} rows; abort at {} ms",
                     n_ssts, n_parts, n_rows, delay_ms);
        auto& table_s = cf.as_compaction_group_view();

        compaction::compaction_descriptor desc(in);
        desc.creator = [&sst_gen] (shard_id) { return sst_gen(); };
        desc.replacer = sstables::replacer_fn_no_op();

        bool threw = false;
        uint64_t keys_at_abort = 0;
        std::chrono::steady_clock::duration compaction_took{};
        // The job body runs inside seastar::async on purpose. The first version was a coroutine
        // that called future::get(), which is only legal in a seastar::thread -- so the intended
        // interleaving never happened and the abort was requested after the compaction had already
        // returned, which read as "it completed" rather than as a broken test.
        run_compaction_task(env, desc.run_identifier, table_s,
                            [&] (compaction::compaction_data& cdata) {
            return seastar::async([&] {
                compaction::compaction_progress_monitor pm;
                const auto t0 = std::chrono::steady_clock::now();
                auto fut = ::compaction::compact_sstables(std::move(desc), cdata, table_s, pm);
                auto waiter = seastar::async([&] {
                    seastar::sleep(std::chrono::milliseconds(delay_ms)).get();
                    keys_at_abort = cdata.total_keys_written;
                    // cdata.stop(), NOT abort.request_abort(). is_stop_requested() tests the
                    // stop_requested STRING, and stop() is what sets it as well as tripping the
                    // abort source -- requesting the abort alone left the string empty, so the
                    // compaction ignored it and ran to completion (1504 ms against an abort at
                    // 30 ms). This is the call the compaction_manager makes on truncate, which is
                    // the path that faulted on AWS.
                    cdata.stop("due to truncate (test)");
                });
                try {
                    std::move(fut).get();
                } catch (...) {
                    threw = true;
                }
                compaction_took = std::chrono::steady_clock::now() - t0;
                std::move(waiter).get();
            });
        }).get();

        testlog.info("pq compaction ran {} ms before the abort took effect, {} keys written at abort",
                     std::chrono::duration_cast<std::chrono::milliseconds>(compaction_took).count(),
                     keys_at_abort);

        // Stopped, not quietly completed. Without this the run could "pass" having never cancelled
        // anything -- which is how the first twelve attempts at this bug looked.
        BOOST_REQUIRE(threw);
        testlog.info("aborted a pq compaction with {} keys written so far", keys_at_abort);

        // The shard survived and the read/write paths still work. On AWS this is exactly what did
        // not hold: the node crash-looped and resumed the same compaction on restart.
        auto after = make_sstable_containing(sst_gen(), make_muts(s, 10, 10)).get();
        BOOST_REQUIRE_GT(after->data_size(), 0u);
        auto rd_frags = fragments_in(after, s, env.make_reader_permit(),
                                     query::full_partition_range, s->full_slice());
        BOOST_REQUIRE_GT(rd_frags.size(), 0u);
    }).get();
}
