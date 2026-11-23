/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>
#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include <seastar/testing/on_internal_error.hh>
#include <seastar/util/closeable.hh>

#include "test/lib/sstable_test_env.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/exception_utils.hh"
#include "test/lib/key_utils.hh"
#include "test/lib/mutation_assertions.hh"
#include "test/lib/mutation_source_test.hh"
#include "mutation/frozen_mutation.hh"
#include "readers/mutation_reader.hh"
#include "schema/schema_builder.hh"
#include "sstables/exceptions.hh"
#include "sstables/mx/partition_codec.hh"
#include "sstables/mx/partition_decoder.hh"
#include "sstables/sstables.hh"
#include "vint-serialization.hh"
#include "replica/memtable.hh"
#include "types/list.hh"
#include "types/map.hh"
#include "types/set.hh"
#include "types/user.hh"

using namespace sstables;

namespace {

// Decodes the first len bytes of the data file of sst as one partition of schema s.
mutation decode_data_file(test_env& env, schema_ptr s, shared_sstable sst, uint64_t len, uint64_t declared_size) {
    auto permit = env.make_reader_permit();
    auto input = sst->data_stream(0, len, permit, {}, {}).get();
    return mx::decode_partition(s, mx::make_data_source_descriptor(*s, *sst), std::move(input), declared_size, permit).get();
}

mutation read_with_full_scan(test_env& env, schema_ptr s, shared_sstable sst) {
    auto reader = sst->make_full_scan_reader(s, env.make_reader_permit());
    auto close_reader = deferred_close(reader);
    auto m = read_mutation_from_mutation_reader(reader).get();
    BOOST_REQUIRE(m);
    BOOST_REQUIRE(!read_mutation_from_mutation_reader(reader).get());
    return std::move(*m);
}

} // anonymous namespace

SEASTAR_TEST_CASE(test_decode_partition_matches_sstable_reader) {
    return test_env::do_with_async([] (test_env& env) {
        using rmg = random_mutation_generator;
        for (auto counters : {rmg::generate_counters::no, rmg::generate_counters::yes}) {
            for (uint32_t seed = 1; seed <= 20; ++seed) {
                rmg gen(counters, local_shard_only::yes, rmg::generate_uncompactable::no, seed, "ks", "cf", rmg::compress::no);
                auto s = gen.schema();
                auto sst = make_sstable_containing(env.make_sstable(s, sstable_version_types::me), {gen()}).get();
                auto decoded = decode_data_file(env, s, sst, sst->data_size(), sst->data_size());
                assert_that(decoded).is_equal_to(read_with_full_scan(env, s, sst));
            }
        }
    });
}

SEASTAR_TEST_CASE(test_decode_partition_rejects_bad_input) {
    return test_env::do_with_async([] (test_env& env) {
        random_mutation_generator gen(random_mutation_generator::generate_counters::no, local_shard_only::yes,
                random_mutation_generator::generate_uncompactable::no, 1, "ks", "cf", random_mutation_generator::compress::no);
        auto s = gen.schema();

        auto two = make_sstable_containing(env.make_sstable(s, sstable_version_types::me), gen(2)).get();
        BOOST_REQUIRE_THROW(decode_data_file(env, s, two, two->data_size(), two->data_size()), malformed_sstable_exception);

        auto one = make_sstable_containing(env.make_sstable(s, sstable_version_types::me), {gen()}).get();
        const auto size = one->data_size();
        // Declared size cuts the partition short.
        BOOST_REQUIRE_THROW(decode_data_file(env, s, one, size, size - 1), malformed_sstable_exception);
        // Input ends before the declared size.
        BOOST_REQUIRE_THROW(decode_data_file(env, s, one, size - 1, size), malformed_sstable_exception);
        BOOST_REQUIRE_THROW(decode_data_file(env, s, one, 0, 0), malformed_sstable_exception);
    });
}

// Data at or before its column's drop is dead and skipped, as the sstable reader
// and a frozen mutation upgrade skip it. Other data the reading schema cannot take
// fails the decode, where a frozen mutation upgrade would drop it silently.
SEASTAR_TEST_CASE(test_decode_partition_rejects_columns_the_schema_cannot_take) {
    return test_env::do_with_async([] (test_env& env) {
        auto v1 = schema_builder(this_smp_shard_count(), "ks", "cf")
                .with_column("pk", utf8_type, column_kind::partition_key)
                .with_column("ck", int32_type, column_kind::clustering_key)
                .with_column("s1", int32_type, column_kind::static_column)
                .with_column("r1", int32_type)
                .with_column("r2", int32_type)
                .with_column("r3", int32_type)
                .with_column("r4", int32_type)
                .with_column("r5", int32_type)
                .build();
        // r2 is gone without a drop record, r3 changed to an incompatible type,
        // r4 was dropped at timestamp 7 and added again, r5 was dropped at 7, r6 is new.
        auto v2 = schema_builder(this_smp_shard_count(), "ks", "cf")
                .with_column("pk", utf8_type, column_kind::partition_key)
                .with_column("ck", int32_type, column_kind::clustering_key)
                .with_column("s1", int32_type, column_kind::static_column)
                .with_column("r1", int32_type)
                .with_column("r3", utf8_type)
                .with_column("r4", int32_type)
                .with_column("r6", int32_type)
                .without_column("r4", int32_type, 7)
                .without_column("r5", int32_type, 7)
                .build();

        // A key of this shard, since the sstable is written here.
        auto pk = tests::generate_partition_key(v1).key();
        auto ck = clustering_key::from_single_value(*v1, serialized(int32_t(1)));
        auto with_cell = [&] (const char* column, api::timestamp_type ts) {
            mutation m(v1, pk);
            m.set_static_cell("s1", data_value(int32_t(0)), 10);
            m.partition().clustered_row(*v1, ck).apply(row_marker(10));
            m.set_clustered_cell(ck, "r1", data_value(int32_t(1)), 10);
            m.set_clustered_cell(ck, column, data_value(int32_t(2)), ts);
            return m;
        };
        auto decode = [&] (const mutation& m) {
            auto sst = make_sstable_containing(env.make_sstable(v1, sstable_version_types::me), {m}).get();
            return decode_data_file(env, v2, sst, sst->data_size(), sst->data_size());
        };

        BOOST_REQUIRE_EXCEPTION(decode(with_cell("r2", 10)), mx::column_mismatch_error,
                exception_predicate::message_contains("is missing from the schema"));
        BOOST_REQUIRE_EXCEPTION(decode(with_cell("r3", 10)), mx::column_mismatch_error,
                exception_predicate::message_contains("incompatible with the schema"));
        BOOST_REQUIRE_EXCEPTION(decode(with_cell("r5", 8)), mx::column_mismatch_error,
                exception_predicate::message_contains("holds data with timestamp 8, after the column was dropped at 7"));

        using dead_or_live = std::tuple<const char*, api::timestamp_type, size_t>;
        for (const auto& [column, ts, cells] : {dead_or_live("r4", 7, 1), dead_or_live("r5", 7, 1), dead_or_live("r4", 8, 2)}) {
            auto m = with_cell(column, ts);
            auto decoded = decode(m);
            assert_that(decoded).is_equal_to(freeze(m).unfreeze_upgrading(v2, v1->get_column_mapping()));
            BOOST_REQUIRE_EQUAL(decoded.partition().clustered_row(*v2, ck).cells().size(), cells);
        }
    });
}

namespace {

const mx::codec_params me_params{sstable_version_types::me};

mutation round_trip(const mutation& m, const encoding_stats& bases, const mx::codec_schema& cs, schema_ptr reader_schema, reader_permit permit) {
    bytes_ostream out;
    mx::encode_partition(m, bases, cs, out);
    return mx::decode_partition(std::move(reader_schema), bases, cs, std::move(out), std::move(permit)).get();
}

// The default bases are below most generated timestamps; the memtable's bases
// are what a flush of m would use.
std::vector<encoding_stats> bases_for(const mutation& m) {
    return {encoding_stats{}, make_memtable(m.schema(), {m}).get()->get_encoding_stats()};
}

void check_round_trip(test_env& env, const mutation& m, const mx::codec_schema& cs) {
    for (const auto& bases : bases_for(m)) {
        assert_that(round_trip(m, bases, cs, m.schema(), env.make_reader_permit())).is_equal_to(m);
    }
}

} // anonymous namespace

SEASTAR_TEST_CASE(test_codec_round_trip_random_mutations) {
    return test_env::do_with_async([] (test_env& env) {
        using rmg = random_mutation_generator;
        for (auto uncompactable : {rmg::generate_uncompactable::no, rmg::generate_uncompactable::yes}) {
            for (uint32_t seed = 1; seed <= 20; ++seed) {
                rmg gen(rmg::generate_counters::no, local_shard_only::yes, uncompactable, seed);
                auto s = gen.schema();
                auto cs = mx::make_codec_schema(*s, *s, me_params);
                for (const auto& m : gen(5)) {
                    check_round_trip(env, m, cs);
                }
            }
        }
    });
}

// Encodings random_mutation_generator does not produce.
SEASTAR_TEST_CASE(test_codec_round_trip_hand_written) {
    return test_env::do_with_async([] (test_env& env) {
        auto udt = user_type_impl::get_instance("ks", to_bytes("udt"), {to_bytes("a"), to_bytes("b")}, {int32_type, utf8_type}, true);
        auto set_t = set_type_impl::get_instance(int32_type, true);
        auto map_t = map_type_impl::get_instance(int32_type, utf8_type, true);
        auto s = schema_builder(this_smp_shard_count(), "ks", "cf")
                .with_column("pk", int32_type, column_kind::partition_key)
                .with_column("ck1", long_type, column_kind::clustering_key)
                .with_column("ck2", uuid_type, column_kind::clustering_key)
                .with_column("s_set", set_t, column_kind::static_column)
                .with_column("r_int", int32_type)
                .with_column("r_text", utf8_type)
                .with_column("r_map", map_t)
                .with_column("r_udt", udt)
                .with_column("r_frozen", list_type_impl::get_instance(int32_type, false))
                .build();
        auto cs = mx::make_codec_schema(*s, *s, me_params);
        auto& r_int = *s->get_column_definition("r_int");
        auto& r_text = *s->get_column_definition("r_text");
        auto& r_map = *s->get_column_definition("r_map");
        auto& r_udt = *s->get_column_definition("r_udt");
        auto& r_frozen = *s->get_column_definition("r_frozen");
        auto& s_set = *s->get_column_definition("s_set");

        auto pk = [&] (int32_t v) { return partition_key::from_single_value(*s, serialized(v)); };
        auto ck = [&] (int64_t v) {
            return clustering_key::from_exploded(*s, {serialized(v), serialized(utils::UUID(0x1234, 0x5678 + v))});
        };
        auto ck_prefix = [&] (int64_t v) { return clustering_key_prefix::from_exploded(*s, {serialized(v)}); };
        auto ldt = [] (int64_t v) { return gc_clock::time_point(gc_clock::duration(v)); };
        auto int_cell = [] (api::timestamp_type ts, int32_t v) { return atomic_cell::make_live(*int32_type, ts, serialized(v)); };

        // Empty partition.
        check_round_trip(env, mutation(s, pk(1)), cs);

        // Partition tombstone only.
        {
            mutation m(s, pk(2));
            m.partition().apply(tombstone(100, ldt(1700000000)));
            check_round_trip(env, m, cs);
        }

        // Static row only, with a complex deletion.
        {
            mutation m(s, pk(3));
            collection_mutation_writer w(tombstone(90, ldt(1700000000)));
            w.push_back(managed_bytes_view(managed_bytes(serialized(int32_t(1)))), atomic_cell::make_live(*bytes_type, 100, bytes_view(), atomic_cell::collection_member::yes));
            w.push_back(managed_bytes_view(managed_bytes(serialized(int32_t(2)))), atomic_cell::make_live(*bytes_type, 100, bytes_view(), atomic_cell::collection_member::yes));
            m.set_static_cell(s_set, std::move(w).finish());
            check_round_trip(env, m, cs);
        }

        // Rows with fixed-length clustering and value types and every cell shape.
        {
            mutation m(s, pk(4));
            const auto ttl = gc_clock::duration(3600);
            const auto expiry = ldt(1800000000);

            // Expiring marker and a cell with the row's timestamp and TTL; an empty value.
            auto& a = m.partition().clustered_row(*s, ck(1));
            a.apply(row_marker(200, ttl, expiry));
            m.set_clustered_cell(ck(1), r_int, atomic_cell::make_live(*int32_type, 200, serialized(int32_t(7)), expiry, ttl));
            m.set_clustered_cell(ck(1), r_text, atomic_cell::make_live(*utf8_type, 200, bytes_view()));
            m.set_clustered_cell(ck(1), r_frozen, atomic_cell::make_live(*r_frozen.type, 201,
                    r_frozen.type->decompose(make_list_value(r_frozen.type, {int32_t(1), int32_t(2)}))));

            // Dead cell, map with a complex deletion, non-frozen UDT.
            m.set_clustered_cell(ck(2), r_int, atomic_cell::make_dead(300, ldt(1700000001)));
            {
                collection_mutation_writer w(tombstone(250, ldt(1700000002)));
                w.push_back(managed_bytes_view(managed_bytes(serialized(int32_t(1)))),
                        atomic_cell::make_live(*utf8_type, 300, serialized(sstring("one")), atomic_cell::collection_member::yes));
                w.push_back(managed_bytes_view(managed_bytes(serialized(int32_t(2)))),
                        atomic_cell::make_dead(300, ldt(1700000003)));
                m.set_clustered_cell(ck(2), r_map, std::move(w).finish());
            }
            {
                collection_mutation_writer w(tombstone{});
                w.push_back(managed_bytes_view(managed_bytes(serialize_field_index(0))),
                        atomic_cell::make_live(*int32_type, 300, serialized(int32_t(5)), atomic_cell::collection_member::yes));
                w.push_back(managed_bytes_view(managed_bytes(serialize_field_index(1))),
                        atomic_cell::make_live(*utf8_type, 300, serialized(sstring("b")), expiry, ttl, atomic_cell::collection_member::yes));
                m.set_clustered_cell(ck(2), r_udt, std::move(w).finish());
            }

            // Shadowable row tombstone.
            m.partition().clustered_row(*s, ck(3)).apply(shadowable_tombstone(400, ldt(1700000004)));
            m.set_clustered_cell(ck(3), r_int, int_cell(500, 3));

            // Timestamps below the bases take the wrapped delta path.
            m.set_clustered_cell(ck(4), r_int, int_cell(1, 4));
            m.partition().clustered_row(*s, ck(4)).apply(tombstone(2, ldt(3)));

            // Adjacent range tombstones meet in an excl_end_incl_start boundary.
            m.partition().apply_row_tombstone(*s, range_tombstone(ck_prefix(10), bound_kind::incl_start,
                    ck_prefix(20), bound_kind::excl_end, tombstone(600, ldt(1700000005))));
            m.partition().apply_row_tombstone(*s, range_tombstone(ck_prefix(20), bound_kind::incl_start,
                    ck_prefix(30), bound_kind::incl_end, tombstone(700, ldt(1700000006))));
            m.set_clustered_cell(ck(25), r_int, int_cell(800, 25));

            check_round_trip(env, m, cs);
        }

        // Partition tombstone with a static row, rows and range tombstones after it,
        // including a row the partition tombstone covers: the codec compacts nothing.
        {
            mutation m(s, pk(5));
            m.partition().apply(tombstone(50, ldt(1700000000)));
            collection_mutation_writer w(tombstone{});
            w.push_back(managed_bytes_view(managed_bytes(serialized(int32_t(1)))), atomic_cell::make_live(*bytes_type, 60, bytes_view(), atomic_cell::collection_member::yes));
            m.set_static_cell(s_set, std::move(w).finish());
            m.partition().clustered_row(*s, ck(1)).apply(row_marker(60));
            m.set_clustered_cell(ck(1), r_int, int_cell(60, 1));
            m.partition().apply_row_tombstone(*s, range_tombstone(ck_prefix(2), bound_kind::incl_start,
                    ck_prefix(4), bound_kind::incl_end, tombstone(70, ldt(1700000001))));
            m.set_clustered_cell(ck(3), r_text, atomic_cell::make_live(*utf8_type, 80, serialized(sstring("x"))));
            m.set_clustered_cell(ck(5), r_int, int_cell(40, 5));
            m.partition().apply_row_tombstone(*s, range_tombstone(ck_prefix(6), bound_kind::excl_start,
                    ck_prefix(8), bound_kind::excl_end, tombstone(90, ldt(1700000002))));
            check_round_trip(env, m, cs);
        }

        // A TTL base above max_ttl. The encoder clamps the base, so the decoder
        // must decode against the clamped base too.
        {
            mutation m(s, pk(6));
            m.set_clustered_cell(ck(1), r_int, atomic_cell::make_live(*int32_type, 100, serialized(int32_t(1)), ldt(1800000000), gc_clock::duration(3600)));
            const encoding_stats bases{.min_timestamp = 100, .min_local_deletion_time = ldt(1800000000), .min_ttl = max_ttl + gc_clock::duration(1)};
            assert_that(round_trip(m, bases, cs, s, env.make_reader_permit())).is_equal_to(m);
        }
    });
}

SEASTAR_TEST_CASE(test_codec_refuses_unsupported_input) {
    return test_env::do_with_async([] (test_env& env) {
        seastar::testing::scoped_no_abort_on_internal_error abort_guard;
        auto counter = schema_builder(this_smp_shard_count(), "ks", "counter")
                .with_column("pk", int32_type, column_kind::partition_key)
                .with_column("c", counter_type)
                .build();
        BOOST_REQUIRE_THROW(mx::make_codec_schema(*counter, *counter, me_params), std::runtime_error);
        auto compact = schema_builder(this_smp_shard_count(), "ks", "compact")
                .with_column("pk", int32_type, column_kind::partition_key)
                .with_column("ck", int32_type, column_kind::clustering_key)
                .with_column("v", int32_type)
                .with(schema_builder::compact_storage::yes)
                .build();
        BOOST_REQUIRE_THROW(mx::make_codec_schema(*compact, *compact, me_params), std::runtime_error);

        // A codec_schema built from a column_mapping only decodes.
        auto s = schema_builder(this_smp_shard_count(), "ks", "cf")
                .with_column("pk", int32_type, column_kind::partition_key)
                .with_column("v", int32_type)
                .build();
        auto decode_only = mx::make_codec_schema(s->get_column_mapping(), {}, *s, me_params);
        bytes_ostream out;
        BOOST_REQUIRE_THROW(mx::encode_partition(mutation(s, partition_key::from_single_value(*s, serialized(int32_t(1)))),
                encoding_stats{}, decode_only, out), std::runtime_error);

        // One clustering key fixed length per clustering column.
        auto two_ck = schema_builder(this_smp_shard_count(), "ks", "two_ck")
                .with_column("pk", int32_type, column_kind::partition_key)
                .with_column("ck1", int32_type, column_kind::clustering_key)
                .with_column("ck2", int32_type, column_kind::clustering_key)
                .with_column("v", int32_type)
                .build();
        BOOST_REQUIRE_THROW(mx::make_codec_schema(two_ck->get_column_mapping(), {int32_type->value_length_if_fixed()}, *two_ck, me_params),
                std::runtime_error);
    });
}

// The codec output for a partition is the partition's bytes in Data.db.
SEASTAR_TEST_CASE(test_codec_output_matches_data_file) {
    return test_env::do_with_async([] (test_env& env) {
        using rmg = random_mutation_generator;
        for (uint32_t seed = 1; seed <= 10; ++seed) {
            rmg gen(rmg::generate_counters::no, local_shard_only::yes, rmg::generate_uncompactable::no, seed, "ks", "cf", rmg::compress::no);
            auto s = gen.schema();
            auto mt = make_memtable(s, gen(10)).get();
            // The memtable drops data covered by tombstones, so encode what it holds.
            utils::chunked_vector<mutation> held;
            {
                auto rd = mt->make_mutation_reader(s, env.make_reader_permit());
                auto close_rd = deferred_close(rd);
                while (auto m = read_mutation_from_mutation_reader(rd).get()) {
                    held.push_back(std::move(*m));
                }
            }
            auto sst = make_sstable_containing(env.make_sstable(s, sstable_version_types::me), mt).get();

            auto cs = mx::make_codec_schema(*s, *s, me_params);
            bytes_ostream encoded;
            for (const auto& m : held) {
                mx::encode_partition(m, mt->get_encoding_stats(), cs, encoded);
            }

            auto input = sst->data_stream(0, sst->data_size(), env.make_reader_permit(), {}, {}, sstable::raw_stream::yes).get();
            auto close_input = deferred_close(input);
            auto data = input.read_exactly(sst->data_size()).get();
            BOOST_REQUIRE_EQUAL(data.size(), sst->data_size());
            BOOST_REQUIRE_EQUAL(encoded.size(), data.size());
            BOOST_REQUIRE(encoded.linearize() == bytes_view(reinterpret_cast<const int8_t*>(data.get()), data.size()));
        }
    });
}

// Data written with an older schema decodes like a frozen mutation upgrade when it
// is live in the new schema or at or before its column's drop. Data newer than the
// drop of a column the new schema lacks fails the decode.
SEASTAR_TEST_CASE(test_codec_decodes_with_newer_schema) {
    return test_env::do_with_async([] (test_env& env) {
        auto map_t = map_type_impl::get_instance(int32_type, int32_type, true);
        auto v1 = schema_builder(this_smp_shard_count(), "ks", "cf")
                .with_column("pk", int32_type, column_kind::partition_key)
                .with_column("ck", int32_type, column_kind::clustering_key)
                .with_column("s_keep", int32_type, column_kind::static_column)
                .with_column("r_keep", int32_type)
                .with_column("r_alter", int32_type)
                .with_column("r_gone", int32_type)
                .with_column("r_readd", int32_type)
                .with_column("m_gone", map_t)
                .with_column("m_readd", map_t)
                .build();
        // s_new and r_new are added, r_gone and m_gone are dropped at 10, r_readd and
        // m_readd are dropped at 10 and added back, r_alter changes to the value-compatible varint.
        auto v2 = schema_builder(v1)
                .with_column("s_new", int32_type, column_kind::static_column)
                .with_column("r_new", int32_type)
                .remove_column("r_gone", 10)
                .remove_column("r_readd", 10)
                .with_column("r_readd", int32_type)
                .remove_column("m_gone", 10)
                .remove_column("m_readd", 10)
                .with_column("m_readd", map_t)
                .alter_column_type("r_alter", varint_type)
                .build();

        auto pk = partition_key::from_single_value(*v1, serialized(int32_t(1)));
        auto ck = clustering_key::from_single_value(*v1, serialized(int32_t(1)));
        auto writer_cs = mx::make_codec_schema(*v1, *v1, me_params);
        auto reader_cs = mx::make_codec_schema(v1->get_column_mapping(), {int32_type->value_length_if_fixed()}, *v2, me_params);
        auto decode = [&] (const mutation& m, const encoding_stats& bases) {
            bytes_ostream out;
            mx::encode_partition(m, bases, writer_cs, out);
            return mx::decode_partition(v2, bases, reader_cs, std::move(out), env.make_reader_permit()).get();
        };
        auto upgraded = [&] (const mutation& m) {
            return freeze(m).unfreeze_upgrading(v2, v1->get_column_mapping());
        };
        // A map with a tombstone and one element, either of them absent at timestamp 0.
        auto set_map = [&] (mutation& m, const char* column, api::timestamp_type tomb_ts, api::timestamp_type element_ts) {
            collection_mutation_writer w(tomb_ts ? tombstone(tomb_ts, gc_clock::time_point(gc_clock::duration(1700000000))) : tombstone());
            if (element_ts) {
                w.push_back(managed_bytes_view(managed_bytes(serialized(int32_t(1)))),
                        atomic_cell::make_live(*int32_type, element_ts, serialized(int32_t(2)), atomic_cell::collection_member::yes));
            }
            m.set_clustered_cell(ck, *v1->get_column_definition(to_bytes(column)), std::move(w).finish());
        };

        mutation m(v1, pk);
        m.set_static_cell("s_keep", data_value(int32_t(9)), 20);
        m.set_clustered_cell(ck, "r_keep", data_value(int32_t(1)), 20);
        m.set_clustered_cell(ck, "r_alter", data_value(int32_t(-7)), 20);
        m.set_clustered_cell(ck, "r_readd", data_value(int32_t(6)), 15);
        for (const auto& bases : bases_for(m)) {
            auto decoded = decode(m, bases);
            assert_that(decoded).is_equal_to(upgraded(m));
            BOOST_REQUIRE_EQUAL(decoded.partition().static_row().size(), 1);
            BOOST_REQUIRE_EQUAL(decoded.partition().clustered_row(*v2, ck).cells().size(), 3);
        }

        // Dead data, at or before the drop: a cell of the dropped r_gone, a cell of
        // r_readd from when it was dropped, a map tombstone and element of each map.
        mutation dead(v1, pk);
        dead.set_clustered_cell(ck, "r_keep", data_value(int32_t(1)), 20);
        dead.set_clustered_cell(ck, "r_gone", data_value(int32_t(3)), 5);
        dead.set_clustered_cell(ck, "r_readd", data_value(int32_t(5)), 10);
        set_map(dead, "m_gone", 9, 10);
        set_map(dead, "m_readd", 9, 0);
        // The map tombstone at the drop goes, the newer element stays.
        mutation dead_tomb_live_element(v1, pk);
        set_map(dead_tomb_live_element, "m_readd", 10, 11);
        for (const auto& mut : {dead, dead_tomb_live_element}) {
            auto decoded = decode(mut, encoding_stats{});
            assert_that(decoded).is_equal_to(upgraded(mut));
            BOOST_REQUIRE_EQUAL(decoded.partition().clustered_row(*v2, ck).cells().size(), 1);
        }

        // Newer than the drop of a column v2 lacks: a cell, a map element, a map tombstone.
        mutation gone(v1, pk);
        gone.set_clustered_cell(ck, "r_gone", data_value(int32_t(3)), 11);
        mutation gone_element(v1, pk);
        set_map(gone_element, "m_gone", 0, 11);
        mutation gone_tomb(v1, pk);
        set_map(gone_tomb, "m_gone", 11, 0);
        for (const auto& mut : {gone, gone_element, gone_tomb}) {
            BOOST_REQUIRE_EXCEPTION(decode(mut, encoding_stats{}), mx::column_mismatch_error,
                    exception_predicate::message_contains("holds data with timestamp 11, after the column was dropped at 10"));
        }
    });
}

// A column dropped and added again as the other kind keeps its name, so a lookup by
// name alone finds a definition whose id indexes the other kind's columns. The old
// cells are dead if at or before the drop, and fail the decode if newer.
SEASTAR_TEST_CASE(test_codec_skips_column_readded_as_other_kind) {
    return test_env::do_with_async([] (test_env& env) {
        auto v1 = schema_builder(this_smp_shard_count(), "ks", "cf")
                .with_column("pk", int32_type, column_kind::partition_key)
                .with_column("ck", int32_type, column_kind::clustering_key)
                .with_column("s1", int32_type, column_kind::static_column)
                .with_column("r1", int32_type)
                .with_column("r2", int32_type)
                .build();
        // r1 is dropped at 10 and added back static; s1 is dropped at 10 and added
        // back regular. r2 takes r1's regular id 0.
        auto v2 = schema_builder(v1)
                .remove_column("r1", 10)
                .with_column("r1", int32_type, column_kind::static_column)
                .remove_column("s1", 10)
                .with_column("s1", int32_type)
                .build();

        // A key of this shard, since the sstable path writes the sstable here.
        auto pk = tests::generate_partition_key(v1).key();
        auto ck = clustering_key::from_single_value(*v1, serialized(int32_t(1)));
        auto regular = [&] (api::timestamp_type ts) {
            mutation m(v1, pk);
            m.set_clustered_cell(ck, "r1", data_value(int32_t(7)), ts);
            m.set_clustered_cell(ck, "r2", data_value(int32_t(2)), 5);
            return m;
        };
        auto statics = [&] (api::timestamp_type ts) {
            mutation m(v1, pk);
            m.set_static_cell("s1", data_value(int32_t(7)), ts);
            m.set_clustered_cell(ck, "r2", data_value(int32_t(2)), 5);
            return m;
        };
        mutation upgraded(v2, pk);
        upgraded.set_clustered_cell(ck, "r2", data_value(int32_t(2)), 5);

        auto writer_cs = mx::make_codec_schema(*v1, *v1, me_params);
        const std::vector<mx::codec_schema> reader_schemas{
            mx::make_codec_schema(*v1, *v2, me_params),
            mx::make_codec_schema(v1->get_column_mapping(), {int32_type->value_length_if_fixed()}, *v2, me_params),
        };
        auto decode_all = [&] (const mutation& m) {
            std::vector<mutation> ret;
            for (const auto& reader_cs : reader_schemas) {
                bytes_ostream out;
                mx::encode_partition(m, encoding_stats{}, writer_cs, out);
                ret.push_back(mx::decode_partition(v2, encoding_stats{}, reader_cs, std::move(out), env.make_reader_permit()).get());
            }
            auto sst = make_sstable_containing(env.make_sstable(v1, sstable_version_types::me), {m}).get();
            ret.push_back(decode_data_file(env, v2, sst, sst->data_size(), sst->data_size()));
            return ret;
        };
        using make_mutation = std::function<mutation(api::timestamp_type)>;
        for (const auto& [make, message] : {std::pair<make_mutation, const char*>(regular, "is regular in the data and static in the schema"),
                                           std::pair<make_mutation, const char*>(statics, "is static in the data and regular in the schema")}) {
            const auto m = make(6);
            assert_that(freeze(m).unfreeze_upgrading(v2, v1->get_column_mapping())).is_equal_to(upgraded);
            for (const auto& decoded : decode_all(m)) {
                assert_that(decoded).is_equal_to(upgraded);
            }
            const auto newer = make(11);
            for (const auto& reader_cs : reader_schemas) {
                bytes_ostream out;
                mx::encode_partition(newer, encoding_stats{}, writer_cs, out);
                BOOST_REQUIRE_EXCEPTION(mx::decode_partition(v2, encoding_stats{}, reader_cs, std::move(out), env.make_reader_permit()).get(),
                        mx::column_mismatch_error, exception_predicate::message_contains(message));
            }
            auto sst = make_sstable_containing(env.make_sstable(v1, sstable_version_types::me), {newer}).get();
            BOOST_REQUIRE_EXCEPTION(decode_data_file(env, v2, sst, sst->data_size(), sst->data_size()),
                    mx::column_mismatch_error, exception_predicate::message_contains(message));
        }
    });
}

namespace {

// Replaces bytes of an encoded partition, checking what it replaces so that a
// layout change fails the test instead of corrupting the wrong field.
bytes_ostream patched(bytes_ostream& encoded, size_t offset, bytes expected, bytes replacement) {
    auto view = encoded.linearize();
    bytes b(view.begin(), view.end());
    BOOST_REQUIRE(bytes_view(b).substr(offset, expected.size()) == bytes_view(expected));
    std::ranges::copy(replacement, b.begin() + offset);
    bytes_ostream out;
    out.write(bytes_view(b));
    return out;
}

} // anonymous namespace

// Corrupt input fails the decode instead of crashing or losing data.
SEASTAR_TEST_CASE(test_codec_rejects_corrupt_input) {
    return test_env::do_with_async([] (test_env& env) {
        auto ldt = [] (int64_t v) { return gc_clock::time_point(gc_clock::duration(v)); };
        // Equal to the data, so that every delta below is one byte and the offsets are fixed.
        const encoding_stats bases{.min_timestamp = 600, .min_local_deletion_time = ldt(1700000000), .min_ttl = gc_clock::duration(0)};
        auto decode = [&] (schema_ptr s, const mx::codec_schema& cs, bytes_ostream in) {
            return mx::decode_partition(std::move(s), bases, cs, std::move(in), env.make_reader_permit()).get();
        };

        auto s = schema_builder(this_smp_shard_count(), "ks", "cf")
                .with_column("pk", int32_type, column_kind::partition_key)
                .with_column("ck", int32_type, column_kind::clustering_key)
                .with_column("v", int32_type)
                .build();
        auto cs = mx::make_codec_schema(*s, *s, me_params);
        auto ck = [&] (int32_t v) { return clustering_key::from_single_value(*s, serialized(v)); };
        mutation m(s, partition_key::from_single_value(*s, serialized(int32_t(1))));
        m.partition().apply_row_tombstone(*s, range_tombstone(ck(10), bound_kind::incl_start, ck(20), bound_kind::incl_end, tombstone(600, ldt(1700000000))));
        m.set_clustered_cell(ck(30), *s->get_column_definition("v"), atomic_cell::make_live(*int32_type, 600, serialized(int32_t(1))));
        bytes_ostream encoded;
        mx::encode_partition(m, bases, cs, encoded);
        assert_that(decode(s, cs, encoded)).is_equal_to(m);
        // Layout: 6 bytes of key, 12 of partition tombstone, then the start marker at 18
        // (flags, kind, 2 bytes of clustering size, block header, 4 bytes of value, body
        // size, 3 bytes of body) and the end marker at 31.

        // Bytes after the end-of-partition marker.
        {
            bytes_ostream trailing(encoded);
            trailing.write(bytes_view(bytes{0}));
            BOOST_REQUIRE_THROW(decode(s, cs, std::move(trailing)), malformed_sstable_exception);
        }
        // Range tombstone clustering key with more components than the schema has.
        BOOST_REQUIRE_THROW(decode(s, cs, patched(encoded, 20, bytes{0, 1}, bytes{0, 7})), malformed_sstable_exception);
        // Range tombstone end bound before its start bound; the assembler would drop it.
        BOOST_REQUIRE_THROW(decode(s, cs, patched(encoded, 36, serialized(int32_t(20)), serialized(int32_t(5)))), malformed_sstable_exception);

        // Missing-columns bitmap with a bit past the column count, in a row of a
        // schema with fewer than 64 columns. The resize of the selector would drop it.
        auto narrow = schema_builder(this_smp_shard_count(), "ks", "narrow")
                .with_column("pk", int32_type, column_kind::partition_key)
                .with_column("ck", int32_type, column_kind::clustering_key)
                .with_column("v", int32_type)
                .with_column("w", int32_type)
                .build();
        auto narrow_cs = mx::make_codec_schema(*narrow, *narrow, me_params);
        mutation nm(narrow, partition_key::from_single_value(*narrow, serialized(int32_t(1))));
        nm.set_clustered_cell(clustering_key::from_single_value(*narrow, serialized(int32_t(1))), *narrow->get_column_definition("v"),
                atomic_cell::make_live(*int32_type, 600, serialized(int32_t(1))));
        bytes_ostream narrow_encoded;
        mx::encode_partition(nm, bases, narrow_cs, narrow_encoded);
        assert_that(decode(narrow, narrow_cs, narrow_encoded)).is_equal_to(nm);
        // Same layout as the wide row below: the bitmap at 26 has bit 1 set, w missing.
        BOOST_REQUIRE_THROW(decode(narrow, narrow_cs, patched(narrow_encoded, 26, bytes{2}, bytes{6})), malformed_sstable_exception);

        // Missing-column count and index in a row of a schema with 64 or more columns.
        auto wide_builder = schema_builder(this_smp_shard_count(), "ks", "wide")
                .with_column("pk", int32_type, column_kind::partition_key)
                .with_column("ck", int32_type, column_kind::clustering_key);
        for (int i = 0; i < 65; ++i) {
            wide_builder.with_column(to_bytes(format("c{:02}", i)), int32_type);
        }
        auto wide = wide_builder.build();
        auto wide_cs = mx::make_codec_schema(*wide, *wide, me_params);
        const auto wide_ck = clustering_key::from_single_value(*wide, serialized(int32_t(1)));
        const auto set_wide_cell = [&] (mutation& m, int i) {
            m.set_clustered_cell(wide_ck, *wide->get_column_definition(to_bytes(format("c{:02}", i))),
                    atomic_cell::make_live(*int32_type, 600, serialized(int32_t(1))));
        };
        mutation wm(wide, partition_key::from_single_value(*wide, serialized(int32_t(1))));
        set_wide_cell(wm, 0);
        set_wide_cell(wm, 1);
        bytes_ostream wide_encoded;
        mx::encode_partition(wm, bases, wide_cs, wide_encoded);
        assert_that(decode(wide, wide_cs, wide_encoded)).is_equal_to(wm);
        // Layout: 18 bytes of partition header, row flags, block header, 4 bytes of
        // clustering value, body size, previous row size, then 63 missing columns and
        // the indexes of the two present columns.
        BOOST_REQUIRE_THROW(decode(wide, wide_cs, patched(wide_encoded, 26, bytes{63, 0, 1}, bytes{127, 0, 1})), malformed_sstable_exception);
        BOOST_REQUIRE_THROW(decode(wide, wide_cs, patched(wide_encoded, 26, bytes{63, 0, 1}, bytes{63, 0, 65})), malformed_sstable_exception);
        // A present column listed twice. The second flip would mark it absent again, and
        // the decoder would read its cell as the next row.
        BOOST_REQUIRE_EXCEPTION(decode(wide, wide_cs, patched(wide_encoded, 26, bytes{63, 0, 1}, bytes{63, 0, 0})),
                malformed_sstable_exception, exception_predicate::message_contains("listed twice"));

        // The same with the missing columns listed: c00 and c01 unset, the other 63 set.
        mutation wm_missing(wide, partition_key::from_single_value(*wide, serialized(int32_t(1))));
        for (int i = 2; i < 65; ++i) {
            set_wide_cell(wm_missing, i);
        }
        bytes_ostream missing_encoded;
        mx::encode_partition(wm_missing, bases, wide_cs, missing_encoded);
        assert_that(decode(wide, wide_cs, missing_encoded)).is_equal_to(wm_missing);
        // The body size takes more than one byte here, so the count follows it and the
        // one-byte previous row size.
        const size_t missing_count_pos = 24 + unsigned_vint::serialized_size_from_first_byte(missing_encoded.linearize()[24]) + 1;
        BOOST_REQUIRE_EXCEPTION(decode(wide, wide_cs, patched(missing_encoded, missing_count_pos, bytes{2, 0, 1}, bytes{2, 0, 0})),
                malformed_sstable_exception, exception_predicate::message_contains("listed twice"));
    });
}
