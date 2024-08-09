/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/sstring.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/align.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/closeable.hh>

#include "sstables/generation_type.hh"
#include "sstables/sstables.hh"
#include "sstables/key.hh"
#include "sstables/open_info.hh"
#include "sstables/version.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "test/lib/scylla_test_case.hh"
#include "test/lib/test_utils.hh"
#include "schema/schema.hh"
#include "compress.hh"
#include "replica/database.hh"
#include "test/boost/sstable_test.hh"
#include "test/lib/tmpdir.hh"
#include "partition_slice_builder.hh"
#include "sstables/sstable_mutation_reader.hh"

#include <boost/range/combine.hpp>

using namespace sstables;

bytes as_bytes(const sstring& s) {
    return { reinterpret_cast<const int8_t*>(s.data()), s.size() };
}

future<> test_using_working_sst(schema_ptr s, sstring dir) {
    return test_env::do_with_async([s = std::move(s), dir = std::move(dir)] (test_env& env) {
        (void)env.reusable_sst(std::move(s), std::move(dir)).get();
    });
}

SEASTAR_TEST_CASE(uncompressed_data) {
    return test_using_working_sst(uncompressed_schema(), uncompressed_dir());
}

static auto make_schema_for_compressed_sstable() {
    return make_shared_schema({}, "ks", "cf", {{"pk", utf8_type}}, {}, {}, {}, utf8_type);
}

SEASTAR_TEST_CASE(compressed_data) {
    auto s = make_schema_for_compressed_sstable();
    return test_using_working_sst(std::move(s), "test/resource/sstables/compressed");
}

SEASTAR_TEST_CASE(composite_index) {
    return test_using_working_sst(composite_schema(), "test/resource/sstables/composite");
}

template<std::invocable<test_env&, sstable_ptr> Func>
inline future<std::invoke_result_t<Func, test_env&, sstable_ptr>>
test_using_reusable_sst(schema_ptr s, sstring dir, sstables::generation_type::int_t gen, Func&& func) {
    using ret_type = std::invoke_result_t<Func, test_env&, sstable_ptr>;
    return test_env::do_with_async_returning<ret_type>([s = std::move(s), dir = std::move(dir), gen, func = std::move(func)] (test_env& env) {
        auto sst = env.reusable_sst(std::move(s), std::move(dir), generation_from_value(gen)).get();
        return func(env, std::move(sst));
    });
}

future<std::vector<partition_key>> index_read(schema_ptr schema, sstring path) {
    return test_using_reusable_sst(std::move(schema), std::move(path), 1, [] (test_env& env, sstable_ptr ptr) {
        auto indexes = sstables::test(ptr).read_indexes(env.make_reader_permit()).get();
        return boost::copy_range<std::vector<partition_key>>(
                indexes | boost::adaptors::transformed([] (const sstables::test::index_entry& e) { return e.key; }));
    });
}

SEASTAR_TEST_CASE(simple_index_read) {
    auto vec = co_await index_read(uncompressed_schema(), uncompressed_dir());
    BOOST_REQUIRE(vec.size() == 4);
}

SEASTAR_TEST_CASE(composite_index_read) {
    auto vec = co_await index_read(composite_schema(), "test/resource/sstables/composite");
    BOOST_REQUIRE(vec.size() == 20);
}

template<uint64_t Position, uint64_t EntryPosition, uint64_t EntryKeySize>
future<> summary_query(schema_ptr schema, sstring path, sstables::generation_type::int_t generation) {
    return test_using_reusable_sst(std::move(schema), path, generation, [] (test_env& env, sstable_ptr ptr) {
        auto entry = sstables::test(ptr).read_summary_entry(Position).get();
        BOOST_REQUIRE(entry.position == EntryPosition);
        BOOST_REQUIRE(entry.key.size() == EntryKeySize);
    });
}

template<uint64_t Position, uint64_t EntryPosition, uint64_t EntryKeySize>
future<> summary_query_fail(schema_ptr schema, sstring path, sstables::generation_type::int_t generation) {
    try {
        co_await summary_query<Position, EntryPosition, EntryKeySize>(std::move(schema), std::move(path), generation);
    } catch (const std::out_of_range&) {
    }
}

SEASTAR_TEST_CASE(small_summary_query_ok) {
    return summary_query<0, 0, 5>(uncompressed_schema(), uncompressed_dir(), 1);
}

SEASTAR_TEST_CASE(small_summary_query_fail) {
    return summary_query_fail<2, 0, 5>(uncompressed_schema(), uncompressed_dir(), 1);
}

SEASTAR_TEST_CASE(small_summary_query_negative_fail) {
    return summary_query_fail<-uint64_t(2), 0, 5>(uncompressed_schema(), uncompressed_dir(), 1);
}

SEASTAR_TEST_CASE(big_summary_query_0) {
    return summary_query<0, 0, 182>(uncompressed_schema(), "test/resource/sstables/bigsummary", 76);
}

SEASTAR_TEST_CASE(big_summary_query_32) {
    return summary_query<32, 0xc4000, 182>(uncompressed_schema(), "test/resource/sstables/bigsummary", 76);
}

// The following two files are just a copy of uncompressed's 1. But the Summary
// is removed (and removed from the TOC as well). We should reconstruct it
// in this case, so the queries should still go through
SEASTAR_TEST_CASE(missing_summary_query_ok) {
    return summary_query<0, 0, 5>(uncompressed_schema(), uncompressed_dir(), 2);
}

SEASTAR_TEST_CASE(missing_summary_query_fail) {
    return summary_query_fail<2, 0, 5>(uncompressed_schema(), uncompressed_dir(), 2);
}

SEASTAR_TEST_CASE(missing_summary_query_negative_fail) {
    return summary_query_fail<-uint64_t(2), 0, 5>(uncompressed_schema(), uncompressed_dir(), 2);
}

// TODO: only one interval is generated with size-based sampling. Test it with a sstable that will actually result
// in two intervals.
#if 0
SEASTAR_TEST_CASE(missing_summary_interval_1_query_ok) {
    return summary_query<1, 19, 6>(uncompressed_schema(1), uncompressed_dir(), 2);
}
#endif

SEASTAR_TEST_CASE(missing_summary_first_last_sane) {
    return test_using_reusable_sst(uncompressed_schema(), uncompressed_dir(), 2, [] (test_env& env, shared_sstable ptr) {
        auto& summary = sstables::test(ptr).get_summary();
        BOOST_REQUIRE(summary.header.size == 1);
        BOOST_REQUIRE(summary.positions.size() == 1);
        BOOST_REQUIRE(summary.entries.size() == 1);
        BOOST_REQUIRE(bytes_view(summary.first_key) == as_bytes("vinna"));
        BOOST_REQUIRE(bytes_view(summary.last_key) == as_bytes("finna"));
    });
}

static future<sstable_ptr> do_write_sst(test_env& env, schema_ptr schema, sstring load_dir, sstring write_dir, sstables::generation_type generation) {
    auto sst = co_await env.reusable_sst(std::move(schema), load_dir, generation);
    sstable_generation_generator gen(generation.as_int());
    sstables::test(sst).change_generation_number(gen());
    co_await sstables::test(sst).change_dir(write_dir);
    co_await sstables::test(sst).store();
    co_return sst;
}

static future<> write_sst_info(schema_ptr schema, sstring load_dir, sstring write_dir, sstables::generation_type generation) {
    return test_env::do_with_async([schema = std::move(schema), load_dir = std::move(load_dir), write_dir = std::move(write_dir),
                                    generation = std::move(generation)] (test_env& env) {
        (void)do_write_sst(env, std::move(schema), std::move(load_dir), std::move(write_dir), std::move(generation)).get();
    });
}

static future<> check_component_integrity(component_type component) {
    tmpdir tmp;
    co_await write_sst_info(make_schema_for_compressed_sstable(), "test/resource/sstables/compressed", tmp.path().string(), sstables::generation_type(1));
    auto file_path_a = sstable::filename("test/resource/sstables/compressed", "ks", "cf", la, sstables::generation_type(1), big, component);
    auto file_path_b = sstable::filename(tmp.path().string(), "ks", "cf", la, sstables::generation_type(2), big, component);
    auto eq = co_await tests::compare_files(file_path_a, file_path_b);
    BOOST_REQUIRE(eq);
}

SEASTAR_TEST_CASE(check_compressed_info_func) {
    return check_component_integrity(component_type::CompressionInfo);
}

future<>
write_and_validate_sst(schema_ptr s, sstring dir, noncopyable_function<void (shared_sstable sst1, shared_sstable sst2)> func) {
    return test_env::do_with_async([s = std::move(s), dir = std::move(dir), func = std::move(func)] (test_env& env) mutable {
        auto sst1 = do_write_sst(env, s, dir, env.tempdir().path().native(), env.new_generation()).get();
        auto sst2 = env.make_sstable(s, sst1->get_version());
        func(std::move(sst1), std::move(sst2));
    }, test_env_config{ .use_uuid = false });
}

SEASTAR_TEST_CASE(check_summary_func) {
    auto s = make_schema_for_compressed_sstable();
    return write_and_validate_sst(std::move(s), "test/resource/sstables/compressed", [] (shared_sstable sst1, shared_sstable sst2) {
        sstables::test(sst2).read_summary().get();

        summary& sst1_s = sstables::test(sst1).get_summary();
        summary& sst2_s = sstables::test(sst2).get_summary();

        BOOST_REQUIRE(::memcmp(&sst1_s.header, &sst2_s.header, sizeof(summary::header)) == 0);
        BOOST_REQUIRE(sst1_s.positions == sst2_s.positions);
        BOOST_REQUIRE(sst1_s.entries == sst2_s.entries);
        BOOST_REQUIRE(sst1_s.first_key.value == sst2_s.first_key.value);
        BOOST_REQUIRE(sst1_s.last_key.value == sst2_s.last_key.value);
    });
}

SEASTAR_TEST_CASE(check_filter_func) {
    return check_component_integrity(component_type::Filter);
}

SEASTAR_TEST_CASE(check_statistics_func) {
    auto s = make_schema_for_compressed_sstable();
    return write_and_validate_sst(std::move(s), "test/resource/sstables/compressed", [] (shared_sstable sst1, shared_sstable sst2) {
        sstables::test(sst2).read_statistics().get();
        statistics& sst1_s = sstables::test(sst1).get_statistics();
        statistics& sst2_s = sstables::test(sst2).get_statistics();

        BOOST_REQUIRE(sst1_s.offsets.elements.size() == sst2_s.offsets.elements.size());
        BOOST_REQUIRE(sst1_s.contents.size() == sst2_s.contents.size());

        for (auto&& e : boost::combine(sst1_s.offsets.elements, sst2_s.offsets.elements)) {
            BOOST_REQUIRE(boost::get<0>(e).second ==  boost::get<1>(e).second);
        }
        // TODO: compare the field contents from both sstables.
    });
}

SEASTAR_TEST_CASE(check_toc_func) {
    auto s = make_schema_for_compressed_sstable();
    return write_and_validate_sst(std::move(s), "test/resource/sstables/compressed", [] (shared_sstable sst1, shared_sstable sst2) {
        sstables::test(sst2).read_toc().get();
        auto& sst1_c = sstables::test(sst1).get_components();
        auto& sst2_c = sstables::test(sst2).get_components();

        BOOST_REQUIRE(sst1_c == sst2_c);
    });
}

SEASTAR_TEST_CASE(uncompressed_random_access_read) {
    return test_using_reusable_sst(uncompressed_schema(), uncompressed_dir(), 1, [] (auto& env, auto sstp) {
        temporary_buffer<char> buf = sstables::test(sstp).data_read(env.make_reader_permit(), 97, 6).get();
        BOOST_REQUIRE(sstring(buf.get(), buf.size()) == "gustaf");
    });
}

SEASTAR_TEST_CASE(compressed_random_access_read) {
    auto s = make_schema_for_compressed_sstable();
    return test_using_reusable_sst(std::move(s), "test/resource/sstables/compressed", 1, [] (auto& env, auto sstp) {
        temporary_buffer<char> buf = sstables::test(sstp).data_read(env.make_reader_permit(), 97, 6).get();
        BOOST_REQUIRE(sstring(buf.get(), buf.size()) == "gustaf");
    });
}


SEASTAR_TEST_CASE(find_key_map) {
    return test_using_reusable_sst(map_schema(), "test/resource/sstables/map_pk", 1, [] (auto& env, auto sstp) {
        schema_ptr s = map_schema();
        auto& summary = sstables::test(sstp)._summary();
        std::vector<data_value> kk;

        auto b1 = to_bytes("2");
        auto b2 = to_bytes("2");

        auto map_type = map_type_impl::get_instance(bytes_type, bytes_type, true);
        auto map_element = std::make_pair<data_value, data_value>(data_value(b1), data_value(b2));
        std::vector<std::pair<data_value, data_value>> map;
        map.push_back(map_element);

        kk.push_back(make_map_value(map_type, map));

        auto key = sstables::key::from_deeply_exploded(*s, kk);
        BOOST_REQUIRE(sstables::test(sstp).binary_search(s->get_partitioner(), summary.entries, key) == 0);
    });
}

SEASTAR_TEST_CASE(find_key_set) {
    return test_using_reusable_sst(set_schema(), "test/resource/sstables/set_pk", 1, [] (auto& env, auto sstp) {
        schema_ptr s = set_schema();
        auto& summary = sstables::test(sstp)._summary();
        std::vector<data_value> kk;

        std::vector<data_value> set;

        bytes b1("1");
        bytes b2("2");

        set.push_back(data_value(b1));
        set.push_back(data_value(b2));
        auto set_type = set_type_impl::get_instance(bytes_type, true);
        kk.push_back(make_set_value(set_type, set));

        auto key = sstables::key::from_deeply_exploded(*s, kk);
        BOOST_REQUIRE(sstables::test(sstp).binary_search(s->get_partitioner(), summary.entries, key) == 0);
    });
}

SEASTAR_TEST_CASE(find_key_list) {
    return test_using_reusable_sst(list_schema(), "test/resource/sstables/list_pk", 1, [] (auto& env, auto sstp) {
        schema_ptr s = set_schema();
        auto& summary = sstables::test(sstp)._summary();
        std::vector<data_value> kk;

        std::vector<data_value> list;

        bytes b1("1");
        bytes b2("2");
        list.push_back(data_value(b1));
        list.push_back(data_value(b2));

        auto list_type = list_type_impl::get_instance(bytes_type, true);
        kk.push_back(make_list_value(list_type, list));

        auto key = sstables::key::from_deeply_exploded(*s, kk);
        BOOST_REQUIRE(sstables::test(sstp).binary_search(s->get_partitioner(), summary.entries, key) == 0);
    });
}


SEASTAR_TEST_CASE(find_key_composite) {
    return test_using_reusable_sst(composite_schema(), "test/resource/sstables/composite", 1, [] (auto& env, auto sstp) {
        schema_ptr s = composite_schema();
        auto& summary = sstables::test(sstp)._summary();
        std::vector<data_value> kk;

        auto b1 = bytes("HCG8Ee7ENWqfCXipk4-Ygi2hzrbfHC8pTtH3tEmV3d9p2w8gJPuMN_-wp1ejLRf4kNEPEgtgdHXa6NoFE7qUig==");
        auto b2 = bytes("VJizqYxC35YpLaPEJNt_4vhbmKJxAg54xbiF1UkL_9KQkqghVvq34rZ6Lm8eRTi7JNJCXcH6-WtNUSFJXCOfdg==");

        kk.push_back(data_value(b1));
        kk.push_back(data_value(b2));

        auto key = sstables::key::from_deeply_exploded(*s, kk);
        BOOST_REQUIRE(sstables::test(sstp).binary_search(s->get_partitioner(), summary.entries, key) == 0);
    });
}

SEASTAR_TEST_CASE(all_in_place) {
    return test_using_reusable_sst(uncompressed_schema(), "test/resource/sstables/bigsummary", 76, [] (auto& env, auto sstp) {
        auto& summary = sstables::test(sstp)._summary();

        int idx = 0;
        for (auto& e: summary.entries) {
            auto key = sstables::key::from_bytes(bytes(e.key));
            BOOST_REQUIRE(sstables::test(sstp).binary_search(sstp->get_schema()->get_partitioner(), summary.entries, key) == idx++);
        }
    });
}

SEASTAR_TEST_CASE(full_index_search) {
    return test_using_reusable_sst(uncompressed_schema(), uncompressed_dir(), 1, [] (auto& env, auto sstp) {
        auto index_list = sstables::test(sstp).read_indexes(env.make_reader_permit()).get();
        int idx = 0;
        for (auto& e : index_list) {
            auto key = key::from_partition_key(*sstp->get_schema(), e.key);
            BOOST_REQUIRE(sstables::test(sstp).binary_search(sstp->get_schema()->get_partitioner(), index_list, key) == idx++);
        }
    });
}

SEASTAR_TEST_CASE(not_find_key_composite_bucket0) {
    return test_using_reusable_sst(composite_schema(), "test/resource/sstables/composite", 1, [] (auto& env, auto sstp) {
        schema_ptr s = composite_schema();
        auto& summary = sstables::test(sstp)._summary();
        std::vector<data_value> kk;

        auto b1 = bytes("ZEunFCoqAidHOrPiU3U6UAvUU01IYGvT3kYtYItJ1ODTk7FOsEAD-dqmzmFNfTDYvngzkZwKrLxthB7ItLZ4HQ==");
        auto b2 = bytes("K-GpWx-QtyzLb12z5oNS0C03d3OzNyBKdYJh1XjHiC53KudoqdoFutHUMFLe6H9Emqv_fhwIJEKEb5Csn72f9A==");

        kk.push_back(data_value(b1));
        kk.push_back(data_value(b2));

        auto key = sstables::key::from_deeply_exploded(*s, kk);
        // (result + 1) * -1 -1 = 0
        BOOST_REQUIRE(sstables::test(sstp).binary_search(s->get_partitioner(), summary.entries, key) == -2);
    });
}

// See CASSANDRA-7593. This sstable writes 0 in the range_start. We need to handle that case as well
SEASTAR_TEST_CASE(wrong_range) {
    return test_using_reusable_sst(uncompressed_schema(), "test/resource/sstables/wrongrange", 114, [] (auto& env, auto sstp) {
        auto range = dht::partition_range::make_singular(make_dkey(uncompressed_schema(), "todata"));
        auto s = columns_schema();
        auto rd = sstp->make_reader(s, env.make_reader_permit(), range, s->full_slice());
        auto close_rd = deferred_close(rd);
        (void)read_mutation_from_mutation_reader(rd).get();
    });
}

SEASTAR_TEST_CASE(statistics_rewrite) {
    return test_env::do_with_async([] (test_env& env) {
        auto uncompressed_dir_copy = env.tempdir().path();
        for (const auto& entry : std::filesystem::directory_iterator(uncompressed_dir().c_str())) {
            std::filesystem::copy(entry.path(), uncompressed_dir_copy / entry.path().filename());
        }
        auto generation_dir = (uncompressed_dir_copy / sstables::staging_dir).native();
        std::filesystem::create_directories(generation_dir);

        auto sstp = env.reusable_sst(uncompressed_schema(), uncompressed_dir_copy.native()).get();
        test::create_links(*sstp, generation_dir).get();
        auto file_path = sstable::filename(generation_dir, "ks", "cf", la, generation_from_value(1), big, component_type::Data);
        auto exists = file_exists(file_path).get();
        BOOST_REQUIRE(exists);

        sstp = env.reusable_sst(uncompressed_schema(), generation_dir).get();
        // mutate_sstable_level results in statistics rewrite
        sstp->mutate_sstable_level(10).get();

        sstp = env.reusable_sst(uncompressed_schema(), generation_dir).get();
        BOOST_REQUIRE(sstp->get_sstable_level() == 10);
    });
}

// Tests for reading a large partition for which the index contains a
// "promoted index", i.e., a sample of the column names inside the partition,
// with which we can avoid reading the entire partition when we look only
// for a specific subset of columns. The test sstable for the read test was
// generated in Cassandra.

static schema_ptr large_partition_schema() {
    static thread_local auto s = [] {
        schema_builder builder(make_shared_schema(
                generate_legacy_id("try1", "data"), "try1", "data",
        // partition key
        {{"t1", utf8_type}},
        // clustering key
        {{"t2", utf8_type}},
        // regular columns
        {{"t3", utf8_type}},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        ""
       ));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return s;
}

static future<shared_sstable> load_large_partition_sst(test_env& env, const sstables::sstable::version_types version) {
    auto s = large_partition_schema();
    auto dir = get_test_dir("large_partition", s);
    return env.reusable_sst(std::move(s), std::move(dir), 3, version);
}

// This is a rudimentary test that reads an sstable exported from Cassandra
// which contains a promoted index. It just checks that the promoted index
// is read from disk, as an unparsed array, and doesn't actually use it to
// search for anything.
SEASTAR_TEST_CASE(promoted_index_read) {
  return for_each_sstable_version([] (const sstables::sstable::version_types version) {
    return test_env::do_with_async([version] (test_env& env) {
        auto sstp = load_large_partition_sst(env, version).get();
        std::vector<sstables::test::index_entry> vec = sstables::test(sstp).read_indexes(env.make_reader_permit()).get();
        BOOST_REQUIRE(vec.size() == 1);
        BOOST_REQUIRE(vec[0].promoted_index_size > 0);
    });
  });
}

// Use an empty string for ck1, ck2, or both, for unbounded ranges.
static query::partition_slice make_partition_slice(const schema& s, sstring ck1, sstring ck2) {
    std::optional<query::clustering_range::bound> b1;
    if (!ck1.empty()) {
        b1.emplace(clustering_key_prefix::from_single_value(
                s, utf8_type->decompose(ck1)));
    }
    std::optional<query::clustering_range::bound> b2;
    if (!ck2.empty()) {
        b2.emplace(clustering_key_prefix::from_single_value(
                s, utf8_type->decompose(ck2)));
    }
    return partition_slice_builder(s).
            with_range(query::clustering_range(b1, b2)).build();
}

// Count the number of CQL rows in one partition between clustering key
// prefix ck1 to ck2.
static future<int> count_rows(test_env& env, sstable_ptr sstp, schema_ptr s, sstring key, sstring ck1, sstring ck2) {
    return seastar::async([&env, sstp, s, key, ck1, ck2] () mutable {
        auto ps = make_partition_slice(*s, ck1, ck2);
        auto pr = dht::partition_range::make_singular(make_dkey(s, key.c_str()));
        auto rd = sstp->make_reader(s, env.make_reader_permit(), pr, ps);
        auto close_rd = deferred_close(rd);
        auto mfopt = rd().get();
        if (!mfopt) {
            return 0;
        }
        int nrows = 0;
        mfopt = rd().get();
        while (mfopt) {
            if (mfopt->is_clustering_row()) {
                nrows++;
            }
            mfopt = rd().get();
        }
        return nrows;
    });
}

// Count the number of CQL rows in one partition
static future<int> count_rows(test_env& env, sstable_ptr sstp, schema_ptr s, sstring key) {
    return seastar::async([&env, sstp, s, key] () mutable {
        auto pr = dht::partition_range::make_singular(make_dkey(s, key.c_str()));
        auto rd = sstp->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
        auto close_rd = deferred_close(rd);
        auto mfopt = rd().get();
        if (!mfopt) {
            return 0;
        }
        int nrows = 0;
        mfopt = rd().get();
        while (mfopt) {
            if (mfopt->is_clustering_row()) {
                nrows++;
            }
            mfopt = rd().get();
        }
        return nrows;
    });
}

// Count the number of CQL rows between clustering key prefix ck1 to ck2
// in all partitions in the sstable (using sstable::read_range_rows).
static future<int> count_rows(test_env& env, sstable_ptr sstp, schema_ptr s, sstring ck1, sstring ck2) {
    return seastar::async([&env, sstp, s, ck1, ck2] () mutable {
        auto ps = make_partition_slice(*s, ck1, ck2);
        auto reader = sstp->make_reader(s, env.make_reader_permit(), query::full_partition_range, ps);
        auto close_reader = deferred_close(reader);
        int nrows = 0;
        auto mfopt = reader().get();
        while (mfopt) {
            mfopt = reader().get();
            BOOST_REQUIRE(mfopt);
            while (!mfopt->is_end_of_partition()) {
                if (mfopt->is_clustering_row()) {
                    nrows++;
                }
                mfopt = reader().get();
            }
            mfopt = reader().get();
        }
        return nrows;
    });
}

// This test reads, using sstable::read_row(), a slice (a range of clustering
// rows) from one large partition in an sstable written in Cassandra.
// This large partition includes 13520 clustering rows, and spans about
// 700 KB on disk. When we ask to read only a part of it, the promoted index
// (included in this sstable) may be used to allow reading only a part of the
// partition from disk. This test doesn't directly verify that the promoted
// index is actually used - and can work even without a promoted index
// support - but can be used to check that adding promoted index read supports
// did not break anything.
// To verify that the promoted index was actually used to reduce the size
// of read from disk, add printouts to the row reading code.
SEASTAR_TEST_CASE(sub_partition_read) {
  schema_ptr s = large_partition_schema();
  return for_each_sstable_version([s] (const sstables::sstable::version_types version) {
    return test_env::do_with_async([s, version] (test_env& env) {
        auto sstp = load_large_partition_sst(env, version).get();
        {
            auto nrows = count_rows(env, sstp, s, "v1", "18wX", "18xB").get();
            // there should be 5 rows (out of 13520 = 20*26*26) in this range:
            // 18wX, 18wY, 18wZ, 18xA, 18xB.
            BOOST_REQUIRE(nrows == 5);
        }
        {
            auto nrows = count_rows(env, sstp, s, "v1", "13aB", "15aA").get();
            // There should be 26*26*2 rows in this range. It spans two
            // promoted-index blocks, so we get to test that case.
            BOOST_REQUIRE(nrows == 2*26*26);
        }
        {
            auto nrows = count_rows(env, sstp, s, "v1", "10aB", "19aA").get();
            // There should be 26*26*9 rows in this range. It spans many
            // promoted-index blocks.
            BOOST_REQUIRE(nrows == 9*26*26);
        }
        {
            auto nrows = count_rows(env, sstp, s, "v1", "0", "z").get();
            // All rows, 20*26*26 of them, are in this range. It spans all
            // the promoted-index blocks, but the range is still bounded
            // on both sides
            BOOST_REQUIRE(nrows == 20*26*26);
        }
        {
            // range that is outside (after) the actual range of the data.
            // No rows should match.
            auto nrows = count_rows(env, sstp, s, "v1", "y", "z").get();
            BOOST_REQUIRE(nrows == 0);
        }
        {
            // range that is outside (before) the actual range of the data.
            // No rows should match.
            auto nrows = count_rows(env, sstp, s, "v1", "_a", "_b").get();
            BOOST_REQUIRE(nrows == 0);
        }
        {
            // half-infinite range
            auto nrows = count_rows(env, sstp, s, "v1", "", "10aA").get();
            BOOST_REQUIRE(nrows == (1*26*26 + 1));
        }
        {
            // half-infinite range
            auto nrows = count_rows(env, sstp, s, "v1", "10aA", "").get();
            BOOST_REQUIRE(nrows == 19*26*26);
        }
        {
            // count all rows, but giving an explicit all-encompasing filter
            auto nrows = count_rows(env, sstp, s, "v1", "", "").get();
            BOOST_REQUIRE(nrows == 20*26*26);
        }
        {
            // count all rows, without a filter
            auto nrows = count_rows(env, sstp, s, "v1").get();
            BOOST_REQUIRE(nrows == 20*26*26);
        }
    });
  });
}

// Same as previous test, just using read_range_rows instead of read_row
// to read parts of potentially more than one partition (in this particular
// sstable, there is actually just one partition).
SEASTAR_TEST_CASE(sub_partitions_read) {
  schema_ptr s = large_partition_schema();
  return for_each_sstable_version([s] (const sstables::sstable::version_types version) {
   return test_env::do_with_async([s, version] (test_env& env) {
        auto sstp = load_large_partition_sst(env, version).get();
        auto nrows = count_rows(env, sstp, s, "18wX", "18xB").get();
        BOOST_REQUIRE(nrows == 5);
   });
  });
}

SEASTAR_TEST_CASE(test_skipping_in_compressed_stream) {
    return seastar::async([] {
        tests::reader_concurrency_semaphore_wrapper semaphore;

        tmpdir tmp;
        auto file_path = (tmp.path() / "test").string();
        file f = open_file_dma(file_path, open_flags::create | open_flags::wo).get();

        file_input_stream_options opts;
        opts.read_ahead = 0;

        compression_parameters cp({
            { compression_parameters::SSTABLE_COMPRESSION, "LZ4Compressor" },
            { compression_parameters::CHUNK_LENGTH_KB, std::to_string(opts.buffer_size/1024) },
        });

        sstables::compression c;
        // this initializes "c"
        auto os = make_file_output_stream(f, file_output_stream_options()).get();
        auto out = make_compressed_file_m_format_output_stream(std::move(os), &c, cp);

        // Make sure that amount of written data is a multiple of chunk_len so that we hit #2143.
        temporary_buffer<char> buf1(c.uncompressed_chunk_length());
        strcpy(buf1.get_write(), "buf1");
        temporary_buffer<char> buf2(c.uncompressed_chunk_length());
        strcpy(buf2.get_write(), "buf2");

        size_t uncompressed_size = 0;
        out.write(buf1.get(), buf1.size()).get();
        uncompressed_size += buf1.size();
        out.write(buf2.get(), buf2.size()).get();
        uncompressed_size += buf2.size();
        out.close().get();

        auto compressed_size = seastar::file_size(file_path).get();
        c.update(compressed_size);

        auto make_is = [&] {
            f = open_file_dma(file_path, open_flags::ro).get();
            return make_compressed_file_m_format_input_stream(f, &c, 0, uncompressed_size, opts, semaphore.make_permit());
        };

        auto expect = [] (input_stream<char>& in, const temporary_buffer<char>& buf) {
            auto b = in.read_exactly(buf.size()).get();
            BOOST_REQUIRE(b == buf);
        };

        auto expect_eof = [] (input_stream<char>& in) {
            auto b = in.read().get();
            BOOST_REQUIRE(b.empty());
        };

        auto in = make_is();
        expect(in, buf1);
        expect(in, buf2);
        expect_eof(in);

        in = make_is();
        in.skip(0).get();
        expect(in, buf1);
        expect(in, buf2);
        expect_eof(in);

        in = make_is();
        expect(in, buf1);
        in.skip(0).get();
        expect(in, buf2);
        expect_eof(in);

        in = make_is();
        expect(in, buf1);
        in.skip(opts.buffer_size).get();
        expect_eof(in);

        in = make_is();
        in.skip(opts.buffer_size * 2).get();
        expect_eof(in);

        in = make_is();
        in.skip(opts.buffer_size).get();
        in.skip(opts.buffer_size).get();
        expect_eof(in);
    });
}

// Test that sstables::key_view::tri_compare(const schema& s, partition_key_view other)
// should correctly compare empty keys. The fact we did this incorrectly was
// noticed while fixing #9375, and a separate issue on it is #10178.
BOOST_AUTO_TEST_CASE(test_empty_key_view_comparison) {
    auto s_ptr = schema_builder("", "")
            .with_column("p", bytes_type, column_kind::partition_key)
            .build();
    const schema& s = *s_ptr;

    sstables::key empty_sstable_key = sstables::key::from_deeply_exploded(s, {data_value(bytes(""))});
    sstables::key_view empty_sstable_key_view = empty_sstable_key;
    partition_key empty_partition_key = partition_key::from_deeply_exploded(s, {data_value(bytes(""))});
    partition_key_view empty_partition_key_view = empty_partition_key;

    // Two empty keys should be equal (this check failed in #10178)
    BOOST_CHECK_EQUAL(std::strong_ordering::equal,
        empty_sstable_key_view.tri_compare(s, empty_partition_key_view));

    // For completeness, compare also an empty key to a non-empty key, and
    // two equal non-empty keys. An empty key is supposed to be less-than
    // a non-empty key.
    sstables::key hello_sstable_key = sstables::key::from_deeply_exploded(s, {data_value(bytes("hello"))});
    sstables::key_view hello_sstable_key_view = hello_sstable_key;
    partition_key hello_partition_key = partition_key::from_deeply_exploded(s, {data_value(bytes("hello"))});
    partition_key_view hello_partition_key_view = hello_partition_key;
    BOOST_CHECK_EQUAL(std::strong_ordering::less,
        empty_sstable_key_view.tri_compare(s, hello_partition_key_view));
    BOOST_CHECK_EQUAL(std::strong_ordering::greater,
        hello_sstable_key_view.tri_compare(s, empty_partition_key_view));
    BOOST_CHECK_EQUAL(std::strong_ordering::equal,
        hello_sstable_key_view.tri_compare(s, hello_partition_key_view));

    // The underlying cause of #10178 was that legacy_form() returned
    // a legacy_compound_view<> which, despite being empty, did not
    // have begin()==end(). So let's reproduce that directly:
    auto lf = empty_partition_key_view.legacy_form(s);
    BOOST_CHECK_EQUAL(0, lf.size());
    BOOST_CHECK(lf.begin() == lf.end());
}

// Test that sstables::parse_path is able to parse the paths of sstables
BOOST_AUTO_TEST_CASE(test_parse_path_good) {
    struct sstable_case {
        std::string_view path;
        std::string_view ks;
        std::string_view cf;
        sstables::entry_descriptor desc;
    };
    const sstable_case sstables[] = {
        {
            "/scylla/system/truncated-38c19fd0fb863310a4b70d0cc66628aa/mc-2-big-Data.db",
            "system",
            "truncated",
            entry_descriptor{
                generation_type{2},
                sstable_version_types::mc,
                sstable_format_types::big,
                component_type::Data
            }
        },
        {
            "/scylla/system/scylla_local-2972ec7ffb2038ddaac1d876f2e3fcbd/mc-3-big-Summary.db",
            "system",
            "scylla_local",
            entry_descriptor{
                generation_type{3},
                sstable_version_types::mc,
                sstable_format_types::big,
                component_type::Summary
            }
        },
        {
            "/scylla/system_distributed/cdc_generation_timestamps-fdf455c4cfec3e009719d7a45436c89d/me-3g9p_0938_0ecz429c6f019i7yuf-big-Index.db",
            "system_distributed",
            "cdc_generation_timestamps",
            entry_descriptor{
                generation_type::from_string("3g9p_0938_0ecz429c6f019i7yuf"),
                sstable_version_types::me,
                sstable_format_types::big,
                component_type::Index
            }
         },
         {
            "/system_schema/columns-24101c25a2ae3af787c1b40ee1aca33f/md-3g9r_04ux_4be4w2d7t8bg6u7nok-big-Statistics.db",
            "system_schema",
            "columns",
            entry_descriptor{
                generation_type::from_string("3g9r_04ux_4be4w2d7t8bg6u7nok"),
                sstable_version_types::md,
                sstable_format_types::big,
                component_type::Statistics
            }
        },
        {
            "/system_schema/columns-24101c25a2ae3af787c1b40ee1aca33f/md-3g9r_04ux_4be4w2d7t8bg6u7nok-big-ReallyBigData.db",
            "system_schema",
            "columns",
            entry_descriptor{
                generation_type::from_string("3g9r_04ux_4be4w2d7t8bg6u7nok"),
                sstable_version_types::md,
                sstable_format_types::big,
                component_type::Unknown
            }
        }
    };
    for (auto& [path, expected_ks, expected_cf, expected_desc] : sstables) {
        auto [desc, ks, cf] = parse_path(path);
        BOOST_CHECK_EQUAL(ks, expected_ks);
        BOOST_CHECK_EQUAL(cf, expected_cf);
        BOOST_CHECK_EQUAL(expected_desc.generation, desc.generation);
        BOOST_CHECK_EQUAL(expected_desc.version, desc.version);
        BOOST_CHECK_EQUAL(expected_desc.format, desc.format);
        BOOST_CHECK_EQUAL(expected_desc.component, desc.component);
    }
}

// Test that sstables::parse_path throws at seeing malformed sstable names
BOOST_AUTO_TEST_CASE(test_parse_path_bad) {
    const std::string_view paths[] = {
        "",
        "/",
        "=",
        "hmm",
        "//-/-",
        "mc-2-big-Data.db",
        "truncated~38c19fd0fb863310a4b70d0cc66628aa/",
        "truncated-38c19fd0fb863310a4b70d0cc66628aa/mc-2-big-Data.db",
        "truncated~38c19fd0fb863310a4b70d0cc66628aa/mc-2-big-Data.db",
        "/scylla/system/truncated~38c19fd0fb863310a4b70d0cc66628aa/404.db",
        "/scylla/system/truncated~38c19fd0fb863310a4b70d0cc66628aa/mc/foo/bar.db",
        "/scylla/system/truncated~38c19fd0fb863310a4b70d0cc66628aa/mc/-------",
        "/scylla/system/truncated~38c19fd0fb863310a4b70d0cc66628aa/mc-2-big-Data.db",
        "/scylla/system/truncated-38c19fd0fb863310a4b70d0cc66628aa/zz-2-big-Data.db",
        "/scylla/system/truncated-38c19fd0fb863310a4b70d0cc66628aa/mc-x-big-Data.db",
        "/scylla/system/truncated-38c19fd0fb863310a4b70d0cc66628aa/mc-i~am~not~an~id-big-Data.db",
        "/scylla/system/truncated~38c19fd0fb863310a4b70d0cc66628aa/mc-2--Data.db",
        "/scylla/system/truncated~38c19fd0fb863310a4b70d0cc66628aa/mc-2-grand-Data.db",
    };
    for (auto path : paths) {
        BOOST_CHECK_THROW(parse_path(path), std::exception);
    }
}
