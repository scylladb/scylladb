/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <seastar/core/sstring.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/align.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/closeable.hh>

#include "sstables/sstables.hh"
#include "compaction/compaction_manager.hh"
#include "sstables/key.hh"
#include "test/lib/sstable_utils.hh"
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "schema.hh"
#include "compress.hh"
#include "database.hh"
#include <memory>
#include "test/boost/sstable_test.hh"
#include "test/lib/tmpdir.hh"
#include "partition_slice_builder.hh"
#include "test/lib/test_services.hh"
#include "cell_locking.hh"
#include "sstables/sstable_mutation_reader.hh"
#include "test/lib/schema_registry.hh"

#include <boost/range/combine.hpp>

using namespace sstables;

bytes as_bytes(const sstring& s) {
    return { reinterpret_cast<const int8_t*>(s.data()), s.size() };
}

future<> test_using_working_sst(schema_ptr s, sstring dir, int64_t gen) {
    return test_env::do_with([s = std::move(s), dir = std::move(dir), gen] (test_env& env) {
        return env.working_sst(std::move(s), std::move(dir), gen);
    });
}

SEASTAR_TEST_CASE(uncompressed_data) {
    return test_using_working_sst(uncompressed_schema(), uncompressed_dir(), 1);
}

static auto make_schema_for_compressed_sstable(::schema_registry& registry) {
    return make_shared_schema(registry, {}, "ks", "cf", {{"pk", utf8_type}}, {}, {}, {}, utf8_type);
}

SEASTAR_THREAD_TEST_CASE(compressed_data) {
    tests::schema_registry_wrapper registry;
    auto s = make_schema_for_compressed_sstable(registry);
    test_using_working_sst(std::move(s), "test/resource/sstables/compressed", 1).get();
}

SEASTAR_TEST_CASE(composite_index) {
    return test_using_working_sst(composite_schema(), "test/resource/sstables/composite", 1);
}

template<typename Func>
inline auto
test_using_reusable_sst(schema_ptr s, sstring dir, unsigned long gen, Func&& func) {
    return test_env::do_with([s = std::move(s), dir = std::move(dir), gen, func = std::move(func)] (test_env& env) {
        return env.reusable_sst(std::move(s), std::move(dir), gen).then([&env, func = std::move(func)] (sstable_ptr sst) mutable {
            return func(env, std::move(sst));
        });
    });
}

future<std::vector<partition_key>> index_read(schema_ptr schema, sstring path) {
    return test_using_reusable_sst(std::move(schema), std::move(path), 1, [] (test_env& env, sstable_ptr ptr) {
        return sstables::test(ptr).read_indexes(env.make_reader_permit()).then([] (auto&& indexes) {
            return boost::copy_range<std::vector<partition_key>>(
                    indexes | boost::adaptors::transformed([] (const sstables::test::index_entry& e) { return e.key; }));
        });
    });
}

SEASTAR_TEST_CASE(simple_index_read) {
    return index_read(uncompressed_schema(), uncompressed_dir()).then([] (auto vec) {
        BOOST_REQUIRE(vec.size() == 4);
    });
}

SEASTAR_TEST_CASE(composite_index_read) {
    return index_read(composite_schema(), "test/resource/sstables/composite").then([] (auto vec) {
        BOOST_REQUIRE(vec.size() == 20);
    });
}

template<uint64_t Position, uint64_t EntryPosition, uint64_t EntryKeySize>
future<> summary_query(schema_ptr schema, sstring path, int generation) {
    return test_using_reusable_sst(std::move(schema), path, generation, [] (test_env& env, sstable_ptr ptr) {
        return sstables::test(ptr).read_summary_entry(Position).then([ptr] (auto entry) {
            BOOST_REQUIRE(entry.position == EntryPosition);
            BOOST_REQUIRE(entry.key.size() == EntryKeySize);
            return make_ready_future<>();
        });
    });
}

template<uint64_t Position, uint64_t EntryPosition, uint64_t EntryKeySize>
future<> summary_query_fail(schema_ptr schema, sstring path, int generation) {
    return summary_query<Position, EntryPosition, EntryKeySize>(std::move(schema), path, generation).then_wrapped([] (auto fut) {
        try {
            fut.get();
        } catch (std::out_of_range& ok) {
            return make_ready_future<>();
        }
        return make_ready_future<>();
    });
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
        return make_ready_future<>();
    });
}

static future<sstable_ptr> do_write_sst(test_env& env, schema_ptr schema, sstring load_dir, sstring write_dir, unsigned long generation) {
    return env.reusable_sst(std::move(schema), load_dir, generation).then([write_dir, generation] (sstable_ptr sst) {
        sstables::test(sst).change_generation_number(generation + 1);
        sstables::test(sst).change_dir(write_dir);
        auto fut = sstables::test(sst).store();
        return std::move(fut).then([sst = std::move(sst)] {
            return make_ready_future<sstable_ptr>(std::move(sst));
        });
    });
}

static future<> write_sst_info(schema_ptr schema, sstring load_dir, sstring write_dir, unsigned long generation) {
    return test_env::do_with([schema = std::move(schema), load_dir = std::move(load_dir), write_dir = std::move(write_dir), generation] (test_env& env) {
        return do_write_sst(env, std::move(schema), load_dir, write_dir, generation).then([] (auto ptr) { return make_ready_future<>(); });
    });
}

using bufptr_t = std::unique_ptr<char [], free_deleter>;
static future<std::pair<bufptr_t, size_t>> read_file(sstring file_path)
{
    return open_file_dma(file_path, open_flags::ro).then([] (file f) {
        return f.size().then([f] (auto size) mutable {
            auto aligned_size = align_up(size, 512UL);
            auto buf = allocate_aligned_buffer<char>(aligned_size, 512UL);
            auto rbuf = buf.get();
            ::memset(rbuf, 0, aligned_size);
            return f.dma_read(0, rbuf, aligned_size).then([size, buf = std::move(buf), f] (auto ret) mutable {
                BOOST_REQUIRE(ret == size);
                std::pair<bufptr_t, size_t> p(std::move(buf), std::move(size));
                return make_ready_future<std::pair<bufptr_t, size_t>>(std::move(p));
            }).finally([f] () mutable { return f.close().finally([f] {}); });
        });
    });
}

struct sstdesc {
    sstring dir;
    int64_t gen;
};

static future<> compare_files(sstdesc file1, sstdesc file2, component_type component) {
    auto file_path = sstable::filename(file1.dir, "ks", "cf", la, file1.gen, big, component);
    return read_file(file_path).then([component, file2] (auto ret) {
        auto file_path = sstable::filename(file2.dir, "ks", "cf", la, file2.gen, big, component);
        return read_file(file_path).then([ret = std::move(ret)] (auto ret2) {
            // assert that both files have the same size.
            BOOST_REQUIRE(ret.second == ret2.second);
            // assert that both files have the same content.
            BOOST_REQUIRE(::memcmp(ret.first.get(), ret2.first.get(), ret.second) == 0);
            // free buf from both files.
        });
    });

}

static future<> check_component_integrity(component_type component) {
    auto tmp = make_lw_shared<tmpdir>();
    auto registry = std::make_unique<tests::schema_registry_wrapper>();
    auto s = make_schema_for_compressed_sstable(*registry);
    return write_sst_info(s, "test/resource/sstables/compressed", tmp->path().string(), 1).then([component, tmp] {
        return compare_files(sstdesc{"test/resource/sstables/compressed", 1 },
                             sstdesc{tmp->path().string(), 2 },
                             component);
    }).then([tmp, registry = std::move(registry)] {});
}

SEASTAR_TEST_CASE(check_compressed_info_func) {
    return check_component_integrity(component_type::CompressionInfo);
}

future<>
write_and_validate_sst(schema_ptr s, sstring dir, noncopyable_function<future<> (shared_sstable sst1, shared_sstable sst2)> func) {
    return test_env::do_with(tmpdir(), [s = std::move(s), dir = std::move(dir), func = std::move(func)] (test_env& env, tmpdir& tmp) mutable {
        return do_write_sst(env, s, dir, tmp.path().string(), 1).then([&env, &tmp, s = std::move(s), func = std::move(func)] (auto sst1) {
            auto sst2 = env.make_sstable(s, tmp.path().string(), 2, sst1->get_version());
            return func(std::move(sst1), std::move(sst2));
        });
    });
}

SEASTAR_TEST_CASE(check_summary_func) {
    auto registry = std::make_unique<tests::schema_registry_wrapper>();
    auto s = make_schema_for_compressed_sstable(*registry);
    return write_and_validate_sst(std::move(s), "test/resource/sstables/compressed", [] (shared_sstable sst1, shared_sstable sst2) {
        return sstables::test(sst2).read_summary().then([sst1, sst2] {
            summary& sst1_s = sstables::test(sst1).get_summary();
            summary& sst2_s = sstables::test(sst2).get_summary();

            BOOST_REQUIRE(::memcmp(&sst1_s.header, &sst2_s.header, sizeof(summary::header)) == 0);
            BOOST_REQUIRE(sst1_s.positions == sst2_s.positions);
            BOOST_REQUIRE(sst1_s.entries == sst2_s.entries);
            BOOST_REQUIRE(sst1_s.first_key.value == sst2_s.first_key.value);
            BOOST_REQUIRE(sst1_s.last_key.value == sst2_s.last_key.value);
            return make_ready_future<>();
        });
    }).finally([registry = std::move(registry)] {});
}

SEASTAR_TEST_CASE(check_filter_func) {
    return check_component_integrity(component_type::Filter);
}

SEASTAR_TEST_CASE(check_statistics_func) {
    auto registry = std::make_unique<tests::schema_registry_wrapper>();
    auto s = make_schema_for_compressed_sstable(*registry);
    return write_and_validate_sst(std::move(s), "test/resource/sstables/compressed", [] (shared_sstable sst1, shared_sstable sst2) {
        return sstables::test(sst2).read_statistics().then([sst1, sst2] {
            statistics& sst1_s = sstables::test(sst1).get_statistics();
            statistics& sst2_s = sstables::test(sst2).get_statistics();

            BOOST_REQUIRE(sst1_s.offsets.elements.size() == sst2_s.offsets.elements.size());
            BOOST_REQUIRE(sst1_s.contents.size() == sst2_s.contents.size());

            for (auto&& e : boost::combine(sst1_s.offsets.elements, sst2_s.offsets.elements)) {
                BOOST_REQUIRE(boost::get<0>(e).second ==  boost::get<1>(e).second);
            }
            // TODO: compare the field contents from both sstables.
            return make_ready_future<>();
        });
    }).finally([registry = std::move(registry)] {});
}

SEASTAR_TEST_CASE(check_toc_func) {
    auto registry = std::make_unique<tests::schema_registry_wrapper>();
    auto s = make_schema_for_compressed_sstable(*registry);
    return write_and_validate_sst(std::move(s), "test/resource/sstables/compressed", [] (shared_sstable sst1, shared_sstable sst2) {
        return sstables::test(sst2).read_toc().then([sst1, sst2] {
            auto& sst1_c = sstables::test(sst1).get_components();
            auto& sst2_c = sstables::test(sst2).get_components();

            BOOST_REQUIRE(sst1_c == sst2_c);
            return make_ready_future<>();
        });
    }).finally([registry = std::move(registry)] {});
}

SEASTAR_TEST_CASE(uncompressed_random_access_read) {
    return test_using_reusable_sst(uncompressed_schema(), uncompressed_dir(), 1, [this] (auto& env, auto sstp) {
        // note: it's important to pass on a shared copy of sstp to prevent its
        // destruction until the continuation finishes reading!
        return sstables::test(sstp).data_read(env.make_reader_permit(), 97, 6).then([sstp] (temporary_buffer<char> buf) {
            BOOST_REQUIRE(sstring(buf.get(), buf.size()) == "gustaf");
            return make_ready_future<>();
        });
    });
}

SEASTAR_TEST_CASE(compressed_random_access_read) {
    auto registry = std::make_unique<tests::schema_registry_wrapper>();
    auto s = make_schema_for_compressed_sstable(*registry);
    return test_using_reusable_sst(std::move(s), "test/resource/sstables/compressed", 1, [this] (auto& env, auto sstp) {
        return sstables::test(sstp).data_read(env.make_reader_permit(), 97, 6).then([sstp] (temporary_buffer<char> buf) {
            BOOST_REQUIRE(sstring(buf.get(), buf.size()) == "gustaf");
            return make_ready_future<>();
        });
    }).finally([registry = std::move(registry)] {});
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
        return make_ready_future<>();
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
        return make_ready_future<>();
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
        return make_ready_future<>();
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
        return make_ready_future<>();
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
        return make_ready_future<>();
    });
}

SEASTAR_TEST_CASE(full_index_search) {
    return test_using_reusable_sst(uncompressed_schema(), uncompressed_dir(), 1, [] (auto& env, auto sstp) {
        return sstables::test(sstp).read_indexes(env.make_reader_permit()).then([sstp] (auto&& index_list) {
            int idx = 0;
            for (auto& e : index_list) {
                auto key = key::from_partition_key(*sstp->get_schema(), e.key);
                BOOST_REQUIRE(sstables::test(sstp).binary_search(sstp->get_schema()->get_partitioner(), index_list, key) == idx++);
            }
        });
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
        return make_ready_future<>();
    });
}

// See CASSANDRA-7593. This sstable writes 0 in the range_start. We need to handle that case as well
SEASTAR_TEST_CASE(wrong_range) {
    return test_using_reusable_sst(uncompressed_schema(), "test/resource/sstables/wrongrange", 114, [] (auto& env, auto sstp) {
        return do_with(dht::partition_range::make_singular(make_dkey(uncompressed_schema(), "todata")), [&env, sstp] (auto& range) {
            auto s = columns_schema();
            return with_closeable(sstp->make_reader(s, env.make_reader_permit(), range, s->full_slice()), [sstp, s] (auto& rd) {
              return read_mutation_from_flat_mutation_reader(rd).then([sstp, s] (auto mutation) {
                return make_ready_future<>();
              });
            });
        });
    });
}

static future<>
test_sstable_exists(sstring dir, unsigned long generation, bool exists) {
    auto file_path = sstable::filename(dir, "ks", "cf", la, generation, big, component_type::Data);
    return open_file_dma(file_path, open_flags::ro).then_wrapped([exists] (future<file> f) {
        if (exists) {
            BOOST_CHECK_NO_THROW(f.get0());
        } else {
            BOOST_REQUIRE_THROW(f.get0(), std::system_error);
        }
        return make_ready_future<>();
    });
}

// We need to be careful not to allow failures in this test to contaminate subsequent runs.
// We will therefore run it in an empty directory, and first link a known SSTable from another
// directory to it.
SEASTAR_TEST_CASE(set_generation) {
    return test_setup::do_with_cloned_tmp_directory(uncompressed_dir(), [] (test_env& env, sstring uncompressed_dir, sstring generation_dir) {
        return env.reusable_sst(uncompressed_schema(), uncompressed_dir, 1).then([generation_dir] (auto sstp) {
            return sstp->create_links(generation_dir).then([sstp] {});
        }).then([&env, generation_dir] {
            return env.reusable_sst(uncompressed_schema(), generation_dir, 1).then([] (auto sstp) {
                return sstp->set_generation(2).then([sstp] {});
            });
        }).then([generation_dir] {
            return test_sstable_exists(generation_dir, 1, false);
        }).then([uncompressed_dir, generation_dir] {
            return compare_files(sstdesc{uncompressed_dir, 1 },
                                 sstdesc{generation_dir, 2 },
                                 component_type::Data);
        });
    });
}

SEASTAR_TEST_CASE(statistics_rewrite) {
    return test_setup::do_with_cloned_tmp_directory(uncompressed_dir(), [] (test_env& env, sstring uncompressed_dir, sstring generation_dir) {
        return env.reusable_sst(uncompressed_schema(), uncompressed_dir, 1).then([generation_dir] (auto sstp) {
            return sstp->create_links(generation_dir).then([sstp] {});
        }).then([generation_dir] {
            return test_sstable_exists(generation_dir, 1, true);
        }).then([&env, generation_dir] {
            return env.reusable_sst(uncompressed_schema(), generation_dir, 1).then([] (auto sstp) {
                // mutate_sstable_level results in statistics rewrite
                return sstp->mutate_sstable_level(10).then([sstp] {});
            });
        }).then([&env, generation_dir] {
            return env.reusable_sst(uncompressed_schema(), generation_dir, 1).then([] (auto sstp) {
                BOOST_REQUIRE(sstp->get_sstable_level() == 10);
                return make_ready_future<>();
            });
        });
    });
}

// Tests for reading a large partition for which the index contains a
// "promoted index", i.e., a sample of the column names inside the partition,
// with which we can avoid reading the entire partition when we look only
// for a specific subset of columns. The test sstable for the read test was
// generated in Cassandra.

static schema_ptr large_partition_schema(::schema_registry& registry) {
    return registry.get_or_load(db::system_keyspace::generate_schema_version("try1", "data"), [&registry] (table_schema_version v) {
        schema_builder builder(
                registry,
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
       );
       builder.with_version(v);
       return builder.build(schema_builder::compact_storage::no);
    });
}

static future<shared_sstable> load_large_partition_sst(::schema_registry& registry, test_env& env, const sstables::sstable::version_types version) {
    auto s = large_partition_schema(registry);
    auto dir = get_test_dir("large_partition", s);
    return env.reusable_sst(std::move(s), std::move(dir), 3, version);
}

// This is a rudimentary test that reads an sstable exported from Cassandra
// which contains a promoted index. It just checks that the promoted index
// is read from disk, as an unparsed array, and doesn't actually use it to
// search for anything.
SEASTAR_TEST_CASE(promoted_index_read) {
  auto registry = std::make_unique<tests::schema_registry_wrapper>();
  return for_each_sstable_version([registry = std::move(registry)] (const sstables::sstable::version_types version) {
    return test_env::do_with([&registry = *registry, version] (test_env& env) {
      return load_large_partition_sst(registry, env, version).then([&registry, &env] (auto sstp) {
        schema_ptr s = large_partition_schema(registry);
        return sstables::test(sstp).read_indexes(env.make_reader_permit()).then([sstp] (std::vector<sstables::test::index_entry> vec) {
            BOOST_REQUIRE(vec.size() == 1);
            BOOST_REQUIRE(vec[0].promoted_index_size > 0);
        });
      });
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
        auto mfopt = rd().get0();
        if (!mfopt) {
            return 0;
        }
        int nrows = 0;
        mfopt = rd().get0();
        while (mfopt) {
            if (mfopt->is_clustering_row()) {
                nrows++;
            }
            mfopt = rd().get0();
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
        auto mfopt = rd().get0();
        if (!mfopt) {
            return 0;
        }
        int nrows = 0;
        mfopt = rd().get0();
        while (mfopt) {
            if (mfopt->is_clustering_row()) {
                nrows++;
            }
            mfopt = rd().get0();
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
        auto mfopt = reader().get0();
        while (mfopt) {
            mfopt = reader().get0();
            BOOST_REQUIRE(mfopt);
            while (!mfopt->is_end_of_partition()) {
                if (mfopt->is_clustering_row()) {
                    nrows++;
                }
                mfopt = reader().get0();
            }
            mfopt = reader().get0();
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
  auto registry = std::make_unique<tests::schema_registry_wrapper>();
  schema_ptr s = large_partition_schema(*registry);
  return for_each_sstable_version([registry = std::move(registry), s] (const sstables::sstable::version_types version) {
    return test_env::do_with_async([&registry = *registry, s, version] (test_env& env) {
        auto sstp = load_large_partition_sst(registry, env, version).get();
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
  auto registry = std::make_unique<tests::schema_registry_wrapper>();
  schema_ptr s = large_partition_schema(*registry);
  return for_each_sstable_version([s, registry = std::move(registry)] (const sstables::sstable::version_types version) {
   return test_env::do_with_async([s, version] (test_env& env) {
        auto sstp = load_large_partition_sst(*s->registry(), env, version).get();
        auto nrows = count_rows(env, sstp, s, "18wX", "18xB").get();
        BOOST_REQUIRE(nrows == 5);
   });
  });
}

SEASTAR_TEST_CASE(test_skipping_in_compressed_stream) {
    return seastar::async([] {
        tmpdir tmp;
        auto file_path = (tmp.path() / "test").string();
        file f = open_file_dma(file_path, open_flags::create | open_flags::wo).get0();

        file_input_stream_options opts;
        opts.read_ahead = 0;

        compression_parameters cp({
            { compression_parameters::SSTABLE_COMPRESSION, "LZ4Compressor" },
            { compression_parameters::CHUNK_LENGTH_KB, std::to_string(opts.buffer_size/1024) },
        });

        sstables::compression c;
        // this initializes "c"
        auto os = make_file_output_stream(f, file_output_stream_options()).get0();
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

        auto compressed_size = f.size().get0();
        c.update(compressed_size);

        auto make_is = [&] {
            f = open_file_dma(file_path, open_flags::ro).get0();
            return make_compressed_file_m_format_input_stream(f, &c, 0, uncompressed_size, opts);
        };

        auto expect = [] (input_stream<char>& in, const temporary_buffer<char>& buf) {
            auto b = in.read_exactly(buf.size()).get0();
            BOOST_REQUIRE(b == buf);
        };

        auto expect_eof = [] (input_stream<char>& in) {
            auto b = in.read().get0();
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
