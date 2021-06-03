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
#include "sstables/compaction_manager.hh"
#include "sstables/key.hh"
#include "test/lib/sstable_utils.hh"
#include <seastar/testing/test_case.hh>
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
#include "sstables/kl/reader_impl.hh"

#include <boost/range/combine.hpp>

using namespace sstables;
using row_consumer = sstables::kl::row_consumer;
using data_consume_rows_context = sstables::kl::data_consume_rows_context;

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

SEASTAR_TEST_CASE(compressed_data) {
    auto s = make_shared_schema({}, "ks", "cf", {}, {}, {}, {}, utf8_type);
    return test_using_working_sst(std::move(s), "test/resource/sstables/compressed", 1);
}

SEASTAR_TEST_CASE(composite_index) {
    return test_using_working_sst(composite_schema(), "test/resource/sstables/composite", 1);
}

template<typename Func>
inline auto
test_using_reusable_sst(schema_ptr s, sstring dir, unsigned long gen, Func&& func) {
    return test_env::do_with([s = std::move(s), dir = std::move(dir), gen, func = std::move(func)] (test_env& env) {
        return env.reusable_sst(std::move(s), std::move(dir), gen).then([func = std::move(func)] (sstable_ptr sst) {
            return func(std::move(sst));
        });
    });
}

future<index_list> index_read(schema_ptr schema, sstring path) {
    return test_using_reusable_sst(std::move(schema), std::move(path), 1, [] (sstable_ptr ptr) {
        return sstables::test(ptr).read_indexes();
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
    return test_using_reusable_sst(std::move(schema), path, generation, [] (sstable_ptr ptr) {
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
    return test_using_reusable_sst(uncompressed_schema(), uncompressed_dir(), 2, [] (shared_sstable ptr) {
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
    auto s = make_shared_schema({}, "ks", "cf", {}, {}, {}, {}, utf8_type);
    return write_sst_info(s, "test/resource/sstables/compressed", tmp->path().string(), 1).then([component, tmp] {
        return compare_files(sstdesc{"test/resource/sstables/compressed", 1 },
                             sstdesc{tmp->path().string(), 2 },
                             component);
    }).then([tmp] {});
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
    auto s = make_shared_schema({}, "ks", "cf", {}, {}, {}, {}, utf8_type);
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
    });
}

SEASTAR_TEST_CASE(check_filter_func) {
    return check_component_integrity(component_type::Filter);
}

SEASTAR_TEST_CASE(check_statistics_func) {
    auto s = make_shared_schema({}, "ks", "cf", {}, {}, {}, {}, utf8_type);
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
    });
}

SEASTAR_TEST_CASE(check_toc_func) {
    auto s = make_shared_schema({}, "ks", "cf", {}, {}, {}, {}, utf8_type);
    return write_and_validate_sst(std::move(s), "test/resource/sstables/compressed", [] (shared_sstable sst1, shared_sstable sst2) {
        return sstables::test(sst2).read_toc().then([sst1, sst2] {
            auto& sst1_c = sstables::test(sst1).get_components();
            auto& sst2_c = sstables::test(sst2).get_components();

            BOOST_REQUIRE(sst1_c == sst2_c);
            return make_ready_future<>();
        });
    });
}

SEASTAR_TEST_CASE(uncompressed_random_access_read) {
    return test_using_reusable_sst(uncompressed_schema(), uncompressed_dir(), 1, [] (auto sstp) {
        // note: it's important to pass on a shared copy of sstp to prevent its
        // destruction until the continuation finishes reading!
        return sstables::test(sstp).data_read(97, 6).then([sstp] (temporary_buffer<char> buf) {
            BOOST_REQUIRE(sstring(buf.get(), buf.size()) == "gustaf");
            return make_ready_future<>();
        });
    });
}

SEASTAR_TEST_CASE(compressed_random_access_read) {
    auto s = make_shared_schema({}, "ks", "cf", {}, {}, {}, {}, utf8_type);
    return test_using_reusable_sst(std::move(s), "test/resource/sstables/compressed", 1, [] (auto sstp) {
        return sstables::test(sstp).data_read(97, 6).then([sstp] (temporary_buffer<char> buf) {
            BOOST_REQUIRE(sstring(buf.get(), buf.size()) == "gustaf");
            return make_ready_future<>();
        });
    });
}

class test_row_consumer : public row_consumer {
public:
    const int64_t desired_timestamp;
    int count_row_start = 0;
    int count_cell = 0;
    int count_deleted_cell = 0;
    int count_range_tombstone = 0;
    int count_row_end = 0;

    test_row_consumer(int64_t t)
        : row_consumer(tests::make_permit(), tracing::trace_state_ptr()
        , default_priority_class()), desired_timestamp(t) {
    }

    virtual proceed consume_row_start(sstables::key_view key, sstables::deletion_time deltime) override {
        BOOST_REQUIRE(bytes_view(key) == as_bytes("vinna"));
        BOOST_REQUIRE(deltime.local_deletion_time == std::numeric_limits<int32_t>::max());
        BOOST_REQUIRE(deltime.marked_for_delete_at == std::numeric_limits<int64_t>::min());
        count_row_start++;
        return proceed::yes;
    }

    virtual proceed consume_cell(bytes_view col_name, fragmented_temporary_buffer::view value,
            int64_t timestamp, int64_t ttl, int64_t expiration) override {
        BOOST_REQUIRE(ttl == 0);
        BOOST_REQUIRE(expiration == 0);
        switch (count_cell) {
        case 0:
            // The silly "cql marker" column
            BOOST_REQUIRE(col_name.size() == 3 && col_name[0] == 0 && col_name[1] == 0 && col_name[2] == 0);
            BOOST_REQUIRE(value.size_bytes() == 0);
            BOOST_REQUIRE(timestamp == desired_timestamp);
            break;
        case 1:
            BOOST_REQUIRE(col_name.size() == 7 && col_name[0] == 0 &&
                    col_name[1] == 4 && col_name[2] == 'c' &&
                    col_name[3] == 'o' && col_name[4] == 'l' &&
                    col_name[5] == '1' && col_name[6] == '\0');
            BOOST_REQUIRE(linearized(value) == as_bytes("daughter"));
            BOOST_REQUIRE(timestamp == desired_timestamp);
            break;
        case 2:
            BOOST_REQUIRE(col_name.size() == 7 && col_name[0] == 0 &&
                    col_name[1] == 4 && col_name[2] == 'c' &&
                    col_name[3] == 'o' && col_name[4] == 'l' &&
                    col_name[5] == '2' && col_name[6] == '\0');
            BOOST_REQUIRE(linearized(value) == bytes({0, 0, 0, 3}));
            BOOST_REQUIRE(timestamp == desired_timestamp);
            break;
        }
        count_cell++;
        return proceed::yes;
    }

    virtual proceed consume_counter_cell(bytes_view col_name, fragmented_temporary_buffer::view value, int64_t timestamp) override {
        BOOST_FAIL("counter cell wasn't expected");
        abort(); // BOOST_FAIL is not marked as [[noreturn]].
    }

    virtual proceed consume_deleted_cell(bytes_view col_name, sstables::deletion_time deltime) override {
        count_deleted_cell++;
        return proceed::yes;
    }

    virtual proceed consume_shadowable_row_tombstone(bytes_view col_name, sstables::deletion_time deltime) override {
        BOOST_FAIL("shdowable row tombstone wasn't expected");
        abort(); // BOOST_FAIL is not marked as [[noreturn]].
    }

    virtual proceed consume_range_tombstone(
            bytes_view start_col, bytes_view end_col,
            sstables::deletion_time deltime) override {
        count_range_tombstone++;
        return proceed::yes;
    }

    virtual proceed consume_row_end() override {
        count_row_end++;
        return proceed::yes;
    }

    virtual void reset(indexable_element) override { }

    virtual position_in_partition_view position() override {
        return position_in_partition_view(position_in_partition_view::partition_start_tag_t{});
    }
};

SEASTAR_TEST_CASE(uncompressed_rows_read_one) {
    return test_using_reusable_sst(uncompressed_schema(), uncompressed_dir(), 1, [] (auto sstp) {
        return do_with(test_row_consumer(1418656871665302), [sstp] (auto& c) {
            auto context = data_consume_rows<data_consume_rows_context>(*uncompressed_schema(), sstp, c, {0, 95}, 95);
            auto fut = context->consume_input();
            return fut.then([sstp, &c, context = std::move(context)] {
                BOOST_REQUIRE(c.count_row_start == 1);
                BOOST_REQUIRE(c.count_cell == 3);
                BOOST_REQUIRE(c.count_deleted_cell == 0);
                BOOST_REQUIRE(c.count_row_end == 1);
                BOOST_REQUIRE(c.count_range_tombstone == 0);
                return make_ready_future<>();
            });
        });
    });
}

SEASTAR_TEST_CASE(compressed_rows_read_one) {
    auto s = make_shared_schema({}, "ks", "cf", {}, {}, {}, {}, utf8_type);
    return test_using_reusable_sst(std::move(s), "test/resource/sstables/compressed", 1, [] (auto sstp) {
        return do_with(test_row_consumer(1418654707438005), [sstp] (auto& c) {
            auto context = data_consume_rows<data_consume_rows_context>(*uncompressed_schema(), sstp, c, {0, 95}, 95);
            auto fut = context->consume_input();
            return fut.then([sstp, &c, context = std::move(context)] {
                BOOST_REQUIRE(c.count_row_start == 1);
                BOOST_REQUIRE(c.count_cell == 3);
                BOOST_REQUIRE(c.count_deleted_cell == 0);
                BOOST_REQUIRE(c.count_row_end == 1);
                BOOST_REQUIRE(c.count_range_tombstone == 0);
                return make_ready_future<>();
            });
        });
    });
}

// Tests for iterating over all rows.

class count_row_consumer : public row_consumer {
public:
    int count_row_start = 0;
    int count_cell = 0;
    int count_deleted_cell = 0;
    int count_row_end = 0;
    int count_range_tombstone = 0;

    count_row_consumer()
        : row_consumer(tests::make_permit(), tracing::trace_state_ptr(), default_priority_class()) {
    }

    virtual proceed consume_row_start(sstables::key_view key, sstables::deletion_time deltime) override {
        count_row_start++;
        return proceed::yes;
    }
    virtual proceed consume_cell(bytes_view col_name, fragmented_temporary_buffer::view value,
            int64_t timestamp, int64_t ttl, int64_t expiration) override {
        count_cell++;
        return proceed::yes;
    }
    virtual proceed consume_counter_cell(bytes_view col_name, fragmented_temporary_buffer::view value, int64_t timestamp) override {
        count_cell++;
        return proceed::yes;
    }
    virtual proceed consume_deleted_cell(bytes_view col_name, sstables::deletion_time deltime) override {
        count_deleted_cell++;
        return proceed::yes;
    }
    virtual proceed consume_row_end() override {
        count_row_end++;
        return proceed::yes;
    }
    virtual proceed consume_shadowable_row_tombstone(bytes_view col_name, sstables::deletion_time deltime) override {
        count_deleted_cell++;
        return proceed::yes;
    }
    virtual proceed consume_range_tombstone(
            bytes_view start_col, bytes_view end_col,
            sstables::deletion_time deltime) override {
        count_range_tombstone++;
        return proceed::yes;
    }
    virtual void reset(indexable_element) override { }
    virtual position_in_partition_view position() override {
        return position_in_partition_view(position_in_partition_view::partition_start_tag_t{});
    }
};


SEASTAR_TEST_CASE(uncompressed_rows_read_all) {
    return test_using_reusable_sst(uncompressed_schema(), uncompressed_dir(), 1, [] (auto sstp) {
        return do_with(count_row_consumer(), [sstp] (auto& c) {
            auto context = data_consume_rows<data_consume_rows_context>(*uncompressed_schema(), sstp, c);
            auto fut = context->consume_input();
            return fut.then([sstp, &c, context = std::move(context)] {
                BOOST_REQUIRE(c.count_row_start == 4);
                BOOST_REQUIRE(c.count_row_end == 4);
                BOOST_REQUIRE(c.count_cell == 4*3);
                BOOST_REQUIRE(c.count_deleted_cell == 0);
                BOOST_REQUIRE(c.count_range_tombstone == 0);
                return make_ready_future<>();
            });
        });
    });
}

SEASTAR_TEST_CASE(compressed_rows_read_all) {
    auto s = make_shared_schema({}, "ks", "cf", {}, {}, {}, {}, utf8_type);
    return test_using_reusable_sst(std::move(s), "test/resource/sstables/compressed", 1, [] (auto sstp) {
        return do_with(count_row_consumer(), [sstp] (auto& c) {
            auto context = data_consume_rows<data_consume_rows_context>(*uncompressed_schema(), sstp, c);
            auto fut = context->consume_input();
            return fut.then([sstp, &c, context = std::move(context)] {
                BOOST_REQUIRE(c.count_row_start == 4);
                BOOST_REQUIRE(c.count_row_end == 4);
                BOOST_REQUIRE(c.count_cell == 4*3);
                BOOST_REQUIRE(c.count_deleted_cell == 0);
                BOOST_REQUIRE(c.count_range_tombstone == 0);
                return make_ready_future<>();
            });
        });
    });
}

// test reading all the rows one by one, using the feature of the
// consume_row_end returning proceed::no message
class pausable_count_row_consumer : public count_row_consumer {
public:
    virtual proceed consume_row_end() override {
        count_row_consumer::consume_row_end();
        return proceed::no;
    }
};

SEASTAR_TEST_CASE(pausable_uncompressed_rows_read_all) {
    return test_using_reusable_sst(uncompressed_schema(), uncompressed_dir(), 1, [] (auto sstp) {
        return do_with(pausable_count_row_consumer(), [sstp] (auto& c) {
            auto context = data_consume_rows<data_consume_rows_context>(*uncompressed_schema(), sstp, c);
            auto fut = context->consume_input();
            return fut.then([sstp, &c, context = std::move(context)] () mutable {
                // After one read, we only get one row
                BOOST_REQUIRE(c.count_row_start == 1);
                BOOST_REQUIRE(c.count_row_end == 1);
                BOOST_REQUIRE(c.count_cell == 1*3);
                BOOST_REQUIRE(c.count_deleted_cell == 0);
                BOOST_REQUIRE(c.count_range_tombstone == 0);
                auto fut = context->consume_input();
                return fut.then([&c, context = std::move(context)] () mutable {
                    // After two reads
                    BOOST_REQUIRE(c.count_row_start == 2);
                    BOOST_REQUIRE(c.count_row_end == 2);
                    BOOST_REQUIRE(c.count_cell == 2*3);
                    BOOST_REQUIRE(c.count_deleted_cell == 0);
                    BOOST_REQUIRE(c.count_range_tombstone == 0);
                    return make_ready_future<>();
                    // FIXME: read until the last row.
                });
            });
        });
    });
}


// Test reading range tombstone (which we we have in collections such as set)
class set_consumer : public count_row_consumer {
public:
    virtual proceed consume_range_tombstone(
            bytes_view start_col, bytes_view end_col,
            sstables::deletion_time deltime) override {
        count_row_consumer::consume_range_tombstone(start_col, end_col, deltime);
        // Note the unique end-of-component markers -1 and 1, specifying a
        // range between start and end of row.
        BOOST_REQUIRE(start_col == bytes({0, 9, 'f', 'a', 'v', 'o', 'r', 'i', 't', 'e', 's', -1}));
        BOOST_REQUIRE(end_col == as_bytes({0, 9, 'f', 'a', 'v', 'o', 'r', 'i', 't', 'e', 's', 1}));
        // Note the range tombstone have an interesting, not default, deltime.
        BOOST_REQUIRE(deltime.local_deletion_time == 1428855312U);
        BOOST_REQUIRE(deltime.marked_for_delete_at == 1428855312063524UL);
        return proceed::yes;
    }
};

SEASTAR_TEST_CASE(read_set) {
    return test_using_reusable_sst(set_schema(), "test/resource/sstables/set", 1, [] (auto sstp) {
        return do_with(set_consumer(), [sstp] (auto& c) {
            auto context = data_consume_rows<data_consume_rows_context>(*uncompressed_schema(), sstp, c);
            auto fut = context->consume_input();
            return fut.then([sstp, &c, context = std::move(context)] {
                BOOST_REQUIRE(c.count_row_start == 1);
                BOOST_REQUIRE(c.count_row_end == 1);
                BOOST_REQUIRE(c.count_cell == 3);
                BOOST_REQUIRE(c.count_deleted_cell == 0);
                BOOST_REQUIRE(c.count_range_tombstone == 1);
               return make_ready_future<>();
            });
        });
    });
}

class ttl_row_consumer : public count_row_consumer {
public:
    const int64_t desired_timestamp;
    ttl_row_consumer(int64_t t) : desired_timestamp(t) { }
    virtual proceed consume_row_start(sstables::key_view key, sstables::deletion_time deltime) override {
        count_row_consumer::consume_row_start(key, deltime);
        BOOST_REQUIRE(bytes_view(key) == as_bytes("nadav"));
        BOOST_REQUIRE(deltime.local_deletion_time == std::numeric_limits<int32_t>::max());
        BOOST_REQUIRE(deltime.marked_for_delete_at == std::numeric_limits<int64_t>::min());
        return proceed::yes;
    }

    virtual proceed consume_cell(bytes_view col_name, fragmented_temporary_buffer::view value,
            int64_t timestamp, int64_t ttl, int64_t expiration) override {
        switch (count_cell) {
        case 0:
            // The silly "cql row marker" cell
            BOOST_REQUIRE(col_name.size() == 3 && col_name[0] == 0 && col_name[1] == 0 && col_name[2] == 0);
            BOOST_REQUIRE(value.size_bytes() == 0);
            BOOST_REQUIRE(timestamp == desired_timestamp);
            BOOST_REQUIRE(ttl == 3600);
            BOOST_REQUIRE(expiration == 1430154618);
            break;
        case 1:
            BOOST_REQUIRE(col_name.size() == 6 && col_name[0] == 0 &&
                    col_name[1] == 3 && col_name[2] == 'a' &&
                    col_name[3] == 'g' && col_name[4] == 'e' &&
                    col_name[5] == '\0');
            BOOST_REQUIRE(linearized(value) == bytes({0, 0, 0, 40}));
            BOOST_REQUIRE(timestamp == desired_timestamp);
            BOOST_REQUIRE(ttl == 3600);
            BOOST_REQUIRE(expiration == 1430154618);
            break;
        }
        count_row_consumer::consume_cell(col_name, value, timestamp, ttl, expiration);
        return proceed::yes;
    }
};

SEASTAR_TEST_CASE(ttl_read) {
    return test_using_reusable_sst(uncompressed_schema(), "test/resource/sstables/ttl", 1, [] (auto sstp) {
        return do_with(ttl_row_consumer(1430151018675502), [sstp] (auto& c) {
            auto context = data_consume_rows<data_consume_rows_context>(*uncompressed_schema(), sstp, c);
            auto fut = context->consume_input();
            return fut.then([sstp, &c, context = std::move(context)] {
                BOOST_REQUIRE(c.count_row_start == 1);
                BOOST_REQUIRE(c.count_cell == 2);
                BOOST_REQUIRE(c.count_deleted_cell == 0);
                BOOST_REQUIRE(c.count_row_end == 1);
                BOOST_REQUIRE(c.count_range_tombstone == 0);
                return make_ready_future<>();
            });
        });
    });
}

class deleted_cell_row_consumer : public count_row_consumer {
public:
    virtual proceed consume_row_start(sstables::key_view key, sstables::deletion_time deltime) override {
        count_row_consumer::consume_row_start(key, deltime);
        BOOST_REQUIRE(bytes_view(key) == as_bytes("nadav"));
        BOOST_REQUIRE(deltime.local_deletion_time == std::numeric_limits<int32_t>::max());
        BOOST_REQUIRE(deltime.marked_for_delete_at == std::numeric_limits<int64_t>::min());
        return proceed::yes;
    }

    virtual proceed consume_deleted_cell(bytes_view col_name, sstables::deletion_time deltime) override {
        count_row_consumer::consume_deleted_cell(col_name, deltime);
        BOOST_REQUIRE(col_name.size() == 6 && col_name[0] == 0 &&
                col_name[1] == 3 && col_name[2] == 'a' &&
                col_name[3] == 'g' && col_name[4] == 'e' &&
                col_name[5] == '\0');
        BOOST_REQUIRE(deltime.local_deletion_time == 1430200516);
        BOOST_REQUIRE(deltime.marked_for_delete_at == 1430200516937621UL);
        return proceed::yes;
    }
};

SEASTAR_TEST_CASE(deleted_cell_read) {
    return test_using_reusable_sst(uncompressed_schema(), "test/resource/sstables/deleted_cell", 2, [] (auto sstp) {
        return do_with(deleted_cell_row_consumer(), [sstp] (auto& c) {
            auto context = data_consume_rows<data_consume_rows_context>(*uncompressed_schema(), sstp, c);
            auto fut = context->consume_input();
            return fut.then([sstp, &c, context = std::move(context)] {
                BOOST_REQUIRE(c.count_row_start == 1);
                BOOST_REQUIRE(c.count_cell == 0);
                BOOST_REQUIRE(c.count_deleted_cell == 1);
                BOOST_REQUIRE(c.count_row_end == 1);
                BOOST_REQUIRE(c.count_range_tombstone == 0);
                return make_ready_future<>();
            });
        });
    });
}


SEASTAR_TEST_CASE(find_key_map) {
    return test_using_reusable_sst(map_schema(), "test/resource/sstables/map_pk", 1, [] (auto sstp) {
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
    return test_using_reusable_sst(set_schema(), "test/resource/sstables/set_pk", 1, [] (auto sstp) {
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
    return test_using_reusable_sst(list_schema(), "test/resource/sstables/list_pk", 1, [] (auto sstp) {
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
    return test_using_reusable_sst(composite_schema(), "test/resource/sstables/composite", 1, [] (auto sstp) {
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
    return test_using_reusable_sst(uncompressed_schema(), "test/resource/sstables/bigsummary", 76, [] (auto sstp) {
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
    return test_using_reusable_sst(uncompressed_schema(), uncompressed_dir(), 1, [] (auto sstp) {
        return sstables::test(sstp).read_indexes().then([sstp] (auto index_list) {
            int idx = 0;
            for (auto& ie: index_list) {
                auto key = key::from_bytes(to_bytes(ie.get_key_bytes()));
                BOOST_REQUIRE(sstables::test(sstp).binary_search(sstp->get_schema()->get_partitioner(), index_list, key) == idx++);
            }
        });
    });
}

SEASTAR_TEST_CASE(not_find_key_composite_bucket0) {
    return test_using_reusable_sst(composite_schema(), "test/resource/sstables/composite", 1, [] (auto sstp) {
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
    return test_using_reusable_sst(uncompressed_schema(), "test/resource/sstables/wrongrange", 114, [] (auto sstp) {
        return do_with(dht::partition_range::make_singular(make_dkey(uncompressed_schema(), "todata")), [sstp] (auto& range) {
            auto s = columns_schema();
            return with_closeable(sstp->make_reader(s, tests::make_permit(), range, s->full_slice()), [sstp, s] (flat_mutation_reader& rd) {
              return read_mutation_from_flat_mutation_reader(rd, db::no_timeout).then([sstp, s] (auto mutation) {
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
    return test_env::do_with([version] (test_env& env) {
      return load_large_partition_sst(env, version).then([] (auto sstp) {
        schema_ptr s = large_partition_schema();
        return sstables::test(sstp).read_indexes().then([sstp] (index_list vec) {
            BOOST_REQUIRE(vec.size() == 1);
            BOOST_REQUIRE(vec.front().get_promoted_index_size() > 0);
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
static future<int> count_rows(sstable_ptr sstp, schema_ptr s, sstring key, sstring ck1, sstring ck2) {
    return seastar::async([sstp, s, key, ck1, ck2] () mutable {
        auto ps = make_partition_slice(*s, ck1, ck2);
        auto pr = dht::partition_range::make_singular(make_dkey(s, key.c_str()));
        auto rd = sstp->make_reader(s, tests::make_permit(), pr, ps);
        auto close_rd = deferred_close(rd);
        auto mfopt = rd(db::no_timeout).get0();
        if (!mfopt) {
            return 0;
        }
        int nrows = 0;
        mfopt = rd(db::no_timeout).get0();
        while (mfopt) {
            if (mfopt->is_clustering_row()) {
                nrows++;
            }
            mfopt = rd(db::no_timeout).get0();
        }
        return nrows;
    });
}

// Count the number of CQL rows in one partition
static future<int> count_rows(sstable_ptr sstp, schema_ptr s, sstring key) {
    return seastar::async([sstp, s, key] () mutable {
        auto pr = dht::partition_range::make_singular(make_dkey(s, key.c_str()));
        auto rd = sstp->make_reader(s, tests::make_permit(), pr, s->full_slice());
        auto close_rd = deferred_close(rd);
        auto mfopt = rd(db::no_timeout).get0();
        if (!mfopt) {
            return 0;
        }
        int nrows = 0;
        mfopt = rd(db::no_timeout).get0();
        while (mfopt) {
            if (mfopt->is_clustering_row()) {
                nrows++;
            }
            mfopt = rd(db::no_timeout).get0();
        }
        return nrows;
    });
}

// Count the number of CQL rows between clustering key prefix ck1 to ck2
// in all partitions in the sstable (using sstable::read_range_rows).
static future<int> count_rows(sstable_ptr sstp, schema_ptr s, sstring ck1, sstring ck2) {
    return seastar::async([sstp, s, ck1, ck2] () mutable {
        auto ps = make_partition_slice(*s, ck1, ck2);
        auto reader = sstp->make_reader(s, tests::make_permit(), query::full_partition_range, ps);
        auto close_reader = deferred_close(reader);
        int nrows = 0;
        auto mfopt = reader(db::no_timeout).get0();
        while (mfopt) {
            mfopt = reader(db::no_timeout).get0();
            BOOST_REQUIRE(mfopt);
            while (!mfopt->is_end_of_partition()) {
                if (mfopt->is_clustering_row()) {
                    nrows++;
                }
                mfopt = reader(db::no_timeout).get0();
            }
            mfopt = reader(db::no_timeout).get0();
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
   return test_env::do_with([s, version] (test_env& env) {
    return load_large_partition_sst(env, version).then([s] (auto sstp) {
        return count_rows(sstp, s, "v1", "18wX", "18xB").then([] (int nrows) {
            // there should be 5 rows (out of 13520 = 20*26*26) in this range:
            // 18wX, 18wY, 18wZ, 18xA, 18xB.
            BOOST_REQUIRE(nrows == 5);
        }).then([sstp, s] () {
            return count_rows(sstp, s, "v1", "13aB", "15aA").then([] (int nrows) {
                // There should be 26*26*2 rows in this range. It spans two
                // promoted-index blocks, so we get to test that case.
                BOOST_REQUIRE(nrows == 2*26*26);
            });
        }).then([sstp, s] () {
            return count_rows(sstp, s, "v1", "10aB", "19aA").then([] (int nrows) {
                // There should be 26*26*9 rows in this range. It spans many
                // promoted-index blocks.
                BOOST_REQUIRE(nrows == 9*26*26);
            });
        }).then([sstp, s] () {
            return count_rows(sstp, s, "v1", "0", "z").then([] (int nrows) {
                // All rows, 20*26*26 of them, are in this range. It spans all
                // the promoted-index blocks, but the range is still bounded
                // on both sides
                BOOST_REQUIRE(nrows == 20*26*26);
            });
        }).then([sstp, s] () {
            // range that is outside (after) the actual range of the data.
            // No rows should match.
            return count_rows(sstp, s, "v1", "y", "z").then([] (int nrows) {
                BOOST_REQUIRE(nrows == 0);
            });
        }).then([sstp, s] () {
            // range that is outside (before) the actual range of the data.
            // No rows should match.
            return count_rows(sstp, s, "v1", "_a", "_b").then([] (int nrows) {
                BOOST_REQUIRE(nrows == 0);
            });
        }).then([sstp, s] () {
            // half-infinite range
            return count_rows(sstp, s, "v1", "", "10aA").then([] (int nrows) {
                BOOST_REQUIRE(nrows == (1*26*26 + 1));
            });
        }).then([sstp, s] () {
            // half-infinite range
            return count_rows(sstp, s, "v1", "10aA", "").then([] (int nrows) {
                BOOST_REQUIRE(nrows == 19*26*26);
            });
        }).then([sstp, s] () {
            // count all rows, but giving an explicit all-encompasing filter
            return count_rows(sstp, s, "v1", "", "").then([] (int nrows) {
                BOOST_REQUIRE(nrows == 20*26*26);
            });
        }).then([sstp, s] () {
            // count all rows, without a filter
            return count_rows(sstp, s, "v1").then([] (int nrows) {
                BOOST_REQUIRE(nrows == 20*26*26);
            });
        });
    });
   });
  });
}

// Same as previous test, just using read_range_rows instead of read_row
// to read parts of potentially more than one partition (in this particular
// sstable, there is actually just one partition).
SEASTAR_TEST_CASE(sub_partitions_read) {
  schema_ptr s = large_partition_schema();
  return for_each_sstable_version([s] (const sstables::sstable::version_types version) {
   return test_env::do_with([s, version] (test_env& env) {
    return load_large_partition_sst(env, version).then([s] (auto sstp) {
        return count_rows(sstp, s, "18wX", "18xB").then([] (int nrows) {
            BOOST_REQUIRE(nrows == 5);
        });
    });
   });
  });
}

// A silly, inefficient but effective, way to compare two files by reading
// them entirely into memory.
static future<> compare_files(sstring file1, sstring file2) {
    return read_file(file1).then([file2] (auto in1) {
        return read_file(file2).then([in1 = std::move(in1)] (auto in2) {
            // assert that both files have the same size.
            BOOST_REQUIRE(in1.second == in2.second);
            // assert that both files have the same content.
            BOOST_REQUIRE(::memcmp(in1.first.get(), in2.first.get(), in1.second) == 0);
        });
    });

}

// This test creates the same data as we previously created with Cassandra
// in the test/resource/sstables/large_partition directory (which we read in the
// promoted_index_read test above). The index file in both sstables - which
// includes the promoted index - should be bit-for-bit identical, otherwise
// we have a problem in our promoted index writing code (or in the data
// writing code, because the promoted index points to offsets in the data).
SEASTAR_TEST_CASE(promoted_index_write) {
    auto s = large_partition_schema();
    return test_setup::do_with_tmp_directory([s] (test_env& env, auto dirname) {
        auto mtp = make_lw_shared<memtable>(s);
        auto key = partition_key::from_exploded(*s, {to_bytes("v1")});
        mutation m(s, key);
        auto col = s->get_column_definition("t3");
        BOOST_REQUIRE(col && !col->is_static());
        for (char i = 'a'; i <= 'z'; i++) {
            for (char j = 'A'; j <= 'Z'; j++) {
                for (int k = 0; k < 20; k++) {
                    auto& row = m.partition().clustered_row(*s,
                            clustering_key::from_exploded(
                                    *s, {to_bytes(format("{:d}{:c}{:c}", k, i, j))}));
                    row.cells().apply(*col,
                            atomic_cell::make_live(*col->type, 2345,
                                    col->type->decompose(sstring(format("z{:c}",i)))));
                    row.apply(row_marker(1234));
                }
            }
        }
        mtp->apply(std::move(m));
        auto sst = env.make_sstable(s, dirname, 100,
                sstables::sstable::version_types::la, big);
        return write_memtable_to_sstable_for_test(*mtp, sst).then([s, dirname] {
            auto large_partition_file = seastar::format("{}/la-3-big-Index.db", get_test_dir("large_partition", s));
            return compare_files(large_partition_file, dirname + "/la-100-big-Index.db");
        }).then([sst, mtp] {});
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
        auto out = make_compressed_file_k_l_format_output_stream(std::move(os), &c, cp);

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
            return make_compressed_file_k_l_format_input_stream(f, &c, 0, uncompressed_size, opts);
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
