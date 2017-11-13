/*
 * Copyright (C) 2015 ScyllaDB
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


#include <boost/test/unit_test.hpp>
#include <boost/range/irange.hpp>

#include "core/sleep.hh"
#include "core/do_with.hh"
#include "core/thread.hh"

#include "tests/test-utils.hh"
#include "tests/mutation_assertions.hh"
#include "tests/mutation_reader_assertions.hh"
#include "tests/tmpdir.hh"
#include "tests/sstable_utils.hh"
#include "tests/simple_schema.hh"

#include "mutation_reader.hh"
#include "schema_builder.hh"
#include "cell_locking.hh"
#include "sstables/sstables.hh"
#include "database.hh"

#include "disk-error-handler.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

static schema_ptr make_schema() {
    return schema_builder("ks", "cf")
        .with_column("pk", bytes_type, column_kind::partition_key)
        .with_column("v", bytes_type, column_kind::regular_column)
        .build();
}

SEASTAR_TEST_CASE(test_combining_two_readers_with_the_same_row) {
    return seastar::async([] {
        auto s = make_schema();

        mutation m1(partition_key::from_single_value(*s, "key1"), s);
        m1.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);

        mutation m2(partition_key::from_single_value(*s, "key1"), s);
        m2.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v2")), 2);

        assert_that(make_combined_reader(make_reader_returning(m1), make_reader_returning(m2)))
            .produces(m2)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_combining_two_non_overlapping_readers) {
    return seastar::async([] {
        auto s = make_schema();

        mutation m1(partition_key::from_single_value(*s, "keyB"), s);
        m1.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);

        mutation m2(partition_key::from_single_value(*s, "keyA"), s);
        m2.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v2")), 2);

        auto cr = make_combined_reader(make_reader_returning(m1), make_reader_returning(m2));
        assert_that(std::move(cr))
            .produces(m2)
            .produces(m1)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_combining_two_partially_overlapping_readers) {
    return seastar::async([] {
        auto s = make_schema();
        auto& slice = s->full_slice();

        mutation m1(partition_key::from_single_value(*s, "keyA"), s);
        m1.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);

        mutation m2(partition_key::from_single_value(*s, "keyB"), s);
        m2.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v2")), 1);

        mutation m3(partition_key::from_single_value(*s, "keyC"), s);
        m3.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v3")), 1);

        assert_that(make_combined_reader(make_reader_returning_many({m1, m2}, slice), make_reader_returning_many({m2, m3}, slice)))
            .produces(m1)
            .produces(m2)
            .produces(m3)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_combining_one_reader_with_many_partitions) {
    return seastar::async([] {
        auto s = make_schema();

        mutation m1(partition_key::from_single_value(*s, "keyA"), s);
        m1.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);

        mutation m2(partition_key::from_single_value(*s, "keyB"), s);
        m2.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v2")), 1);

        mutation m3(partition_key::from_single_value(*s, "keyC"), s);
        m3.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v3")), 1);

        std::vector<mutation_reader> v;
        v.push_back(make_reader_returning_many({m1, m2, m3}));
        assert_that(make_combined_reader(std::move(v), mutation_reader::forwarding::no))
            .produces(m1)
            .produces(m2)
            .produces(m3)
            .produces_end_of_stream();
    });
}

static mutation make_mutation_with_key(schema_ptr s, dht::decorated_key dk) {
    mutation m(std::move(dk), s);
    m.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);
    return m;
}

static mutation make_mutation_with_key(schema_ptr s, const char* key) {
    return make_mutation_with_key(s, dht::global_partitioner().decorate_key(*s, partition_key::from_single_value(*s, bytes(key))));
}

SEASTAR_TEST_CASE(test_filtering) {
    return seastar::async([] {
        auto s = make_schema();

        auto m1 = make_mutation_with_key(s, "key1");
        auto m2 = make_mutation_with_key(s, "key2");
        auto m3 = make_mutation_with_key(s, "key3");
        auto m4 = make_mutation_with_key(s, "key4");

        // All pass
        assert_that(make_filtering_reader(make_reader_returning_many({m1, m2, m3, m4}),
                 [] (const streamed_mutation& m) { return true; }))
            .produces(m1)
            .produces(m2)
            .produces(m3)
            .produces(m4)
            .produces_end_of_stream();

        // None pass
        assert_that(make_filtering_reader(make_reader_returning_many({m1, m2, m3, m4}),
                 [] (const streamed_mutation& m) { return false; }))
            .produces_end_of_stream();

        // Trim front
        assert_that(make_filtering_reader(make_reader_returning_many({m1, m2, m3, m4}),
                [&] (const streamed_mutation& m) { return !m.key().equal(*s, m1.key()); }))
            .produces(m2)
            .produces(m3)
            .produces(m4)
            .produces_end_of_stream();

        assert_that(make_filtering_reader(make_reader_returning_many({m1, m2, m3, m4}),
            [&] (const streamed_mutation& m) { return !m.key().equal(*s, m1.key()) && !m.key().equal(*s, m2.key()); }))
            .produces(m3)
            .produces(m4)
            .produces_end_of_stream();

        // Trim back
        assert_that(make_filtering_reader(make_reader_returning_many({m1, m2, m3, m4}),
                 [&] (const streamed_mutation& m) { return !m.key().equal(*s, m4.key()); }))
            .produces(m1)
            .produces(m2)
            .produces(m3)
            .produces_end_of_stream();

        assert_that(make_filtering_reader(make_reader_returning_many({m1, m2, m3, m4}),
                 [&] (const streamed_mutation& m) { return !m.key().equal(*s, m4.key()) && !m.key().equal(*s, m3.key()); }))
            .produces(m1)
            .produces(m2)
            .produces_end_of_stream();

        // Trim middle
        assert_that(make_filtering_reader(make_reader_returning_many({m1, m2, m3, m4}),
                 [&] (const streamed_mutation& m) { return !m.key().equal(*s, m3.key()); }))
            .produces(m1)
            .produces(m2)
            .produces(m4)
            .produces_end_of_stream();

        assert_that(make_filtering_reader(make_reader_returning_many({m1, m2, m3, m4}),
                 [&] (const streamed_mutation& m) { return !m.key().equal(*s, m2.key()) && !m.key().equal(*s, m3.key()); }))
            .produces(m1)
            .produces(m4)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_combining_two_readers_with_one_reader_empty) {
    return seastar::async([] {
        auto s = make_schema();
        mutation m1(partition_key::from_single_value(*s, "key1"), s);
        m1.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);

        assert_that(make_combined_reader(make_reader_returning(m1), make_empty_reader()))
            .produces(m1)
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_combining_two_empty_readers) {
    return seastar::async([] {
        assert_that(make_combined_reader(make_empty_reader(), make_empty_reader()))
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_combining_one_empty_reader) {
    return seastar::async([] {
        std::vector<mutation_reader> v;
        v.push_back(make_empty_reader());
        assert_that(make_combined_reader(std::move(v), mutation_reader::forwarding::no))
            .produces_end_of_stream();
    });
}

std::vector<dht::decorated_key> generate_keys(schema_ptr s, int count) {
    auto keys = boost::copy_range<std::vector<dht::decorated_key>>(
        boost::irange(0, count) | boost::adaptors::transformed([s] (int key) {
            auto pk = partition_key::from_single_value(*s, int32_type->decompose(data_value(key)));
            return dht::global_partitioner().decorate_key(*s, std::move(pk));
        }));
    return std::move(boost::range::sort(keys, dht::decorated_key::less_comparator(s)));
}

std::vector<dht::ring_position> to_ring_positions(const std::vector<dht::decorated_key>& keys) {
    return boost::copy_range<std::vector<dht::ring_position>>(keys | boost::adaptors::transformed([] (const dht::decorated_key& key) {
        return dht::ring_position(key);
    }));
}

SEASTAR_TEST_CASE(test_fast_forwarding_combining_reader) {
    return seastar::async([] {
        auto s = make_schema();

        auto keys = generate_keys(s, 7);
        auto ring = to_ring_positions(keys);

        std::vector<std::vector<mutation>> mutations {
            {
                make_mutation_with_key(s, keys[0]),
                make_mutation_with_key(s, keys[1]),
                make_mutation_with_key(s, keys[2]),
            },
            {
                make_mutation_with_key(s, keys[2]),
                make_mutation_with_key(s, keys[3]),
                make_mutation_with_key(s, keys[4]),
            },
            {
                make_mutation_with_key(s, keys[1]),
                make_mutation_with_key(s, keys[3]),
                make_mutation_with_key(s, keys[5]),
            },
            {
                make_mutation_with_key(s, keys[0]),
                make_mutation_with_key(s, keys[5]),
                make_mutation_with_key(s, keys[6]),
            },
        };

        auto make_reader = [&] (const dht::partition_range& pr) {
            std::vector<mutation_reader> readers;
            boost::range::transform(mutations, std::back_inserter(readers), [&pr] (auto& ms) {
                return make_reader_returning_many(ms, pr);
            });
            return make_combined_reader(std::move(readers), mutation_reader::forwarding::yes);
        };

        auto pr = dht::partition_range::make_open_ended_both_sides();
        assert_that(make_reader(pr))
            .produces(keys[0])
            .produces(keys[1])
            .produces(keys[2])
            .produces(keys[3])
            .produces(keys[4])
            .produces(keys[5])
            .produces(keys[6])
            .produces_end_of_stream();

        pr = dht::partition_range::make(ring[0], ring[0]);
            assert_that(make_reader(pr))
                    .produces(keys[0])
                    .produces_end_of_stream()
                    .fast_forward_to(dht::partition_range::make(ring[1], ring[1]))
                    .produces(keys[1])
                    .produces_end_of_stream()
                    .fast_forward_to(dht::partition_range::make(ring[3], ring[4]))
                    .produces(keys[3])
            .fast_forward_to(dht::partition_range::make({ ring[4], false }, ring[5]))
                    .produces(keys[5])
                    .produces_end_of_stream()
            .fast_forward_to(dht::partition_range::make_starting_with(ring[6]))
                    .produces(keys[6])
                    .produces_end_of_stream();
    });
}

struct sst_factory {
    schema_ptr s;
    sstring path;
    unsigned gen;
    int level;

    sst_factory(schema_ptr s, const sstring& path, unsigned gen, int level)
        : s(s)
        , path(path)
        , gen(gen)
        , level(level)
    {}

    sstables::shared_sstable operator()() {
        auto sst = sstables::make_sstable(s, path, gen, sstables::sstable::version_types::la, sstables::sstable::format_types::big);
        sst->set_unshared();

        //TODO set sstable level, to make the test more interesting

        return sst;
    }
};

SEASTAR_TEST_CASE(combined_mutation_reader_test) {
    return seastar::async([] {
        //logging::logger_registry().set_logger_level("database", logging::log_level::trace);

        simple_schema s;

        const auto pkeys = s.make_pkeys(4);
        const auto ckeys = s.make_ckeys(4);

        std::vector<mutation> base_mutations = boost::copy_range<std::vector<mutation>>(
                pkeys | boost::adaptors::transformed([&s](const auto& k) { return mutation(k, s.schema()); }));

        // Data layout:
        //   d[xx]
        // b[xx][xx]c
        // a[x    x]

        int i{0};

        // sstable d
        std::vector<mutation> table_d_mutations;

        i = 1;
        table_d_mutations.emplace_back(base_mutations[i]);
        s.add_row(table_d_mutations.back(), ckeys[i], sprint("val_d_%i", i));

        i = 2;
        table_d_mutations.emplace_back(base_mutations[i]);
        s.add_row(table_d_mutations.back(), ckeys[i], sprint("val_d_%i", i));
        const auto t_static_row = s.add_static_row(table_d_mutations.back(), sprint("%i_static_val", i));

        // sstable b
        std::vector<mutation> table_b_mutations;

        i = 0;
        table_b_mutations.emplace_back(base_mutations[i]);
        s.add_row(table_b_mutations.back(), ckeys[i], sprint("val_b_%i", i));

        i = 1;
        table_b_mutations.emplace_back(base_mutations[i]);
        s.add_row(table_b_mutations.back(), ckeys[i], sprint("val_b_%i", i));

        // sstable c
        std::vector<mutation> table_c_mutations;

        i = 2;
        table_c_mutations.emplace_back(base_mutations[i]);
        const auto t_row = s.add_row(table_c_mutations.back(), ckeys[i], sprint("val_c_%i", i));

        i = 3;
        table_c_mutations.emplace_back(base_mutations[i]);
        s.add_row(table_c_mutations.back(), ckeys[i], sprint("val_c_%i", i));

        // sstable a
        std::vector<mutation> table_a_mutations;

        i = 0;
        table_a_mutations.emplace_back(base_mutations[i]);
        s.add_row(table_a_mutations.back(), ckeys[i], sprint("val_a_%i", i));

        i = 3;
        table_a_mutations.emplace_back(base_mutations[i]);
        s.add_row(table_a_mutations.back(), ckeys[i], sprint("val_a_%i", i));

        auto tmp = make_lw_shared<tmpdir>();

        unsigned gen{0};

        std::vector<sstables::shared_sstable> tables = {
                make_sstable_containing(sst_factory(s.schema(), tmp->path, gen++, 0), table_a_mutations),
                make_sstable_containing(sst_factory(s.schema(), tmp->path, gen++, 1), table_b_mutations),
                make_sstable_containing(sst_factory(s.schema(), tmp->path, gen++, 1), table_c_mutations),
                make_sstable_containing(sst_factory(s.schema(), tmp->path, gen++, 2), table_d_mutations)
        };

        auto cs = sstables::make_compaction_strategy(sstables::compaction_strategy_type::leveled, {});
        auto sstables = make_lw_shared<sstables::sstable_set>(cs.make_sstable_set(s.schema()));

        std::vector<mutation_reader> sstable_mutation_readers;

        for (auto table : tables) {
            sstables->insert(table);

            sstable_mutation_readers.emplace_back(table->read_range_rows(
                    s.schema(),
                    query::full_partition_range,
                    s.schema()->full_slice(),
                    seastar::default_priority_class(),
                    no_resource_tracking(),
                    streamed_mutation::forwarding::no,
                    mutation_reader::forwarding::yes));
        }

        auto list_reader = make_combined_reader(std::move(sstable_mutation_readers), mutation_reader::forwarding::yes);

        auto incremental_reader = make_range_sstable_reader(
                s.schema(),
                sstables,
                query::full_partition_range,
                s.schema()->full_slice(),
                seastar::default_priority_class(),
                no_resource_tracking(),
                nullptr,
                streamed_mutation::forwarding::no,
                mutation_reader::forwarding::yes);

        // merge c[0] with d[1]
        i = 2;
        auto c_d_merged = mutation(pkeys[i], s.schema());
        s.add_row(c_d_merged, ckeys[i], sprint("val_c_%i", i), t_row);
        s.add_static_row(c_d_merged, sprint("%i_static_val", i), t_static_row);

        assert_that(std::move(list_reader))
            .produces(table_a_mutations.front())
            .produces(table_b_mutations[1])
            .produces(c_d_merged)
            .produces(table_a_mutations.back());

        assert_that(std::move(incremental_reader))
            .produces(table_a_mutations.front())
            .produces(table_b_mutations[1])
            .produces(c_d_merged)
            .produces(table_a_mutations.back());
    });
}

static const std::size_t new_reader_base_cost{16 * 1024};

template<typename EventuallySucceedingFunction>
static bool eventually_true(EventuallySucceedingFunction&& f) {
    const unsigned max_attempts = 10;
    unsigned attempts = 0;
    while (true) {
        if (f()) {
            return true;
        }

        if (++attempts < max_attempts) {
            seastar::sleep(std::chrono::milliseconds(1 << attempts)).get0();
        } else {
            return false;
        }
    }

    return false;
}

#define REQUIRE_EVENTUALLY_EQUAL(a, b) BOOST_REQUIRE(eventually_true([&] { return a == b; }))


sstables::shared_sstable create_sstable(simple_schema& sschema, const sstring& path) {
    std::vector<mutation> mutations;
    mutations.reserve(1 << 14);

    for (std::size_t p = 0; p < (1 << 10); ++p) {
        mutation m(sschema.make_pkey(p), sschema.schema());
        sschema.add_static_row(m, sprint("%i_static_val", p));

        for (std::size_t c = 0; c < (1 << 4); ++c) {
            sschema.add_row(m, sschema.make_ckey(c), sprint("val_%i", c));
        }

        mutations.emplace_back(std::move(m));
        thread::yield();
    }

    return make_sstable_containing([&] {
            return make_lw_shared<sstables::sstable>(sschema.schema(), path, 0, sstables::sstable::version_types::la, sstables::sstable::format_types::big);
        }
        , mutations);
}


class tracking_reader : public mutation_reader::impl {
    mutation_reader _reader;
    std::size_t _call_count{0};
    std::size_t _ff_count{0};
public:
    tracking_reader(semaphore* resources_sem, schema_ptr schema, lw_shared_ptr<sstables::sstable> sst)
        : _reader(sst->read_range_rows(
                        schema,
                        query::full_partition_range,
                        schema->full_slice(),
                        default_priority_class(),
                        reader_resource_tracker(resources_sem),
                        streamed_mutation::forwarding::no,
                        mutation_reader::forwarding::yes)) {
    }

    virtual future<streamed_mutation_opt> operator()() override {
        ++_call_count;
        return _reader();
    }

    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        ++_ff_count;
        // Don't forward this to the underlying reader, it will force us
        // to come up with meaningful partition-ranges which is hard and
        // unecessary for these tests.
        return make_ready_future<>();
    }

    std::size_t call_count() const {
        return _call_count;
    }

    std::size_t ff_count() const {
        return _ff_count;
    }
};

class reader_wrapper {
    mutation_reader _reader;
    tracking_reader* _tracker{nullptr};

public:
    reader_wrapper(
            const restricted_mutation_reader_config& config,
            schema_ptr schema,
            lw_shared_ptr<sstables::sstable> sst) {
        auto ms = mutation_source([this, &config, sst=std::move(sst)] (schema_ptr schema, const dht::partition_range&) {
            auto tracker_ptr = std::make_unique<tracking_reader>(config.resources_sem, std::move(schema), std::move(sst));
            _tracker = tracker_ptr.get();
            return mutation_reader(std::move(tracker_ptr));
        });

        _reader = make_restricted_reader(config, std::move(ms), std::move(schema));
    }

    future<streamed_mutation_opt> operator()() {
        return _reader();
    }

    future<> fast_forward_to(const dht::partition_range& pr) {
        return _reader.fast_forward_to(pr);
    }

    std::size_t call_count() const {
        return _tracker ? _tracker->call_count() : 0;
    }

    std::size_t ff_count() const {
        return _tracker ? _tracker->ff_count() : 0;
    }

    bool created() const {
        return bool(_tracker);
    }
};

struct restriction_data {
    std::unique_ptr<semaphore> reader_semaphore;
    restricted_mutation_reader_config config;

    restriction_data(std::size_t units,
            std::chrono::nanoseconds timeout = {},
            std::size_t max_queue_length = std::numeric_limits<std::size_t>::max())
        : reader_semaphore(std::make_unique<semaphore>(units)) {
        config.resources_sem = reader_semaphore.get();
        config.timeout = timeout;
        config.max_queue_length = max_queue_length;
    }
};


class dummy_file_impl : public file_impl {
    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) override {
        return make_ready_future<size_t>(0);
    }

    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        return make_ready_future<size_t>(0);
    }

    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) override {
        return make_ready_future<size_t>(0);
    }

    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        return make_ready_future<size_t>(0);
    }

    virtual future<> flush(void) override {
        return make_ready_future<>();
    }

    virtual future<struct stat> stat(void) override {
        return make_ready_future<struct stat>();
    }

    virtual future<> truncate(uint64_t length) override {
        return make_ready_future<>();
    }

    virtual future<> discard(uint64_t offset, uint64_t length) override {
        return make_ready_future<>();
    }

    virtual future<> allocate(uint64_t position, uint64_t length) override {
        return make_ready_future<>();
    }

    virtual future<uint64_t> size(void) override {
        return make_ready_future<uint64_t>(0);
    }

    virtual future<> close() override {
        return make_ready_future<>();
    }

    virtual subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override {
        throw std::bad_function_call();
    }

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, const io_priority_class& pc) override {
        temporary_buffer<uint8_t> buf(1024);

        memset(buf.get_write(), 0xff, buf.size());

        return make_ready_future<temporary_buffer<uint8_t>>(std::move(buf));
    }
};

SEASTAR_TEST_CASE(reader_restriction_file_tracking) {
    return async([&] {
        restriction_data rd(4 * 1024);

        {
            reader_resource_tracker resource_tracker(rd.config.resources_sem);

            auto tracked_file = resource_tracker.track(
                    file(shared_ptr<file_impl>(make_shared<dummy_file_impl>())));

            BOOST_REQUIRE_EQUAL(4 * 1024, rd.reader_semaphore->available_units());

            auto buf1 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(3 * 1024, rd.reader_semaphore->available_units());

            auto buf2 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(2 * 1024, rd.reader_semaphore->available_units());

            auto buf3 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(1 * 1024, rd.reader_semaphore->available_units());

            auto buf4 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(0 * 1024, rd.reader_semaphore->available_units());

            auto buf5 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(-1 * 1024, rd.reader_semaphore->available_units());

            // Reassing buf1, should still have the same amount of units.
            buf1 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(-1 * 1024, rd.reader_semaphore->available_units());

            // Move buf1 to the heap, so that we can safely destroy it
            auto buf1_ptr = std::make_unique<temporary_buffer<char>>(std::move(buf1));
            BOOST_REQUIRE_EQUAL(-1 * 1024, rd.reader_semaphore->available_units());

            buf1_ptr.reset();
            BOOST_REQUIRE_EQUAL(0 * 1024, rd.reader_semaphore->available_units());

            // Move tracked_file to the heap, so that we can safely destroy it.
            auto tracked_file_ptr = std::make_unique<file>(std::move(tracked_file));
            tracked_file_ptr.reset();

            // Move buf4 to the heap, so that we can safely destroy it
            auto buf4_ptr = std::make_unique<temporary_buffer<char>>(std::move(buf4));
            BOOST_REQUIRE_EQUAL(0 * 1024, rd.reader_semaphore->available_units());

            // Releasing buffers that overlived the tracked-file they
            // originated from should succeed.
            buf4_ptr.reset();
            BOOST_REQUIRE_EQUAL(1 * 1024, rd.reader_semaphore->available_units());
        }

        // All units should have been deposited back.
        REQUIRE_EVENTUALLY_EQUAL(4 * 1024, rd.reader_semaphore->available_units());
    });
}

SEASTAR_TEST_CASE(restricted_reader_reading) {
    return async([&] {
        restriction_data rd(new_reader_base_cost);

        {
            simple_schema s;
            auto tmp = make_lw_shared<tmpdir>();
            auto sst = create_sstable(s, tmp->path);

            auto reader1 = reader_wrapper(rd.config, s.schema(), sst);

            reader1().get();

            BOOST_REQUIRE_LE(rd.reader_semaphore->available_units(), 0);
            BOOST_REQUIRE_EQUAL(reader1.call_count(), 1);

            auto reader2 = reader_wrapper(rd.config, s.schema(), sst);
            auto read_fut = reader2();

            // reader2 shouldn't be allowed just yet.
            BOOST_REQUIRE_EQUAL(reader2.call_count(), 0);

            // Move reader1 to the heap, so that we can safely destroy it.
            auto reader1_ptr = std::make_unique<reader_wrapper>(std::move(reader1));
            reader1_ptr.reset();

            // reader1's destruction should've made some space for reader2 by now.
            REQUIRE_EVENTUALLY_EQUAL(reader2.call_count(), 1);
            read_fut.get();

            {
                // Consume all available units.
                const auto consume_guard = consume_units(*rd.reader_semaphore, rd.reader_semaphore->current());

                // Already allowed readers should not be blocked anymore even if
                // there are no more units available.
                read_fut = reader2();
                BOOST_REQUIRE_EQUAL(reader2.call_count(), 2);
                read_fut.get();
            }
        }

        // All units should have been deposited back.
        REQUIRE_EVENTUALLY_EQUAL(new_reader_base_cost, rd.reader_semaphore->available_units());
    });
}

SEASTAR_TEST_CASE(restricted_reader_timeout) {
    return async([&] {
        restriction_data rd(new_reader_base_cost, std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::milliseconds{10}));

        {
            simple_schema s;
            auto tmp = make_lw_shared<tmpdir>();
            auto sst = create_sstable(s, tmp->path);

            auto reader1 = reader_wrapper(rd.config, s.schema(), sst);
            reader1().get();

            auto reader2 = reader_wrapper(rd.config, s.schema(), sst);
            auto read_fut = reader2();

            seastar::sleep(std::chrono::milliseconds(20)).get();

            // The read should have timed out.
            BOOST_REQUIRE(read_fut.failed());
            BOOST_REQUIRE_THROW(std::rethrow_exception(read_fut.get_exception()), semaphore_timed_out);
        }

        // All units should have been deposited back.
        REQUIRE_EVENTUALLY_EQUAL(new_reader_base_cost, rd.reader_semaphore->available_units());
    });
}

SEASTAR_TEST_CASE(restricted_reader_max_queue_length) {
    return async([&] {
        restriction_data rd(new_reader_base_cost, {}, 1);

        {
            simple_schema s;
            auto tmp = make_lw_shared<tmpdir>();
            auto sst = create_sstable(s, tmp->path);

            auto reader1_ptr = std::make_unique<reader_wrapper>(rd.config, s.schema(), sst);
            (*reader1_ptr)().get();

            auto reader2_ptr = std::make_unique<reader_wrapper>(rd.config, s.schema(), sst);
            auto read_fut = (*reader2_ptr)();

            // The queue should now be full.
            BOOST_REQUIRE_THROW(reader_wrapper(rd.config, s.schema(), sst), std::runtime_error);

            reader1_ptr.reset();
            read_fut.get();
        }

        REQUIRE_EVENTUALLY_EQUAL(new_reader_base_cost, rd.reader_semaphore->available_units());
    });
}

SEASTAR_TEST_CASE(restricted_reader_create_reader) {
    return async([&] {
        restriction_data rd(new_reader_base_cost);

        {
            simple_schema s;
            auto tmp = make_lw_shared<tmpdir>();
            auto sst = create_sstable(s, tmp->path);

            {
                auto reader = reader_wrapper(rd.config, s.schema(), sst);
                // This fast-forward is stupid, I know but the
                // underlying dummy reader won't care, so it's fine.
                reader.fast_forward_to(query::full_partition_range).get();

                BOOST_REQUIRE(reader.created());
                BOOST_REQUIRE_EQUAL(reader.call_count(), 0);
                BOOST_REQUIRE_EQUAL(reader.ff_count(), 1);
            }

            {
                auto reader = reader_wrapper(rd.config, s.schema(), sst);
                reader().get();

                BOOST_REQUIRE(reader.created());
                BOOST_REQUIRE_EQUAL(reader.call_count(), 1);
                BOOST_REQUIRE_EQUAL(reader.ff_count(), 0);
            }
        }

        REQUIRE_EVENTUALLY_EQUAL(new_reader_base_cost, rd.reader_semaphore->available_units());
    });
}
