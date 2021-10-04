/*
 * Copyright (C) 2017-present ScyllaDB
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


#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/closeable.hh>

#include "mutation.hh"
#include "mutation_fragment.hh"
#include "test/lib/mutation_source_test.hh"
#include "flat_mutation_reader.hh"
#include "mutation_reader.hh"
#include "schema_builder.hh"
#include "memtable.hh"
#include "row_cache.hh"
#include "test/lib/tmpdir.hh"
#include "repair/repair.hh"
#include "mutation_partition_view.hh"

#include "test/lib/simple_schema.hh"
#include "test/lib/flat_mutation_reader_assertions.hh"
#include "test/lib/log.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/random_schema.hh"

#include <boost/range/adaptor/map.hpp>

struct mock_consumer {
    struct result {
        size_t _depth;
        size_t _consume_new_partition_call_count = 0;
        size_t _consume_tombstone_call_count = 0;
        size_t _consume_end_of_partition_call_count = 0;
        bool _consume_end_of_stream_called = false;
        std::vector<mutation_fragment> _fragments;
    };
    const schema& _schema;
    reader_permit _permit;
    result _result;
    mock_consumer(const schema& s, reader_permit permit, size_t depth) : _schema(s), _permit(std::move(permit)) {
        _result._depth = depth;
    }
    stop_iteration update_depth() {
        --_result._depth;
        return _result._depth < 1 ? stop_iteration::yes : stop_iteration::no;
    }
    void consume_new_partition(const dht::decorated_key& dk) {
        ++_result._consume_new_partition_call_count;
    }
    stop_iteration consume(tombstone t) {
        ++_result._consume_tombstone_call_count;
        return stop_iteration::no;
    }
    stop_iteration consume(static_row&& sr) {
        _result._fragments.push_back(mutation_fragment(_schema, _permit, std::move(sr)));
        return update_depth();
    }
    stop_iteration consume(clustering_row&& cr) {
        _result._fragments.push_back(mutation_fragment(_schema, _permit, std::move(cr)));
        return update_depth();
    }
    stop_iteration consume(range_tombstone&& rt) {
        _result._fragments.push_back(mutation_fragment(_schema, _permit, std::move(rt)));
        return update_depth();
    }
    stop_iteration consume_end_of_partition() {
        ++_result._consume_end_of_partition_call_count;
        return update_depth();
    }
    result consume_end_of_stream() {
        _result._consume_end_of_stream_called = true;
        return std::move(_result);
    }
};

static size_t count_fragments(mutation m) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto r = flat_mutation_reader_from_mutations(semaphore.make_permit(), {m});
    auto close_reader = deferred_close(r);
    size_t res = 0;
    auto mfopt = r().get0();
    while (bool(mfopt)) {
        ++res;
        mfopt = r().get0();
    }
    return res;
}

SEASTAR_TEST_CASE(test_flat_mutation_reader_consume_single_partition) {
    return seastar::async([] {
        tests::reader_concurrency_semaphore_wrapper semaphore;
        tests::schema_registry_wrapper registry;
        for_each_mutation(registry, [&] (const mutation& m) {
            size_t fragments_in_m = count_fragments(m);
            for (size_t depth = 1; depth <= fragments_in_m + 1; ++depth) {
                auto r = flat_mutation_reader_from_mutations(semaphore.make_permit(), {m});
                auto close_reader = deferred_close(r);
                auto result = r.consume(mock_consumer(*m.schema(), semaphore.make_permit(), depth)).get0();
                BOOST_REQUIRE(result._consume_end_of_stream_called);
                BOOST_REQUIRE_EQUAL(1, result._consume_new_partition_call_count);
                BOOST_REQUIRE_EQUAL(1, result._consume_end_of_partition_call_count);
                BOOST_REQUIRE_EQUAL(m.partition().partition_tombstone() ? 1 : 0, result._consume_tombstone_call_count);
                auto r2 = assert_that(flat_mutation_reader_from_mutations(semaphore.make_permit(), {m}));
                r2.produces_partition_start(m.decorated_key(), m.partition().partition_tombstone());
                for (auto& mf : result._fragments) {
                    r2.produces(*m.schema(), mf);
                }
            }
        });
    });
}

SEASTAR_TEST_CASE(test_flat_mutation_reader_consume_two_partitions) {
    return seastar::async([] {
        tests::reader_concurrency_semaphore_wrapper semaphore;
        tests::schema_registry_wrapper registry;
        auto test = [&semaphore] (mutation m1, mutation m2) {
            size_t fragments_in_m1 = count_fragments(m1);
            size_t fragments_in_m2 = count_fragments(m2);
            for (size_t depth = 1; depth < fragments_in_m1; ++depth) {
                auto r = flat_mutation_reader_from_mutations(semaphore.make_permit(), {m1, m2});
                auto close_r = deferred_close(r);
                auto result = r.consume(mock_consumer(*m1.schema(), semaphore.make_permit(), depth)).get0();
                BOOST_REQUIRE(result._consume_end_of_stream_called);
                BOOST_REQUIRE_EQUAL(1, result._consume_new_partition_call_count);
                BOOST_REQUIRE_EQUAL(1, result._consume_end_of_partition_call_count);
                BOOST_REQUIRE_EQUAL(m1.partition().partition_tombstone() ? 1 : 0, result._consume_tombstone_call_count);
                auto r2 = flat_mutation_reader_from_mutations(semaphore.make_permit(), {m1, m2});
                auto close_r2 = deferred_close(r2);
                auto start = r2().get0();
                BOOST_REQUIRE(start);
                BOOST_REQUIRE(start->is_partition_start());
                for (auto& mf : result._fragments) {
                    auto mfopt = r2().get0();
                    BOOST_REQUIRE(mfopt);
                    BOOST_REQUIRE(mf.equal(*m1.schema(), *mfopt));
                }
            }
            for (size_t depth = fragments_in_m1; depth < fragments_in_m1 + fragments_in_m2 + 1; ++depth) {
                auto r = flat_mutation_reader_from_mutations(semaphore.make_permit(), {m1, m2});
                auto close_r = deferred_close(r);
                auto result = r.consume(mock_consumer(*m1.schema(), semaphore.make_permit(), depth)).get0();
                BOOST_REQUIRE(result._consume_end_of_stream_called);
                BOOST_REQUIRE_EQUAL(2, result._consume_new_partition_call_count);
                BOOST_REQUIRE_EQUAL(2, result._consume_end_of_partition_call_count);
                size_t tombstones_count = 0;
                if (m1.partition().partition_tombstone()) {
                    ++tombstones_count;
                }
                if (m2.partition().partition_tombstone()) {
                    ++tombstones_count;
                }
                BOOST_REQUIRE_EQUAL(tombstones_count, result._consume_tombstone_call_count);
                auto r2 = flat_mutation_reader_from_mutations(semaphore.make_permit(), {m1, m2});
                auto close_r2 = deferred_close(r2);
                auto start = r2().get0();
                BOOST_REQUIRE(start);
                BOOST_REQUIRE(start->is_partition_start());
                for (auto& mf : result._fragments) {
                    auto mfopt = r2().get0();
                    BOOST_REQUIRE(mfopt);
                    if (mfopt->is_partition_start() || mfopt->is_end_of_partition()) {
                        mfopt = r2().get0();
                    }
                    BOOST_REQUIRE(mfopt);
                    BOOST_REQUIRE(mf.equal(*m1.schema(), *mfopt));
                }
            }
        };
        for_each_mutation_pair(registry, [&] (auto&& m, auto&& m2, are_equal) {
            if (m.decorated_key().less_compare(*m.schema(), m2.decorated_key())) {
                test(m, m2);
            } else if (m2.decorated_key().less_compare(*m.schema(), m.decorated_key())) {
                test(m2, m);
            }
        });
    });
}

SEASTAR_TEST_CASE(test_fragmenting_and_freezing) {
    return seastar::async([] {
        tests::reader_concurrency_semaphore_wrapper semaphore;
        tests::schema_registry_wrapper registry;
        for_each_mutation(registry, [&] (const mutation& m) {
            std::vector<frozen_mutation> fms;

            fragment_and_freeze(flat_mutation_reader_from_mutations(semaphore.make_permit(), { mutation(m) }), [&] (auto fm, bool frag) {
                BOOST_REQUIRE(!frag);
                fms.emplace_back(std::move(fm));
                return make_ready_future<stop_iteration>(stop_iteration::no);
            }, std::numeric_limits<size_t>::max()).get0();

            BOOST_REQUIRE_EQUAL(fms.size(), 1);

            auto m1 = fms.back().unfreeze(m.schema());
            BOOST_REQUIRE_EQUAL(m, m1);

            fms.clear();

            std::optional<bool> fragmented;
            fragment_and_freeze(flat_mutation_reader_from_mutations(semaphore.make_permit(), { mutation(m) }), [&] (auto fm, bool frag) {
                BOOST_REQUIRE(!fragmented || *fragmented == frag);
                *fragmented = frag;
                fms.emplace_back(std::move(fm));
                return make_ready_future<stop_iteration>(stop_iteration::no);
            }, 1).get0();

            auto&& rows = m.partition().non_dummy_rows();
            auto expected_fragments = std::distance(rows.begin(), rows.end())
                                      + m.partition().row_tombstones().size()
                                      + !m.partition().static_row().empty();
            BOOST_REQUIRE_EQUAL(fms.size(), std::max(expected_fragments, size_t(1)));
            BOOST_REQUIRE(expected_fragments < 2 || *fragmented);

            auto m2 = fms.back().unfreeze(m.schema());
            fms.pop_back();
            mutation_application_stats app_stats;
            while (!fms.empty()) {
                m2.partition().apply(*m.schema(), fms.back().partition(), *m.schema(), app_stats);
                fms.pop_back();
            }
            BOOST_REQUIRE_EQUAL(m, m2);
        });

        auto test_random_streams = [&semaphore] (random_mutation_generator&& gen) {
            for (auto i = 0; i < 4; i++) {
                auto muts = gen(4);
                auto s = muts[0].schema();

                std::vector<frozen_mutation> frozen;

                // Freeze all
                fragment_and_freeze(flat_mutation_reader_from_mutations(semaphore.make_permit(), muts), [&] (auto fm, bool frag) {
                    BOOST_REQUIRE(!frag);
                    frozen.emplace_back(fm);
                    return make_ready_future<stop_iteration>(stop_iteration::no);
                }, std::numeric_limits<size_t>::max()).get0();
                BOOST_REQUIRE_EQUAL(muts.size(), frozen.size());
                for (auto j = 0u; j < muts.size(); j++) {
                    BOOST_REQUIRE_EQUAL(muts[j], frozen[j].unfreeze(s));
                }

                // Freeze first
                frozen.clear();
                fragment_and_freeze(flat_mutation_reader_from_mutations(semaphore.make_permit(), muts), [&] (auto fm, bool frag) {
                    BOOST_REQUIRE(!frag);
                    frozen.emplace_back(fm);
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }, std::numeric_limits<size_t>::max()).get0();
                BOOST_REQUIRE_EQUAL(frozen.size(), 1);
                BOOST_REQUIRE_EQUAL(muts[0], frozen[0].unfreeze(s));

                // Fragment and freeze all
                frozen.clear();
                fragment_and_freeze(flat_mutation_reader_from_mutations(semaphore.make_permit(), muts), [&] (auto fm, bool frag) {
                    frozen.emplace_back(fm);
                    return make_ready_future<stop_iteration>(stop_iteration::no);
                }, 1).get0();
                std::vector<mutation> unfrozen;
                while (!frozen.empty()) {
                    auto m = frozen.front().unfreeze(s);
                    frozen.erase(frozen.begin());
                    if (unfrozen.empty() || !unfrozen.back().decorated_key().equal(*s, m.decorated_key())) {
                        unfrozen.emplace_back(std::move(m));
                    } else {
                        unfrozen.back().apply(std::move(m));
                    }
                }
                BOOST_REQUIRE_EQUAL(muts, unfrozen);
            }
        };

        test_random_streams(random_mutation_generator(registry, random_mutation_generator::generate_counters::no));
        test_random_streams(random_mutation_generator(registry, random_mutation_generator::generate_counters::yes));
    });
}

SEASTAR_THREAD_TEST_CASE(test_flat_mutation_reader_move_buffer_content_to) {
    struct dummy_reader_impl : public flat_mutation_reader::impl {
        using flat_mutation_reader::impl::impl;
        virtual future<> fill_buffer() override { return make_ready_future<>(); }
        virtual future<> next_partition() override { return make_ready_future<>(); }
        virtual future<> fast_forward_to(const dht::partition_range&) override { return make_ready_future<>(); }
        virtual future<> fast_forward_to(position_range) override { return make_ready_future<>(); }
        virtual future<> close() noexcept override { return make_ready_future<>(); };
    };

    simple_schema s;
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto permit = semaphore.make_permit();
    auto pkey = s.make_pkey(1);
    auto mut_orig = mutation(s.schema(), pkey);

    auto mf_range = boost::irange(0, 50) | boost::adaptors::transformed([&] (auto n) {
        return s.make_row(permit, s.make_ckey(n), "a_16_byte_value_");
    });
    for (auto&& mf : mf_range) {
        mut_orig.apply(mf);
    }

    // Set a small size so we can fill the buffer at least two times, without
    // having to have loads of data.
    const auto max_buffer_size = size_t{100};

    auto reader = flat_mutation_reader_from_mutations(semaphore.make_permit(), {mut_orig}, dht::partition_range::make_open_ended_both_sides());
    auto close_reader = deferred_close(reader);
    auto dummy_impl = std::make_unique<dummy_reader_impl>(s.schema(), semaphore.make_permit());
    reader.set_max_buffer_size(max_buffer_size);

    reader.fill_buffer().get();
    BOOST_REQUIRE(reader.is_buffer_full());
    auto expected_buf_size = reader.buffer_size();

    // This should take the fast path, as dummy's buffer is empty.
    reader.move_buffer_content_to(*dummy_impl);
    BOOST_CHECK(reader.is_buffer_empty());
    BOOST_CHECK_EQUAL(reader.buffer_size(), 0);
    BOOST_CHECK_EQUAL(dummy_impl->buffer_size(), expected_buf_size);

    reader.fill_buffer().get();
    BOOST_REQUIRE(!reader.is_buffer_empty());
    expected_buf_size += reader.buffer_size();

    // This should take the slow path, as dummy's buffer is not empty.
    reader.move_buffer_content_to(*dummy_impl);
    BOOST_CHECK(reader.is_buffer_empty());
    BOOST_CHECK_EQUAL(reader.buffer_size(), 0);
    BOOST_CHECK_EQUAL(dummy_impl->buffer_size(), expected_buf_size);

    while (!reader.is_end_of_stream()) {
        reader.fill_buffer().get();
        expected_buf_size += reader.buffer_size();

        reader.move_buffer_content_to(*dummy_impl);
        BOOST_CHECK(reader.is_buffer_empty());
        BOOST_CHECK_EQUAL(reader.buffer_size(), 0);
        BOOST_CHECK_EQUAL(dummy_impl->buffer_size(), expected_buf_size);
    }

    auto dummy_reader = flat_mutation_reader(std::move(dummy_impl));
    auto close_dummy_reader = deferred_close(dummy_reader);
    auto mut_new = read_mutation_from_flat_mutation_reader(dummy_reader).get0();

    assert_that(mut_new)
        .has_mutation()
        .is_equal_to(mut_orig);
}

SEASTAR_TEST_CASE(test_multi_range_reader) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto permit = semaphore.make_permit();

        auto keys = s.make_pkeys(10);
        auto ring = s.to_ring_positions(keys);

        auto crs = boost::copy_range<std::vector<mutation_fragment>>(boost::irange(0, 3) | boost::adaptors::transformed([&] (auto n) {
            return s.make_row(permit, s.make_ckey(n), "value");
        }));

        auto ms = boost::copy_range<std::vector<mutation>>(keys | boost::adaptors::transformed([&] (auto& key) {
            auto m = mutation(s.schema(), key);
            for (auto& mf : crs) {
                m.apply(mf);
            }
            return m;
        }));

        auto source = mutation_source([&] (schema_ptr, reader_permit permit, const dht::partition_range& range) {
            return flat_mutation_reader_from_mutations(std::move(permit), ms, range);
        });

        const auto empty_ranges = dht::partition_range_vector{};
        const auto single_ranges = dht::partition_range_vector{
                dht::partition_range::make(ring[1], ring[2]),
        };
        const auto multiple_ranges = dht::partition_range_vector {
                dht::partition_range::make(ring[1], ring[2]),
                dht::partition_range::make_singular(ring[4]),
                dht::partition_range::make(ring[6], ring[8]),
        };
        const auto empty_generator = [] { return std::optional<dht::partition_range>{}; };
        const auto single_generator = [r = std::optional<dht::partition_range>(single_ranges.front())] () mutable {
            return std::exchange(r, {});
        };
        const auto multiple_generator = [it = multiple_ranges.cbegin(), end = multiple_ranges.cend()] () mutable -> std::optional<dht::partition_range> {
            if (it == end) {
                return std::nullopt;
            }
            return *(it++);
        };
        auto fft_range = dht::partition_range::make_starting_with(ring[9]);

        // Generator ranges are single pass, so we need a new range each time they are used.
        auto run_test = [&] (auto make_empty_ranges, auto make_single_ranges, auto make_multiple_ranges) {
            testlog.info("empty ranges");
            assert_that(make_flat_multi_range_reader(s.schema(), semaphore.make_permit(), source, make_empty_ranges(), s.schema()->full_slice()))
                    .produces_end_of_stream()
                    .fast_forward_to(fft_range)
                    .produces(ms[9])
                    .produces_end_of_stream();

            testlog.info("single range");
            assert_that(make_flat_multi_range_reader(s.schema(), semaphore.make_permit(), source, make_single_ranges(), s.schema()->full_slice()))
                    .produces(ms[1])
                    .produces(ms[2])
                    .produces_end_of_stream()
                    .fast_forward_to(fft_range)
                    .produces(ms[9])
                    .produces_end_of_stream();

            testlog.info("read full partitions and fast forward");
            assert_that(make_flat_multi_range_reader(s.schema(), semaphore.make_permit(), source, make_multiple_ranges(), s.schema()->full_slice()))
                    .produces(ms[1])
                    .produces(ms[2])
                    .produces(ms[4])
                    .produces(ms[6])
                    .fast_forward_to(fft_range)
                    .produces(ms[9])
                    .produces_end_of_stream();

            testlog.info("read, skip partitions and fast forward");
            assert_that(make_flat_multi_range_reader(s.schema(), semaphore.make_permit(), source, make_multiple_ranges(), s.schema()->full_slice()))
                    .produces_partition_start(keys[1])
                    .next_partition()
                    .produces_partition_start(keys[2])
                    .produces_row_with_key(crs[0].as_clustering_row().key())
                    .next_partition()
                    .produces(ms[4])
                    .next_partition()
                    .produces_partition_start(keys[6])
                    .produces_row_with_key(crs[0].as_clustering_row().key())
                    .produces_row_with_key(crs[1].as_clustering_row().key())
                    .fast_forward_to(fft_range)
                    .next_partition()
                    .produces_partition_start(keys[9])
                    .next_partition()
                    .produces_end_of_stream();
        };

        testlog.info("vector version");
        run_test(
                [&] { return empty_ranges; },
                [&] { return single_ranges; },
                [&] { return multiple_ranges; });

        testlog.info("generator version");
        run_test(
                [&] { return empty_generator; },
                [&] { return single_generator; },
                [&] { return multiple_generator; });
    });
}

using reversed_partitions = seastar::bool_class<class reversed_partitions_tag>;
using skip_after_first_fragment = seastar::bool_class<class skip_after_first_fragment_tag>;
using skip_after_first_partition = seastar::bool_class<class skip_after_first_partition_tag>;
using in_thread = seastar::bool_class<class in_thread_tag>;

struct flat_stream_consumer {
    schema_ptr _schema;
    schema_ptr _reversed_schema;
    reader_permit _permit;
    skip_after_first_fragment _skip_partition;
    skip_after_first_partition _skip_stream;
    std::vector<mutation> _mutations;
    std::optional<position_in_partition> _previous_position;
    bool _inside_partition = false;
private:
    void verify_order(position_in_partition_view pos) {
        const schema& s = _reversed_schema ? *_reversed_schema : *_schema;
        position_in_partition::less_compare cmp(s);
        BOOST_REQUIRE(!_previous_position || _previous_position->is_static_row() || cmp(*_previous_position, pos));
    }
public:
    flat_stream_consumer(schema_ptr s, reader_permit permit, reversed_partitions reversed,
                         skip_after_first_fragment skip_partition = skip_after_first_fragment::no,
                         skip_after_first_partition skip_stream = skip_after_first_partition::no)
        : _schema(std::move(s))
        , _reversed_schema(reversed ? _schema->make_reversed() : nullptr)
        , _permit(std::move(permit))
        , _skip_partition(skip_partition)
        , _skip_stream(skip_stream)
    { }
    void consume_new_partition(dht::decorated_key dk) {
        BOOST_REQUIRE(!_inside_partition);
        BOOST_REQUIRE(!_previous_position);
        _mutations.emplace_back(_schema, dk);
        _inside_partition = true;
    }
    void consume(tombstone pt) {
        BOOST_REQUIRE(_inside_partition);
        BOOST_REQUIRE(!_previous_position);
        BOOST_REQUIRE_GE(_mutations.size(), 1);
        _mutations.back().partition().apply(pt);
    }
    stop_iteration consume(static_row&& sr) {
        BOOST_REQUIRE(_inside_partition);
        BOOST_REQUIRE(!_previous_position);
        BOOST_REQUIRE_GE(_mutations.size(), 1);
        _previous_position.emplace(sr.position());
        _mutations.back().partition().apply(*_schema, mutation_fragment(*_schema, _permit, std::move(sr)));
        return stop_iteration(bool(_skip_partition));
    }
    stop_iteration consume(clustering_row&& cr) {
        BOOST_REQUIRE(_inside_partition);
        verify_order(cr.position());
        BOOST_REQUIRE_GE(_mutations.size(), 1);
        _previous_position.emplace(cr.position());
        _mutations.back().partition().apply(*_schema, mutation_fragment(*_schema, _permit, std::move(cr)));
        return stop_iteration(bool(_skip_partition));
    }
    stop_iteration consume(range_tombstone&& rt) {
        BOOST_REQUIRE(_inside_partition);
        auto pos = rt.position();
        verify_order(pos);
        BOOST_REQUIRE_GE(_mutations.size(), 1);
        _previous_position.emplace(pos);
        if (_reversed_schema) {
            rt.reverse(); // undo the reversing
        }
        _mutations.back().partition().apply(*_schema, mutation_fragment(*_schema, _permit, std::move(rt)));
        return stop_iteration(bool(_skip_partition));
    }
    stop_iteration consume_end_of_partition() {
        BOOST_REQUIRE(_inside_partition);
        BOOST_REQUIRE_GE(_mutations.size(), 1);
        _previous_position = std::nullopt;
        _inside_partition = false;
        return stop_iteration(bool(_skip_stream));
    }
    std::vector<mutation> consume_end_of_stream() {
        BOOST_REQUIRE(!_inside_partition);
        return std::move(_mutations);
    }
};

void test_flat_stream(schema_ptr s, std::vector<mutation> muts, reversed_partitions reversed, in_thread thread) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto reversed_msg = reversed ? ", reversed partitions" : "";

    auto consume_fn = [&] (flat_mutation_reader& fmr, flat_stream_consumer fsc) {
        if (thread) {
            assert(bool(!reversed));
            return fmr.consume_in_thread(std::move(fsc));
        } else {
            if (reversed) {
                return with_closeable(make_reversing_reader(make_flat_mutation_reader<delegating_reader>(fmr), query::max_result_size(size_t(1) << 20)),
                        [fsc = std::move(fsc)] (flat_mutation_reader& reverse_reader) mutable {
                    return reverse_reader.consume(std::move(fsc));
                }).get0();
            }
            return fmr.consume(std::move(fsc)).get0();
        }
    };

  {
    testlog.info("Consume all{}", reversed_msg);
    auto fmr = flat_mutation_reader_from_mutations(semaphore.make_permit(), muts);
    auto close_fmr = deferred_close(fmr);
    auto muts2 = consume_fn(fmr, flat_stream_consumer(s, semaphore.make_permit(), reversed));
    BOOST_REQUIRE_EQUAL(muts, muts2);
  }

  {
    testlog.info("Consume first fragment from partition{}", reversed_msg);
    auto fmr = flat_mutation_reader_from_mutations(semaphore.make_permit(), muts);
    auto close_fmr = deferred_close(fmr);
    auto muts2 = consume_fn(fmr, flat_stream_consumer(s, semaphore.make_permit(), reversed, skip_after_first_fragment::yes));
    BOOST_REQUIRE_EQUAL(muts.size(), muts2.size());
    for (auto j = 0u; j < muts.size(); j++) {
        BOOST_REQUIRE(muts[j].decorated_key().equal(*muts[j].schema(), muts2[j].decorated_key()));
        auto& mp = muts2[j].partition();
        BOOST_REQUIRE_LE(mp.static_row().empty() + mp.clustered_rows().calculate_size() + mp.row_tombstones().size(), 1);
        auto m = muts[j];
        m.apply(muts2[j]);
        BOOST_REQUIRE_EQUAL(m, muts[j]);
    }
  }

  {
    testlog.info("Consume first partition{}", reversed_msg);
    auto fmr = flat_mutation_reader_from_mutations(semaphore.make_permit(), muts);
    auto close_fmr = deferred_close(fmr);
    auto muts2 = consume_fn(fmr, flat_stream_consumer(s, semaphore.make_permit(), reversed, skip_after_first_fragment::no,
                                             skip_after_first_partition::yes));
    BOOST_REQUIRE_EQUAL(muts2.size(), 1);
    BOOST_REQUIRE_EQUAL(muts2[0], muts[0]);
  }

    if (thread) {
        auto filter = flat_mutation_reader::filter([&] (const dht::decorated_key& dk) {
            for (auto j = size_t(0); j < muts.size(); j += 2) {
                if (dk.equal(*s, muts[j].decorated_key())) {
                    return false;
                }
            }
            return true;
        });
        testlog.info("Consume all, filtered");
        auto fmr = flat_mutation_reader_from_mutations(semaphore.make_permit(), muts);
        auto close_fmr = deferred_close(fmr);
        auto muts2 = fmr.consume_in_thread(flat_stream_consumer(s, semaphore.make_permit(), reversed), std::move(filter));
        BOOST_REQUIRE_EQUAL(muts.size() / 2, muts2.size());
        for (auto j = size_t(1); j < muts.size(); j += 2) {
            BOOST_REQUIRE_EQUAL(muts[j], muts2[j / 2]);
        }
    }
}

SEASTAR_TEST_CASE(test_consume_flat) {
    return seastar::async([] {
        auto test_random_streams = [&] (random_mutation_generator&& gen) {
            for (auto i = 0; i < 4; i++) {
                auto muts = gen(4);
                test_flat_stream(gen.schema(), muts, reversed_partitions::no, in_thread::no);
                test_flat_stream(gen.schema(), muts, reversed_partitions::yes, in_thread::no);
                test_flat_stream(gen.schema(), muts, reversed_partitions::no, in_thread::yes);
            }
        };

        tests::schema_registry_wrapper registry;

        test_random_streams(random_mutation_generator(registry, random_mutation_generator::generate_counters::no));
        test_random_streams(random_mutation_generator(registry, random_mutation_generator::generate_counters::yes));
    });
}

SEASTAR_TEST_CASE(test_make_forwardable) {
    return seastar::async([] {
        simple_schema s;
        tests::reader_concurrency_semaphore_wrapper semaphore;
        auto permit = semaphore.make_permit();

        auto keys = s.make_pkeys(10);

        auto crs = boost::copy_range < std::vector <
                   mutation_fragment >> (boost::irange(0, 3) | boost::adaptors::transformed([&](auto n) {
                       return s.make_row(permit, s.make_ckey(n), "value");
                   }));

        auto ms = boost::copy_range < std::vector < mutation >> (keys | boost::adaptors::transformed([&](auto &key) {
            auto m = mutation(s.schema(), key);
            for (auto &mf : crs) {
                m.apply(mf);
            }
            return m;
        }));

        auto make_reader = [&] (auto& range) {
            return assert_that(
                make_forwardable(flat_mutation_reader_from_mutations(semaphore.make_permit(), ms, range, streamed_mutation::forwarding::no)));
        };

        auto test = [&] (auto& rd, auto& partition) {
            rd.produces_partition_start(partition.decorated_key(), partition.partition().partition_tombstone());
            rd.produces_end_of_stream();
            rd.fast_forward_to(position_range::all_clustered_rows());
            for (auto &row : partition.partition().clustered_rows()) {
                rd.produces_row_with_key(row.key());
            }
            rd.produces_end_of_stream();
            rd.next_partition();
        };

        auto rd = make_reader(query::full_partition_range);

        for (auto& partition : ms) {
            test(rd, partition);
        }

        auto single_range = dht::partition_range::make_singular(ms[0].decorated_key());

        auto rd2 = make_reader(single_range);

        rd2.produces_partition_start(ms[0].decorated_key(), ms[0].partition().partition_tombstone());
        rd2.produces_end_of_stream();
        rd2.fast_forward_to(position_range::all_clustered_rows());
        rd2.produces_row_with_key(ms[0].partition().clustered_rows().begin()->key());
        rd2.produces_row_with_key(std::next(ms[0].partition().clustered_rows().begin())->key());

        auto remaining_range = dht::partition_range::make_starting_with({ms[0].decorated_key(), false});

        rd2.fast_forward_to(remaining_range);

        for (auto i = size_t(1); i < ms.size(); ++i) {
            test(rd2, ms[i]);
        }
    });
}

SEASTAR_TEST_CASE(test_abandoned_flat_mutation_reader_from_mutation) {
    return seastar::async([] {
        tests::reader_concurrency_semaphore_wrapper semaphore;
        tests::schema_registry_wrapper registry;
        for_each_mutation(registry, [&] (const mutation& m) {
            auto rd = flat_mutation_reader_from_mutations(semaphore.make_permit(), {mutation(m)});
            auto close_rd = deferred_close(rd);
            rd().get();
            rd().get();
            // We rely on AddressSanitizer telling us if nothing was leaked.
        });
    });
}

static std::vector<mutation> squash_mutations(std::vector<mutation> mutations) {
    if (mutations.empty()) {
        return {};
    }
    std::map<dht::decorated_key, mutation, dht::ring_position_less_comparator> merged_muts{
            dht::ring_position_less_comparator{*mutations.front().schema()}};
    for (const auto& mut : mutations) {
        auto [it, inserted] = merged_muts.try_emplace(mut.decorated_key(), mut);
        if (!inserted) {
            it->second.apply(mut);
        }
    }
    return boost::copy_range<std::vector<mutation>>(merged_muts | boost::adaptors::map_values);
}

SEASTAR_THREAD_TEST_CASE(test_mutation_reader_from_mutations_as_mutation_source) {
    auto populate = [] (schema_ptr, const std::vector<mutation> &muts) {
        return mutation_source([=] (
                schema_ptr schema,
                reader_permit permit,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                const io_priority_class&,
                tracing::trace_state_ptr,
                streamed_mutation::forwarding fwd_sm,
                mutation_reader::forwarding) mutable {
            return flat_mutation_reader_from_mutations(std::move(permit), squash_mutations(muts), range, slice, fwd_sm);
        });
    };
    run_mutation_source_tests(populate);
}

SEASTAR_THREAD_TEST_CASE(test_mutation_reader_from_fragments_as_mutation_source) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto populate = [] (schema_ptr, const std::vector<mutation> &muts) {
        return mutation_source([=] (
                schema_ptr schema,
                reader_permit permit,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                const io_priority_class&,
                tracing::trace_state_ptr,
                streamed_mutation::forwarding fwd_sm,
                mutation_reader::forwarding) mutable {
            auto get_fragments = [&permit, &muts] {
                std::deque<mutation_fragment> fragments;
                auto rd = flat_mutation_reader_from_mutations(permit, squash_mutations(muts));
                auto close_rd = deferred_close(rd);
                rd.consume_pausable([&fragments] (mutation_fragment mf) {
                    fragments.emplace_back(std::move(mf));
                    return stop_iteration::no;
                }).get();
                return fragments;
            };

            auto rd = make_flat_mutation_reader_from_fragments(schema, permit, get_fragments(), range, slice);
            if (fwd_sm) {
                return make_forwardable(std::move(rd));
            }
            return rd;
        });
    };
    run_mutation_source_tests(populate);
}

SEASTAR_THREAD_TEST_CASE(test_reverse_reader_memory_limit) {
    simple_schema schema;
    tests::reader_concurrency_semaphore_wrapper semaphore;

    struct phony_consumer {
        void consume_new_partition(const dht::decorated_key&) { }
        void consume(tombstone) { }
        stop_iteration consume(static_row&&) { return stop_iteration::no; }
        stop_iteration consume(clustering_row&&) { return stop_iteration::no; }
        stop_iteration consume(range_tombstone&&) { return stop_iteration::no; }
        stop_iteration consume_end_of_partition() { return stop_iteration::no; }
        void consume_end_of_stream() { }
    };

    auto test_with_partition = [&] (bool with_static_row) {
        testlog.info("Testing with_static_row={}", with_static_row);
        const auto pk = "pk1";
        auto mut = schema.new_mutation(pk);
        const size_t desired_mut_size = 1 * 1024 * 1024;
        const size_t row_size = 10 * 1024;

        if (with_static_row) {
            schema.add_static_row(mut, "s1");
        }

        for (size_t i = 0; i < desired_mut_size / row_size; ++i) {
            schema.add_row(mut, schema.make_ckey(++i), sstring(row_size, '0'));
        }

        const uint64_t hard_limit = size_t(1) << 18;
        auto reverse_reader = make_reversing_reader(flat_mutation_reader_from_mutations(semaphore.make_permit(), {mut}),
                query::max_result_size(size_t(1) << 10, hard_limit));
        auto close_reverse_reader = deferred_close(reverse_reader);

        try {
            reverse_reader.consume(phony_consumer{}).get();
            BOOST_FAIL("No exception thrown for reversing overly big partition");
        } catch (const std::runtime_error& e) {
            testlog.info("Got exception with message: {}", e.what());
            auto str = sstring(e.what());
            const auto expected_str = format(
                    "Memory usage of reversed read exceeds hard limit of {} (configured via max_memory_for_unlimited_query_hard_limit), while reading partition {}",
                    hard_limit,
                    pk);

            BOOST_REQUIRE_EQUAL(str.find(expected_str), 0);
        } catch (...) {
            throw;
        }
    };

    test_with_partition(true);
    test_with_partition(false);
}

SEASTAR_THREAD_TEST_CASE(test_reverse_reader_reads_in_native_reverse_order) {
    using namespace tests::data_model;
    using key_range = nonwrapping_interval<mutation_description::key>;

    std::mt19937 engine(tests::random::get_int<uint32_t>());

    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto permit = semaphore.make_permit();

    auto rnd_schema_spec = tests::make_random_schema_specification(
            get_name(),
            std::uniform_int_distribution<size_t>(1, 2),
            std::uniform_int_distribution<size_t>(1, 8));
    auto rnd_schema = tests::random_schema(engine(), *rnd_schema_spec);

    auto forward_schema = rnd_schema.schema();
    auto reverse_schema = forward_schema->make_reversed();

    auto forward_mt = make_lw_shared<memtable>(forward_schema);
    auto reverse_mt = make_lw_shared<memtable>(reverse_schema);

    for (size_t pk = 0; pk != 8; ++pk) {
        auto mut = rnd_schema.new_mutation(pk);

        if (forward_schema->has_static_columns()) {
            rnd_schema.add_static_row(engine, mut);
        }

        auto ckeys = rnd_schema.make_ckeys(8);

        for (size_t ck = 0; ck != ckeys.size(); ++ck) {
            const auto& ckey = ckeys.at(ck);
            if (ck % 4 == 0 && ck + 3 < ckeys.size()) {
                const auto& ckey_1 = ckeys.at(ck + 1);
                const auto& ckey_2 = ckeys.at(ck + 2);
                const auto& ckey_3 = ckeys.at(ck + 3);
                rnd_schema.delete_range(engine, mut, key_range::make({ckey, true}, {ckey_2, true}));
                rnd_schema.delete_range(engine, mut, key_range::make({ckey, true}, {ckey_3, true}));
                rnd_schema.delete_range(engine, mut, key_range::make({ckey_1, true}, {ckey_3, true}));
            }
            rnd_schema.add_row(engine, mut, ckey);
        }

        forward_mt->apply(mut.build(forward_schema));
        reverse_mt->apply(mut.build(reverse_schema));
    }

    auto reversed_forward_reader = assert_that(make_reversing_reader(forward_mt->make_flat_reader(forward_schema, permit), query::max_result_size(1 << 20)));

    auto reverse_reader = reverse_mt->make_flat_reader(reverse_schema, permit);
    auto deferred_reverse_close = deferred_close(reverse_reader);

    while (auto mf_opt = reverse_reader().get()) {
        auto& mf = *mf_opt;
        reversed_forward_reader.produces(*forward_schema, mf);
    }
    reversed_forward_reader.produces_end_of_stream();
}

SEASTAR_THREAD_TEST_CASE(test_reverse_reader_is_mutation_source) {
    std::list<query::partition_slice> reversed_slices;
    auto populate = [&reversed_slices] (schema_ptr s, const std::vector<mutation> &muts) {
        auto reverse_schema = s->make_reversed();
        auto reverse_mt = make_lw_shared<memtable>(reverse_schema);
        for (const auto& mut : muts) {
            reverse_mt->apply(reverse(mut));
        }

        return mutation_source([=, &reversed_slices] (
                schema_ptr schema,
                reader_permit permit,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                const io_priority_class& pc,
                tracing::trace_state_ptr trace_ptr,
                streamed_mutation::forwarding fwd_sm,
                mutation_reader::forwarding fwd_mr) mutable {
            flat_mutation_reader rd(nullptr);

            schema = schema->make_reversed();
            const auto reversed = slice.options.contains(query::partition_slice::option::reversed);
            if (reversed) {
                reversed_slices.emplace_back(query::half_reverse_slice(*schema, slice));
                rd = flat_mutation_reader_from_mutations(std::move(permit), squash_mutations(muts), range, reversed_slices.back());
            } else {
                reversed_slices.emplace_back(query::reverse_slice(*schema, slice));
                // We don't want the memtable reader to read in reverse.
                reversed_slices.back().options.remove(query::partition_slice::option::reversed);
                rd = reverse_mt->make_flat_reader(schema, std::move(permit), range, reversed_slices.back(), pc, std::move(trace_ptr),
                        streamed_mutation::forwarding::no, fwd_mr);
            }

            rd = make_reversing_reader(std::move(rd), query::max_result_size(1 << 20));

            if (fwd_sm) {
                return make_forwardable(std::move(rd));
            }
            return rd;
        });
    };
    run_mutation_source_tests(populate);
}
