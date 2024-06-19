/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <seastar/core/thread.hh>
#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/closeable.hh>

#include <fmt/ranges.h>

#include "mutation/mutation.hh"
#include "mutation/mutation_fragment.hh"
#include "readers/mutation_reader.hh"
#include "test/lib/mutation_source_test.hh"
#include "readers/reversing_v2.hh"
#include "readers/delegating_v2.hh"
#include "readers/multi_range.hh"
#include "replica/memtable.hh"
#include "row_cache.hh"
#include "mutation/mutation_rebuilder.hh"
#include "utils/assert.hh"
#include "utils/to_string.hh"

#include "test/lib/simple_schema.hh"
#include "test/lib/mutation_reader_assertions.hh"
#include "test/lib/log.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/random_schema.hh"
#include "test/lib/test_utils.hh"

#include "readers/from_mutations_v2.hh"
#include "readers/from_fragments_v2.hh"
#include "readers/forwardable_v2.hh"
#include "readers/compacting.hh"
#include "readers/nonforwardable.hh"

struct mock_consumer {
    struct result {
        ssize_t _depth;
        size_t _consume_new_partition_call_count = 0;
        size_t _consume_tombstone_call_count = 0;
        size_t _consume_end_of_partition_call_count = 0;
        bool _consume_end_of_stream_called = false;
        std::vector<mutation_fragment_v2> _fragments;
    };
    const schema& _schema;
    reader_permit _permit;
    result _result;
    mock_consumer(const schema& s, reader_permit permit, size_t depth) : _schema(s), _permit(std::move(permit)) {
        _result._depth = depth;
        testlog.debug("mock_consumer [{}] constructed: depth={}", fmt::ptr(this), _result._depth);
    }
    stop_iteration update_depth() {
        --_result._depth;
        auto stop = _result._depth < 1 ? stop_iteration::yes : stop_iteration::no;
        testlog.debug("mock_consumer [{}]: update_depth: depth={} stop_iteration={}", fmt::ptr(this), _result._depth, stop);
        return stop;
    }
    void consume_new_partition(const dht::decorated_key& dk) {
        ++_result._consume_new_partition_call_count;
        testlog.debug("mock_consumer [{}]: consume_new_partition: {}: {}", fmt::ptr(this), dk, _result._consume_new_partition_call_count);
    }
    stop_iteration consume(tombstone t) {
        ++_result._consume_tombstone_call_count;
        testlog.debug("mock_consumer [{}]: consume tombstone: {}", fmt::ptr(this), _result._consume_tombstone_call_count);
        return stop_iteration::no;
    }
    stop_iteration consume(static_row&& sr) {
        testlog.debug("mock_consumer [{}]: consume static_row", fmt::ptr(this));
        _result._fragments.emplace_back(_schema, _permit, std::move(sr));
        return update_depth();
    }
    stop_iteration consume(clustering_row&& cr) {
        testlog.debug("mock_consumer [{}]: consume clustering_row: {}", fmt::ptr(this), cr.position());
        _result._fragments.emplace_back(_schema, _permit, std::move(cr));
        return update_depth();
    }
    stop_iteration consume(range_tombstone_change&& rtc) {
        testlog.debug("mock_consumer [{}]: consume range_tombstone_change: {} {}", fmt::ptr(this), rtc.position(), rtc.tombstone());
        _result._fragments.emplace_back(_schema, _permit, std::move(rtc));
        return update_depth();
    }
    stop_iteration consume_end_of_partition() {
        ++_result._consume_end_of_partition_call_count;
        testlog.debug("mock_consumer [{}]: consume_end_of_partition: {}", fmt::ptr(this), _result._consume_end_of_partition_call_count);
        return update_depth();
    }
    result consume_end_of_stream() {
        _result._consume_end_of_stream_called = true;
        testlog.debug("mock_consumer [{}]: consume_end_of_stream", fmt::ptr(this));
        return std::move(_result);
    }
};

static size_t count_fragments(mutation m) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto r = make_mutation_reader_from_mutations_v2(m.schema(), semaphore.make_permit(), m);
    auto close_reader = deferred_close(r);
    size_t res = 0;
    auto mfopt = r().get();
    while (bool(mfopt)) {
        ++res;
        mfopt = r().get();
    }
    return res;
}

SEASTAR_THREAD_TEST_CASE(test_mutation_reader_consume_single_partition) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    for_each_mutation([&] (const mutation& m) {
        size_t fragments_in_m = count_fragments(m);
        for (size_t depth = 1; depth <= fragments_in_m + 1; ++depth) {
            auto r = make_mutation_reader_from_mutations_v2(m.schema(), semaphore.make_permit(), m);
            auto close_reader = deferred_close(r);
            auto result = r.consume(mock_consumer(*m.schema(), semaphore.make_permit(), depth)).get();
            BOOST_REQUIRE(result._consume_end_of_stream_called);
            BOOST_REQUIRE_EQUAL(1, result._consume_new_partition_call_count);
            BOOST_REQUIRE_EQUAL(1, result._consume_end_of_partition_call_count);
            BOOST_REQUIRE_EQUAL(m.partition().partition_tombstone() ? 1 : 0, result._consume_tombstone_call_count);
            auto r2 = assert_that(make_mutation_reader_from_mutations_v2(m.schema(), semaphore.make_permit(), m));
            r2.produces_partition_start(m.decorated_key(), m.partition().partition_tombstone());
            if (result._fragments.empty()) {
                continue;
            }
            for (auto& mf : result._fragments) {
                r2.produces(*m.schema(), mf);
            }
        }
    });
}

SEASTAR_THREAD_TEST_CASE(test_mutation_reader_consume_two_partitions) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto test = [&semaphore] (mutation m1, mutation m2) {
        size_t fragments_in_m1 = count_fragments(m1);
        size_t fragments_in_m2 = count_fragments(m2);
        for (size_t depth = 1; depth < fragments_in_m1; ++depth) {
            auto r = make_mutation_reader_from_mutations_v2(m1.schema(), semaphore.make_permit(), {m1, m2});
            auto close_r = deferred_close(r);
            auto result = r.consume(mock_consumer(*m1.schema(), semaphore.make_permit(), depth)).get();
            BOOST_REQUIRE(result._consume_end_of_stream_called);
            BOOST_REQUIRE_EQUAL(1, result._consume_new_partition_call_count);
            BOOST_REQUIRE_EQUAL(1, result._consume_end_of_partition_call_count);
            BOOST_REQUIRE_EQUAL(m1.partition().partition_tombstone() ? 1 : 0, result._consume_tombstone_call_count);
            auto r2 = make_mutation_reader_from_mutations_v2(m1.schema(), semaphore.make_permit(), {m1, m2});
            auto close_r2 = deferred_close(r2);
            auto start = r2().get();
            BOOST_REQUIRE(start);
            BOOST_REQUIRE(start->is_partition_start());
            for (auto& mf : result._fragments) {
                auto mfopt = r2().get();
                BOOST_REQUIRE(mfopt);
                BOOST_REQUIRE(mf.equal(*m1.schema(), *mfopt));
            }
        }
        for (size_t depth = fragments_in_m1; depth < fragments_in_m1 + fragments_in_m2 + 1; ++depth) {
            auto r = make_mutation_reader_from_mutations_v2(m1.schema(), semaphore.make_permit(), {m1, m2});
            auto close_r = deferred_close(r);
            auto result = r.consume(mock_consumer(*m1.schema(), semaphore.make_permit(), depth)).get();
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
            auto r2 = make_mutation_reader_from_mutations_v2(m1.schema(), semaphore.make_permit(), {m1, m2});
            auto close_r2 = deferred_close(r2);
            auto start = r2().get();
            BOOST_REQUIRE(start);
            BOOST_REQUIRE(start->is_partition_start());
            for (auto& mf : result._fragments) {
                auto mfopt = r2().get();
                BOOST_REQUIRE(mfopt);
                if (mfopt->is_partition_start() || mfopt->is_end_of_partition()) {
                    mfopt = r2().get();
                }
                BOOST_REQUIRE(mfopt);
                BOOST_REQUIRE(mf.equal(*m1.schema(), *mfopt));
            }
        }
    };
    for_each_mutation_pair([&] (auto&& m, auto&& m2, are_equal) {
        if (m.decorated_key().less_compare(*m.schema(), m2.decorated_key())) {
            test(m, m2);
        } else if (m2.decorated_key().less_compare(*m.schema(), m.decorated_key())) {
            test(m2, m);
        }
    });
}

SEASTAR_THREAD_TEST_CASE(test_fragmenting_and_freezing) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    for_each_mutation([&] (const mutation& m) {
        std::vector<frozen_mutation> fms;

        fragment_and_freeze(make_mutation_reader_from_mutations_v2(m.schema(), semaphore.make_permit(), mutation(m)), [&] (auto fm, bool frag) {
            BOOST_REQUIRE(!frag);
            fms.emplace_back(std::move(fm));
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }, std::numeric_limits<size_t>::max()).get();

        BOOST_REQUIRE_EQUAL(fms.size(), 1);

        auto m1 = fms.back().unfreeze(m.schema());
        BOOST_REQUIRE_EQUAL(m, m1);

        fms.clear();

        std::optional<bool> fragmented;
        fragment_and_freeze(make_mutation_reader_from_mutations_v2(m.schema(), semaphore.make_permit(), mutation(m)), [&] (auto fm, bool frag) {
            BOOST_REQUIRE(!fragmented || *fragmented == frag);
            *fragmented = frag;
            fms.emplace_back(std::move(fm));
            return make_ready_future<stop_iteration>(stop_iteration::no);
        }, 1).get();

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
            fragment_and_freeze(make_mutation_reader_from_mutations_v2(gen.schema(), semaphore.make_permit(), muts), [&] (auto fm, bool frag) {
                BOOST_REQUIRE(!frag);
                frozen.emplace_back(fm);
                return make_ready_future<stop_iteration>(stop_iteration::no);
            }, std::numeric_limits<size_t>::max()).get();
            BOOST_REQUIRE_EQUAL(muts.size(), frozen.size());
            for (auto j = 0u; j < muts.size(); j++) {
                BOOST_REQUIRE_EQUAL(muts[j], frozen[j].unfreeze(s));
            }

            // Freeze first
            frozen.clear();
            fragment_and_freeze(make_mutation_reader_from_mutations_v2(gen.schema(), semaphore.make_permit(), muts), [&] (auto fm, bool frag) {
                BOOST_REQUIRE(!frag);
                frozen.emplace_back(fm);
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }, std::numeric_limits<size_t>::max()).get();
            BOOST_REQUIRE_EQUAL(frozen.size(), 1);
            BOOST_REQUIRE_EQUAL(muts[0], frozen[0].unfreeze(s));

            // Fragment and freeze all
            frozen.clear();
            fragment_and_freeze(make_mutation_reader_from_mutations_v2(gen.schema(), semaphore.make_permit(), muts), [&] (auto fm, bool frag) {
                frozen.emplace_back(fm);
                return make_ready_future<stop_iteration>(stop_iteration::no);
            }, 1).get();
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

    test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::no));
    test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::yes));
}

SEASTAR_THREAD_TEST_CASE(test_mutation_reader_move_buffer_content_to) {
    struct dummy_reader_impl : public mutation_reader::impl {
        using mutation_reader::impl::impl;
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

    const auto prange = dht::partition_range::make_open_ended_both_sides();
    auto reader = make_mutation_reader_from_mutations_v2(s.schema(), semaphore.make_permit(), {mut_orig}, prange);
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

    auto dummy_reader = mutation_reader(std::move(dummy_impl));
    auto close_dummy_reader = deferred_close(dummy_reader);
    auto mut_new = read_mutation_from_mutation_reader(dummy_reader).get();

    assert_that(mut_new)
        .has_mutation()
        .is_equal_to(mut_orig);
}

SEASTAR_THREAD_TEST_CASE(test_multi_range_reader) {
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
        return make_mutation_reader_from_mutations_v2(s.schema(), std::move(permit), ms, range);
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
    std::optional<mutation_rebuilder_v2> _mut;
    std::optional<position_in_partition> _previous_position;
    tombstone _current_tombstone;
    circular_buffer<range_tombstone_change> _reversed_rtcs;
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
        _mut.emplace(_schema);
        _mut->consume_new_partition(dk);
        _inside_partition = true;
    }
    void consume(tombstone pt) {
        BOOST_REQUIRE(_inside_partition);
        BOOST_REQUIRE(!_previous_position);
        BOOST_REQUIRE(_mut);
        _mut->consume(pt);
    }
    stop_iteration consume(static_row&& sr) {
        BOOST_REQUIRE(_inside_partition);
        BOOST_REQUIRE(!_previous_position);
        BOOST_REQUIRE(_mut);
        _previous_position.emplace(sr.position());
        _mut->consume(std::move(sr));
        return stop_iteration(bool(_skip_partition));
    }
    stop_iteration consume(clustering_row&& cr) {
        BOOST_REQUIRE(_inside_partition);
        verify_order(cr.position());
        BOOST_REQUIRE(_mut);
        _previous_position.emplace(cr.position());
        _mut->consume(std::move(cr));
        return stop_iteration(bool(_skip_partition));
    }
    stop_iteration consume(range_tombstone_change&& rtc) {
        BOOST_REQUIRE(_inside_partition);
        auto pos = rtc.position();
        verify_order(pos);
        BOOST_REQUIRE(_mut);
        _previous_position.emplace(pos);
        if (_reversed_schema) {
            //FIXME: until mutation rebuilder has to do v1 conversion and hence requires rtc to be fed in schema order
            _reversed_rtcs.emplace_front(std::move(pos).reversed(), std::exchange(_current_tombstone, rtc.tombstone()));
        } else {
            _mut->consume(std::move(rtc));
        }
        return stop_iteration(bool(_skip_partition));
    }
    stop_iteration consume_end_of_partition() {
        BOOST_REQUIRE(_inside_partition);
        BOOST_REQUIRE(_mut);
        BOOST_REQUIRE(!_current_tombstone);
        _previous_position = std::nullopt;
        _inside_partition = false;
        for (auto&& rtc : _reversed_rtcs) {
            _mut->consume(std::move(rtc));
        }
        _reversed_rtcs.clear();
        auto mut_opt = _mut->consume_end_of_stream();
        BOOST_REQUIRE(mut_opt);
        _mutations.emplace_back(std::move(*mut_opt));
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

    auto consume_fn = [&] (mutation_reader& fmr, flat_stream_consumer fsc) {
        if (thread) {
            SCYLLA_ASSERT(bool(!reversed));
            return fmr.consume_in_thread(std::move(fsc));
        } else {
            if (reversed) {
                return with_closeable(make_reversing_reader(make_delegating_reader(fmr), query::max_result_size(size_t(1) << 20)),
                        [fsc = std::move(fsc)] (mutation_reader& reverse_reader) mutable {
                    return reverse_reader.consume(std::move(fsc));
                }).get();
            }
            return fmr.consume(std::move(fsc)).get();
        }
    };

  {
    testlog.info("Consume all{}", reversed_msg);
    auto fmr = make_mutation_reader_from_mutations_v2(s, semaphore.make_permit(), muts);
    auto close_fmr = deferred_close(fmr);
    auto muts2 = consume_fn(fmr, flat_stream_consumer(s, semaphore.make_permit(), reversed));
    BOOST_REQUIRE_EQUAL(muts, muts2);
  }

  {
    testlog.info("Consume first fragment from partition{}", reversed_msg);
    auto fmr = make_mutation_reader_from_mutations_v2(s, semaphore.make_permit(), muts);
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
    auto fmr = make_mutation_reader_from_mutations_v2(s, semaphore.make_permit(), muts);
    auto close_fmr = deferred_close(fmr);
    auto muts2 = consume_fn(fmr, flat_stream_consumer(s, semaphore.make_permit(), reversed, skip_after_first_fragment::no,
                                             skip_after_first_partition::yes));
    BOOST_REQUIRE_EQUAL(muts2.size(), 1);
    BOOST_REQUIRE_EQUAL(muts2[0], muts[0]);
  }

    if (thread) {
        auto filter = mutation_reader::filter([&] (const dht::decorated_key& dk) {
            for (auto j = size_t(0); j < muts.size(); j += 2) {
                if (dk.equal(*s, muts[j].decorated_key())) {
                    return false;
                }
            }
            return true;
        });
        testlog.info("Consume all, filtered");
        auto fmr = make_mutation_reader_from_mutations_v2(s, semaphore.make_permit(), muts);
        auto close_fmr = deferred_close(fmr);
        auto muts2 = fmr.consume_in_thread(flat_stream_consumer(s, semaphore.make_permit(), reversed), std::move(filter));
        BOOST_REQUIRE_EQUAL(muts.size() / 2, muts2.size());
        for (auto j = size_t(1); j < muts.size(); j += 2) {
            BOOST_REQUIRE_EQUAL(muts[j], muts2[j / 2]);
        }
    }
}

SEASTAR_THREAD_TEST_CASE(test_consume_flat) {
    auto test_random_streams = [&] (random_mutation_generator&& gen) {
        for (auto i = 0; i < 4; i++) {
            auto muts = gen(4);
            test_flat_stream(gen.schema(), muts, reversed_partitions::no, in_thread::no);
            test_flat_stream(gen.schema(), muts, reversed_partitions::yes, in_thread::no);
            test_flat_stream(gen.schema(), muts, reversed_partitions::no, in_thread::yes);
        }
    };

    test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::no));
    test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::yes));
}

SEASTAR_THREAD_TEST_CASE(test_make_forwardable) {
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
            make_forwardable(make_mutation_reader_from_mutations_v2(s.schema(), semaphore.make_permit(), ms, range, streamed_mutation::forwarding::no)));
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
}

SEASTAR_THREAD_TEST_CASE(test_make_forwardable_next_partition) {
    simple_schema s;
    tests::reader_concurrency_semaphore_wrapper semaphore;
    const auto permit = semaphore.make_permit();

    auto make_reader = [&](std::vector<mutation> mutations, const dht::partition_range& pr) {
        auto result = make_mutation_reader_from_mutations_v2(s.schema(),
            permit,
            std::move(mutations),
            pr,
            streamed_mutation::forwarding::yes);
        return assert_that(std::move(result)).exact();
    };

    const auto pk1 = s.make_pkey(1);
    auto m1 = mutation(s.schema(), pk1);
    s.add_static_row(m1, "test-static-1");

    const auto pk2 = s.make_pkey(2);
    auto m2 = mutation(s.schema(), pk2);
    s.add_static_row(m2, "test-static-2");

    dht::ring_position_comparator cmp{*s.schema()};
    BOOST_CHECK_EQUAL(cmp(m1.decorated_key(), m2.decorated_key()), std::strong_ordering::less);

    auto rd = make_reader({m1, m2}, query::full_partition_range);
    rd.fill_buffer().get();
    rd.next_partition();
    rd.produces_partition_start(m1.decorated_key(), m1.partition().partition_tombstone());
    rd.produces_static_row(
        {{s.schema()->get_column_definition(to_bytes("s1")), to_bytes("test-static-1")}});
    rd.produces_end_of_stream();

    rd.next_partition();
    rd.produces_partition_start(m2.decorated_key(), m2.partition().partition_tombstone());
    rd.produces_static_row(
        {{s.schema()->get_column_definition(to_bytes("s1")), to_bytes("test-static-2")}});
    rd.produces_end_of_stream();

    rd.next_partition();
    rd.produces_end_of_stream();
}

SEASTAR_THREAD_TEST_CASE(test_make_nonforwardable) {
    simple_schema s;
    tests::reader_concurrency_semaphore_wrapper semaphore;
    const auto permit = semaphore.make_permit();

    auto make_reader = [&](std::vector<mutation> mutations,
        bool single_partition,
        const dht::partition_range& pr)
    {
        auto result = make_mutation_reader_from_mutations_v2(s.schema(),
            permit,
            std::move(mutations),
            pr,
            streamed_mutation::forwarding::yes);
        result = make_nonforwardable(std::move(result), single_partition);
        return assert_that(std::move(result)).exact();
    };

    const auto pk1 = s.make_pkey(1);
    auto m1 = mutation(s.schema(), pk1);
    m1.apply(s.make_row(permit, s.make_ckey(11), "value1"));

    const auto pk2 = s.make_pkey(2);
    auto m2 = mutation(s.schema(), pk2);
    m2.apply(s.make_row(permit, s.make_ckey(22), "value2"));

    const auto pk3 = s.make_pkey(3);
    auto m3 = mutation(s.schema(), pk3);
    m3.apply(s.make_row(permit, s.make_ckey(33), "value3"));

    dht::ring_position_comparator cmp{*s.schema()};
    BOOST_CHECK_EQUAL(cmp(m1.decorated_key(), m2.decorated_key()), std::strong_ordering::less);
    BOOST_CHECK_EQUAL(cmp(m2.decorated_key(), m3.decorated_key()), std::strong_ordering::less);

    // no input -> no output
    {
        auto rd = make_reader({}, false, query::full_partition_range);
        rd.produces_end_of_stream();
    }

    // next_partition()
    {
        auto check = [&] (flat_reader_assertions_v2 rd) {
            rd.produces_partition_start(m1.decorated_key(), m1.partition().partition_tombstone());
            rd.next_partition();
            rd.produces_partition_start(m2.decorated_key(), m2.partition().partition_tombstone());
            rd.produces_row_with_key(m2.partition().clustered_rows().begin()->key());
            rd.produces_partition_end();
            rd.produces_end_of_stream();
        };

        // buffer is not empty
        check(make_reader({m1, m2}, false, query::full_partition_range));

        // buffer is empty
        {
            auto rd = make_reader({m1, m2}, false, query::full_partition_range);
            rd.set_max_buffer_size(1);
            check(std::move(rd));
        }
    }

    // fast_forward_to()
    {
        const auto m1_range = dht::partition_range::make_singular(m1.decorated_key());
        auto rd = make_reader({m1, m2}, false, m1_range);
        rd.set_max_buffer_size(1);

        rd.produces_partition_start(m1.decorated_key(), m1.partition().partition_tombstone());

        const auto m2_range = dht::partition_range::make_singular(m2.decorated_key());
        rd.fast_forward_to(m2_range);
        rd.produces_partition_start(m2.decorated_key(), m2.partition().partition_tombstone());
        rd.produces_row_with_key(m2.partition().clustered_rows().begin()->key());
        rd.produces_partition_end();

        rd.next_partition();
        rd.produces_end_of_stream();
    }

    // single_partition
    {
        auto rd = make_reader({m1, m2}, true, query::full_partition_range);
        rd.set_max_buffer_size(1);

        rd.produces_partition_start(m1.decorated_key(), m1.partition().partition_tombstone());
        rd.produces_row_with_key(m1.partition().clustered_rows().begin()->key());

        rd.next_partition();
        rd.produces_end_of_stream();

        rd.next_partition();
        rd.produces_end_of_stream();
    }

    // single_partition with fast_forward_to
    {
        const auto m1_range = dht::partition_range::make_singular(m1.decorated_key());
        auto rd = make_reader({m1, m2}, true, m1_range);
        rd.set_max_buffer_size(1);

        rd.produces_partition_start(m1.decorated_key(), m1.partition().partition_tombstone());

        const auto m2_range = dht::partition_range::make_singular(m2.decorated_key());
        rd.fast_forward_to(m2_range);
        rd.produces_end_of_stream();

        rd.next_partition();
        rd.produces_end_of_stream();
    }

    // static row
    {
        s.add_static_row(m1, "test-static");
        const auto m1_range = dht::partition_range::make_singular(m1.decorated_key());
        auto rd = make_reader({m1, m2}, false, m1_range);
        rd.set_max_buffer_size(1);
        rd.produces_partition_start(m1.decorated_key(), m1.partition().partition_tombstone());
        rd.produces_static_row(
            {{s.schema()->get_column_definition(to_bytes("s1")), to_bytes("test-static")}});
        rd.produces_row(
            m1.partition().clustered_rows().begin()->key(),
            {{s.schema()->get_column_definition(to_bytes("v")), to_bytes("value1")}}
        );
        rd.produces_partition_end();
        rd.produces_end_of_stream();
    }
}

SEASTAR_THREAD_TEST_CASE(test_make_nonforwardable_from_mutations_as_mutation_source) {
    auto populate = [] (schema_ptr, const std::vector<mutation> &muts) {
        return mutation_source([=] (
            schema_ptr schema,
            reader_permit permit,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            tracing::trace_state_ptr,
            streamed_mutation::forwarding fwd_sm,
            mutation_reader::forwarding) mutable {
            auto squashed_muts = squash_mutations(muts);
            const auto single_partition = squashed_muts.size() == 1;
            auto reader = make_mutation_reader_from_mutations_v2(schema,
                std::move(permit),
                std::move(squashed_muts),
                range,
                slice,
                streamed_mutation::forwarding::yes);
            reader = make_nonforwardable(std::move(reader), single_partition);
            if (fwd_sm) {
                reader = make_forwardable(std::move(reader));
            }
            return reader;
        });
    };
    run_mutation_source_tests(populate);
}

SEASTAR_THREAD_TEST_CASE(test_abandoned_mutation_reader_from_mutation) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    for_each_mutation([&] (const mutation& m) {
        auto rd = make_mutation_reader_from_mutations_v2(m.schema(), semaphore.make_permit(), mutation(m));
        auto close_rd = deferred_close(rd);
        rd().get();
        rd().get();
        // We rely on AddressSanitizer telling us if nothing was leaked.
    });
}

SEASTAR_THREAD_TEST_CASE(test_mutation_reader_from_mutations_as_mutation_source) {
    auto populate = [] (schema_ptr, const std::vector<mutation> &muts) {
        return mutation_source([=] (
                schema_ptr schema,
                reader_permit permit,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                tracing::trace_state_ptr,
                streamed_mutation::forwarding fwd_sm,
                mutation_reader::forwarding) mutable {
            return make_mutation_reader_from_mutations_v2(schema, std::move(permit), squash_mutations(muts), range, slice, fwd_sm);
        });
    };
    run_mutation_source_tests(populate);
}

SEASTAR_THREAD_TEST_CASE(test_mutation_reader_from_mutations_v2_as_mutation_source) {
    auto populate = [] (schema_ptr, const std::vector<mutation>& muts) {
        return mutation_source([=] (
                schema_ptr schema,
                reader_permit permit,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                tracing::trace_state_ptr,
                streamed_mutation::forwarding fwd_sm,
                mutation_reader::forwarding) mutable {
            return make_mutation_reader_from_mutations_v2(schema, std::move(permit), squash_mutations(muts), range, slice, fwd_sm);
        });
    };
    run_mutation_source_tests(populate);
}

SEASTAR_THREAD_TEST_CASE(test_mutation_reader_from_fragments_v2_as_mutation_source) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto populate = [] (schema_ptr, const std::vector<mutation> &muts) {
        return mutation_source([=] (
                schema_ptr schema,
                reader_permit permit,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                tracing::trace_state_ptr,
                streamed_mutation::forwarding fwd_sm,
                mutation_reader::forwarding) mutable {
            auto get_fragments = [&permit, &muts] {
                std::deque<mutation_fragment_v2> fragments;
                auto rd = make_mutation_reader_from_mutations_v2(muts.front().schema(), permit, squash_mutations(muts));
                auto close_rd = deferred_close(rd);
                while (auto mfopt = rd().get()) {
                    fragments.emplace_back(std::move(*mfopt));
                }
                return fragments;
            };

            auto rd = make_mutation_reader_from_fragments(schema, permit, get_fragments(), range, slice);
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
        stop_iteration consume(range_tombstone_change&&) { return stop_iteration::no; }
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
        auto reverse_reader = make_reversing_reader(make_mutation_reader_from_mutations_v2(schema.schema(), semaphore.make_permit(), mut),
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
    using key_range = interval<mutation_description::key>;

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

    auto forward_mt = make_lw_shared<replica::memtable>(forward_schema);
    auto reverse_mt = make_lw_shared<replica::memtable>(reverse_schema);

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

    auto compacted = [] (mutation_reader rd) {
        return make_compacting_reader(std::move(rd),
                                      gc_clock::time_point::max(),
                                      [] (const dht::decorated_key&) { return api::max_timestamp; },
                                      tombstone_gc_state(nullptr));
    };

    auto reversed_forward_reader = assert_that(compacted(
            make_reversing_reader(forward_mt->make_flat_reader(forward_schema, permit),
                                  query::max_result_size(1 << 20))));

    auto reverse_reader = compacted(reverse_mt->make_flat_reader(reverse_schema, permit));
    auto deferred_reverse_close = deferred_close(reverse_reader);

    while (auto mf_opt = reverse_reader().get()) {
        auto& mf = *mf_opt;
        reversed_forward_reader.produces(*forward_schema, mf);
    }
    reversed_forward_reader.produces_end_of_stream();
}

SEASTAR_THREAD_TEST_CASE(test_reverse_reader_v2_is_mutation_source) {
    auto populate = [] (schema_ptr s, const std::vector<mutation> &muts) {
        auto reverse_schema = s->make_reversed();
        auto reverse_muts = std::vector<mutation>();
        reverse_muts.reserve(muts.size());
        for (const auto& mut : muts) {
            reverse_muts.emplace_back(reverse(mut));
        }

        return mutation_source([muts = squash_mutations(muts), reverse_muts = squash_mutations(reverse_muts)] (
                schema_ptr schema,
                reader_permit permit,
                const dht::partition_range& range,
                const query::partition_slice& slice,
                tracing::trace_state_ptr trace_ptr,
                streamed_mutation::forwarding fwd_sm,
                mutation_reader::forwarding fwd_mr) mutable {
            mutation_reader rd(nullptr);
            std::vector<mutation>* selected_muts;
            auto reversed_slice = std::make_unique<query::partition_slice>(query::reverse_slice(*schema, slice));

            schema = schema->make_reversed();
            const auto reversed = slice.is_reversed();
            if (reversed) {
                selected_muts = &muts;
            } else {
                // We don't want the memtable reader to read in reverse.
                reversed_slice->options.remove(query::partition_slice::option::reversed);
                selected_muts = &reverse_muts;
            }

            rd = make_mutation_reader_from_mutations_v2(schema, std::move(permit), *selected_muts, range, *reversed_slice);
            rd = make_reversing_reader(std::move(rd), query::max_result_size(1 << 20), std::move(reversed_slice));

            if (fwd_sm) {
                return make_forwardable(std::move(rd));
            }
            return rd;
        });
    };
    run_mutation_source_tests(populate);
}

SEASTAR_THREAD_TEST_CASE(test_allow_reader_early_destruction) {
    struct test_reader_v2_impl : public mutation_reader::impl {
        using mutation_reader::impl::impl;
        virtual future<> fill_buffer() override { return make_ready_future<>(); }
        virtual future<> next_partition() override { return make_ready_future<>(); }
        virtual future<> fast_forward_to(const dht::partition_range&) override { return make_ready_future<>(); }
        virtual future<> fast_forward_to(position_range) override { return make_ready_future<>(); }
        virtual future<> close() noexcept override { return make_ready_future<>(); };
    };

    simple_schema s;
    tests::reader_concurrency_semaphore_wrapper semaphore;
    // This reader is not closed, but didn't start any operations, so it's safe for it to be destroyed.
    auto reader_v2 = make_mutation_reader<test_reader_v2_impl>(s.schema(), semaphore.make_permit());
}
