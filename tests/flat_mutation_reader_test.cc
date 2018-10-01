/*
 * Copyright (C) 2017 ScyllaDB
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
#include <seastar/tests/test-utils.hh>

#include "mutation.hh"
#include "mutation_fragment.hh"
#include "mutation_source_test.hh"
#include "flat_mutation_reader.hh"
#include "mutation_reader.hh"
#include "schema_builder.hh"
#include "memtable.hh"
#include "row_cache.hh"
#include "sstables/sstables.hh"
#include "tmpdir.hh"
#include "sstable_test.hh"
#include "repair/repair.hh"

#include "tests/test_services.hh"
#include "tests/simple_schema.hh"
#include "flat_mutation_reader_assertions.hh"

struct mock_consumer {
    struct result {
        size_t _depth;
        size_t _consume_new_partition_call_count = 0;
        size_t _consume_tombstone_call_count = 0;
        size_t _consume_end_of_partition_call_count = 0;
        bool _consume_end_of_stream_called = false;
        std::vector<mutation_fragment> _fragments;
    };
    result _result;
    mock_consumer(size_t depth) {
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
        _result._fragments.push_back(mutation_fragment(std::move(sr)));
        return update_depth();
    }
    stop_iteration consume(clustering_row&& cr) {
        _result._fragments.push_back(mutation_fragment(std::move(cr)));
        return update_depth();
    }
    stop_iteration consume(range_tombstone&& rt) {
        _result._fragments.push_back(mutation_fragment(std::move(rt)));
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
    auto r = flat_mutation_reader_from_mutations({m});
    size_t res = 0;
    auto mfopt = r(db::no_timeout).get0();
    while (bool(mfopt)) {
        ++res;
        mfopt = r(db::no_timeout).get0();
    }
    return res;
}

SEASTAR_TEST_CASE(test_flat_mutation_reader_consume_single_partition) {
    return seastar::async([] {
        for_each_mutation([&] (const mutation& m) {
            size_t fragments_in_m = count_fragments(m);
            for (size_t depth = 1; depth <= fragments_in_m + 1; ++depth) {
                auto r = flat_mutation_reader_from_mutations({m});
                auto result = r.consume(mock_consumer(depth), db::no_timeout).get0();
                BOOST_REQUIRE(result._consume_end_of_stream_called);
                BOOST_REQUIRE_EQUAL(1, result._consume_new_partition_call_count);
                BOOST_REQUIRE_EQUAL(1, result._consume_end_of_partition_call_count);
                BOOST_REQUIRE_EQUAL(m.partition().partition_tombstone() ? 1 : 0, result._consume_tombstone_call_count);
                auto r2 = assert_that(flat_mutation_reader_from_mutations({m}));
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
        auto test = [] (mutation m1, mutation m2) {
            size_t fragments_in_m1 = count_fragments(m1);
            size_t fragments_in_m2 = count_fragments(m2);
            for (size_t depth = 1; depth < fragments_in_m1; ++depth) {
                auto r = flat_mutation_reader_from_mutations({m1, m2});
                auto result = r.consume(mock_consumer(depth), db::no_timeout).get0();
                BOOST_REQUIRE(result._consume_end_of_stream_called);
                BOOST_REQUIRE_EQUAL(1, result._consume_new_partition_call_count);
                BOOST_REQUIRE_EQUAL(1, result._consume_end_of_partition_call_count);
                BOOST_REQUIRE_EQUAL(m1.partition().partition_tombstone() ? 1 : 0, result._consume_tombstone_call_count);
                auto r2 = flat_mutation_reader_from_mutations({m1, m2});
                auto start = r2(db::no_timeout).get0();
                BOOST_REQUIRE(start);
                BOOST_REQUIRE(start->is_partition_start());
                for (auto& mf : result._fragments) {
                    auto mfopt = r2(db::no_timeout).get0();
                    BOOST_REQUIRE(mfopt);
                    BOOST_REQUIRE(mf.equal(*m1.schema(), *mfopt));
                }
            }
            for (size_t depth = fragments_in_m1; depth < fragments_in_m1 + fragments_in_m2 + 1; ++depth) {
                auto r = flat_mutation_reader_from_mutations({m1, m2});
                auto result = r.consume(mock_consumer(depth), db::no_timeout).get0();
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
                auto r2 = flat_mutation_reader_from_mutations({m1, m2});
                auto start = r2(db::no_timeout).get0();
                BOOST_REQUIRE(start);
                BOOST_REQUIRE(start->is_partition_start());
                for (auto& mf : result._fragments) {
                    auto mfopt = r2(db::no_timeout).get0();
                    BOOST_REQUIRE(mfopt);
                    if (mfopt->is_partition_start() || mfopt->is_end_of_partition()) {
                        mfopt = r2(db::no_timeout).get0();
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
    });
}

SEASTAR_TEST_CASE(test_fragmenting_and_freezing) {
    return seastar::async([] {
        storage_service_for_tests ssft;

        for_each_mutation([&] (const mutation& m) {
            std::vector<frozen_mutation> fms;

            fragment_and_freeze(flat_mutation_reader_from_mutations({ mutation(m) }), [&] (auto fm, bool frag) {
                BOOST_REQUIRE(!frag);
                fms.emplace_back(std::move(fm));
                return make_ready_future<stop_iteration>(stop_iteration::no);
            }, std::numeric_limits<size_t>::max()).get0();

            BOOST_REQUIRE_EQUAL(fms.size(), 1);

            auto m1 = fms.back().unfreeze(m.schema());
            BOOST_REQUIRE_EQUAL(m, m1);

            fms.clear();

            stdx::optional<bool> fragmented;
            fragment_and_freeze(flat_mutation_reader_from_mutations({ mutation(m) }), [&] (auto fm, bool frag) {
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
            while (!fms.empty()) {
                m2.partition().apply(*m.schema(), fms.back().partition(), *m.schema());
                fms.pop_back();
            }
            BOOST_REQUIRE_EQUAL(m, m2);
        });

        auto test_random_streams = [] (random_mutation_generator&& gen) {
            for (auto i = 0; i < 4; i++) {
                auto muts = gen(4);
                auto s = muts[0].schema();

                std::vector<frozen_mutation> frozen;

                // Freeze all
                fragment_and_freeze(flat_mutation_reader_from_mutations(muts), [&] (auto fm, bool frag) {
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
                fragment_and_freeze(flat_mutation_reader_from_mutations(muts), [&] (auto fm, bool frag) {
                    BOOST_REQUIRE(!frag);
                    frozen.emplace_back(fm);
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }, std::numeric_limits<size_t>::max()).get0();
                BOOST_REQUIRE_EQUAL(frozen.size(), 1);
                BOOST_REQUIRE_EQUAL(muts[0], frozen[0].unfreeze(s));

                // Fragment and freeze all
                frozen.clear();
                fragment_and_freeze(flat_mutation_reader_from_mutations(muts), [&] (auto fm, bool frag) {
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

        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::no));
        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::yes));
    });
}


SEASTAR_TEST_CASE(test_partition_checksum) {
    return seastar::async([] {
        for_each_mutation_pair([] (auto&& m1, auto&& m2, are_equal eq) {
            auto get_hash = [] (mutation m) {
                return partition_checksum::compute(flat_mutation_reader_from_mutations({ m }),
                                                   repair_checksum::streamed).get0();
            };
            auto h1 = get_hash(m1);
            auto h2 = get_hash(m2);
            if (eq) {
                if (h1 != h2) {
                    BOOST_FAIL(sprint("Hash should be equal for %s and %s", m1, m2));
                }
            } else {
                // We're using a strong hasher, collision should be unlikely
                if (h1 == h2) {
                    BOOST_FAIL(sprint("Hash should be different for %s and %s", m1, m2));
                }
            }
        });

        auto test_random_streams = [] (random_mutation_generator&& gen) {
            for (auto i = 0; i < 4; i++) {
                auto muts = gen(4);
                auto muts2 = muts;
                std::vector<partition_checksum> checksum;
                while (!muts2.empty()) {
                    auto chk = partition_checksum::compute(flat_mutation_reader_from_mutations(muts2),
                                                           repair_checksum::streamed).get0();
                    BOOST_REQUIRE(boost::count(checksum, chk) == 0);
                    checksum.emplace_back(chk);
                    muts2.pop_back();
                }
                std::vector<partition_checksum> individually_computed_checksums(muts.size());
                for (auto k = 0u; k < muts.size(); k++) {
                    auto chk = partition_checksum::compute(flat_mutation_reader_from_mutations({ muts[k] }),
                                                           repair_checksum::streamed).get0();
                    for (auto j = 0u; j < (muts.size() - k); j++) {
                        individually_computed_checksums[j].add(chk);
                    }
                }
                BOOST_REQUIRE_EQUAL(checksum, individually_computed_checksums);
            }
        };

        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::no));
        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::yes));
    });
}

SEASTAR_THREAD_TEST_CASE(test_flat_mutation_reader_move_buffer_content_to) {
    struct dummy_reader_impl : public flat_mutation_reader::impl {
        using flat_mutation_reader::impl::impl;
        virtual future<> fill_buffer(db::timeout_clock::time_point) override { return make_ready_future<>(); }
        virtual void next_partition() { }
        virtual future<> fast_forward_to(const dht::partition_range&, db::timeout_clock::time_point) override { return make_ready_future<>(); }
        virtual future<> fast_forward_to(position_range, db::timeout_clock::time_point) override { return make_ready_future<>(); }
    };

    simple_schema s;
    auto pkey = s.make_pkey(1);
    auto mut_orig = mutation(s.schema(), pkey);

    auto mf_range = boost::irange(0, 50) | boost::adaptors::transformed([&] (auto n) {
        return s.make_row(s.make_ckey(n), "a_16_byte_value_");
    });
    for (auto&& mf : mf_range) {
        mut_orig.apply(mf);
    }

    // Set a small size so we can fill the buffer at least two times, without
    // having to have loads of data.
    const auto max_buffer_size = size_t{100};

    auto reader = flat_mutation_reader_from_mutations({mut_orig}, dht::partition_range::make_open_ended_both_sides());
    auto dummy_impl = std::make_unique<dummy_reader_impl>(s.schema());
    reader.set_max_buffer_size(max_buffer_size);

    reader.fill_buffer(db::no_timeout).get();
    BOOST_REQUIRE(reader.is_buffer_full());
    auto expected_buf_size = reader.buffer_size();

    // This should take the fast path, as dummy's buffer is empty.
    reader.move_buffer_content_to(*dummy_impl);
    BOOST_CHECK(reader.is_buffer_empty());
    BOOST_CHECK_EQUAL(reader.buffer_size(), 0);
    BOOST_CHECK_EQUAL(dummy_impl->buffer_size(), expected_buf_size);

    reader.fill_buffer(db::no_timeout).get();
    BOOST_REQUIRE(!reader.is_buffer_empty());
    expected_buf_size += reader.buffer_size();

    // This should take the slow path, as dummy's buffer is not empty.
    reader.move_buffer_content_to(*dummy_impl);
    BOOST_CHECK(reader.is_buffer_empty());
    BOOST_CHECK_EQUAL(reader.buffer_size(), 0);
    BOOST_CHECK_EQUAL(dummy_impl->buffer_size(), expected_buf_size);

    while (!reader.is_end_of_stream()) {
        reader.fill_buffer(db::no_timeout).get();
        expected_buf_size += reader.buffer_size();

        reader.move_buffer_content_to(*dummy_impl);
        BOOST_CHECK(reader.is_buffer_empty());
        BOOST_CHECK_EQUAL(reader.buffer_size(), 0);
        BOOST_CHECK_EQUAL(dummy_impl->buffer_size(), expected_buf_size);
    }

    auto dummy_reader = flat_mutation_reader(std::move(dummy_impl));
    auto mut_new = read_mutation_from_flat_mutation_reader(dummy_reader, db::no_timeout).get0();

    assert_that(mut_new)
        .has_mutation()
        .is_equal_to(mut_orig);
}

SEASTAR_TEST_CASE(test_multi_range_reader) {
    return seastar::async([] {
        simple_schema s;

        auto keys = s.make_pkeys(10);
        auto ring = s.to_ring_positions(keys);

        auto crs = boost::copy_range<std::vector<mutation_fragment>>(boost::irange(0, 3) | boost::adaptors::transformed([&] (auto n) {
            return s.make_row(s.make_ckey(n), "value");
        }));

        auto ms = boost::copy_range<std::vector<mutation>>(keys | boost::adaptors::transformed([&] (auto& key) {
            auto m = mutation(s.schema(), key);
            for (auto& mf : crs) {
                m.apply(mf);
            }
            return m;
        }));

        auto source = mutation_source([&] (schema_ptr, const dht::partition_range& range) {
            return flat_mutation_reader_from_mutations(ms, range);
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
            BOOST_TEST_MESSAGE("empty ranges");
            assert_that(make_flat_multi_range_reader(s.schema(), source, make_empty_ranges(), s.schema()->full_slice()))
                    .produces_end_of_stream()
                    .fast_forward_to(fft_range)
                    .produces(ms[9])
                    .produces_end_of_stream();

            BOOST_TEST_MESSAGE("single range");
            assert_that(make_flat_multi_range_reader(s.schema(), source, make_single_ranges(), s.schema()->full_slice()))
                    .produces(ms[1])
                    .produces(ms[2])
                    .produces_end_of_stream()
                    .fast_forward_to(fft_range)
                    .produces(ms[9])
                    .produces_end_of_stream();

            BOOST_TEST_MESSAGE("read full partitions and fast forward");
            assert_that(make_flat_multi_range_reader(s.schema(), source, make_multiple_ranges(), s.schema()->full_slice()))
                    .produces(ms[1])
                    .produces(ms[2])
                    .produces(ms[4])
                    .produces(ms[6])
                    .fast_forward_to(fft_range)
                    .produces(ms[9])
                    .produces_end_of_stream();

            BOOST_TEST_MESSAGE("read, skip partitions and fast forward");
            assert_that(make_flat_multi_range_reader(s.schema(), source, make_multiple_ranges(), s.schema()->full_slice()))
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

        BOOST_TEST_MESSAGE("vector version");
        run_test(
                [&] { return empty_ranges; },
                [&] { return single_ranges; },
                [&] { return multiple_ranges; });

        BOOST_TEST_MESSAGE("generator version");
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
    reversed_partitions _reversed;
    skip_after_first_fragment _skip_partition;
    skip_after_first_partition _skip_stream;
    std::vector<mutation> _mutations;
    stdx::optional<position_in_partition> _previous_position;
    bool _inside_partition = false;
private:
    void verify_order(position_in_partition_view pos) {
        position_in_partition::less_compare cmp(*_schema);
        if (!_reversed) {
            BOOST_REQUIRE(!_previous_position || _previous_position->is_static_row()
                          || cmp(*_previous_position, pos));
        } else {
            BOOST_REQUIRE(!_previous_position || _previous_position->is_static_row()
                          || cmp(pos, *_previous_position));
        }
    }
public:
    flat_stream_consumer(schema_ptr s, reversed_partitions reversed,
                         skip_after_first_fragment skip_partition = skip_after_first_fragment::no,
                         skip_after_first_partition skip_stream = skip_after_first_partition::no)
        : _schema(std::move(s))
        , _reversed(reversed)
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
        _mutations.back().partition().apply(*_schema, mutation_fragment(std::move(sr)));
        return stop_iteration(bool(_skip_partition));
    }
    stop_iteration consume(clustering_row&& cr) {
        BOOST_REQUIRE(_inside_partition);
        verify_order(cr.position());
        BOOST_REQUIRE_GE(_mutations.size(), 1);
        _previous_position.emplace(cr.position());
        _mutations.back().partition().apply(*_schema, mutation_fragment(std::move(cr)));
        return stop_iteration(bool(_skip_partition));
    }
    stop_iteration consume(range_tombstone&& rt) {
        BOOST_REQUIRE(_inside_partition);
        auto pos = _reversed ? rt.end_position() : rt.position();
        verify_order(pos);
        BOOST_REQUIRE_GE(_mutations.size(), 1);
        _previous_position.emplace(pos);
        _mutations.back().partition().apply(*_schema, mutation_fragment(std::move(rt)));
        return stop_iteration(bool(_skip_partition));
    }
    stop_iteration consume_end_of_partition() {
        BOOST_REQUIRE(_inside_partition);
        BOOST_REQUIRE_GE(_mutations.size(), 1);
        _previous_position = stdx::nullopt;
        _inside_partition = false;
        return stop_iteration(bool(_skip_stream));
    }
    std::vector<mutation> consume_end_of_stream() {
        BOOST_REQUIRE(!_inside_partition);
        return std::move(_mutations);
    }
};

void test_flat_stream(schema_ptr s, std::vector<mutation> muts, reversed_partitions reversed, in_thread thread) {
    auto reversed_msg = reversed ? ", reversed partitions" : "";

    auto consume_fn = [&] (flat_mutation_reader& fmr, flat_stream_consumer fsc) {
        if (thread) {
            assert(bool(!reversed));
            return fmr.consume_in_thread(std::move(fsc), db::no_timeout);
        } else {
            auto reversed_flag = flat_mutation_reader::consume_reversed_partitions(bool(reversed));
            return fmr.consume(std::move(fsc), db::no_timeout, reversed_flag).get0();
        }
    };

    BOOST_TEST_MESSAGE(sprint("Consume all%s", reversed_msg));
    auto fmr = flat_mutation_reader_from_mutations(muts);
    auto muts2 = consume_fn(fmr, flat_stream_consumer(s, reversed));
    BOOST_REQUIRE_EQUAL(muts, muts2);

    BOOST_TEST_MESSAGE(sprint("Consume first fragment from partition%s", reversed_msg));
    fmr = flat_mutation_reader_from_mutations(muts);
    muts2 = consume_fn(fmr, flat_stream_consumer(s, reversed, skip_after_first_fragment::yes));
    BOOST_REQUIRE_EQUAL(muts.size(), muts2.size());
    for (auto j = 0u; j < muts.size(); j++) {
        BOOST_REQUIRE(muts[j].decorated_key().equal(*muts[j].schema(), muts2[j].decorated_key()));
        auto& mp = muts2[j].partition();
        BOOST_REQUIRE_LE(mp.static_row().empty() + mp.clustered_rows().calculate_size() + mp.row_tombstones().size(), 1);
        auto m = muts[j];
        m.apply(muts2[j]);
        BOOST_REQUIRE_EQUAL(m, muts[j]);
    }

    BOOST_TEST_MESSAGE(sprint("Consume first partition%s", reversed_msg));
    fmr = flat_mutation_reader_from_mutations(muts);
    muts2 = consume_fn(fmr, flat_stream_consumer(s, reversed, skip_after_first_fragment::no,
                                             skip_after_first_partition::yes));
    BOOST_REQUIRE_EQUAL(muts2.size(), 1);
    BOOST_REQUIRE_EQUAL(muts2[0], muts[0]);

    if (thread) {
        auto filter = [&] (const dht::decorated_key& dk) {
            for (auto j = size_t(0); j < muts.size(); j += 2) {
                if (dk.equal(*s, muts[j].decorated_key())) {
                    return false;
                }
            }
            return true;
        };
        BOOST_TEST_MESSAGE("Consume all, filtered");
        fmr = flat_mutation_reader_from_mutations(muts);
        muts2 = fmr.consume_in_thread(flat_stream_consumer(s, reversed), std::move(filter), db::no_timeout);
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

        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::no));
        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::yes));
    });
}

SEASTAR_TEST_CASE(test_make_forwardable) {
    return seastar::async([] {
        simple_schema s;

        auto keys = s.make_pkeys(10);

        auto crs = boost::copy_range < std::vector <
                   mutation_fragment >> (boost::irange(0, 3) | boost::adaptors::transformed([&](auto n) {
                       return s.make_row(s.make_ckey(n), "value");
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
                make_forwardable(flat_mutation_reader_from_mutations(ms, range, streamed_mutation::forwarding::no)));
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
        for_each_mutation([&] (const mutation& m) {
            auto rd = flat_mutation_reader_from_mutations({mutation(m)});
            rd(db::no_timeout).get();
            rd(db::no_timeout).get();
            // We rely on AddressSanitizer telling us if nothing was leaked.
        });
    });
}
