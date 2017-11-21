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
#include "streamed_mutation.hh"
#include "mutation_source_test.hh"
#include "flat_mutation_reader.hh"
#include "mutation_reader.hh"
#include "schema_builder.hh"
#include "memtable.hh"
#include "mutation_reader_assertions.hh"
#include "row_cache.hh"
#include "sstables/sstables.hh"
#include "tmpdir.hh"
#include "sstable_test.hh"

#include "disk-error-handler.hh"
#include "tests/test_services.hh"
#include "tests/simple_schema.hh"
#include "flat_mutation_reader_assertions.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

static void test_double_conversion_through_mutation_reader(const std::vector<mutation>& mutations) {
    BOOST_REQUIRE(!mutations.empty());
    auto schema = mutations[0].schema();
    auto base_reader = make_reader_returning_many(mutations);
    auto flat_reader = flat_mutation_reader_from_mutation_reader(schema,
                                                                 std::move(base_reader),
                                                                 streamed_mutation::forwarding::no);
    auto normal_reader = mutation_reader_from_flat_mutation_reader(std::move(flat_reader));
    for (auto& m : mutations) {
        auto smopt = normal_reader().get0();
        BOOST_REQUIRE(smopt);
        auto mopt = mutation_from_streamed_mutation(std::move(*smopt)).get0();
        BOOST_REQUIRE(mopt);
        BOOST_REQUIRE_EQUAL(m, *mopt);
    }
    BOOST_REQUIRE(!normal_reader().get0());
}

static void check_two_readers_are_the_same(schema_ptr schema, mutation_reader& normal_reader, flat_mutation_reader& flat_reader) {
    auto smopt = normal_reader().get0();
    BOOST_REQUIRE(smopt);
    auto mfopt = flat_reader().get0();
    BOOST_REQUIRE(mfopt);
    BOOST_REQUIRE(mfopt->is_partition_start());
    BOOST_REQUIRE(smopt->decorated_key().equal(*schema, mfopt->as_mutable_partition_start().key()));
    BOOST_REQUIRE_EQUAL(smopt->partition_tombstone(), mfopt->as_mutable_partition_start().partition_tombstone());
    mutation_fragment_opt sm_mfopt;
    while (bool(sm_mfopt = (*smopt)().get0())) {
        mfopt = flat_reader().get0();
        BOOST_REQUIRE(mfopt);
        BOOST_REQUIRE(sm_mfopt->equal(*schema, *mfopt));
    }
    mfopt = flat_reader().get0();
    BOOST_REQUIRE(mfopt);
    BOOST_REQUIRE(mfopt->is_end_of_partition());
}

static void test_conversion_to_flat_mutation_reader_through_mutation_reader(const std::vector<mutation>& mutations) {
    BOOST_REQUIRE(!mutations.empty());
    auto schema = mutations[0].schema();
    auto base_reader = make_reader_returning_many(mutations);
    auto flat_reader = flat_mutation_reader_from_mutation_reader(schema,
                                                                 std::move(base_reader),
                                                                 streamed_mutation::forwarding::no);
    for (auto& m : mutations) {
        auto normal_reader = make_reader_returning(m);
        check_two_readers_are_the_same(schema, normal_reader, flat_reader);
    }
}

static void test_conversion(const std::vector<mutation>& mutations) {
    BOOST_REQUIRE(!mutations.empty());
    auto schema = mutations[0].schema();
    auto flat_reader = flat_mutation_reader_from_mutations(std::vector<mutation>(mutations), streamed_mutation::forwarding::no);
    for (auto& m : mutations) {
        mutation_opt m2 = read_mutation_from_flat_mutation_reader(schema, flat_reader).get0();
        BOOST_REQUIRE(m2);
        BOOST_REQUIRE_EQUAL(m, *m2);
    }
    BOOST_REQUIRE(!read_mutation_from_flat_mutation_reader(schema, flat_reader).get0());
}

/*
 * =================
 * ===== Tests =====
 * =================
 */

SEASTAR_TEST_CASE(test_conversions_through_mutation_reader_single_mutation) {
    return seastar::async([] {
        for_each_mutation([&] (const mutation& m) {
            test_double_conversion_through_mutation_reader({m});
            test_conversion_to_flat_mutation_reader_through_mutation_reader({m});
        });
    });
}

SEASTAR_TEST_CASE(test_double_conversion_through_mutation_reader_two_mutations) {
    return seastar::async([] {
        for_each_mutation_pair([&] (auto&& m, auto&& m2, are_equal) {
            if (m.decorated_key().less_compare(*m.schema(), m2.decorated_key())) {
                test_double_conversion_through_mutation_reader({m, m2});
                test_conversion_to_flat_mutation_reader_through_mutation_reader({m, m2});
            } else if (m2.decorated_key().less_compare(*m.schema(), m.decorated_key())) {
                test_double_conversion_through_mutation_reader({m2, m});
                test_conversion_to_flat_mutation_reader_through_mutation_reader({m2, m});
            }
        });
    });
}

SEASTAR_TEST_CASE(test_conversions_single_mutation) {
    return seastar::async([] {
        for_each_mutation([&] (const mutation& m) {
            test_conversion({m});
        });
    });
}

SEASTAR_TEST_CASE(test_double_conversion_two_mutations) {
    return seastar::async([] {
        for_each_mutation_pair([&] (auto&& m, auto&& m2, are_equal) {
            if (m.decorated_key().less_compare(*m.schema(), m2.decorated_key())) {
                test_conversion({m, m2});
            } else if (m2.decorated_key().less_compare(*m.schema(), m.decorated_key())) {
                test_conversion({m2, m});
            }
        });
    });
}

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
        return _result;
    }
};

static size_t count_fragments(mutation m) {
    auto r = flat_mutation_reader_from_mutations({m}, streamed_mutation::forwarding::no);
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
        for_each_mutation([&] (const mutation& m) {
            size_t fragments_in_m = count_fragments(m);
            for (size_t depth = 1; depth <= fragments_in_m + 1; ++depth) {
                auto r = flat_mutation_reader_from_mutations({m}, streamed_mutation::forwarding::no);
                auto result = r.consume(mock_consumer(depth)).get0();
                BOOST_REQUIRE(result._consume_end_of_stream_called);
                BOOST_REQUIRE_EQUAL(1, result._consume_new_partition_call_count);
                BOOST_REQUIRE_EQUAL(1, result._consume_end_of_partition_call_count);
                BOOST_REQUIRE_EQUAL(m.partition().partition_tombstone() ? 1 : 0, result._consume_tombstone_call_count);
                auto r2 = flat_mutation_reader_from_mutations({m}, streamed_mutation::forwarding::no);
                auto start = r2().get0();
                BOOST_REQUIRE(start);
                BOOST_REQUIRE(start->is_partition_start());
                for (auto& mf : result._fragments) {
                    auto mfopt = r2().get0();
                    BOOST_REQUIRE(mfopt);
                    BOOST_REQUIRE(mf.equal(*m.schema(), *mfopt));
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
                auto r = flat_mutation_reader_from_mutations({m1, m2}, streamed_mutation::forwarding::no);
                auto result = r.consume(mock_consumer(depth)).get0();
                BOOST_REQUIRE(result._consume_end_of_stream_called);
                BOOST_REQUIRE_EQUAL(1, result._consume_new_partition_call_count);
                BOOST_REQUIRE_EQUAL(1, result._consume_end_of_partition_call_count);
                BOOST_REQUIRE_EQUAL(m1.partition().partition_tombstone() ? 1 : 0, result._consume_tombstone_call_count);
                auto r2 = flat_mutation_reader_from_mutations({m1, m2}, streamed_mutation::forwarding::no);
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
                auto r = flat_mutation_reader_from_mutations({m1, m2}, streamed_mutation::forwarding::no);
                auto result = r.consume(mock_consumer(depth)).get0();
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
                auto r2 = flat_mutation_reader_from_mutations({m1, m2}, streamed_mutation::forwarding::no);
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
                return partition_checksum::compute(flat_mutation_reader_from_mutations({ m }, streamed_mutation::forwarding::no),
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
                    auto chk = partition_checksum::compute(flat_mutation_reader_from_mutations(muts2, streamed_mutation::forwarding::no),
                                                           repair_checksum::streamed).get0();
                    BOOST_REQUIRE(boost::count(checksum, chk) == 0);
                    checksum.emplace_back(chk);
                    muts2.pop_back();
                }
                std::vector<partition_checksum> individually_computed_checksums(muts.size());
                for (auto k = 0u; k < muts.size(); k++) {
                    auto chk = partition_checksum::compute(flat_mutation_reader_from_mutations({ muts[k] }, streamed_mutation::forwarding::no),
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

SEASTAR_TEST_CASE(test_multi_range_reader) {
    return seastar::async([] {
        simple_schema s;

        auto keys = s.make_pkeys(10);
        auto ring = s.to_ring_positions(keys);

        auto ms = boost::copy_range<std::vector<mutation>>(keys | boost::adaptors::transformed([&s] (auto& key) {
            return mutation(key, s.schema());
        }));

        auto source = mutation_source([&] (schema_ptr, const dht::partition_range& range) {
            return make_reader_returning_many(std::move(ms), range);
        });

        auto ranges = dht::partition_range_vector {
                dht::partition_range::make(ring[1], ring[2]),
                dht::partition_range::make_singular(ring[4]),
                dht::partition_range::make(ring[6], ring[8]),
        };
        auto fft_range = dht::partition_range::make_starting_with(ring[9]);

        assert_that(make_flat_multi_range_reader(s.schema(), std::move(source), ranges, s.schema()->full_slice()))
                .produces_partition_start(keys[1])
                .produces_partition_end()
                .produces_partition_start(keys[2])
                .produces_partition_end()
                .produces_partition_start(keys[4])
                .produces_partition_end()
                .produces_partition_start(keys[6])
                .produces_partition_end()
                .fast_forward_to(fft_range)
                .produces_partition_start(keys[9])
                .produces_partition_end()
                .produces_end_of_stream();
    });
}

using reversed_partitions = seastar::bool_class<class reversed_partitions_tag>;
using skip_after_first_fragment = seastar::bool_class<class skip_after_first_fragment_tag>;
using skip_after_first_partition = seastar::bool_class<class skip_after_first_partition_tag>;

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
        _mutations.emplace_back(dk, _schema);
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

void test_flat_stream(schema_ptr s, std::vector<mutation> muts, reversed_partitions reversed) {
    auto reversed_msg = reversed ? ", reversed partitions" : "";
    auto reversed_flag = flat_mutation_reader::consume_reversed_partitions(bool(reversed));

    BOOST_TEST_MESSAGE(sprint("Consume all%s", reversed_msg));
    auto fmr = flat_mutation_reader_from_mutations(muts, streamed_mutation::forwarding::no);
    auto muts2 = fmr.consume(flat_stream_consumer(s, reversed), reversed_flag).get0();
    BOOST_REQUIRE_EQUAL(muts, muts2);

    BOOST_TEST_MESSAGE(sprint("Consume first fragment from partition%s", reversed_msg));
    fmr = flat_mutation_reader_from_mutations(muts, streamed_mutation::forwarding::no);
    muts2 = fmr.consume(flat_stream_consumer(s, reversed, skip_after_first_fragment::yes), reversed_flag).get0();
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
    fmr = flat_mutation_reader_from_mutations(muts, streamed_mutation::forwarding::no);
    muts2 = fmr.consume(flat_stream_consumer(s, reversed, skip_after_first_fragment::no,
                                             skip_after_first_partition::yes),
                        reversed_flag).get0();
    BOOST_REQUIRE_EQUAL(muts2.size(), 1);
    BOOST_REQUIRE_EQUAL(muts2[0], muts[0]);
}

SEASTAR_TEST_CASE(test_consume_flat) {
    return seastar::async([] {
        auto test_random_streams = [&] (random_mutation_generator&& gen) {
            for (auto i = 0; i < 4; i++) {
                auto muts = gen(4);
                test_flat_stream(gen.schema(), muts, reversed_partitions::no);
                test_flat_stream(gen.schema(), muts, reversed_partitions::yes);
            }
        };

        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::no));
        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::yes));
    });
}
