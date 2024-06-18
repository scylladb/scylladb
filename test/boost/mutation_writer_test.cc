/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <fmt/ranges.h>
#include <seastar/core/thread.hh>
#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/bool_class.hh>
#include <seastar/util/closeable.hh>

#include "mutation/mutation_fragment.hh"
#include "mutation/mutation_rebuilder.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "readers/from_mutations_v2.hh"
#include "mutation_writer/multishard_writer.hh"
#include "mutation_writer/timestamp_based_splitting_writer.hh"
#include "mutation_writer/partition_based_splitting_writer.hh"
#include "mutation_writer/token_group_based_splitting_writer.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/mutation_reader_assertions.hh"
#include "test/lib/mutation_assertions.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/random_schema.hh"
#include "test/lib/log.hh"
#include "test/lib/test_utils.hh"

#include <boost/range/adaptor/map.hpp>
#include "readers/from_mutations_v2.hh"
#include "readers/empty_v2.hh"
#include "readers/generating_v2.hh"
#include "readers/combined.hh"

using namespace mutation_writer;

struct generate_error_tag { };
using generate_error = bool_class<generate_error_tag>;


constexpr unsigned many_partitions() {
    return
#ifndef SEASTAR_DEBUG
	300
#else
	10
#endif
	;
}

SEASTAR_TEST_CASE(test_multishard_writer) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto test_random_streams = [&e] (random_mutation_generator&& gen, size_t partition_nr, generate_error error = generate_error::no) {
            for (auto i = 0; i < 3; i++) {
                auto muts = gen(partition_nr);
                std::vector<size_t> shards_before(smp::count, 0);
                std::vector<size_t> shards_after(smp::count, 0);
                schema_ptr s = gen.schema();

                for (auto& m : muts) {
                    auto shard = s->get_sharder().shard_for_reads(m.token());
                    shards_before[shard]++;
                }
                auto source_reader = partition_nr > 0 ? make_mutation_reader_from_mutations_v2(gen.schema(), make_reader_permit(e), muts) : make_empty_flat_reader_v2(s, make_reader_permit(e));
                auto close_source_reader = deferred_close(source_reader);
                auto& sharder = s->get_sharder();
                size_t partitions_received = distribute_reader_and_consume_on_shards(s, sharder,
                    std::move(source_reader),
                    [&sharder, &shards_after, error] (mutation_reader reader) mutable {
                        if (error) {
                          return reader.close().then([] {
                            return make_exception_future<>(std::runtime_error("Failed to write"));
                          });
                        }
                        return with_closeable(std::move(reader), [&sharder, &shards_after] (mutation_reader& reader) {
                          return repeat([&sharder, &shards_after, &reader] () mutable {
                            return reader().then([&sharder, &shards_after] (mutation_fragment_v2_opt mf_opt) mutable {
                                if (mf_opt) {
                                    if (mf_opt->is_partition_start()) {
                                        auto shard = sharder.shard_for_reads(mf_opt->as_partition_start().key().token());
                                        BOOST_REQUIRE_EQUAL(shard, this_shard_id());
                                        shards_after[shard]++;
                                    }
                                    return make_ready_future<stop_iteration>(stop_iteration::no);
                                } else {
                                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                                }
                            });
                          });
                        });
                    }
                ).get();
                BOOST_REQUIRE_EQUAL(partitions_received, partition_nr);
                BOOST_REQUIRE_EQUAL(shards_after, shards_before);
            }
        };

        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::no, local_shard_only::no), 0);
        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::yes, local_shard_only::no), 0);

        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::no, local_shard_only::no), 1);
        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::yes, local_shard_only::no), 1);

        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::no, local_shard_only::no), many_partitions());
        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::yes, local_shard_only::no), many_partitions());

        try {
            test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::no, local_shard_only::no), many_partitions(), generate_error::yes);
            BOOST_ASSERT(false);
        } catch (...) {
        }

        try {
            test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::yes, local_shard_only::no), many_partitions(), generate_error::yes);
            BOOST_ASSERT(false);
        } catch (...) {
        }
    });
}

SEASTAR_TEST_CASE(test_multishard_writer_producer_aborts) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto test_random_streams = [&e] (random_mutation_generator&& gen, size_t partition_nr, generate_error error = generate_error::no) {
            auto muts = gen(partition_nr);
            schema_ptr s = gen.schema();
            auto source_reader = partition_nr > 0 ? make_mutation_reader_from_mutations_v2(s, make_reader_permit(e), muts) : make_empty_flat_reader_v2(s, make_reader_permit(e));
            auto close_source_reader = deferred_close(source_reader);
            int mf_produced = 0;
            auto get_next_mutation_fragment = [&source_reader, &mf_produced] () mutable {
                if (mf_produced++ > 800) {
                    return make_exception_future<mutation_fragment_v2_opt>(std::runtime_error("the producer failed"));
                } else {
                    return source_reader();
                }
            };
            auto& sharder = s->get_sharder();
            try {
                distribute_reader_and_consume_on_shards(s, sharder,
                    make_generating_reader_v2(s, make_reader_permit(e), std::move(get_next_mutation_fragment)),
                    [&sharder, error] (mutation_reader reader) mutable {
                        if (error) {
                          return reader.close().then([] {
                            return make_exception_future<>(std::runtime_error("Failed to write"));
                          });
                        }
                        return with_closeable(std::move(reader), [&sharder] (mutation_reader& reader) {
                          return repeat([&sharder, &reader] () mutable {
                            return reader().then([&sharder] (mutation_fragment_v2_opt mf_opt) mutable {
                                if (mf_opt) {
                                    if (mf_opt->is_partition_start()) {
                                        auto shard = sharder.shard_for_reads(mf_opt->as_partition_start().key().token());
                                        BOOST_REQUIRE_EQUAL(shard, this_shard_id());
                                    }
                                    return make_ready_future<stop_iteration>(stop_iteration::no);
                                } else {
                                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                                }
                            });
                          });
                        });
                    }
                ).get();
            } catch (...) {
                // The distribute_reader_and_consume_on_shards is expected to fail and not block forever
            }
        };

        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::no, local_shard_only::yes), 1000, generate_error::no);
        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::no, local_shard_only::yes), 1000, generate_error::yes);
    });
}

namespace {

using classify_by_var_t = std::variant<classify_by_timestamp, classify_by_token_group>;

class test_bucket_writer {
    schema_ptr _schema;
    reader_permit _permit;
    classify_by_var_t _classify;
    std::unordered_map<int64_t, std::vector<mutation>>& _buckets;

    std::optional<int64_t> _bucket_id;
    mutation_opt _current_mutation;
    bool _is_first_mutation = true;

    range_tombstone_change _current_rtc;

    size_t _throw_after;
    size_t _mutation_consumed = 0;

public:
    class expected_exception : public std::exception {
    public:
        virtual const char* what() const noexcept override {
            return "expected_exception";
        }
    };

private:
    void check_bucket_id(int64_t bucket_id) {
        if (_bucket_id) {
            BOOST_REQUIRE_EQUAL(bucket_id, *_bucket_id);
        } else {
            _bucket_id = bucket_id;
        }
    }

    void check_timestamp(api::timestamp_type ts) {
        if (!std::holds_alternative<classify_by_timestamp>(_classify)) {
            return;
        }
        check_bucket_id(std::get<classify_by_timestamp>(_classify)(ts));
    }
    void check_token(dht::token t) {
        if (!std::holds_alternative<classify_by_token_group>(_classify)) {
            return;
        }
        check_bucket_id(std::get<classify_by_token_group>(_classify)(t));
    }

    void verify_column_bucket_id(const atomic_cell_or_collection& cell, const column_definition& cdef) {
        if (cdef.is_atomic()) {
            check_timestamp(cell.as_atomic_cell(cdef).timestamp());
        } else if (cdef.type->is_collection() || cdef.type->is_user_type()) {
            cell.as_collection_mutation().with_deserialized(*cdef.type, [this] (collection_mutation_view_description mv) {
                for (const auto& c: mv.cells) {
                    check_timestamp(c.second.timestamp());
                }
            });
        } else {
            BOOST_FAIL(fmt::format("Failed to verify column bucket id: column {} is of unknown type {}", cdef.name_as_text(), cdef.type->name()));
        }
    }
    void verify_row_bucket_id(const row& r, column_kind kind) {
        r.for_each_cell([this, kind] (column_id id, const atomic_cell_or_collection& cell) {
            verify_column_bucket_id(cell, _schema->column_at(kind, id));
        });
    }
    void verify_partition_tombstone(tombstone tomb) {
        if (tomb) {
            check_timestamp(tomb.timestamp);
        }
    }
    void verify_static_row(const static_row& sr) {
        verify_row_bucket_id(sr.cells(), column_kind::static_column);
    }
    void verify_clustering_row(const clustering_row& cr) {
        if (!cr.marker().is_missing()) {
            check_timestamp(cr.marker().timestamp());
        }
        if (cr.tomb()) {
            check_timestamp(cr.tomb().tomb().timestamp);
        }
        verify_row_bucket_id(cr.cells(), column_kind::regular_column);
    }
    void verify_range_tombstone_change(const range_tombstone_change& rtc) {
        if (rtc.tombstone()) {
            check_timestamp(rtc.tombstone().timestamp);
        }
    }

    void maybe_throw() {
        if (_mutation_consumed++ >= _throw_after) {
            throw(expected_exception());
        }
    }

public:
    test_bucket_writer(schema_ptr schema, reader_permit permit, classify_by_var_t classify, std::unordered_map<int64_t,
            std::vector<mutation>>& buckets, size_t throw_after = std::numeric_limits<size_t>::max())
        : _schema(std::move(schema))
        , _permit(std::move(permit))
        , _classify(std::move(classify))
        , _buckets(buckets)
        , _current_rtc(position_in_partition::before_all_clustered_rows(), tombstone())
        , _throw_after(throw_after)
    { }
    void consume_new_partition(const dht::decorated_key& dk) {
        maybe_throw();
        BOOST_REQUIRE(!_current_mutation);
        _current_mutation = mutation(_schema, dk);
        check_token(dk.token());
    }
    void consume(tombstone partition_tombstone) {
        maybe_throw();
        BOOST_REQUIRE(_current_mutation);
        verify_partition_tombstone(partition_tombstone);
        _current_mutation->partition().apply(partition_tombstone);
    }
    stop_iteration consume(static_row&& sr) {
        maybe_throw();
        BOOST_REQUIRE(_current_mutation);
        verify_static_row(sr);
        _current_mutation->apply(mutation_fragment(*_schema, _permit, std::move(sr)));
        return stop_iteration::no;
    }
    stop_iteration consume(clustering_row&& cr) {
        maybe_throw();
        BOOST_REQUIRE(_current_mutation);
        verify_clustering_row(cr);
        _current_mutation->apply(mutation_fragment(*_schema, _permit, std::move(cr)));
        return stop_iteration::no;
    }
    stop_iteration consume(range_tombstone_change&& rtc) {
        maybe_throw();
        BOOST_REQUIRE(_current_mutation);
        verify_range_tombstone_change(rtc);
        if (_current_rtc.tombstone()) {
            auto rt = range_tombstone(_current_rtc.position(), position_in_partition_view::before_key(rtc.position()), _current_rtc.tombstone());
            _current_mutation->apply(mutation_fragment(*_schema, _permit, std::move(rt)));
        }
        _current_rtc = std::move(rtc);
        return stop_iteration::no;
    }
    stop_iteration consume_end_of_partition() {
        maybe_throw();
        BOOST_REQUIRE(_current_mutation);
        BOOST_REQUIRE(_bucket_id);
        BOOST_REQUIRE(!_current_rtc.tombstone());
        auto& bucket = _buckets[*_bucket_id];

        if (_is_first_mutation) {
            BOOST_REQUIRE(bucket.empty());
            _is_first_mutation = false;
        }

        bucket.emplace_back(std::move(*_current_mutation));
        _current_mutation = std::nullopt;
        return stop_iteration::no;
    }
    void consume_end_of_stream() {
        BOOST_REQUIRE(!_current_mutation);
    }
};

} // anonymous namespace

using bucket_map_t = std::unordered_map<int64_t, std::vector<mutation>>;

// requires seastar thread.
static void assert_that_segregator_produces_correct_data(const bucket_map_t& buckets, std::vector<mutation>& muts, reader_permit permit, tests::random_schema& random_schema);

SEASTAR_THREAD_TEST_CASE(test_timestamp_based_splitting_mutation_writer) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto random_spec = tests::make_random_schema_specification(
            get_name(),
            std::uniform_int_distribution<size_t>(1, 4),
            std::uniform_int_distribution<size_t>(2, 4),
            std::uniform_int_distribution<size_t>(2, 8),
            std::uniform_int_distribution<size_t>(2, 8));
    auto random_schema = tests::random_schema{tests::random::get_int<uint32_t>(), *random_spec};

    testlog.info("Random schema:\n{}", random_schema.cql());

    auto ts_gen = [&, underlying = tests::default_timestamp_generator()] (std::mt19937& engine,
            tests::timestamp_destination ts_dest, api::timestamp_type min_timestamp) -> api::timestamp_type {
        if (ts_dest == tests::timestamp_destination::partition_tombstone ||
                ts_dest == tests::timestamp_destination::row_marker ||
                ts_dest == tests::timestamp_destination::row_tombstone ||
                ts_dest == tests::timestamp_destination::collection_tombstone) {
            if (tests::random::get_int<int>(0, 10, engine)) {
                return api::missing_timestamp;
            }
        }
        return underlying(engine, ts_dest, min_timestamp);
    };

    auto muts = tests::generate_random_mutations(random_schema, ts_gen).get();

    auto classify_fn = [] (api::timestamp_type ts) {
        return int64_t(ts % 2);
    };

    std::unordered_map<int64_t, std::vector<mutation>> buckets;

    auto consumer = [&] (mutation_reader bucket_reader) {
        return with_closeable(std::move(bucket_reader), [&] (mutation_reader& rd) {
            return rd.consume(test_bucket_writer(random_schema.schema(), rd.permit(), classify_fn, buckets));
        });
    };

    segregate_by_timestamp(make_mutation_reader_from_mutations_v2(random_schema.schema(), semaphore.make_permit(), muts), classify_fn, std::move(consumer)).get();

    testlog.debug("Data split into {} buckets: {}", buckets.size(), boost::copy_range<std::vector<int64_t>>(buckets | boost::adaptors::map_keys));

    assert_that_segregator_produces_correct_data(buckets, muts, semaphore.make_permit(), random_schema);
}

static void assert_that_segregator_produces_correct_data(const bucket_map_t& buckets, std::vector<mutation>& muts, reader_permit permit, tests::random_schema& random_schema) {
    auto bucket_readers = boost::copy_range<std::vector<mutation_reader>>(buckets | boost::adaptors::map_values |
            boost::adaptors::transformed([&random_schema, &permit] (std::vector<mutation> muts) { return make_mutation_reader_from_mutations_v2(random_schema.schema(), permit, std::move(muts)); }));
    auto reader = make_combined_reader(random_schema.schema(), permit, std::move(bucket_readers), streamed_mutation::forwarding::no,
            mutation_reader::forwarding::no);
    auto close_reader = deferred_close(reader);

    const auto now = gc_clock::now();
    for (auto& m : muts) {
        m.partition().compact_for_compaction(*random_schema.schema(), always_gc, m.decorated_key(), now, tombstone_gc_state(nullptr));
    }

    std::vector<mutation> combined_mutations;
    while (auto m = read_mutation_from_mutation_reader(reader).get()) {
        m->partition().compact_for_compaction(*random_schema.schema(), always_gc, m->decorated_key(), now, tombstone_gc_state(nullptr));
        combined_mutations.emplace_back(std::move(*m));
    }

    BOOST_REQUIRE_EQUAL(combined_mutations.size(), muts.size());
    for (size_t i = 0; i < muts.size(); ++i) {
        testlog.debug("Comparing mutation #{}", i);
        assert_that(combined_mutations[i]).is_equal_to(muts[i]);
    }
}

SEASTAR_THREAD_TEST_CASE(test_timestamp_based_splitting_mutation_writer_abort) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto random_spec = tests::make_random_schema_specification(
            get_name(),
            std::uniform_int_distribution<size_t>(1, 4),
            std::uniform_int_distribution<size_t>(2, 4),
            std::uniform_int_distribution<size_t>(2, 8),
            std::uniform_int_distribution<size_t>(2, 8));
    auto random_schema = tests::random_schema{tests::random::get_int<uint32_t>(), *random_spec};

    testlog.info("Random schema:\n{}", random_schema.cql());

    auto ts_gen = [&, underlying = tests::default_timestamp_generator()] (std::mt19937& engine,
            tests::timestamp_destination ts_dest, api::timestamp_type min_timestamp) -> api::timestamp_type {
        if (ts_dest == tests::timestamp_destination::partition_tombstone ||
                ts_dest == tests::timestamp_destination::row_marker ||
                ts_dest == tests::timestamp_destination::row_tombstone ||
                ts_dest == tests::timestamp_destination::collection_tombstone) {
            if (tests::random::get_int<int>(0, 10, engine)) {
                return api::missing_timestamp;
            }
        }
        return underlying(engine, ts_dest, min_timestamp);
    };

    auto muts = tests::generate_random_mutations(random_schema, ts_gen).get();

    auto classify_fn = [] (api::timestamp_type ts) {
        return int64_t(ts % 2);
    };

    std::unordered_map<int64_t, std::vector<mutation>> buckets;

    int throw_after = tests::random::get_int(muts.size() - 1);
    testlog.info("Will raise exception after {}/{} mutations", throw_after, muts.size());
    auto consumer = [&] (mutation_reader bucket_reader) {
        return with_closeable(std::move(bucket_reader), [&] (mutation_reader& rd) {
            return rd.consume(test_bucket_writer(random_schema.schema(), rd.permit(), classify_fn, buckets, throw_after));
        });
    };

    try {
        segregate_by_timestamp(make_mutation_reader_from_mutations_v2(random_schema.schema(), semaphore.make_permit(), muts), classify_fn, std::move(consumer)).get();
    } catch (const test_bucket_writer::expected_exception&) {
        BOOST_TEST_PASSPOINT();
    } catch (const seastar::broken_promise&) {
        // Tolerated until we properly abort readers
        BOOST_TEST_PASSPOINT();
    }
}

// Check that the partition_based_splitting_mutation_writer can fix reordered partitions
SEASTAR_THREAD_TEST_CASE(test_partition_based_splitting_mutation_writer) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto random_spec = tests::make_random_schema_specification(
            get_name(),
            std::uniform_int_distribution<size_t>(1, 2),
            std::uniform_int_distribution<size_t>(0, 2),
            std::uniform_int_distribution<size_t>(1, 2),
            std::uniform_int_distribution<size_t>(0, 1));

    auto random_schema = tests::random_schema{tests::random::get_int<uint32_t>(), *random_spec};

    const auto input_mutations = tests::generate_random_mutations(
            random_schema,
            tests::default_timestamp_generator(),
            tests::no_expiry_expiry_generator(),
            std::uniform_int_distribution<size_t>(100, 1000), // partitions
            std::uniform_int_distribution<size_t>(1, 4), // rows
            std::uniform_int_distribution<size_t>(0, 1)).get(); // range tombstones

    auto shuffled_input_mutations = input_mutations;
    shuffled_input_mutations.emplace_back(*shuffled_input_mutations.begin()); // Have a duplicate partition as well.
    std::shuffle(shuffled_input_mutations.begin(), shuffled_input_mutations.end(), tests::random::gen());

    testlog.info("input_mutations.size()={}", input_mutations.size());

    std::vector<std::vector<mutation>> output_mutations;
    size_t next_index = 0;

    auto consumer = [&] (mutation_reader rd) {
        const auto index = next_index++;
        output_mutations.emplace_back();
        BOOST_REQUIRE_EQUAL(output_mutations.size(), next_index);

        return async([&, index, rd = std::move(rd)] () mutable {
            auto close_rd = deferred_close(rd);
            mutation_rebuilder_v2 mut_builder(rd.schema());
            mutation_fragment_stream_validating_filter validator("test", *rd.schema(), mutation_fragment_stream_validation_level::clustering_key);
            while (auto mf_opt = rd().get()) {
                if (mf_opt->is_partition_start()) {
                    const auto& key = mf_opt->as_partition_start().key();
                    validator(key);
                }
                validator(*mf_opt);
                if (mf_opt->is_end_of_partition()) {
                    output_mutations[index].emplace_back(*std::move(mut_builder).consume_end_of_stream());
                } else {
                    std::move(*mf_opt).consume(mut_builder);
                }
            }
            validator.on_end_of_stream();
        });
    };
    auto check_and_reset = [&] {
        std::vector<mutation_reader> readers;
        auto close_readers = defer([&] {
            for (auto& rd : readers) {
                rd.close().get();
            }
        });
        for (auto muts : output_mutations) {
            readers.emplace_back(make_mutation_reader_from_mutations_v2(random_schema.schema(), semaphore.make_permit(), std::move(muts)));
        }
        auto rd = assert_that(make_combined_reader(random_schema.schema(), semaphore.make_permit(), std::move(readers)));
        for (const auto& mut : input_mutations) {
            rd.produces(mut);
        }
        output_mutations.clear();
        next_index = 0;
    };

    for (const size_t max_memory : {1'000, 10'000, 1'000'000, 10'000'000, 100'000'000}) {
        testlog.info("Segregating with in-memory method (max_memory={})", max_memory);
        mutation_writer::segregate_by_partition(
                make_mutation_reader_from_mutations_v2(random_schema.schema(), semaphore.make_permit(), shuffled_input_mutations),
                mutation_writer::segregate_config{max_memory},
                consumer).get();
        testlog.info("Done segregating with in-memory method (max_memory={}): input segregated into {} buckets", max_memory, output_mutations.size());
        check_and_reset();
    }

}

SEASTAR_THREAD_TEST_CASE(test_token_group_based_splitting_mutation_writer) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto random_spec = tests::make_random_schema_specification(
            get_name(),
            std::uniform_int_distribution<size_t>(1, 4),
            std::uniform_int_distribution<size_t>(2, 4),
            std::uniform_int_distribution<size_t>(2, 8),
            std::uniform_int_distribution<size_t>(2, 8));
    auto random_schema = tests::random_schema{tests::random::get_int<uint32_t>(), *random_spec};

    testlog.info("Random schema:\n{}", random_schema.cql());

    size_t partition_count = many_partitions();

    auto muts = tests::generate_random_mutations(
            random_schema,
            tests::default_timestamp_generator(),
            tests::no_expiry_expiry_generator(),
            std::uniform_int_distribution<size_t>(partition_count, partition_count),
            std::uniform_int_distribution<size_t>(1, 1)).get(); // only 1 row

    auto classify_fn = [] (dht::token t) -> mutation_writer::token_group_id {
        return dht::compaction_group_of(1, t);
    };

    std::unordered_map<int64_t, std::vector<mutation>> buckets;

    auto consumer = [&] (mutation_reader bucket_reader) {
        return with_closeable(std::move(bucket_reader), [&] (mutation_reader& rd) {
            return rd.consume(test_bucket_writer(random_schema.schema(), rd.permit(), classify_fn, buckets));
        });
    };

    segregate_by_token_group(make_mutation_reader_from_mutations_v2(random_schema.schema(), semaphore.make_permit(), muts), classify_fn, std::move(consumer)).get();

    testlog.info("Data split into {} buckets: {}", buckets.size(), boost::copy_range<std::vector<int64_t>>(buckets | boost::adaptors::map_keys));

    assert_that_segregator_produces_correct_data(buckets, muts, semaphore.make_permit(), random_schema);
}
