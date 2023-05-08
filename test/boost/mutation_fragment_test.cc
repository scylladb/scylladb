/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <seastar/core/thread.hh>
#include <seastar/testing/on_internal_error.hh>
#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/closeable.hh>

#include "test/lib/mutation_source_test.hh"
#include "mutation/mutation_fragment.hh"
#include "mutation/frozen_mutation.hh"
#include "test/lib/test_services.hh"
#include "schema/schema_builder.hh"
#include "test/boost/total_order_check.hh"
#include "schema_upgrader.hh"
#include "readers/combined.hh"
#include "replica/memtable.hh"
#include "mutation/mutation_rebuilder.hh"

#include "test/lib/mutation_assertions.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/fragment_scatterer.hh"

#include <boost/range/algorithm/transform.hpp>
#include "readers/from_mutations_v2.hh"

SEASTAR_TEST_CASE(test_mutation_merger_conforms_to_mutation_source) {
    return seastar::async([] {
        tests::reader_concurrency_semaphore_wrapper semaphore;
        run_mutation_source_tests([&](schema_ptr s, const std::vector<mutation>& partitions) -> mutation_source {
            // We create a mutation source which combines N memtables.
            // The input fragments are spread among the memtables according to some selection logic,

            const int n = 5;

            std::vector<lw_shared_ptr<replica::memtable>> memtables;
            for (int i = 0; i < n; ++i) {
                memtables.push_back(make_lw_shared<replica::memtable>(s));
            }

            for (auto&& m : partitions) {
                auto rd = make_flat_mutation_reader_from_mutations_v2(s, semaphore.make_permit(), {m});
                auto close_rd = deferred_close(rd);
                auto muts = rd.consume(fragment_scatterer(s, n)).get();
                for (int i = 0; i < n; ++i) {
                    memtables[i]->apply(std::move(muts[i]));
                }
            }

            return mutation_source([memtables] (schema_ptr s,
                    reader_permit permit,
                    const dht::partition_range& range,
                    const query::partition_slice& slice,
                    const io_priority_class& pc,
                    tracing::trace_state_ptr trace_state,
                    streamed_mutation::forwarding fwd,
                    mutation_reader::forwarding fwd_mr)
            {
                std::vector<flat_mutation_reader_v2> readers;
                for (int i = 0; i < n; ++i) {
                    readers.push_back(memtables[i]->make_flat_reader(s, permit, range, slice, pc, trace_state, fwd, fwd_mr));
                }
                return make_combined_reader(s, std::move(permit), std::move(readers), fwd, fwd_mr);
            });
        });
    });
}

SEASTAR_TEST_CASE(test_range_tombstones_stream) {
    return seastar::async([] {
        auto s = schema_builder("ks", "cf")
                .with_column("pk", int32_type, column_kind::partition_key)
                .with_column("ck1", int32_type, column_kind::clustering_key)
                .with_column("ck2", int32_type, column_kind::clustering_key)
                .with_column("r", int32_type)
                .build();
        tests::reader_concurrency_semaphore_wrapper semaphore;

        auto pk = partition_key::from_single_value(*s, int32_type->decompose(0));
        auto create_ck = [&] (std::vector<int> v) {
            std::vector<bytes> vs;
            boost::transform(v, std::back_inserter(vs), [] (int x) { return int32_type->decompose(x); });
            return clustering_key_prefix::from_exploded(*s, std::move(vs));
        };

        tombstone t0(0, { });
        tombstone t1(1, { });

        auto rt1 = range_tombstone(create_ck({ 1 }), t0, bound_kind::incl_start, create_ck({ 1, 3 }), bound_kind::incl_end);
        auto rt2 = range_tombstone(create_ck({ 1, 1 }), t1, bound_kind::incl_start, create_ck({ 1, 3 }), bound_kind::excl_end);
        auto rt3 = range_tombstone(create_ck({ 1, 1 }), t0,  bound_kind::incl_start, create_ck({ 2 }), bound_kind::incl_end);
        auto rt4 = range_tombstone(create_ck({ 2 }), t0, bound_kind::incl_start, create_ck({ 2, 2 }), bound_kind::incl_end);

        auto permit = semaphore.make_permit();

        mutation_fragment cr1(*s, permit, clustering_row(create_ck({ 0, 0 })));
        mutation_fragment cr2(*s, permit, clustering_row(create_ck({ 1, 0 })));
        mutation_fragment cr3(*s, permit, clustering_row(create_ck({ 1, 1 })));
        auto cr4 = rows_entry(create_ck({ 1, 2 }));
        auto cr5 = rows_entry(create_ck({ 1, 3 }));

        range_tombstone_stream rts(*s, permit);
        rts.apply(range_tombstone(rt1));
        rts.apply(range_tombstone(rt2));
        rts.apply(range_tombstone(rt4));

        mutation_fragment_opt mf = rts.get_next(cr1);
        BOOST_REQUIRE(!mf);

        mf = rts.get_next(cr2);
        BOOST_REQUIRE(mf && mf->is_range_tombstone());
        auto expected1 = range_tombstone(create_ck({ 1 }), t0, bound_kind::incl_start, create_ck({ 1, 1 }), bound_kind::excl_end);
        BOOST_REQUIRE(mf->as_range_tombstone().equal(*s, expected1));

        mf = rts.get_next(cr2);
        BOOST_REQUIRE(!mf);

        mf = rts.get_next(mutation_fragment(*s, permit, range_tombstone(rt3)));
        BOOST_REQUIRE(mf && mf->is_range_tombstone());
        BOOST_REQUIRE(mf->as_range_tombstone().equal(*s, rt2));

        mf = rts.get_next(cr3);
        BOOST_REQUIRE(!mf);

        mf = rts.get_next(cr4);
        BOOST_REQUIRE(!mf);

        mf = rts.get_next(cr5);
        BOOST_REQUIRE(mf && mf->is_range_tombstone());
        auto expected2 = range_tombstone(create_ck({ 1, 3 }), t0, bound_kind::incl_start, create_ck({ 1, 3 }), bound_kind::incl_end);
        BOOST_REQUIRE(mf->as_range_tombstone().equal(*s, expected2));

        mf = rts.get_next();
        BOOST_REQUIRE(mf && mf->is_range_tombstone());
        BOOST_REQUIRE(mf->as_range_tombstone().equal(*s, rt4));

        mf = rts.get_next();
        BOOST_REQUIRE(!mf);
    });
}

static
composite cell_name(const schema& s, const clustering_key& ck, const column_definition& col) {
    if (s.is_dense()) {
        return composite::serialize_value(ck.components(s), s.is_compound());
    } else {
        const managed_bytes_view column_name = bytes_view(col.name());
        return composite::serialize_value(boost::range::join(
                boost::make_iterator_range(ck.begin(s), ck.end(s)),
                boost::make_iterator_range(&column_name, &column_name + 1)),
            s.is_compound());
    }
}

static
composite cell_name_for_static_column(const schema& s, const column_definition& cdef) {
    const bytes_view column_name = cdef.name();
    return composite::serialize_static(s, boost::make_iterator_range(&column_name, &column_name + 1));
}

inline
composite composite_for_key(const schema& s, const clustering_key& ck) {
    return composite::serialize_value(ck.components(s), s.is_compound());
}

inline
composite composite_before_key(const schema& s, const clustering_key& ck) {
    return composite::serialize_value(ck.components(s), s.is_compound(), composite::eoc::start);
}

inline
composite composite_after_prefixed(const schema& s, const clustering_key& ck) {
    return composite::serialize_value(ck.components(s), s.is_compound(), composite::eoc::end);
}

inline
position_in_partition position_for_row(const clustering_key& ck) {
    return position_in_partition(position_in_partition::clustering_row_tag_t(), ck);
}

inline
position_in_partition position_before(const clustering_key& ck) {
    return position_in_partition(position_in_partition::range_tag_t(), bound_view(ck, bound_kind::incl_start));
}

inline
position_in_partition position_after_prefixed(const clustering_key& ck) {
    return position_in_partition(position_in_partition::range_tag_t(), bound_view(ck, bound_kind::incl_end));
}

SEASTAR_TEST_CASE(test_ordering_of_position_in_partition_and_composite_view) {
    return seastar::async([] {
        auto s = schema_builder("ks", "cf")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("ck1", int32_type, column_kind::clustering_key)
            .with_column("ck2", int32_type, column_kind::clustering_key)
            .with_column("s1", int32_type, column_kind::static_column)
            .with_column("v", int32_type)
            .build();

        const column_definition& v_def = *s->get_column_definition("v");
        const column_definition& s_def = *s->get_column_definition("s1");

        auto make_ck = [&] (int ck1, int ck2) {
            std::vector<data_value> cells;
            cells.push_back(data_value(ck1));
            cells.push_back(data_value(ck2));
            return clustering_key::from_deeply_exploded(*s, cells);
        };

        auto ck1 = make_ck(1, 2);
        auto ck2 = make_ck(2, 1);
        auto ck3 = make_ck(2, 3);
        auto ck4 = make_ck(3, 1);

        using cmp = position_in_partition::composite_tri_compare;
        total_order_check<cmp, position_in_partition, composite>(cmp(*s))
            .next(cell_name_for_static_column(*s, s_def))
                .equal_to(position_range::full().start())
            .next(position_before(ck1))
                .equal_to(composite_before_key(*s, ck1))
                .equal_to(composite_for_key(*s, ck1))
                .equal_to(position_for_row(ck1))
            .next(cell_name(*s, ck1, v_def))
            .next(position_after_prefixed(ck1))
                .equal_to(composite_after_prefixed(*s, ck1))
            .next(position_before(ck2))
                .equal_to(composite_before_key(*s, ck2))
                .equal_to(composite_for_key(*s, ck2))
                .equal_to(position_for_row(ck2))
            .next(cell_name(*s, ck2, v_def))
            .next(position_after_prefixed(ck2))
                .equal_to(composite_after_prefixed(*s, ck2))
            .next(position_before(ck3))
                .equal_to(composite_before_key(*s, ck3))
                .equal_to(composite_for_key(*s, ck3))
                .equal_to(position_for_row(ck3))
            .next(cell_name(*s, ck3, v_def))
            .next(position_after_prefixed(ck3))
                .equal_to(composite_after_prefixed(*s, ck3))
            .next(position_before(ck4))
                .equal_to(composite_before_key(*s, ck4))
                .equal_to(composite_for_key(*s, ck4))
                .equal_to(position_for_row(ck4))
            .next(cell_name(*s, ck4, v_def))
            .next(position_after_prefixed(ck4))
                .equal_to(composite_after_prefixed(*s, ck4))
            .next(position_range::full().end())
            .check();
    });
}

SEASTAR_TEST_CASE(test_ordering_of_position_in_partition_and_composite_view_in_a_dense_table) {
    return seastar::async([] {
        auto s = schema_builder("ks", "cf")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("ck1", int32_type, column_kind::clustering_key)
            .with_column("ck2", int32_type, column_kind::clustering_key)
            .with_column("v", int32_type)
            .set_is_dense(true)
            .build();

        auto make_ck = [&] (int ck1, std::optional<int> ck2 = std::nullopt) {
            std::vector<data_value> cells;
            cells.push_back(data_value(ck1));
            if (ck2) {
                cells.push_back(data_value(ck2));
            }
            return clustering_key::from_deeply_exploded(*s, cells);
        };

        auto ck1 = make_ck(1);
        auto ck2 = make_ck(1, 2);
        auto ck3 = make_ck(2);
        auto ck4 = make_ck(2, 3);
        auto ck5 = make_ck(2, 4);
        auto ck6 = make_ck(3);

        using cmp = position_in_partition::composite_tri_compare;
        total_order_check<cmp, position_in_partition, composite>(cmp(*s))
            .next(composite())
            .next(position_range::full().start())
            .next(position_before(ck1))
                .equal_to(composite_before_key(*s, ck1))
                .equal_to(composite_for_key(*s, ck1))
                .equal_to(position_for_row(ck1))
            // .next(position_after(ck1)) // FIXME: #1446
            .next(position_before(ck2))
                .equal_to(composite_before_key(*s, ck2))
                .equal_to(composite_for_key(*s, ck2))
                .equal_to(position_for_row(ck2))
            .next(position_after_prefixed(ck2))
                .equal_to(composite_after_prefixed(*s, ck2))
            .next(position_after_prefixed(ck1)) // prefix of ck2
                .equal_to(composite_after_prefixed(*s, ck1))
            .next(position_before(ck3))
                .equal_to(composite_before_key(*s, ck3))
                .equal_to(composite_for_key(*s, ck3))
                .equal_to(position_for_row(ck3))
            // .next(position_after(ck3)) // FIXME: #1446
            .next(position_before(ck4))
                .equal_to(composite_before_key(*s, ck4))
                .equal_to(composite_for_key(*s, ck4))
                .equal_to(position_for_row(ck4))
            .next(position_after_prefixed(ck4))
                .equal_to(composite_after_prefixed(*s, ck4))
            .next(position_before(ck5))
                .equal_to(composite_before_key(*s, ck5))
                .equal_to(composite_for_key(*s, ck5))
                .equal_to(position_for_row(ck5))
            .next(position_after_prefixed(ck5))
                .equal_to(composite_after_prefixed(*s, ck5))
            .next(position_after_prefixed(ck3)) // prefix of ck4-ck5
                .equal_to(composite_after_prefixed(*s, ck3))
            .next(position_before(ck6))
                .equal_to(composite_before_key(*s, ck6))
                .equal_to(composite_for_key(*s, ck6))
                .equal_to(position_for_row(ck6))
            .next(position_after_prefixed(ck6))
                .equal_to(composite_after_prefixed(*s, ck6))
            .next(position_range::full().end())
            .check();
    });
}

SEASTAR_TEST_CASE(test_schema_upgrader_is_equivalent_with_mutation_upgrade) {
    return seastar::async([] {
        tests::reader_concurrency_semaphore_wrapper semaphore;
        for_each_mutation_pair([&](const mutation& m1, const mutation& m2, are_equal eq) {
            if (m1.schema()->version() != m2.schema()->version()) {
                // upgrade m1 to m2's schema

                auto reader = transform(make_flat_mutation_reader_from_mutations_v2(m1.schema(), semaphore.make_permit(), {m1}), schema_upgrader_v2(m2.schema()));
                auto close_reader = deferred_close(reader);
                auto from_upgrader = read_mutation_from_flat_mutation_reader(reader).get0();

                auto regular = m1;
                regular.upgrade(m2.schema());

                assert_that(from_upgrader).has_mutation().is_equal_to(regular);
            }
        });
    });
}

SEASTAR_THREAD_TEST_CASE(test_mutation_fragment_mutate_exception_safety) {
    struct dummy_exception { };

    simple_schema s;

    reader_concurrency_semaphore sem(reader_concurrency_semaphore::for_tests{}, get_name(), 1, 100);
    auto stop_sem = deferred_stop(sem);
    auto permit = sem.make_tracking_only_permit(s.schema().get(), get_name(), db::no_timeout, {});

    const auto available_res = sem.available_resources();
    const sstring val(1024, 'a');

    // partition start
    {
        try {
            auto ps = mutation_fragment(*s.schema(), permit, partition_start(s.make_pkey(0), {}));
            ps.mutate_as_partition_start(*s.schema(), [&] (partition_start&) {
                throw dummy_exception{};
            });
        } catch (dummy_exception&) { }
        BOOST_REQUIRE(available_res == sem.available_resources());
    }

    // static row
    {
        try {
            auto sr = s.make_static_row(permit, val);
            // Copy to move to our permit.
            sr = mutation_fragment(*s.schema(), permit, sr);
            sr.mutate_as_clustering_row(*s.schema(), [&] (clustering_row&) {
                throw dummy_exception{};
            });
        } catch (dummy_exception&) { }
        BOOST_REQUIRE(available_res == sem.available_resources());
    }

    // clustering row
    {
        try {
            auto cr = s.make_row(permit, s.make_ckey(0), val);
            // Copy to move to our permit.
            cr = mutation_fragment(*s.schema(), permit, cr);
            cr.mutate_as_clustering_row(*s.schema(), [&] (clustering_row&) {
                throw dummy_exception{};
            });
        } catch (dummy_exception&) { }
        BOOST_REQUIRE(available_res == sem.available_resources());
    }

    // range tombstone
    {
        try {
            auto rt = mutation_fragment(*s.schema(), permit, s.make_range_tombstone(query::clustering_range::make_ending_with(s.make_ckey(0))));
            rt.mutate_as_range_tombstone(*s.schema(), [&] (range_tombstone&) {
                throw dummy_exception{};
            });
        } catch (dummy_exception&) { }
        BOOST_REQUIRE(available_res == sem.available_resources());
    }
}

SEASTAR_THREAD_TEST_CASE(test_mutation_fragment_stream_validator) {
    testing::scoped_no_abort_on_internal_error _;

    simple_schema ss;

    const auto dkeys = ss.make_pkeys(3);
    const auto& dk_ = dkeys[0];
    const auto& dk0 = dkeys[1];
    const auto& dk1 = dkeys[2];
    const auto ck0 = ss.make_ckey(0);
    const auto ck1 = ss.make_ckey(1);
    const auto ck2 = ss.make_ckey(2);
    const auto ck3 = ss.make_ckey(3);

    reader_concurrency_semaphore sem(reader_concurrency_semaphore::for_tests{}, get_name(), 1, 100);
    auto stop_sem = deferred_stop(sem);
    auto permit = sem.make_tracking_only_permit(ss.schema().get(), get_name(), db::no_timeout, {});

    auto expect = [&] (bool expect_valid, const char* desc, unsigned at, auto&& first_mf, auto&&... mf) {
        std::vector<mutation_fragment_v2> mfs;
        {
            bool need_inject_ps = false;
            if constexpr (std::is_same_v<std::remove_reference_t<decltype(first_mf)>, mutation_fragment_v2>) {
                need_inject_ps = !first_mf.is_partition_start();
            } else {
                need_inject_ps = !std::is_same_v<std::remove_reference_t<decltype(first_mf)>, partition_start>;
            }
            if (need_inject_ps) {
                testlog.trace("Injecting partition start");
                mfs.emplace_back(*ss.schema(), permit, partition_start(dk_, {}));
                if (at != std::numeric_limits<unsigned>::max()) {
                    ++at;
                }
            }
            mfs.emplace_back(*ss.schema(), permit, std::move(first_mf));
            auto _ = std::vector<mutation_fragment_v2*>{&mfs.emplace_back(*ss.schema(), permit, std::move(mf))..., };
        }

        testlog.info("Checking scenario {} with validator", desc);
        {
            unsigned i = 0;
            mutation_fragment_stream_validator validator(*ss.schema());
            bool valid = true;
            for (const auto& mf : mfs) {
                testlog.trace("validate fragment [{}] {} @ {}", i, mf.mutation_fragment_kind(), mf.position());
                valid &= bool(validator(mf));
                if (expect_valid) {
                    if (!valid) {
                        BOOST_FAIL(fmt::format("Unexpected invalid fragment {} @ {}", mf.mutation_fragment_kind(), mf.position()));
                    }
                } else {
                    if (i == at && valid) {
                        BOOST_FAIL(fmt::format("Unexpected valid fragment {} @ {}", mf.mutation_fragment_kind(), mf.position()));
                    }
                }
                ++i;
            }
            if (expect_valid || i <= at) {
                valid &= bool(validator.on_end_of_stream());
                BOOST_REQUIRE(valid == expect_valid);
            }
        }

        testlog.info("Checking scenario {} with validating filter", desc);
        {
            unsigned i = 0;
            mutation_fragment_stream_validating_filter validator(get_name(), *ss.schema(), mutation_fragment_stream_validation_level::clustering_key);
            for (const auto& mf : mfs) {
                testlog.trace("validate fragment [{}] {} @ {}", i, mf.mutation_fragment_kind(), mf.position());
                try {
                    validator(mf);
                    if (!expect_valid && i == at) {
                        BOOST_FAIL(fmt::format("Unexpected valid fragment {} @ {}", mf.mutation_fragment_kind(), mf.position()));
                    }
                } catch (invalid_mutation_fragment_stream& e) {
                    if (expect_valid || i < at) {
                        BOOST_FAIL(fmt::format("Unexpected invalid fragment {} @ {}: {}", mf.mutation_fragment_kind(), mf.position(), e));
                    } else {
                        testlog.trace("Got expected exception for fragment {} @ {}: {}", mf.mutation_fragment_kind(), mf.position(), e);
                    }
                }
                ++i;
            }
            if (expect_valid || i <= at) {
                try {
                    validator.on_end_of_stream();
                    if (!expect_valid) {
                        BOOST_FAIL("Unexpected valid EOS");
                    }
                } catch (invalid_mutation_fragment_stream& e) {
                    if (expect_valid) {
                        BOOST_FAIL(fmt::format("Unexpected invalid EOS: {}", e));
                    } else {
                        testlog.trace("Got expected exception at EOS: {}", e);
                    }
                }
            }
        }
    };

    auto expect_valid = [&] (const char* desc, auto&&... mf) {
        return expect(true, desc, std::numeric_limits<unsigned>::max(), std::move(mf)...);
    };

    auto expect_invalid_at_eos = [&] (const char* desc, auto&&... mf) {
        return expect(false, desc, std::numeric_limits<unsigned>::max(), std::move(mf)...);
    };

    auto expect_invalid_at_fragment = [&] (const char* desc, unsigned at, auto&&... mf) {
        return expect(false, desc, at, std::move(mf)...);
    };

    expect_valid(
            "kitchen sink",
            partition_start(dk0, {}),
            ss.make_static_row_v2(permit, "v"),
            ss.make_row_v2(permit, ck0, "ck0"),
            ss.make_row_v2(permit, ck1, "ck1"),
            range_tombstone_change(position_in_partition::after_key(*ss.schema(), ck1), {ss.new_tombstone()}),
            range_tombstone_change(position_in_partition::after_key(*ss.schema(), ck1), {ss.new_tombstone()}),
            range_tombstone_change(position_in_partition::before_key(ck2), {ss.new_tombstone()}),
            ss.make_row_v2(permit, ck2, "ck2"),
            range_tombstone_change(position_in_partition::after_key(*ss.schema(), ck2), {}),
            partition_end{},
            partition_start(dk1, {}),
            partition_end{});

    expect_valid(
            "static row alone",
            partition_start(dk0, {}),
            ss.make_static_row_v2(permit, "v"),
            partition_end{});

    expect_valid(
            "clustering row alone",
            partition_start(dk0, {}),
            ss.make_row_v2(permit, ck0, "ck0"),
            partition_end{});

    expect_valid(
            "2 range tombstone changes",
            partition_start(dk0, {}),
            range_tombstone_change(position_in_partition::after_key(*ss.schema(), ck1), {ss.new_tombstone()}),
            range_tombstone_change(position_in_partition::after_key(*ss.schema(), ck2), {}),
            partition_end{});

    expect_valid(
            "null range tombstone change alone",
            partition_start(dk0, {}),
            range_tombstone_change(position_in_partition::after_key(*ss.schema(), ck2), {}),
            partition_end{});

    expect_invalid_at_eos(
            "missing partition end at EOS",
            partition_start(dk0, {}));

    expect_invalid_at_fragment(
            "active range tombstone end at partition end",
            2,
            partition_start(dk0, {}),
            range_tombstone_change(position_in_partition::after_key(*ss.schema(), ck1), {ss.new_tombstone()}),
            partition_end{});

    const auto ps = mutation_fragment_v2(*ss.schema(), permit, partition_start(dk1, {}));
    const auto sr = ss.make_static_row_v2(permit, "v");
    const auto cr = ss.make_row_v2(permit, ck2, "ck2");
    const auto rtc = mutation_fragment_v2(*ss.schema(), permit, range_tombstone_change(position_in_partition::after_key(*ss.schema(), ck1), {ss.new_tombstone()}));
    const auto pe = mutation_fragment_v2(*ss.schema(), permit, partition_end{});

    auto check_invalid_after = [&] (auto&& mf_raw, std::initializer_list<const mutation_fragment_v2*> invalid_mfs) {
        auto mf = mutation_fragment_v2(*ss.schema(), permit, std::move(mf_raw));
        for (const auto invalid_mf : invalid_mfs) {
            std::string desc;
            if (mf.position().region() == partition_region::clustered) {
                desc = fmt::format("{} @ {} after {} @ {}", invalid_mf->mutation_fragment_kind(), invalid_mf->position(), mf.mutation_fragment_kind(), mf.position());
            } else {
                desc = fmt::format("{} after {}", invalid_mf->mutation_fragment_kind(), mf.mutation_fragment_kind());
            }

            expect_invalid_at_fragment(
                    desc.c_str(),
                    1,
                    mutation_fragment_v2(*ss.schema(), permit, mf),
                    mutation_fragment_v2(*ss.schema(), permit, *invalid_mf));
        }
    };

    check_invalid_after(partition_start(dk0, {}), {&ps});
    check_invalid_after(sr, {&sr, &ps});
    check_invalid_after(cr, {&ps, &sr, &cr});
    check_invalid_after(rtc, {&ps, &sr});
    check_invalid_after(pe, {&sr, &cr, &rtc, &pe});
}

SEASTAR_THREAD_TEST_CASE(test_mutation_fragment_stream_validator_mixed_api_usage) {
    simple_schema ss;

    const auto dkeys = ss.make_pkeys(3);
    const auto& dk_ = dkeys[0];
    const auto& dk0 = dkeys[1];
    const auto ck0 = ss.make_ckey(0);
    const auto ck1 = ss.make_ckey(1);
    const auto ck2 = ss.make_ckey(2);
    const auto ck3 = ss.make_ckey(3);

    reader_concurrency_semaphore sem(reader_concurrency_semaphore::for_tests{}, get_name(), 1, 100);
    auto stop_sem = deferred_stop(sem);
    auto permit = sem.make_tracking_only_permit(ss.schema().get(), get_name(), db::no_timeout, {});

    mutation_fragment_stream_validator validator(*ss.schema());

    using mf_kind = mutation_fragment_v2::kind;

    BOOST_REQUIRE(validator(mf_kind::partition_start, {}));
    BOOST_REQUIRE(validator(dk_.token()));
    BOOST_REQUIRE(validator(mf_kind::static_row, position_in_partition_view(position_in_partition_view::static_row_tag_t{}), {}));
    BOOST_REQUIRE(validator(mf_kind::clustering_row, {}));
    BOOST_REQUIRE(validator(mf_kind::clustering_row, {}));
    BOOST_REQUIRE(validator(mf_kind::clustering_row, position_in_partition_view::for_key(ck0), {}));
    BOOST_REQUIRE(validator(mf_kind::clustering_row, {}));
    BOOST_REQUIRE(!validator(mf_kind::clustering_row, position_in_partition_view::for_key(ck0), {}));
    BOOST_REQUIRE(validator(mf_kind::clustering_row, {}));
    BOOST_REQUIRE(validator(mf_kind::clustering_row, position_in_partition_view::for_key(ck1), {}));
    BOOST_REQUIRE(validator(mf_kind::clustering_row, {}));
    BOOST_REQUIRE(validator(mf_kind::range_tombstone_change, position_in_partition::after_key(*ss.schema(), ck1), {}));
    BOOST_REQUIRE(validator(mf_kind::range_tombstone_change, position_in_partition::after_key(*ss.schema(), ck1), {}));
    BOOST_REQUIRE(validator(mf_kind::partition_end, {}));
    BOOST_REQUIRE(validator(dk0));
    BOOST_REQUIRE(!validator(dk0));
}
