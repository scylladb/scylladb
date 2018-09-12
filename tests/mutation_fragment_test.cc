/*
 * Copyright (C) 2016 ScyllaDB
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

#include "mutation_source_test.hh"
#include "mutation_fragment.hh"
#include "frozen_mutation.hh"
#include "tests/test_services.hh"
#include "schema_builder.hh"
#include "total_order_check.hh"
#include "schema_upgrader.hh"
#include "memtable.hh"

#include "mutation_assertions.hh"

// A StreamedMutationConsumer which distributes fragments randomly into several mutations.
class fragment_scatterer {
    std::vector<mutation>& _mutations;
    size_t _next = 0;
private:
    template<typename Func>
    void for_each_target(Func&& func) {
        // round-robin
        func(_mutations[_next % _mutations.size()]);
        ++_next;
    }
public:
    fragment_scatterer(std::vector<mutation>& muts)
        : _mutations(muts)
    { }

    void consume_new_partition(const dht::decorated_key&) {}

    stop_iteration consume(tombstone t) {
        for_each_target([&] (mutation& m) {
            m.partition().apply(t);
        });
        return stop_iteration::no;
    }

    stop_iteration consume(range_tombstone&& rt) {
        for_each_target([&] (mutation& m) {
            m.partition().apply_row_tombstone(*m.schema(), std::move(rt));
        });
        return stop_iteration::no;
    }

    stop_iteration consume(static_row&& sr) {
        for_each_target([&] (mutation& m) {
            m.partition().static_row().apply(*m.schema(), column_kind::static_column, std::move(sr.cells()));
        });
        return stop_iteration::no;
    }

    stop_iteration consume(clustering_row&& cr) {
        for_each_target([&] (mutation& m) {
            auto& dr = m.partition().clustered_row(*m.schema(), std::move(cr.key()));
            dr.apply(cr.tomb());
            dr.apply(cr.marker());
            dr.cells().apply(*m.schema(), column_kind::regular_column, std::move(cr.cells()));
        });
        return stop_iteration::no;
    }

    stop_iteration consume_end_of_partition() {
        return stop_iteration::no;
    }

    stop_iteration consume_end_of_stream() {
        return stop_iteration::no;
    }
};

SEASTAR_TEST_CASE(test_mutation_merger_conforms_to_mutation_source) {
    return seastar::async([] {
        run_mutation_source_tests([](schema_ptr s, const std::vector<mutation>& partitions) -> mutation_source {
            // We create a mutation source which combines N memtables.
            // The input fragments are spread among the memtables according to some selection logic,

            const int n = 5;

            std::vector<lw_shared_ptr<memtable>> memtables;
            for (int i = 0; i < n; ++i) {
                memtables.push_back(make_lw_shared<memtable>(s));
            }

            for (auto&& m : partitions) {
                std::vector<mutation> muts;
                for (int i = 0; i < n; ++i) {
                    muts.push_back(mutation(m.schema(), m.decorated_key()));
                }
                auto rd = flat_mutation_reader_from_mutations({m});
                rd.consume(fragment_scatterer{muts}, db::no_timeout).get();
                for (int i = 0; i < n; ++i) {
                    memtables[i]->apply(std::move(muts[i]));
                }
            }

            return mutation_source([memtables] (schema_ptr s,
                    const dht::partition_range& range,
                    const query::partition_slice& slice,
                    const io_priority_class& pc,
                    tracing::trace_state_ptr trace_state,
                    streamed_mutation::forwarding fwd,
                    mutation_reader::forwarding fwd_mr)
            {
                std::vector<flat_mutation_reader> readers;
                for (int i = 0; i < n; ++i) {
                    readers.push_back(memtables[i]->make_flat_reader(s, range, slice, pc, trace_state, fwd, fwd_mr));
                }
                return make_combined_reader(s, std::move(readers), fwd, fwd_mr);
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

        mutation_fragment cr1 = clustering_row(create_ck({ 0, 0 }));
        mutation_fragment cr2 = clustering_row(create_ck({ 1, 0 }));
        mutation_fragment cr3 = clustering_row(create_ck({ 1, 1 }));
        auto cr4 = rows_entry(create_ck({ 1, 2 }));
        auto cr5 = rows_entry(create_ck({ 1, 3 }));

        range_tombstone_stream rts(*s);
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

        mf = rts.get_next(mutation_fragment(range_tombstone(rt3)));
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
        const bytes_view column_name = col.name();
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

        auto make_ck = [&] (int ck1, stdx::optional<int> ck2 = stdx::nullopt) {
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
        for_each_mutation_pair([](const mutation& m1, const mutation& m2, are_equal eq) {
            if (m1.schema()->version() != m2.schema()->version()) {
                // upgrade m1 to m2's schema

                auto reader = transform(flat_mutation_reader_from_mutations({m1}), schema_upgrader(m2.schema()));
                auto from_upgrader = read_mutation_from_flat_mutation_reader(reader, db::no_timeout).get0();

                auto regular = m1;
                regular.upgrade(m2.schema());

                assert_that(from_upgrader).has_mutation().is_equal_to(regular);
            }
        });
    });
}
