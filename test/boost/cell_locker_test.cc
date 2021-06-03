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

#include <seastar/testing/test_case.hh>

#include <seastar/core/thread.hh>

#include "cell_locking.hh"
#include "mutation.hh"
#include "schema_builder.hh"

using namespace std::literals::chrono_literals;

static schema_ptr make_schema()
{
    return schema_builder("ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("ck", bytes_type, column_kind::clustering_key)
            .with_column("s1", bytes_type, column_kind::static_column)
            .with_column("s2", bytes_type, column_kind::static_column)
            .with_column("s3", bytes_type, column_kind::static_column)
            .with_column("r1", bytes_type)
            .with_column("r2", bytes_type)
            .with_column("r3", bytes_type)
            .build();
}

static schema_ptr make_alternative_schema()
{
    return schema_builder("ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("ck", bytes_type, column_kind::clustering_key)
            .with_column("s0", bytes_type, column_kind::static_column)
            .with_column("s1", bytes_type, column_kind::static_column)
            .with_column("s2.5", bytes_type, column_kind::static_column)
            .with_column("s3", bytes_type, column_kind::static_column)
            .with_column("r0", bytes_type)
            .with_column("r1", bytes_type)
            .with_column("r2.5", bytes_type)
            .with_column("r3", bytes_type)
            .build();
}

static schema_ptr make_schema_disjoint_with_others()
{
    return schema_builder("ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("ck", bytes_type, column_kind::clustering_key)
            .with_column("s8", bytes_type, column_kind::static_column)
            .with_column("s9", bytes_type, column_kind::static_column)
            .with_column("r8", bytes_type)
            .with_column("r9", bytes_type)
            .build();
}

static thread_local data_value empty_value = data_value(to_bytes(""));

static db::timeout_clock::time_point no_timeout {
    db::timeout_clock::duration(std::numeric_limits<db::timeout_clock::duration::rep>::max())
};

static auto make_row(const sstring& key, std::initializer_list<sstring> cells) {
    return std::pair<sstring, std::initializer_list<sstring>>(key, cells);
}

static mutation make_mutation(schema_ptr s, const sstring& pk, std::initializer_list<sstring> static_cells,
                              std::initializer_list<std::pair<sstring, std::initializer_list<sstring>>> clustering_cells)
{
    auto m = mutation(s, partition_key::from_single_value(*s, to_bytes(pk)));
    for (auto&& c : static_cells) {
        m.set_static_cell(to_bytes(c), empty_value, api::new_timestamp());
    }
    for (auto&& r : clustering_cells) {
        auto ck = clustering_key::from_single_value(*s, to_bytes(r.first));
        for (auto&& c : r.second) {
            m.set_clustered_cell(ck, to_bytes(c), empty_value, api::new_timestamp());
        }
    }
    return m;
}

SEASTAR_TEST_CASE(test_simple_locking_cells) {
    return seastar::async([&] {
        auto destroy = [] (auto) { };

        auto s = make_schema();
        cell_locker_stats cl_stats;
        cell_locker cl(s, cl_stats);

        auto m = make_mutation(s, "0", { "s1", "s3" }, {
            make_row("one", { "r1", "r2" }),
            make_row("two", { "r2", "r3" }),
        });

        auto l1 = cl.lock_cells(m.decorated_key(), partition_cells_range(m.partition()), no_timeout).get0();
        auto f2 = cl.lock_cells(m.decorated_key(), partition_cells_range(m.partition()), no_timeout);
        BOOST_REQUIRE(!f2.available());

        destroy(std::move(l1));
        destroy(f2.get0());
    });
}

SEASTAR_TEST_CASE(test_disjoint_mutations) {
    return seastar::async([&] {
        auto s = make_schema();
        cell_locker_stats cl_stats;
        cell_locker cl(s, cl_stats);

        auto m1 = make_mutation(s, "0", { "s1" }, {
                make_row("one", { "r1", "r2" }),
                make_row("two", { "r3" }),
        });
        auto m2 = make_mutation(s, "0", { "s2" }, {
                make_row("two", { "r1", "r2" }),
                make_row("one", { "r3" }),
        });

        auto m3 = mutation(s, partition_key::from_single_value(*s, to_bytes("1")));
        m3.partition() = mutation_partition(*s, m1.partition());

        auto l1 = cl.lock_cells(m1.decorated_key(), partition_cells_range(m1.partition()), no_timeout).get0();
        auto l2 = cl.lock_cells(m2.decorated_key(), partition_cells_range(m2.partition()), no_timeout).get0();
        auto l3 = cl.lock_cells(m3.decorated_key(), partition_cells_range(m3.partition()), no_timeout).get0();
    });
}

SEASTAR_TEST_CASE(test_single_cell_overlap) {
    return seastar::async([&] {
        auto destroy = [] (auto) { };

        auto s = make_schema();
        cell_locker_stats cl_stats;
        cell_locker cl(s, cl_stats);

        auto m1 = make_mutation(s, "0", { "s1" }, {
                make_row("one", { "r1", "r2" }),
                make_row("two", { "r3" }),
        });
        auto m2 = make_mutation(s, "0", { "s1" }, {
                make_row("two", { "r1", "r2" }),
                make_row("one", { "r3" }),
        });
        auto m3 = make_mutation(s, "0", { "s2" }, {
                make_row("two", { "r1" }),
                make_row("one", { "r2", "r3" }),
        });

        auto l1 = cl.lock_cells(m1.decorated_key(), partition_cells_range(m1.partition()), no_timeout).get0();
        auto f2 = cl.lock_cells(m2.decorated_key(), partition_cells_range(m2.partition()), no_timeout);
        BOOST_REQUIRE(!f2.available());
        destroy(std::move(l1));
        auto l2 = f2.get0();
        auto f3 = cl.lock_cells(m3.decorated_key(), partition_cells_range(m3.partition()), no_timeout);
        BOOST_REQUIRE(!f3.available());
        destroy(std::move(l2));
        auto l3 = f3.get0();
    });
}

SEASTAR_TEST_CASE(test_schema_change) {
    return seastar::async([&] {
        auto destroy = [] (auto) { };

        auto s1 = make_schema();
        auto s2 = make_alternative_schema();
        cell_locker_stats cl_stats;
        cell_locker cl(s1, cl_stats);

        auto m1 = make_mutation(s1, "0", { "s1", "s2", "s3"}, {
            make_row("one", { "r1", "r2", "r3" }),
        });

        // disjoint with m1
        auto m2 = make_mutation(s2, "0", { "s0", "s2.5"}, {
                make_row("one", { "r0", "r2.5" }),
                make_row("two", { "r1", "r3" }),
        });

        // overlaps with m1
        auto m3 = make_mutation(s2, "0", { "s1" }, {
                make_row("one", { "r1", "r3" }),
        });

        auto l1 = cl.lock_cells(m1.decorated_key(), partition_cells_range(m1.partition()), no_timeout).get0();

        destroy(std::move(m1));
        destroy(std::move(s1));
        cl.set_schema(s2);

        auto l2 = cl.lock_cells(m2.decorated_key(), partition_cells_range(m2.partition()), no_timeout).get0();
        auto f3 = cl.lock_cells(m3.decorated_key(), partition_cells_range(m3.partition()), no_timeout);
        BOOST_REQUIRE(!f3.available());
        destroy(std::move(l1));
        auto l3 = f3.get0();

        auto s3 = make_schema_disjoint_with_others();
        cl.set_schema(s3);

        auto m4 = make_mutation(s3, "0", { "s8", "s9"}, {
                make_row("one", { "r8", "r9" }),
                make_row("two", { "r8", "r9" }),
        });
        auto l4 = cl.lock_cells(m4.decorated_key(), partition_cells_range(m4.partition()), no_timeout).get0();
    });
}

SEASTAR_TEST_CASE(test_timed_out) {
        return seastar::async([&] {
            auto destroy = [] (auto) { };

            auto s = make_schema();
            cell_locker_stats cl_stats;
            cell_locker cl(s, cl_stats);

            auto m1 = make_mutation(s, "0", { "s1", "s2", "s3"}, {
                    make_row("one", { "r2", "r3" }),
            });
            auto m2 = make_mutation(s, "0", { }, {
                    make_row("one", { "r1", "r2" }),
            });

            auto l1 = cl.lock_cells(m1.decorated_key(), partition_cells_range(m1.partition()), no_timeout).get0();

            auto timeout = db::timeout_clock::now();
            forward_jump_clocks(1h);
            BOOST_REQUIRE_THROW(cl.lock_cells(m2.decorated_key(), partition_cells_range(m2.partition()), timeout).get0(),
                                timed_out_error);

            auto f2 = cl.lock_cells(m2.decorated_key(), partition_cells_range(m2.partition()), no_timeout);
            BOOST_REQUIRE(!f2.available());
            destroy(std::move(l1));
            auto l2 = f2.get0();
        });
}

SEASTAR_TEST_CASE(test_locker_stats) {
    return seastar::async([&] {
        auto destroy = [] (auto) { };

        auto s = make_schema();
        cell_locker_stats cl_stats;
        cell_locker cl(s, cl_stats);

        auto m1 = make_mutation(s, "0", { "s2", "s3" }, {
                make_row("one", { "r1", "r2" }),
        });

        auto m2 = make_mutation(s, "0", { "s1", "s3" }, {
                make_row("one", { "r2", "r3" }),
        });

        auto l1 = cl.lock_cells(m1.decorated_key(), partition_cells_range(m1.partition()), no_timeout).get0();
        BOOST_REQUIRE_EQUAL(cl_stats.lock_acquisitions, 4);
        BOOST_REQUIRE_EQUAL(cl_stats.operations_waiting_for_lock, 0);

        auto f2 = cl.lock_cells(m2.decorated_key(), partition_cells_range(m2.partition()), no_timeout);
        BOOST_REQUIRE_EQUAL(cl_stats.lock_acquisitions, 5);
        BOOST_REQUIRE_EQUAL(cl_stats.operations_waiting_for_lock, 1);
        BOOST_REQUIRE(!f2.available());

        destroy(std::move(l1));
        destroy(f2.get0());
        BOOST_REQUIRE_EQUAL(cl_stats.lock_acquisitions, 8);
        BOOST_REQUIRE_EQUAL(cl_stats.operations_waiting_for_lock, 0);
    });
}
