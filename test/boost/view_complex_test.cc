/*
 * Copyright (C) 2018-present ScyllaDB
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
#include <boost/range/adaptor/map.hpp>

#include "database.hh"
#include "db/view/view_builder.hh"
#include "sstables/compaction_manager.hh"

#include <seastar/testing/test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"

#include "db/config.hh"

using namespace std::literals::chrono_literals;

// This test checks various cases where a base table row disappears - or does
// not disappear - when its last column is deleted (with DELETE or by setting
// it to null). We want to confirm that the view row disappears - or does not
// disappear - accordingly. This reproduces
// https://issues.apache.org/jira/browse/CASSANDRA-14393
void test_partial_delete_unselected_column(cql_test_env& e, std::function<void()>&& maybe_flush) {
    e.execute_cql("create table cf (p int, c int, a int, b int, primary key (p, c))").get();
    e.execute_cql("create materialized view vcf as select p, c from cf "
                  "where p is not null and c is not null "
                  "primary key (p, c)").get();

    BOOST_TEST_PASSPOINT();
    e.execute_cql("update cf using timestamp 10 set b = 1 where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
    });

    BOOST_TEST_PASSPOINT();
    e.execute_cql("delete b from cf using timestamp 11 where p = 1 and c = 1").get();
    // Because above we used "update" to insert the b=1 cell, a so-called
    // row-marker is not added, and when we delete this cell, all trace of
    // this row disappears from the base table. Accordingly, it should
    // disappear from the view as well:
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().is_empty();
    });

    BOOST_TEST_PASSPOINT();
    e.execute_cql("update cf using timestamp 1 set a = 1 where p = 1 and c = 1").get();
    // Above we deleted only the "b" cell, not the entire row, so when we add
    // "a" with an earlier timestamp, it is not shadowed by the deletion, and
    // we have a row in the base table (and accordingly, in the view).
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
    });

    BOOST_TEST_PASSPOINT();
    e.execute_cql("update cf using timestamp 18 set a = 1 where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
    });

    // This tests the same thing as the "DELETE" test above (deleting the only
    // cell causes no trace of the row to remain, and the row disappears from
    // the view as well) - it's just that we delete the cell by setting it to
    // "null" instead of using the "DELETE" command. See also
    // https://issues.apache.org/jira/browse/CASSANDRA-11805.
    BOOST_TEST_PASSPOINT();
    e.execute_cql("update cf using timestamp 20 set a = null where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().is_empty();
    });

    BOOST_TEST_PASSPOINT();
    // We now insert a row to the base table. It's without values for the
    // non-key columns, but the row nevertheless exists (this is implemented
    // via a "row marker"). None of the updates we did above with higher
    // timestamps delete this row - only its individual cells. So the row now
    // exists in the base table, so should also exist in the view table.
    e.execute_cql("insert into cf (p, c) values (1, 1) using timestamp 15").get();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
    });
}

SEASTAR_TEST_CASE(test_partial_delete_unselected_column_without_flush) {
    return do_with_cql_env_thread([] (auto& e) {
        test_partial_delete_unselected_column(e, [] { });
    });
}

SEASTAR_TEST_CASE(test_partial_delete_unselected_column_with_flush) {
    auto cfg = make_shared<db::config>();
    cfg->enable_cache(false);
    return do_with_cql_env_thread([] (auto& e) {
        test_partial_delete_unselected_column(e, [&] {
            e.local_db().flush_all_memtables().get();
        });
    }, cfg);
}

void test_partial_delete_selected_column(cql_test_env& e, std::function<void()>&& maybe_flush) {
    e.execute_cql("create table cf (p int, c int, a int, b int, e int, f int, primary key (p, c))").get();
    e.execute_cql("create materialized view vcf as select a, b from cf "
                  "where p is not null and c is not null "
                  "primary key (p, c)").get();

    BOOST_TEST_PASSPOINT();
    e.execute_cql("update cf using timestamp 10 set b = 1 where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            { },
            {int32_type->decompose(1)}
        }});
    });

    BOOST_TEST_PASSPOINT();
    e.execute_cql("delete b from cf using timestamp 11 where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().is_empty();
    });

    BOOST_TEST_PASSPOINT();
    e.execute_cql("update cf using timestamp 1 set a = 1 where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            { }
        }});
    });

    BOOST_TEST_PASSPOINT();
    e.execute_cql("delete a from cf using timestamp 1 where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().is_empty();
    });

    BOOST_TEST_PASSPOINT();
    e.execute_cql("insert into cf (p, c) values (1, 1) using timestamp 0").get();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            { },
            { }
        }});
    });

    BOOST_TEST_PASSPOINT();
    e.execute_cql("update cf using timestamp 12 set b = 1 where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            { },
            {int32_type->decompose(1)}
        }});
    });

    BOOST_TEST_PASSPOINT();
    e.execute_cql("delete b from cf using timestamp 13 where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            { },
            { }
        }});
    });

    BOOST_TEST_PASSPOINT();
    e.execute_cql("delete from cf using timestamp 14 where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().is_empty();
    });

    BOOST_TEST_PASSPOINT();
    e.execute_cql("insert into cf (p, c) values (1, 1) using timestamp 15").get();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            { },
            { }
        }});
    });

    BOOST_TEST_PASSPOINT();
    e.execute_cql("update cf using timestamp 15 and ttl 100 set b = 1 where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            { },
            {int32_type->decompose(1)}
        }});
    });

    forward_jump_clocks(101s);

    BOOST_TEST_PASSPOINT();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            { },
            { }
        }});
    });

    BOOST_TEST_PASSPOINT();
    e.execute_cql("delete from cf using timestamp 15 where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().is_empty();
    });

    BOOST_TEST_PASSPOINT();
    // removal generated by unselected column should not shadow selected column with smaller timestamp
    e.execute_cql("update cf using timestamp 18 set e = 1 where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            { },
            { }
        }});
    });

    BOOST_TEST_PASSPOINT();
    e.execute_cql("update cf using timestamp 18 set e = null where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().is_empty();
    });

    BOOST_TEST_PASSPOINT();
    e.execute_cql("update cf using timestamp 16 set a = 1 where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            { }
        }});
    });
}

SEASTAR_TEST_CASE(test_partial_delete_selected_column_without_flush) {
    return do_with_cql_env_thread([] (auto& e) {
        test_partial_delete_selected_column(e, [] { });
    });
}

SEASTAR_TEST_CASE(test_partial_delete_selected_column_with_flush) {
    auto cfg = make_shared<db::config>();
    cfg->enable_cache(false);
    return do_with_cql_env_thread([] (auto& e) {
        test_partial_delete_selected_column(e, [&] {
            e.local_db().flush_all_memtables().get();
        });
    }, cfg);
}

void test_update_column_in_view_pk_with_ttl(cql_test_env& e, std::function<void()>&& maybe_flush) {
    e.execute_cql("create table cf (p int primary key, a int, b int)").get();
    e.execute_cql("create materialized view vcf as select * from cf "
                  "where p is not null and a is not null "
                  "primary key (a, p)").get();

    e.execute_cql("update cf set a = 1 where p = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            { },
        }});
    });

    e.execute_cql("delete a from cf where p = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().is_empty();
    });

    e.execute_cql("insert into cf (p) values (1)").get();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().is_empty();
    });

    e.execute_cql("update cf using ttl 100 set a = 10 where p = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(10)},
            {int32_type->decompose(1)},
            { },
        }});
    });

    e.execute_cql("update cf set b = 100 where p = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(10)},
            {int32_type->decompose(1)},
            {int32_type->decompose(100)}
        }});
    });

    forward_jump_clocks(101s);

    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().is_empty();
    });
}

SEASTAR_TEST_CASE(test_update_column_in_view_pk_with_ttl_without_flush) {
    return do_with_cql_env_thread([] (auto& e) {
        test_update_column_in_view_pk_with_ttl(e, [] { });
    });
}

SEASTAR_TEST_CASE(test_update_column_in_view_pk_with_ttl_with_flush) {
    auto cfg = make_shared<db::config>();
    cfg->enable_cache(false);
    return do_with_cql_env_thread([] (auto& e) {
        test_update_column_in_view_pk_with_ttl(e, [&] {
            e.local_db().flush_all_memtables().get();
        });
    }, cfg);
}

SEASTAR_TEST_CASE(test_unselected_column_can_preserve_ttld_row_maker) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c int, v int, primary key (p, c))").get();
        e.execute_cql("create materialized view vcf as select p, c from cf "
                      "where p is not null and c is not null "
                      "primary key (c, p)").get();

        e.execute_cql("insert into cf (p, c) values (0, 0) using ttl 100").get();
        e.execute_cql("update cf using ttl 0 set v = 0 where p = 0 and c = 0").get();
        forward_jump_clocks(101s);
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf").get0();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(0)}, {int32_type->decompose(0)}, }});
        });
    });
}

void test_update_column_not_in_view(cql_test_env& e, std::function<void()>&& maybe_flush) {
    e.execute_cql("create table cf (p int, c int, v1 int, v2 int, primary key (p, c))").get();
    e.execute_cql("create materialized view vcf as select p, c from cf "
                  "where p is not null and c is not null "
                  "primary key (c, p)").get();

    e.execute_cql("update cf using timestamp 0 set v1 = 1 where p = 0 and c = 0").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(0)},
            {int32_type->decompose(0)}
        }});
    });

    e.execute_cql("delete v1 from cf using timestamp 1 where p = 0 and c = 0").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().is_empty();
    });

    e.execute_cql("update cf using timestamp 1 set v1 = 1 where p = 0 and c = 0").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().is_empty();
    });

    e.execute_cql("update cf using timestamp 2 set v2 = 1 where p = 0 and c = 0").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(0)},
            {int32_type->decompose(0)}
        }});
    });

    e.execute_cql("delete v1 from cf using timestamp 3 where p = 0 and c = 0").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(0)},
            {int32_type->decompose(0)}
        }});
    });

    e.execute_cql("delete v2 from cf using timestamp 4 where p = 0 and c = 0").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().is_empty();
    });

    e.execute_cql("update cf using ttl 100 set v2 = 1 where p = 0 and c = 0").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(0)},
            {int32_type->decompose(0)}
        }});
    });

    forward_jump_clocks(101s);

    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().is_empty();
    });

    e.execute_cql("update cf set v2 = 1 where p = 0 and c = 0").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(0)},
            {int32_type->decompose(0)}
        }});
    });

    assert_that_failed(e.execute_cql("alter table cf drop v2;"));
}

SEASTAR_TEST_CASE(test_update_column_not_in_view_without_flush) {
    return do_with_cql_env_thread([] (auto& e) {
        test_update_column_not_in_view(e, [] { });
    });
}

SEASTAR_TEST_CASE(test_update_column_not_in_view_with_flush) {
    auto cfg = make_shared<db::config>();
    cfg->enable_cache(false);
    return do_with_cql_env_thread([] (auto& e) {
        test_update_column_not_in_view(e, [&] {
            e.local_db().flush_all_memtables().get();
        });
    }, cfg);
}

void test_partial_update_with_unselected_collections(cql_test_env& e, std::function<void()>&& maybe_flush) {
e.execute_cql("create table cf (p int, c int, a int, b int, l list<int>, s set<int>, m map<int,text>, primary key (p, c))").get();
    e.execute_cql("create materialized view vcf as select a, b from cf "
                  "where p is not null and c is not null "
                  "primary key (c, p)").get();

    e.execute_cql("update cf set l=l+[1,2,3] where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            { },
            { }
        }});
    });

    e.execute_cql("update cf set l=l-[1,2] where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            { },
            { }
        }});
    });

    e.execute_cql("update cf set b = 3 where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            { },
            {int32_type->decompose(3)}
        }});
    });

    e.execute_cql("update cf set b=null, l=l-[3], s=s-{3} where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().is_empty();
    });

    e.execute_cql("update cf set m=m+{3:'text'}, l=l-[1], s=s-{2} where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            { },
            { }
        }});
    });

    assert_that_failed(e.execute_cql("alter table cf drop m;"));
}

void test_partial_update_with_unselected_udt(cql_test_env& e, std::function<void()>&& maybe_flush) {
    e.execute_cql("create type ut (a int, b int)").get();
    e.execute_cql("create table cf (p int, c int, a int, b int, u ut, primary key (p, c))").get();
    e.execute_cql("create materialized view vcf as select a, b from cf "
                  "where p is not null and c is not null "
                  "primary key (c, p)").get();

    e.execute_cql("update cf set u.a = 1 where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            { },
            { }
        }});
    });

    e.execute_cql("update cf set b = 3 where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            { },
            {int32_type->decompose(3)}
        }});
    });

    e.execute_cql("update cf set b=null, u.a = null where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().is_empty();
    });

    e.execute_cql("update cf set u = (1, 1) where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            { },
            { }
        }});
    });

    assert_that_failed(e.execute_cql("alter table cf drop m;"));
}

SEASTAR_TEST_CASE(test_partial_update_with_unselected_udt_without_flush) {
    return do_with_cql_env_thread([] (auto& e) {
        test_partial_update_with_unselected_udt(e, [] { });
    });
}

SEASTAR_TEST_CASE(test_partial_update_with_unselected_udt_with_flush) {
    auto cfg = make_shared<db::config>();
    cfg->enable_cache(false);
    return do_with_cql_env_thread([] (auto& e) {
        test_partial_update_with_unselected_udt(e, [&] {
            e.local_db().flush_all_memtables().get();
        });
    }, cfg);
}

SEASTAR_TEST_CASE(test_partial_update_with_unselected_collections_without_flush) {
    return do_with_cql_env_thread([] (auto& e) {
        test_partial_update_with_unselected_collections(e, [] { });
    });
}

SEASTAR_TEST_CASE(test_partial_update_with_unselected_collections_with_flush) {
    auto cfg = make_shared<db::config>();
    cfg->enable_cache(false);
    return do_with_cql_env_thread([] (auto& e) {
        test_partial_update_with_unselected_collections(e, [&] {
            e.local_db().flush_all_memtables().get();
        });
    }, cfg);
}

void test_unselected_columns_ttl(cql_test_env& e, std::function<void()>&& maybe_flush) {
    e.execute_cql("create table cf (p int, c int, v int, primary key (p, c))").get();
    e.execute_cql("create materialized view vcf as select p, c from cf "
                  "where p is not null and c is not null "
                  "primary key (c, p)").get();

    e.execute_cql("insert into cf (p, c) values (1, 1) using ttl 100").get();
    e.execute_cql("update cf using ttl 1000 set v = 0 where p = 1 and c = 1").get();
    maybe_flush();

    forward_jump_clocks(101s);

    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)}
        }});
    });

    e.execute_cql("delete v from cf where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().is_empty();
    });

    e.execute_cql("insert into cf (p, c) values (1, 1)").get();
    e.execute_cql("update cf using ttl 100 set v = 0 where p = 1 and c = 1").get();
    e.execute_cql("insert into cf (p, c) values (3, 3) using ttl 100").get();

    forward_jump_clocks(101s);

    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)}
        }});
        msg = e.execute_cql("select * from vcf where p = 3 and c = 3").get0();
        assert_that(msg).is_rows().is_empty();
    });

    e.execute_cql("update cf set v = 0 where p = 3 and c = 3").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 3 and c = 3").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(3)},
            {int32_type->decompose(3)}
        }});
    });
}

SEASTAR_TEST_CASE(test_unselected_columns_ttl_without_flush) {
    return do_with_cql_env_thread([] (auto& e) {
        test_unselected_columns_ttl(e, [] { });
    });
}

SEASTAR_TEST_CASE(test_unselected_columns_ttl_with_flush) {
    auto cfg = make_shared<db::config>();
    cfg->enable_cache(false);
    return do_with_cql_env_thread([] (auto& e) {
        test_unselected_columns_ttl(e, [&] {
            e.local_db().flush_all_memtables().get();
        });
    }, cfg);
}

void test_partition_deletion(cql_test_env& e, std::function<void()>&& maybe_flush) {
    e.execute_cql("create table cf (p int, a int, b int, c int, primary key (p))").get();
    e.execute_cql("create materialized view vcf as select * from cf "
                  "where p is not null and a is not null "
                  "primary key (p, a)").get();

    e.execute_cql("insert into cf (p, a, b, c) values (1, 1, 1, 1) using timestamp 0").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            {int32_type->decompose(1)}
        }});
    });

    e.execute_cql("update cf using timestamp 1 set a = null where p = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().is_empty();
    });

    e.execute_cql("delete from cf using timestamp 2 where p = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().is_empty();
    });

    e.execute_cql("update cf using timestamp 3 set a = 1, b = 1 where p = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            { }
        }});
    });
}

SEASTAR_TEST_CASE(test_partition_deletion_without_flush) {
    return do_with_cql_env_thread([] (auto& e) {
        test_partition_deletion(e, [] { });
    });
}

SEASTAR_TEST_CASE(test_partition_deletion_with_flush) {
    auto cfg = make_shared<db::config>();
    cfg->enable_cache(false);
    return do_with_cql_env_thread([] (auto& e) {
        test_partition_deletion(e, [&] {
            e.local_db().flush_all_memtables().get();
        });
    }, cfg);
}

void test_commutative_row_deletion(cql_test_env& e, std::function<void()>&& maybe_flush) {
    e.execute_cql("create table cf (p int, v1 int, v2 int, primary key (p))").get();
    e.execute_cql("create materialized view vcf as select * from cf "
                  "where p is not null and v1 is not null "
                  "primary key (v1, p)").get();

    e.execute_cql("insert into cf (p, v1, v2) values (3, 1, 3) using timestamp 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select v2, writetime(v2) from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(3)},
            {long_type->decompose(1L)}
        }});
    });

    e.execute_cql("delete from cf using timestamp 2 where p = 3").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select v2, writetime(v2) from vcf").get0();
        assert_that(msg).is_rows().is_empty();
    });

    e.execute_cql("insert into cf (p, v1) values (3, 1) using timestamp 3").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select v1, p, v2, writetime(v2) from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(3)},
            { },
            { }
        }});
    });

    e.execute_cql("update cf using timestamp 4 set v1 = 2 where p = 3").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select v1, p, v2, writetime(v2) from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(2)},
            {int32_type->decompose(3)},
            { },
            { }
        }});
    });

    e.execute_cql("update cf using timestamp 5 set v1 = 1 where p = 3").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select v1, p, v2, writetime(v2) from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(3)},
            { },
            { }
        }});
    });

    e.local_db().get_compaction_manager().submit_major_compaction(&e.local_db().find_column_family("ks", "vcf")).get();
}

SEASTAR_TEST_CASE(test_commutative_row_deletion_without_flush) {
    return do_with_cql_env_thread([] (auto& e) {
        test_commutative_row_deletion(e, [] { });
    });
}

SEASTAR_TEST_CASE(test_commutative_row_deletion_with_flush) {
    auto cfg = make_shared<db::config>();
    cfg->enable_cache(false);
    return do_with_cql_env_thread([] (auto& e) {
        test_commutative_row_deletion(e, [&] {
            e.local_db().flush_all_memtables().get();
        });
    }, cfg);
}

SEASTAR_TEST_CASE(test_unselected_column_with_expired_marker) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c int, a int, b int, primary key (p, c))").get();
        e.execute_cql("create materialized view vcf as select p, c, b from cf "
                      "where p is not null and c is not null "
                      "primary key (c, p)").get();

        e.execute_cql("update cf set a = 1 where p = 1 and c = 1").get();
        e.local_db().flush_all_memtables().get();
        e.execute_cql("insert into cf (p, c) values (1, 1) using ttl 100").get();
        e.local_db().flush_all_memtables().get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf").get0();
            assert_that(msg).is_rows().with_rows({{
                {int32_type->decompose(1)},
                {int32_type->decompose(1)},
                { }
            }});
        });

        forward_jump_clocks(101s);

        eventually([&] {
            auto msg = e.execute_cql("select * from vcf").get0();
            assert_that(msg).is_rows().with_rows({{
                {int32_type->decompose(1)},
                {int32_type->decompose(1)},
                { }
            }});
        });

        e.execute_cql("update cf set a = null where p = 1 and c = 1").get();
        e.local_db().flush_all_memtables().get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf").get0();
            assert_that(msg).is_rows().is_empty();
        });

        e.execute_cql("update cf using timestamp 1 set b = 1 where p = 1 and c = 1").get();
        e.local_db().flush_all_memtables().get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf").get0();
            assert_that(msg).is_rows().with_rows({{
                {int32_type->decompose(1)},
                {int32_type->decompose(1)},
                {int32_type->decompose(1)}
            }});
        });

    });
}

void test_update_with_column_timestamp_smaller_than_pk(cql_test_env& e, std::function<void()>&& maybe_flush) {
    e.execute_cql("create table cf (p int, v1 int, v2 int, primary key (p))").get();
    e.execute_cql("create materialized view vcf as select * from cf "
                  "where p is not null and v1 is not null "
                  "primary key (v1, p)").get();

    e.execute_cql("insert into cf (p, v1, v2) values (3, 1, 3) using timestamp 6").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select v1, p, v2, writetime(v2) from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(3)},
            {int32_type->decompose(3)},
            {long_type->decompose(6L)}
        }});
    });

    e.execute_cql("insert into cf (p) values (3) using timestamp 20").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select v1, p, v2, writetime(v2) from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(3)},
            {int32_type->decompose(3)},
            {long_type->decompose(6L)}
        }});
    });

    e.execute_cql("update cf using timestamp 7 set v1 = 2 where p = 3").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select v1, p, v2, writetime(v2) from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(2)},
            {int32_type->decompose(3)},
            {int32_type->decompose(3)},
            {long_type->decompose(6L)}
        }});
    });

    e.execute_cql("update cf using timestamp 8 set v1 = 1 where p = 3").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select v1, p, v2, writetime(v2) from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(3)},
            {int32_type->decompose(3)},
            {long_type->decompose(6L)}
        }});
    });
}

SEASTAR_TEST_CASE(test_update_with_column_timestamp_smaller_than_pk_without_flush) {
    return do_with_cql_env_thread([] (auto& e) {
        test_update_with_column_timestamp_smaller_than_pk(e, [] { });
    });
}

SEASTAR_TEST_CASE(test_update_with_column_timestamp_smaller_than_pk_with_flush) {
    auto cfg = make_shared<db::config>();
    cfg->enable_cache(false);
    return do_with_cql_env_thread([] (auto& e) {
        test_update_with_column_timestamp_smaller_than_pk(e, [&] {
            e.local_db().flush_all_memtables().get();
        });
    }, cfg);
}

void test_expired_marker_with_limit(cql_test_env& e, std::function<void()>&& maybe_flush) {
    e.execute_cql("create table cf (p int, a int, b int, primary key (p))").get();
    e.execute_cql("create materialized view vcf1 as select * from cf "
                  "where p is not null and a is not null "
                  "primary key (p, a)").get();
    e.execute_cql("create materialized view vcf2 as select * from cf "
                  "where p is not null and a is not null "
                  "primary key (a, p)").get();

    for (int i = 1; i <= 100; i++) {
        e.execute_cql(format("insert into cf (p, a, b) values ({:d}, {:d}, {:d})", i, i, i)).get();
    }
    for (int i = 1; i <= 100; i++) {
        if (i % 50 != 0) {
            e.execute_cql(format("delete a from cf where p = {:d}", i)).get();
        }
    }

    maybe_flush();

    for (auto view : {"vcf1", "vcf2"}) {
        eventually([&] {
            auto msg = e.execute_cql(format("select * from {} limit 1", view)).get0();
            assert_that(msg).is_rows().with_size(1);
            msg = e.execute_cql(format("select * from {} limit 2", view)).get0();
            assert_that(msg).is_rows().with_size(2);
            msg = e.execute_cql(format("select * from {}", view)).get0();
            assert_that(msg).is_rows().with_rows({
                {{int32_type->decompose(50)}, {int32_type->decompose(50)}, {int32_type->decompose(50)}},
                {{int32_type->decompose(100)}, {int32_type->decompose(100)}, {int32_type->decompose(100)}},
            });

        });
    }
}

SEASTAR_TEST_CASE(test_expired_marker_with_limit_without_flush) {
    return do_with_cql_env_thread([] (auto& e) {
        test_expired_marker_with_limit(e, [] { });
    });
}

SEASTAR_TEST_CASE(test_expired_marker_with_limit_with_flush) {
    auto cfg = make_shared<db::config>();
    cfg->enable_cache(false);
    return do_with_cql_env_thread([] (auto& e) {
        test_expired_marker_with_limit(e, [&] {
            e.local_db().flush_all_memtables().get();
        });
    }, cfg);
}

void test_update_with_column_timestamp_bigger_than_pk(cql_test_env& e, std::function<void()>&& maybe_flush) {
    e.execute_cql("create table cf (p int, a int, b int, primary key (p))").get();
    e.execute_cql("create materialized view vcf as select * from cf "
                  "where p is not null and a is not null "
                  "primary key (p, a)").get();

    e.execute_cql("delete from cf using timestamp 0 where p = 1").get();
    maybe_flush();

    e.execute_cql("insert into cf (p, a, b) values (1, 1, 1) using timestamp 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            {int32_type->decompose(1)}
        }});
    });

    e.execute_cql("update cf using timestamp 10 set b = 2 where p = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            {int32_type->decompose(2)}
        }});
    });

    e.execute_cql("update cf using timestamp 2 set a = 2 where p = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(2)},
            {int32_type->decompose(2)}
        }});
    });

    e.local_db().get_compaction_manager().submit_major_compaction(&e.local_db().find_column_family("ks", "vcf")).get();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf limit 1").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(2)},
            {int32_type->decompose(2)}
        }});
    });

    e.execute_cql("update cf using timestamp 11 set a = 1 where p = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf limit 1").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            {int32_type->decompose(2)}
        }});
    });

    e.execute_cql("update cf using timestamp 12 set a = null where p = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf limit 1").get0();
        assert_that(msg).is_rows().is_empty();
    });

    e.execute_cql("update cf using timestamp 13 set a = 1 where p = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf limit 1").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            {int32_type->decompose(2)}
        }});
    });
}

SEASTAR_TEST_CASE(test_update_with_column_timestamp_bigger_than_pk_without_flush) {
    return do_with_cql_env_thread([] (auto& e) {
        test_update_with_column_timestamp_bigger_than_pk(e, [] { });
    });
}

SEASTAR_TEST_CASE(test_update_with_column_timestamp_bigger_than_pk_with_flush) {
    auto cfg = make_shared<db::config>();
    cfg->enable_cache(false);
    return do_with_cql_env_thread([] (auto& e) {
        test_update_with_column_timestamp_bigger_than_pk(e, [&] {
            e.local_db().flush_all_memtables().get();
        });
    }, cfg);
}

void test_no_regular_base_column_in_view_pk(cql_test_env& e, std::function<void()>&& maybe_flush) {
    e.execute_cql("create table cf (p int, c int, v1 int, v2 int, primary key (p, c))").get();
    e.execute_cql("create materialized view vcf as select * from cf "
                  "where p is not null and c is not null "
                  "primary key (c, p)").get();

    e.execute_cql("update cf using timestamp 1 set v1 = 1 where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            { }
        }});
    });

    e.execute_cql("update cf using timestamp 2 set v1 = null, v2 = 1 where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            { },
            {int32_type->decompose(1)}
        }});
    });

    e.execute_cql("update cf using timestamp 2 set v2 = null where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().is_empty();
    });

    e.execute_cql("insert into cf (p, c) values (1, 1) using timestamp 3").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            { },
            { }
        }});
    });

    e.execute_cql("delete from cf using timestamp 4 where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().is_empty();
    });

    e.execute_cql("update cf using timestamp 5 set v2 = 1 where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            { },
            {int32_type->decompose(1)}
        }});
    });
}

SEASTAR_TEST_CASE(test_no_regular_base_column_in_view_pk_without_flush) {
    return do_with_cql_env_thread([] (auto& e) {
        test_no_regular_base_column_in_view_pk(e, [] { });
    });
}

SEASTAR_TEST_CASE(test_no_regular_base_column_in_view_pk_with_flush) {
    auto cfg = make_shared<db::config>();
    cfg->enable_cache(false);
    return do_with_cql_env_thread([] (auto& e) {
        test_no_regular_base_column_in_view_pk(e, [&] {
            e.local_db().flush_all_memtables().get();
        });
    }, cfg);
}

SEASTAR_TEST_CASE(test_shadowing_row_marker) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, v1 int, v2 int, primary key (p))").get();
        e.execute_cql("create materialized view vcf as select * from cf "
                      "where p is not null and v1 is not null "
                      "primary key (v1, p)").get();

        e.execute_cql("insert into cf (p, v1, v2) values (1, 1, 1)").get();

        e.execute_cql("update cf set v1 = null where p = 1").get();
        e.local_db().flush_all_memtables().get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf").get0();
            assert_that(msg).is_rows().is_empty();
        });

        e.execute_cql("update cf using ttl 100 set v1 = 1 where p = 1").get();
        e.local_db().flush_all_memtables().get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf").get0();
            assert_that(msg).is_rows().with_rows({{
                {int32_type->decompose(1)},
                {int32_type->decompose(1)},
                {int32_type->decompose(1)}
            }});
        });

        forward_jump_clocks(101s);

        eventually([&] {
            auto msg = e.execute_cql("select * from vcf").get0();
            assert_that(msg).is_rows().is_empty();
        });
    });
}

void test_marker_timestamp_is_not_shadowed_by_previous_update(cql_test_env& e, std::function<void()>&& maybe_flush) {
    e.execute_cql("create table cf (p int, c int, v1 int, v2 int, primary key (p, c))").get();
    e.execute_cql("create materialized view vcf as select p, c, v1 from cf "
                  "where p is not null and c is not null "
                  "primary key (c, p)").get();

    e.execute_cql("insert into cf (p, c, v1, v2) VALUES(1, 1, 1, 1) using ttl 100").get();
    maybe_flush();
    e.execute_cql("update cf using ttl 1000 set v2 = 1 where p = 1 and c = 1").get();
    maybe_flush();
    e.execute_cql("delete v2 from cf where p = 1 and c = 1").get();
    maybe_flush();
    forward_jump_clocks(101s);
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().is_empty();
    });
}

SEASTAR_TEST_CASE(test_marker_timestamp_is_not_shadowed_by_previous_update_without_flush) {
    return do_with_cql_env_thread([] (auto& e) {
        test_marker_timestamp_is_not_shadowed_by_previous_update(e, [] { });
    });
}

SEASTAR_TEST_CASE(test_marker_timestamp_is_not_shadowed_by_previous_updatewith_flush) {
    auto cfg = make_shared<db::config>();
    cfg->enable_cache(false);
    return do_with_cql_env_thread([] (auto& e) {
        test_marker_timestamp_is_not_shadowed_by_previous_update(e, [&] {
            e.local_db().flush_all_memtables().get();
        });
    }, cfg);
}

// A reproducer for issue #3362, not involving TTLs.
// The test involves a view that selects no column except the base's primary
// key, so view rows contain no cells besides a row marker, so as a base
// row appears and disappears as we update and delete individual cells in
// that row, we need to insert and delete the row marker with varying
// timestamps to make sure the view row appears and disappears as needed.
// But as we shall see, after enough trickery, we run out of timestamps
// to use to revive the row marker, and fail to revive it. So to fix
// issue #3362, we needed to remember all cells separately ("virtual
// cells").
SEASTAR_TEST_CASE(test_3362_no_ttls) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c int, a int, b int, primary key (p, c))").get();
        e.execute_cql("create materialized view vcf as select p, c from cf "
                      "where p is not null and c is not null "
                      "primary key (p, c)").get();

        // In row p=1 c=1, insert two cells - b=1 at timestamp 10, a=1 at timestamp 20:
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf using timestamp 10 set b = 1 where p = 1 and c = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });

        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf using timestamp 20 set a = 1 where p = 1 and c = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });


        // Delete just a=1 (with timestamp 21). The base row will still exist (with b=1),
        // and accordingly the view row too:
        BOOST_TEST_PASSPOINT();
        e.execute_cql("delete a from cf using timestamp 21 where p = 1 and c = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });

        // At this point, we still have the base row with b=1 at timestamp 10
        // (and a=1 was deleted at timestamp 21). If we delete the b=1 at
        // timestamp 11, nothing will remain in the base row, and the view
        // row should disappear as well:
        BOOST_TEST_PASSPOINT();
        e.execute_cql("delete b from cf using timestamp 11 where p = 1 and c = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
            assert_that(msg).is_rows().is_empty();
        });

        // Now we finally reproduce #3362: We now add b=1 again, at timestamp
        // 12 (it was earlier deleted in timestamp 11). The base row is live
        // again, and so should the view row.
        // With issue #3362, the view row failed to become alive. The reason
        // is that to make the above is_empty() succeed, the implementation
        // deletes the row marker with timestamp 21 (the maximal timestamp
        // seen in the row). But now, we add a row marker again with the same
        // timestamp 21, but the deletion wins so the row marker is still
        // missing. (note that had data won over deletions, the is_empty()
        // test above would have failed instead).
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf using timestamp 12 set b = 1 where p = 1 and c = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
    });
}

// This is another reproducer for issue #3362, using TTLs instead of
// numerous back-and-forth additions and deletions.
SEASTAR_TEST_CASE(test_3362_with_ttls) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c int, a int, b int, primary key (p, c))").get();
        e.execute_cql("create materialized view vcf as select p, c from cf "
                      "where p is not null and c is not null "
                      "primary key (p, c)").get();

        // In row p=1 c=1, insert two cells - a=1 with ttl, and b=1 without
        // ttl. The ttl'ed cell is inserted first, with a newer timestamp.
        // The problem is that the view row's marker gets, with a new
        // timestamp, a ttl. Then, when we go to add another column with an
        // older timestamp, and try to set the row marker without a
        // ttl - the older timestamp of this update looses, and we wrongly
        // remain with a ttl on the view row marker.
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf using timestamp 2 and ttl 100 set a = 1 where p = 1 and c = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf using timestamp 1 set b = 1 where p = 1 and c = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
        // Pass the time 101 seconds forward. Cell 'a' will have expired, but
        // cell 'b' will still exist, so the base row still exists and the
        // corresponding view row should also exist too.
        forward_jump_clocks(101s);
        BOOST_TEST_PASSPOINT();
        // verify that the base row still exists (cell b didn't expire)
        eventually([&] {
            auto msg = e.execute_cql("select * from cf where p = 1 and c = 1").get0();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)}, {}, {{int32_type->decompose(1)}} }});
        });
        BOOST_TEST_PASSPOINT();
        // verify that the view row still exists too.
        // This check failing is issue #3362.
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
    });
}

// ‎‎‎The following are more test for issue #3362, same as test_3362_not_ttls
// and test_3362_with_ttls, just with a collection with items "1" and "2"
// instead of separate columns a and b. For brevity, comments were removed,
// so refer to the comments in the original code above.
enum class collection_kind { set, list, map };
void do_test_3362_no_ttls_with_collections(cql_test_env& e, collection_kind t) {
    sstring type, pref, suf;
    switch (t) {
    case collection_kind::set:
        type = "set<int>";
        pref = "{";
        suf = "}";
        break;
    case collection_kind::list:
        type = "list<int>";
        pref = "[";
        suf = "]";
        break;
    case collection_kind::map:
        type = "map<int, int>";
        pref = "{";
        suf = " : 17}";
        break;
    }
    e.execute_cql(format("create table cf (p int, c int, a {}, primary key (p, c))", type)).get();
    e.execute_cql("create materialized view vcf as select p, c from cf "
            "where p is not null and c is not null "
            "primary key (p, c)").get();
    e.execute_cql(format("update cf using timestamp 10 set a = a + {}2{} where p = 1 and c = 1", pref, suf)).get();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
    });
    e.execute_cql(format("update cf using timestamp 20 set a = a + {}1{} where p = 1 and c = 1", pref, suf)).get();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
    });
    if (t == collection_kind::map) {
        e.execute_cql("delete a[1] from cf using timestamp 21 where p = 1 and c = 1").get();
    } else {
        e.execute_cql(format("update cf using timestamp 21 set a = a - {}1{} where p = 1 and c = 1", pref, suf)).get();
    }
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
    });
    if (t == collection_kind::map) {
        e.execute_cql("delete a[2] from cf using timestamp 11 where p = 1 and c = 1").get();
    } else {
        e.execute_cql(format("update cf using timestamp 11 set a = a - {}2{} where p = 1 and c = 1", pref, suf)).get();
    }
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().is_empty();
    });
    e.execute_cql(format("update cf using timestamp 12 set a = a + {}2{} where p = 1 and c = 1", pref, suf)).get();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
    });
}
SEASTAR_TEST_CASE(test_3362_no_ttls_with_set) {
    return do_with_cql_env_thread([] (auto& e) {
        do_test_3362_no_ttls_with_collections(e, collection_kind::set);
    });
}
SEASTAR_TEST_CASE(test_3362_no_ttls_with_list) {
    return do_with_cql_env_thread([] (auto& e) {
        do_test_3362_no_ttls_with_collections(e, collection_kind::list);
    });
}
SEASTAR_TEST_CASE(test_3362_no_ttls_with_map) {
    return do_with_cql_env_thread([] (auto& e) {
        do_test_3362_no_ttls_with_collections(e, collection_kind::map);
    });
}

void do_test_3362_with_ttls_with_collections(cql_test_env& e, collection_kind t) {
    sstring type, pref, suf;
    switch (t) {
    case collection_kind::set:
        type = "set<int>";
        pref = "{";
        suf = "}";
        break;
    case collection_kind::list:
        type = "list<int>";
        pref = "[";
        suf = "]";
        break;
    case collection_kind::map:
        type = "map<int, int>";
        pref = "{";
        suf = " : 17}";
        break;
    }
    e.execute_cql(format("create table cf (p int, c int, a {}, primary key (p, c))", type)).get();
    e.execute_cql("create materialized view vcf as select p, c from cf "
            "where p is not null and c is not null "
            "primary key (p, c)").get();
    e.execute_cql(format("update cf using timestamp 2 and ttl 100 set a = a + {}1{} where p = 1 and c = 1", pref, suf)).get();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
    });
    e.execute_cql(format("update cf using timestamp 1 set a = a + {}2{} where p = 1 and c = 1", pref, suf)).get();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
    });
    forward_jump_clocks(101s);
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
    });
}
SEASTAR_TEST_CASE(test_3362_with_ttls_with_set) {
    return do_with_cql_env_thread([] (auto& e) {
        do_test_3362_with_ttls_with_collections(e, collection_kind::set);
    });
}
SEASTAR_TEST_CASE(test_3362_with_ttls_with_list) {
    return do_with_cql_env_thread([] (auto& e) {
        do_test_3362_with_ttls_with_collections(e, collection_kind::list);
    });
}
SEASTAR_TEST_CASE(test_3362_with_ttls_with_map) {
    return do_with_cql_env_thread([] (auto& e) {
        do_test_3362_with_ttls_with_collections(e, collection_kind::map);
    });
}

// This is a version of test_3362_with_ttls with frozen collection fields
// instead of integer fields in test_3362_with_ttls. The intention is to
// verify that we properly fixed #3362 in this case - by replacing the
// frozen collection by a single virtual cell, not a collection.
SEASTAR_TEST_CASE(test_3362_with_ttls_frozen) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c int, a frozen<set<int>>, b frozen<set<int>>, primary key (p, c))").get();
        e.execute_cql("create materialized view vcf as select p, c from cf "
                      "where p is not null and c is not null "
                      "primary key (p, c)").get();
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf using timestamp 2 and ttl 100 set a = {1,2} where p = 1 and c = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf using timestamp 1 set b = {3,4} where p = 1 and c = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
        forward_jump_clocks(101s);
        BOOST_TEST_PASSPOINT();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
    });
}

// This is a version of test_3362_with_ttls with the added twist that the
// unselected column involved did not exist when the base table and view
// were originally created, but only added later with an "alter table".
// For this test to work, "alter table" will need to add the virtual
// columns in the view table for the newly created unselected column in
// the base table.
SEASTAR_TEST_CASE(test_3362_with_ttls_alter_add) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c int, primary key (p, c))").get();
        e.execute_cql("create materialized view vcf as select p, c from cf "
                      "where p is not null and c is not null "
                      "primary key (p, c)").get();
        // Add with "alter table" two additional columns to the base table -
        // a and b. These are not selected in the materialized view, and we
        // want to check that they are treated like unselected columns
        // (namely, virtual columns are added to the view).
        e.execute_cql("alter table cf add a int").get();
        e.execute_cql("alter table cf add b int").get();
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf using timestamp 2 and ttl 100 set a = 1 where p = 1 and c = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf using timestamp 1 set b = 1 where p = 1 and c = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
        forward_jump_clocks(101s);
        BOOST_TEST_PASSPOINT();
        eventually([&] {
            auto msg = e.execute_cql("select * from cf where p = 1 and c = 1").get0();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)}, {}, {{int32_type->decompose(1)}} }});
        });
        BOOST_TEST_PASSPOINT();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
    });
}

// test_3362_with_ttls_alter_add() above is about handling changes to virtual
// columns as the base table columns change, but only for the "add" case.
// Theoretically we could have had problems in the "drop" and "rename" cases
// as well, but today, those are not supported:
// 1. Today we do not allow "alter table drop" to drop any column from a base
//    table with views - even unselected columns.
//    If we every do allow this, we need to also check that we drop the
//    virtual column from the view.
// 2. Today we do not allow "alter table rename" to rename any non-pk
//    column, so unselected columns also cannot be renamed. If this
//    limitation is ever lifted, we will need to check that if we
//    rename an unselected base column, the virtual column in the view is
//    also renamed.


// Tests that after the fixes for issue #3362, various miscellaneous
// combinations of appearance and disappearance of unselected base cells
// and row markers which happen to cause view_updates::do_delete_old_entry()
// (i.e., deletion of the view row), work as expected.
SEASTAR_TEST_CASE(test_3362_row_deletion_1) {
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c int, a int, b int, primary key (p, c))").get();
        e.execute_cql("create materialized view vcf as select p, c from cf "
                      "where p is not null and c is not null "
                      "primary key (p, c)").get();
        // In row p=1 c=1:
        //  1. Insert a cell a=1, at timestamp 2
        //  2. Delete the cell a=1, at timestamp 10. The base row is now gone
        //     and so should the view row, and do_delete_old_entry() is called.
        //  3. Insert a full row for p=1 c=1 at timestamp 1. This is an
        //     "insert" so it also inserts a row marker. We already have
        //     a newer (ts=10) deletion of the cell a, but cell b is still
        //     alive and so is the row marker.
        //  4. Delete cell b at timestamp 3. Now both cells are dead, but
        //     the row should still alive and the view row should still exist.
        BOOST_TEST_PASSPOINT();
        // step 1:
        e.execute_cql("update cf using timestamp 2 set a = 1 where p = 1 and c = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
        BOOST_TEST_PASSPOINT();
        // step 2:
        e.execute_cql("delete a from cf using timestamp 10 where p = 1 and c = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
            assert_that(msg).is_rows().is_empty();
        });
        // step 3:
        BOOST_TEST_PASSPOINT();
        e.execute_cql("insert into cf (p, c) values (1, 1) using timestamp 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
        BOOST_TEST_PASSPOINT();
        // step 4:
        e.execute_cql("delete b from cf using timestamp 3 where p = 1 and c = 1").get();
        // the base row should now be empty but still exist (there's still the row marker)
        auto msg = e.execute_cql("select * from cf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)}, {}, {} }});
        // FIXME: Testing this is hard - we want to check that the already
        // existing view row does NOT disappear, so "eventually()" doesn't
        // help. We don't know how much we need wait before we can safely
        // conclude that the view updates will never cause this row to disappear.
        // I think we need a testing-only feature to be able to wait until the
        // backlog of view updates is fully consumed.
        seastar::sleep(std::chrono::seconds(1)).get();
        msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        // The row should still exists, because the row marker is still
        // alive. It was a bug that the row marker was deleted too,
        // because of a wrong row marker deletion set for timestamp 10.
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
    });
}

SEASTAR_TEST_CASE(test_3362_row_deletion_2) {
    // Verify that do_delete_old_entry()'s r.apply(update.tomb()) works as expected
    return do_with_cql_env_thread([] (auto& e) {
        e.execute_cql("create table cf (p int, c int, a int, b int, primary key (p, c))").get();
        e.execute_cql("create materialized view vcf as select p, c from cf "
                      "where p is not null and c is not null "
                      "primary key (p, c)").get();
        // In row p=1 c=1, insert two cells - b=1 at timestamp 1, a=1 at timestamp 2.
        // We use "update", not "insert", so there will not be a row marker.
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf using timestamp 1 set b = 1 where p = 1 and c = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf using timestamp 2 set a = 1 where p = 1 and c = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
        // Delete the entire base row, with timestamp 10. The view row should
        // also disappear.
        BOOST_TEST_PASSPOINT();
        e.execute_cql("delete from cf using timestamp 10 where p = 1 and c = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
            assert_that(msg).is_rows().is_empty();
        });
        // Reinsert an (unselected) cell in row p=1 c=1 at timestamp 3.
        // This is *before* the timestamp of the row's deletion (which was 10)
        // so the row should NOT reappear. For this to work, it is important
        // that view_updates::do_delete_old_entry() call r.apply(update.tomb()).
        BOOST_TEST_PASSPOINT();
        e.execute_cql("update cf using timestamp 3 set b = 1 where p = 1 and c = 1").get();
        // FIXME: Testing this is tough - since we expect no new row to appear
        // "eventually()" doesn't help. So can we know how much to wait before
        // deciding that as expected, no row was added???
        seastar::sleep(std::chrono::seconds(2)).get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
            assert_that(msg).is_rows().is_empty();
        });
        BOOST_TEST_PASSPOINT();
        // If we reinsert the cell at timestamp 11, after the deletion, the base
        // row will re-emerge, and so should the view row
        e.execute_cql("update cf using timestamp 11 set b = 1 where p = 1 and c = 1").get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
            assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
        });
    });
}
