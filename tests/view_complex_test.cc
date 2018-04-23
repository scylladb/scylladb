/*
 * Copyright (C) 2018 ScyllaDB
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

#include "tests/test-utils.hh"
#include "tests/cql_test_env.hh"
#include "tests/cql_assertions.hh"

#include "db/config.hh"

using namespace std::literals::chrono_literals;

// Requires on #3362
#if 0
void test_partial_delete_unselected_column(cql_test_env& e, std::function<void()>&& maybe_flush) {
    e.execute_cql("create table cf (p int, c int, a int, b int, primary key (p, c))").get();
    e.execute_cql("create materialized view vcf as select p, c from cf "
                  "where p is not null and c is not null "
                  "primary key (p, c)").get();

    e.execute_cql("update cf using timestamp 10 set b = 1 where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
    });

    e.execute_cql("delete b from cf using timestamp 11 where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().is_empty();
    });

    e.execute_cql("update cf using timestamp 1 set a = 1 where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
    });

    e.execute_cql("update cf using timestamp 18 set a = 1 where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().with_rows({{ {int32_type->decompose(1)}, {int32_type->decompose(1)} }});
    });

    e.execute_cql("update cf using timestamp 20 set a = null where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().is_empty();
    });

    e.execute_cql("insert into cf (p, c) values (1, 1) using timestamp 15").get();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().is_empty();
    });
}

SEASTAR_TEST_CASE(test_partial_delete_unselected_column_without_flush) {
    return do_with_cql_env_thread([] (auto& e) {
        test_partial_delete_unselected_column(e, [] { });
    });
}

SEASTAR_TEST_CASE(test_partial_delete_unselected_column_with_flush) {
    db::config cfg;
    cfg.enable_cache(false);
    return do_with_cql_env_thread([] (auto& e) {
        test_partial_delete_unselected_column(e, [&] {
            e.local_db().flush_all_memtables().get();
        });
    }, cfg);
}
#endif

void test_partial_delete_selected_column(cql_test_env& e, std::function<void()>&& maybe_flush) {
    e.execute_cql("create table cf (p int, c int, a int, b int, e int, f int, primary key (p, c))").get();
    e.execute_cql("create materialized view vcf as select a, b from cf "
                  "where p is not null and c is not null "
                  "primary key (p, c)").get();

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

    e.execute_cql("delete b from cf using timestamp 11 where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().is_empty();
    });

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

    e.execute_cql("delete a from cf using timestamp 1 where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().is_empty();
    });

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

    e.execute_cql("delete from cf using timestamp 14 where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().is_empty();
    });

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

    e.execute_cql("update cf using ttl 3 set b = 1 where p = 1 and c = 1").get();
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

    forward_jump_clocks(4s);

    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(1)},
            {int32_type->decompose(1)},
            { },
            { }
        }});
    });

    e.execute_cql("delete from cf using timestamp 15 where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().is_empty();
    });

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

    e.execute_cql("update cf using timestamp 18 set e = null where p = 1 and c = 1").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf where p = 1 and c = 1").get0();
        assert_that(msg).is_rows().is_empty();
    });

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
    db::config cfg;
    cfg.enable_cache(false);
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

    e.execute_cql("update cf using ttl 5 set a = 10 where p = 1").get();
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

    forward_jump_clocks(6s);

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
    db::config cfg;
    cfg.enable_cache(false);
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

        e.execute_cql("insert into cf (p, c) values (0, 0) using ttl 60").get();
        e.execute_cql("update cf using ttl 0 set v = 0 where p = 0 and c = 0").get();
        forward_jump_clocks(65s);
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

    e.execute_cql("update cf using ttl 3 set v2 = 1 where p = 0 and c = 0").get();
    maybe_flush();
    eventually([&] {
        auto msg = e.execute_cql("select * from vcf").get0();
        assert_that(msg).is_rows().with_rows({{
            {int32_type->decompose(0)},
            {int32_type->decompose(0)}
        }});
    });

    forward_jump_clocks(3s);

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
    db::config cfg;
    cfg.enable_cache(false);
    return do_with_cql_env_thread([] (auto& e) {
        test_update_column_not_in_view(e, [&] {
            e.local_db().flush_all_memtables().get();
        });
    }, cfg);
}

void test_partial_update_with_unselected_collections(cql_test_env& e, std::function<void()>&& maybe_flush) {
e.execute_cql("create table cf (p int, c int, a int, b int, l list<int>, s set<int>, m map<int,int>, primary key (p, c))").get();
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

    e.execute_cql("update cf set m=m+{3:3}, l=l-[1], s=s-{2} where p = 1 and c = 1").get();
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

SEASTAR_TEST_CASE(test_partial_update_with_unselected_collections_without_flush) {
    return do_with_cql_env_thread([] (auto& e) {
        test_partial_update_with_unselected_collections(e, [] { });
    });
}

SEASTAR_TEST_CASE(test_partial_update_with_unselected_collections_with_flush) {
    db::config cfg;
    cfg.enable_cache(false);
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

    e.execute_cql("insert into cf (p, c) values (1, 1) using ttl 3").get();
    e.execute_cql("update cf using ttl 1000 set v = 0 where p = 1 and c = 1").get();
    maybe_flush();

    forward_jump_clocks(4s);

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
    e.execute_cql("update cf using ttl 3 set v = 0 where p = 1 and c = 1").get();
    e.execute_cql("insert into cf (p, c) values (3, 3) using ttl 3").get();

    forward_jump_clocks(4s);

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
    db::config cfg;
    cfg.enable_cache(false);
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
    db::config cfg;
    cfg.enable_cache(false);
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
    db::config cfg;
    cfg.enable_cache(false);
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
        e.execute_cql("insert into cf (p, c) values (1, 1) using ttl 5").get();
        e.local_db().flush_all_memtables().get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf").get0();
            assert_that(msg).is_rows().with_rows({{
                {int32_type->decompose(1)},
                {int32_type->decompose(1)},
                { }
            }});
        });

        forward_jump_clocks(6s);

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
    db::config cfg;
    cfg.enable_cache(false);
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
        e.execute_cql(sprint("insert into cf (p, a, b) values (%d, %d, %d)", i, i, i)).get();
    }
    for (int i = 1; i <= 100; i++) {
        if (i % 50 != 0) {
            e.execute_cql(sprint("delete a from cf where p = %d", i)).get();
        }
    }

    maybe_flush();

    for (auto view : {"vcf1", "vcf2"}) {
        eventually([&] {
            auto msg = e.execute_cql(sprint("select * from %s limit 1", view)).get0();
            assert_that(msg).is_rows().with_size(1);
            msg = e.execute_cql(sprint("select * from %s limit 2", view)).get0();
            assert_that(msg).is_rows().with_size(2);
            msg = e.execute_cql(sprint("select * from %s", view)).get0();
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
    db::config cfg;
    cfg.enable_cache(false);
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
    db::config cfg;
    cfg.enable_cache(false);
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
    db::config cfg;
    cfg.enable_cache(false);
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

        e.execute_cql("update cf using ttl 5 set v1 = 1 where p = 1").get();
        e.local_db().flush_all_memtables().get();
        eventually([&] {
            auto msg = e.execute_cql("select * from vcf").get0();
            assert_that(msg).is_rows().with_rows({{
                {int32_type->decompose(1)},
                {int32_type->decompose(1)},
                {int32_type->decompose(1)}
            }});
        });

        forward_jump_clocks(6s);

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

    e.execute_cql("insert into cf (p, c, v1, v2) VALUES(1, 1, 1, 1) using ttl 5").get();
    maybe_flush();
    e.execute_cql("update cf using ttl 1000 set v2 = 1 where p = 1 and c = 1").get();
    maybe_flush();
    e.execute_cql("delete v2 from cf where p = 1 and c = 1").get();
    maybe_flush();
    forward_jump_clocks(6s);
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
    db::config cfg;
    cfg.enable_cache(false);
    return do_with_cql_env_thread([] (auto& e) {
        test_marker_timestamp_is_not_shadowed_by_previous_update(e, [&] {
            e.local_db().flush_all_memtables().get();
        });
    }, cfg);
}
