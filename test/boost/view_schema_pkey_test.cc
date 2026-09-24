/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */


#include <boost/test/unit_test.hpp>

#include "db/view/view_builder.hh"

#include "test/lib/eventually.hh"
#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"

BOOST_AUTO_TEST_SUITE(view_schema_pkey_test)

using namespace std::literals::chrono_literals;

SEASTAR_TEST_CASE(test_base_non_pk_columns_in_view_partition_key_are_non_emtpy) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p1 int, p2 text, c text, v text, primary key ((p1, p2), c))").get();
        e.execute_cql("insert into cf (p1, p2, c, v) values (1, '', '', '')").get();

        size_t id = 0;
        auto make_view_name = [&id] { return format("vcf_{:d}", id++); };

        auto views_matching = {
            "create materialized view {} as select * from cf "
            "where p1 is not null and p2 is not null and c is not null and v is not null "
            "primary key (p1, v, c, p2)",

            "create materialized view {} as select * from cf "
            "where p1 is not null and p2 is not null and c is not null and v is not null "
            "primary key ((p2, v), c, p1)",

            "create materialized view {} as select * from cf "
            "where p1 is not null and p2 is not null and c is not null and v is not null "
            "primary key ((v, p2), c, p1)",

            "create materialized view {} as select * from cf "
            "where p1 is not null and p2 is not null and c is not null and v is not null "
            "primary key ((c, v), p1, p2)",

            "create materialized view {} as select * from cf "
            "where p1 is not null and p2 is not null and c is not null and v is not null "
            "primary key ((v, c), p1, p2)"
        };
        for (auto&& view : views_matching) {
            auto name = make_view_name();
            auto f = e.local_view_builder().wait_until_built("ks", name);
            e.execute_cql(fmt::format(fmt::runtime(view), name)).get();
            f.get();
            auto msg = e.execute_cql(format("select p1, p2, c, v from {}", name)).get();
            assert_that(msg).is_rows()
                    .with_size(1)
                    .with_row({
                            {int32_type->decompose(1)},
                            {utf8_type->decompose(data_value(""))},
                            {utf8_type->decompose(data_value(""))},
                            {utf8_type->decompose(data_value(""))},
                    });
        }
    });
}

BOOST_AUTO_TEST_SUITE_END()
