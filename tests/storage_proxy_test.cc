/*
 * Copyright 2015 Cloudius Systems
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

#define BOOST_TEST_DYN_LINK

#include <seastar/core/thread.hh>
#include <seastar/tests/test-utils.hh>

#include "tests/cql_test_env.hh"
#include "tests/mutation_source_test.hh"
#include "tests/result_set_assertions.hh"
#include "service/storage_proxy.hh"
#include "partition_slice_builder.hh"

static query::result to_data_query_result(mutation_reader& reader, const query::partition_slice& slice) {
    query::result::builder builder(slice);
    auto now = gc_clock::now();
    while (true) {
        mutation_opt mo = reader().get0();
        if (!mo) {
            break;
        }
        auto pb = builder.add_partition(*mo->schema(), mo->key());
        mo->partition().query(pb, *mo->schema(), now);
    }
    return builder.build();
}

static query::result_set to_result_set(schema_ptr s, mutation_reader& reader) {
    auto slice = partition_slice_builder(*s).build();
    return query::result_set::from_raw_result(s, slice, to_data_query_result(reader, slice));
}

SEASTAR_TEST_CASE(test_make_local_reader) {
    return do_with_cql_env([](cql_test_env& e) {
        return seastar::async([&] {
            e.execute_cql("create keyspace ks2 with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").get();
            e.execute_cql("create table ks2.cf (k blob, v int, primary key (k));").get();
            e.execute_cql(
                "begin unlogged batch \n"
                    "  insert into ks2.cf (k, v) values (0x01, 0); \n"
                    "  insert into ks2.cf (k, v) values (0x02, 0); \n"
                    "  insert into ks2.cf (k, v) values (0x03, 0); \n"
                    "  insert into ks2.cf (k, v) values (0x04, 0); \n"
                    "  insert into ks2.cf (k, v) values (0x05, 0); \n"
                    "apply batch;").get();

            auto s = e.local_db().find_schema("ks2", "cf");

            {
                auto reader = service::get_storage_proxy().local().make_local_reader(s->id(), query::full_partition_range);
                assert_that(to_result_set(s, reader))
                    .has_size(5)
                    .has(a_row().with_column(bytes("k"), data_value(bytes("\01"))))
                    .has(a_row().with_column(bytes("k"), data_value(bytes("\02"))))
                    .has(a_row().with_column(bytes("k"), data_value(bytes("\03"))))
                    .has(a_row().with_column(bytes("k"), data_value(bytes("\04"))))
                    .has(a_row().with_column(bytes("k"), data_value(bytes("\05"))));
            }

            {
                auto reader = service::get_storage_proxy().local().make_local_reader(s->id(),
                    query::partition_range(
                        {dht::ring_position(dht::minimum_token(), dht::ring_position::token_bound::start)},
                        {dht::ring_position(dht::maximum_token(), dht::ring_position::token_bound::end)}));
                assert_that(to_result_set(s, reader))
                    .has_size(5)
                    .has(a_row().with_column(bytes("k"), data_value(bytes("\01"))))
                    .has(a_row().with_column(bytes("k"), data_value(bytes("\02"))))
                    .has(a_row().with_column(bytes("k"), data_value(bytes("\03"))))
                    .has(a_row().with_column(bytes("k"), data_value(bytes("\04"))))
                    .has(a_row().with_column(bytes("k"), data_value(bytes("\05"))));
            }

            {
                auto reader = service::get_storage_proxy().local().make_local_reader(s->id(),
                    query::partition_range(
                        {dht::ring_position(dht::minimum_token(), dht::ring_position::token_bound::start)},
                        {dht::ring_position(dht::minimum_token(), dht::ring_position::token_bound::start)}));
                assert_that(to_result_set(s, reader)).is_empty();
            }

            {
                auto reader = service::get_storage_proxy().local().make_local_reader(s->id(),
                    query::partition_range(
                        {dht::ring_position(dht::maximum_token(), dht::ring_position::token_bound::start)},
                        {dht::ring_position(dht::maximum_token(), dht::ring_position::token_bound::start)}));
                assert_that(to_result_set(s, reader)).is_empty();
            }
        });
    });
}
