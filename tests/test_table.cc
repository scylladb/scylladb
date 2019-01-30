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

#include "tests/test_table.hh"

std::pair<schema_ptr, std::vector<dht::decorated_key>> create_test_cf(cql_test_env& env, unsigned partition_count, unsigned row_per_partition_count) {
    env.execute_cql("CREATE KEYSPACE multishard_mutation_query_cache_ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};").get();
    env.execute_cql("CREATE TABLE multishard_mutation_query_cache_ks.test (pk int, ck int, v int, PRIMARY KEY(pk, ck));").get();

    const auto insert_id = env.prepare("INSERT INTO multishard_mutation_query_cache_ks.test (\"pk\", \"ck\", \"v\") VALUES (?, ?, ?);").get0();

    auto s = env.local_db().find_column_family("multishard_mutation_query_cache_ks", "test").schema();

    std::vector<dht::decorated_key> pkeys;

    for (int pk = 0; pk < int(partition_count); ++pk) {
        pkeys.emplace_back(dht::global_partitioner().decorate_key(*s, partition_key::from_single_value(*s, data_value(pk).serialize())));
        for (int ck = 0; ck < int(row_per_partition_count); ++ck) {
            env.execute_prepared(insert_id, {{
                    cql3::raw_value::make_value(data_value(pk).serialize()),
                    cql3::raw_value::make_value(data_value(ck).serialize()),
                    cql3::raw_value::make_value(data_value(pk ^ ck).serialize())}}).get();
        }
    }

    return std::pair(std::move(s), std::move(pkeys));
}
