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

namespace test {

std::pair<schema_ptr, std::vector<dht::decorated_key>> create_test_table(cql_test_env& env, const sstring& ks_name, const sstring& table_name,
        unsigned partition_count, unsigned row_per_partition_count) {
    env.execute_cql(sprint("CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}};", ks_name)).get();
    env.execute_cql(sprint("CREATE TABLE {}.{} (pk int, ck int, v int, PRIMARY KEY(pk, ck));", ks_name, table_name)).get();

    const auto insert_id = env.prepare(sprint("INSERT INTO {}.{} (\"pk\", \"ck\", \"v\") VALUES (?, ?, ?);")).get0();

    auto s = env.local_db().find_column_family(ks_name, table_name).schema();

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

} // namespace test
