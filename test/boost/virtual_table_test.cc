/*
 * Copyright (C) 2021-present ScyllaDB
 *
 * Modified by ScyllaDB
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
#include "test/lib/test_services.hh"

#include "db/virtual_table.hh"
#include "db/system_keyspace.hh"

namespace db {

class test_table : public virtual_table {
public:
    test_table() : virtual_table(build_schema()) {}

    static schema_ptr build_schema() {
        auto id = generate_legacy_id(system_keyspace::NAME, "test");
        return schema_builder(system_keyspace::NAME, "test", std::make_optional(id))
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("v", int32_type)
            .with_version(system_keyspace::generate_schema_version(id))
            .build();
    }

    mutation_source as_mutation_source() override {
        throw std::runtime_error("Not implemented");
    }

    void test_set_cell() {
        mutation m(_s, partition_key::from_single_value(*_s, data_value(666).serialize_nonnull()));
        row& cr = m.partition().clustered_row(*_s, clustering_key::from_single_value(*_s, data_value(10).serialize_nonnull())).cells();

        set_cell(cr, "v", 8);

        auto result_cell = cr.cell_at(0).as_atomic_cell(column_definition("v", int32_type, column_kind::regular_column));
        auto result = result_cell.serialize();

        BOOST_REQUIRE(result[result.size() - 1] == 8);

        BOOST_CHECK_THROW(set_cell(cr, "nonexistent_column", 20), std::runtime_error);
    }
};

}

SEASTAR_TEST_CASE(test_set_cell) {
    auto table = db::test_table();
    table.test_set_cell();

    return make_ready_future<>();
}