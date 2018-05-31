
/*
 * Copyright (C) 2015 ScyllaDB
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

#include "database.hh"
#include "perf.hh"
#include <seastar/core/app-template.hh>

static atomic_cell make_atomic_cell(data_type dt, bytes value) {
    return atomic_cell::make_live(*dt, 0, value);
};

int main(int argc, char* argv[]) {
    return app_template().run_deprecated(argc, argv, [] {
        auto s = make_lw_shared(schema({}, "ks", "cf",
            {{"p1", utf8_type}}, {{"c1", int32_type}}, {{"r1", int32_type}}, {}, utf8_type));

        memtable mt(s);

        std::cout << "Timing mutation of single column within one row...\n";

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(2)});
        bytes value = int32_type->decompose(3);

        time_it([&] {
            mutation m(s, key);
            const column_definition& col = *s->get_column_definition("r1");
            m.set_clustered_cell(c_key, col, make_atomic_cell(col.type, value));
            mt.apply(std::move(m));
        });
        engine().exit(0);
    });
}
