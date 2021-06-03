/*
 * Copyright (C) 2021-present ScyllaDB
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

#include "db/virtual_table.hh"
#include "db/system_keyspace.hh"
#include "schema.hh"

#include "test/lib/mutation_source_test.hh"

#include <seastar/testing/thread_test_case.hh>

class memtable_filling_test_vt : public db::memtable_filling_virtual_table {
    std::vector<mutation> _mutations;
public:
    memtable_filling_test_vt(schema_ptr s, std::vector<mutation> mutations)
            : memtable_filling_virtual_table(s)
            , _mutations(std::move(mutations)) {}

    future<> execute(std::function<void(mutation)> mutation_sink, db::timeout_clock::time_point timeout) override {
        return with_timeout(timeout, do_for_each(_mutations, [mutation_sink = std::move(mutation_sink)] (const mutation& m) { mutation_sink(m); }));
    }
};

SEASTAR_THREAD_TEST_CASE(test_memtable_filling_vt_as_mutation_source) {
    std::unique_ptr<memtable_filling_test_vt> table; // Used to prolong table's life

    run_mutation_source_tests([&table] (schema_ptr s, const std::vector<mutation>& mutations, gc_clock::time_point) -> mutation_source {
        table = std::make_unique<memtable_filling_test_vt>(s, mutations);
        return table->as_mutation_source();
    });
}
