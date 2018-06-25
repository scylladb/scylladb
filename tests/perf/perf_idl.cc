/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *
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

#include <seastar/tests/perf/perf_tests.hh>

#include "tests/simple_schema.hh"

#include "frozen_mutation.hh"

namespace tests {

class frozen_mutation {
    simple_schema _schema;

    mutation _one_small_row;
    ::frozen_mutation _frozen_one_small_row;
public:
    frozen_mutation()
        : _one_small_row(_schema.schema(), _schema.make_pkey(0))
        , _frozen_one_small_row(_one_small_row)
    {
        _one_small_row.apply(_schema.make_row(_schema.make_ckey(0), "value"));
        _frozen_one_small_row = freeze(_one_small_row);
    }

    schema_ptr schema() const { return _schema.schema(); }

    const mutation& one_small_row() const { return _one_small_row; }
    const ::frozen_mutation& frozen_one_small_row() const { return _frozen_one_small_row; }
};

PERF_TEST_F(frozen_mutation, freeze_one_small_row)
{
    auto frozen = freeze(one_small_row());
    perf_tests::do_not_optimize(frozen);
}

PERF_TEST_F(frozen_mutation, unfreeze_one_small_row)
{
    auto m = frozen_one_small_row().unfreeze(schema());
    perf_tests::do_not_optimize(m);
}

PERF_TEST_F(frozen_mutation, apply_one_small_row)
{
    auto m = mutation(schema(), frozen_one_small_row().key(*schema()));
    m.partition().apply(*schema(), frozen_one_small_row().partition(), *schema());
    perf_tests::do_not_optimize(m);
}

}