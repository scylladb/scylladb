/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/testing/perf_tests.hh>

#include "test/lib/simple_schema.hh"
#include "test/perf/perf.hh"

#include "mutation/frozen_mutation.hh"
#include "mutation/mutation_partition_view.hh"

namespace tests {

class frozen_mutation {
    simple_schema _schema;
    perf::reader_concurrency_semaphore_wrapper _semaphore;

    mutation _one_small_row;
    ::frozen_mutation _frozen_one_small_row;
public:
    frozen_mutation()
        : _semaphore(__FILE__)
        , _one_small_row(_schema.schema(), _schema.make_pkey(0))
        , _frozen_one_small_row(_one_small_row)
    {
        _one_small_row.apply(_schema.make_row(_semaphore.make_permit(), _schema.make_ckey(0), "value"));
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
    auto m = mutation(schema(), frozen_one_small_row().key());
    mutation_application_stats app_stats;
    m.partition().apply(*schema(), frozen_one_small_row().partition(), *schema(), app_stats);
    perf_tests::do_not_optimize(m);
}

}
