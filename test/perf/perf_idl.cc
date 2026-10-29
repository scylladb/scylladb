/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/testing/perf_tests.hh>

#include "utils/assert.hh"
#include "test/lib/simple_schema.hh"
#include "test/perf/perf.hh"

#include "mutation/frozen_mutation.hh"
#include "mutation/mutation_partition_view.hh"
#include "serializer.hh"
#include "serializer_impl.hh"
#include "idl/frozen_mutation.dist.hh"
#include "idl/frozen_mutation.dist.impl.hh"

namespace tests {

class frozen_mutation {
    simple_schema _schema;
    perf::reader_concurrency_semaphore_wrapper _semaphore;

    mutation _one_small_row;
    ::frozen_mutation _frozen_one_small_row;

    mutation _multi_kb;
    ::frozen_mutation _frozen_multi_kb;
    bytes_ostream _serialized_multi_kb;
public:
    frozen_mutation()
        : _semaphore(__FILE__)
        , _one_small_row(_schema.schema(), _schema.make_pkey(0))
        , _frozen_one_small_row(_one_small_row)
        , _multi_kb(_schema.schema(), _schema.make_pkey(1))
        , _frozen_multi_kb(_multi_kb)
    {
        _one_small_row.apply(_schema.make_row(_semaphore.make_permit(), _schema.make_ckey(0), "value"));
        _frozen_one_small_row = freeze(_one_small_row);

        auto value = sstring(200, 'x');
        for (int i = 0; i < 20; i++) {
            _multi_kb.apply(_schema.make_row(_semaphore.make_permit(), _schema.make_ckey(i), value));
        }
        _frozen_multi_kb = freeze(_multi_kb);
        auto frozen_size = _frozen_multi_kb.representation().size();
        throwing_assert(frozen_size > 2048 && frozen_size < bytes_ostream::max_chunk_size());
        ser::serialize(_serialized_multi_kb, _frozen_multi_kb);
    }
    schema_ptr schema() const { return _schema.schema(); }

    const mutation& one_small_row() const { return _one_small_row; }
    const ::frozen_mutation& frozen_one_small_row() const { return _frozen_one_small_row; }

    const mutation& multi_kb() const { return _multi_kb; }
    const ::frozen_mutation& frozen_multi_kb() const { return _frozen_multi_kb; }
    const bytes_ostream& serialized_multi_kb() const { return _serialized_multi_kb; }
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

PERF_TEST_F(frozen_mutation, freeze_multi_kb)
{
    auto frozen = freeze(multi_kb());
    perf_tests::do_not_optimize(frozen);
}

PERF_TEST_F(frozen_mutation, unfreeze_multi_kb)
{
    auto m = frozen_multi_kb().unfreeze(schema());
    perf_tests::do_not_optimize(m);
}

// The RPC receive path: deserialize a frozen_mutation from its wire form.
PERF_TEST_F(frozen_mutation, deserialize_multi_kb)
{
    auto in = ser::as_input_stream(serialized_multi_kb());
    auto frozen = ser::deserialize(in, std::type_identity<::frozen_mutation>());
    perf_tests::do_not_optimize(frozen);
}

class bytes_ostream_equality {
    static std::pair<bytes_ostream, bytes_ostream> make(size_t size) {
        bytes data(size, 0);
        for (size_t i = 0; i < size; ++i) {
            data[i] = int8_t(i * 7);
        }
        // Writer-built (default 512-byte doubling chunks) vs one reserved chunk.
        bytes_ostream chunked;
        for (size_t off = 0; off < size; off += 64) {
            chunked.write(bytes_view(data).substr(off, 64));
        }
        bytes_ostream single;
        single.reserve(size);
        single.write(data);
        throwing_assert(chunked == single);
        return {std::move(chunked), std::move(single)};
    }
public:
    std::pair<bytes_ostream, bytes_ostream> small = make(128);
    std::pair<bytes_ostream, bytes_ostream> large = make(64 * 1024);
};

PERF_TEST_F(bytes_ostream_equality, small)
{
    perf_tests::do_not_optimize(small.first == small.second);
}

PERF_TEST_F(bytes_ostream_equality, large)
{
    perf_tests::do_not_optimize(large.first == large.second);
}

}
