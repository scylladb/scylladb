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

    future<> execute(std::function<void(mutation)> mutation_sink) override {
        return with_timeout(db::no_timeout, do_for_each(_mutations, [mutation_sink = std::move(mutation_sink)] (const mutation& m) { mutation_sink(m); }));
    }
};

class streaming_test_vt : public db::streaming_virtual_table {
    schema_ptr _s;
    std::vector<mutation> _mutations;
public:
    streaming_test_vt(schema_ptr s, std::vector<mutation> mutations)
            : streaming_virtual_table(s)
            , _s(s)
            , _mutations(std::move(mutations)) {}

    virtual future<> execute(reader_permit permit, db::result_collector& rc) override {
        return async([this, permit, &rc] {
            auto mt = make_lw_shared<memtable>(_s);
            do_for_each(_mutations, [mt] (const mutation& m) {
                mt->apply(m);
            }).get();
            auto rdr = mt->make_flat_reader(_s, permit);
            auto close_rdr = deferred_close(rdr);
            rdr.consume_pausable([&rc] (mutation_fragment mf) {
                return rc.take(std::move(mf)).then([] { return stop_iteration::no; });
            }).get();
        });
    }
};

SEASTAR_THREAD_TEST_CASE(test_memtable_filling_vt_as_mutation_source) {
    std::unique_ptr<memtable_filling_test_vt> table; // Used to prolong table's life

    run_mutation_source_tests([&table] (schema_ptr s, const std::vector<mutation>& mutations, gc_clock::time_point) -> mutation_source {
        table = std::make_unique<memtable_filling_test_vt>(s, mutations);
        return table->as_mutation_source();
    });
}

SEASTAR_THREAD_TEST_CASE(test_streaming_vt_as_mutation_source) {
    std::unique_ptr<streaming_test_vt> table; // Used to prolong table's life

    run_mutation_source_tests([&table] (schema_ptr s, const std::vector<mutation>& mutations, gc_clock::time_point) -> mutation_source {
        table = std::make_unique<streaming_test_vt>(s, mutations);
        return mutation_source([ms = table->as_mutation_source()] (schema_ptr s,
                reader_permit permit,
                const dht::partition_range& pr,
                const query::partition_slice& slice,
                const io_priority_class& pc,
                tracing::trace_state_ptr trace_state,
                streamed_mutation::forwarding stream_fwd,
                mutation_reader::forwarding) {
            return ms.make_reader(s, permit, pr, slice, pc, trace_state, stream_fwd, mutation_reader::forwarding::no);
        });
    }, nullptr, false /* with_partition_range_forwarding */);
}
