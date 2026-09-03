/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "mutation/canonical_mutation.hh"
#include "mutation/mutation.hh"
#include "schema/schema_builder.hh"
#include "utils/UUID_gen.hh"
#include <seastar/core/app-template.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/reactor.hh>
#include <seastar/testing/linux_perf_event.hh>

// Builds a partition with rows [first_row, first_row + rows), all columns set.
static mutation make_mutation(schema_ptr s, const partition_key& key, int first_row, int rows,
        const std::vector<sstring>& cnames, const bytes& value, api::timestamp_type ts) {
    mutation m(s, key);
    for (int i = 0; i < rows; i++) {
        auto ck = clustering_key::from_exploded(*s, {int32_type->decompose(first_row + i)});
        deletable_row& dr = m.partition().clustered_row(*s, ck);
        dr.apply(row_marker(ts));
        for (const sstring& cname : cnames) {
            const column_definition& col = *s->get_column_definition(to_bytes(cname));
            dr.cells().apply(col, atomic_cell::make_live(*col.type, ts, value));
        }
    }
    return m;
}

// Prints the average duration, retired instruction count and allocation count of a
// single call of @op. The instruction and allocation counts are the more telling of
// the three, as they do not depend on whatever else the machine happens to be running.
template <typename Op>
static void time_op(const char* name, size_t iterations, Op op) {
    using clk = std::chrono::steady_clock;
    clk::duration total{};
    uint64_t allocations = 0;
    uint64_t instructions = 0;
    auto instructions_counter = linux_perf_event::user_instructions_retired();
    instructions_counter.enable();
    for (size_t i = 0; i < iterations; i++) {
        auto allocations_before = memory::stats().mallocs();
        auto instructions_before = instructions_counter.read();
        auto start = clk::now();
        op();
        auto end = clk::now();
        instructions += instructions_counter.read() - instructions_before;
        total += end - start;
        allocations += memory::stats().mallocs() - allocations_before;
    }
    instructions_counter.disable();
    fmt::print("{:<46}{:>8.1f} us {:>12.0f} instr {:>9.0f} allocs\n", name,
            std::chrono::duration<double, std::micro>(total).count() / iterations,
            double(instructions) / iterations, double(allocations) / iterations);
}

// Builds a schema with @column_count blob columns, and a second version of the same
// table which can represent everything the first can, so that a mutation of the one
// has to be converted to be deserialized with the other. Both are versions of one
// table, as to_mutation() refuses to deserialize a mutation of another one.
static std::pair<schema_ptr, schema_ptr> make_schemas(size_t column_count, std::vector<sstring>& cnames) {
    auto make_builder = [id = table_id(utils::UUID_gen::get_time_UUID())] {
        return schema_builder(this_smp_shard_count(), "ks", "cf", id)
            .with_column("p1", utf8_type, column_kind::partition_key)
            .with_column("c1", int32_type, column_kind::clustering_key);
    };
    auto builder = make_builder();
    for (size_t i = 0; i < column_count; i++) {
        cnames.push_back(fmt::format("b{}", i + 1));
        builder.with_column(to_bytes(cnames.back()), bytes_type);
    }
    auto other_version_builder = make_builder();
    for (const sstring& cname : cnames) {
        other_version_builder.with_column(to_bytes(cname), bytes_type);
    }
    return {builder.build(), other_version_builder.with_column("b_extra", bytes_type).build()};
}

// Times deserialization of a canonical_mutation, which is what group0 command
// application and merging, topology coordinator operations, tablet metadata change
// hints and validation, batchlog replay and schema mutation merging all do for every
// mutation they handle. The source of the same schema version is the common case, the
// one of another version is measured to show what the conversion costs on top.
static void time_canonical_mutation_deserialization(int rows, size_t column_count, size_t value_size,
        size_t iterations) {
    std::vector<sstring> cnames;
    auto [s, s2] = make_schemas(column_count, cnames);

    fmt::print("\nTiming deserialization of a canonical_mutation of {} rows x {} columns x {} bytes...\n",
            rows, column_count, value_size);

    const api::timestamp_type ts = 1;
    const bytes value(bytes::initialized_later(), value_size);
    auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
    const canonical_mutation cm(make_mutation(s, key, 0, rows, cnames, value, ts));
    const canonical_mutation cm_other_version(make_mutation(s2, key, 0, rows, cnames, value, ts));

    time_op("to_mutation", iterations, [&, s = s] { auto m = cm.to_mutation(s); });
    time_op("to_mutation, other schema version", iterations,
            [&, s = s] { auto m = cm_other_version.to_mutation(s); });
}

int main(int argc, char* argv[]) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("column-count", bpo::value<size_t>()->default_value(1), "column count")
        ("rows", bpo::value<int>()->default_value(1000), "rows per partition")
        ("value-size", bpo::value<size_t>()->default_value(64), "cell value size")
        ("iterations", bpo::value<size_t>()->default_value(300), "number of iterations to average over");
    return app.run_deprecated(argc, argv, [&] {
        time_canonical_mutation_deserialization(app.configuration()["rows"].as<int>(),
                app.configuration()["column-count"].as<size_t>(),
                app.configuration()["value-size"].as<size_t>(),
                app.configuration()["iterations"].as<size_t>());
        engine().exit(0);
    });
}
