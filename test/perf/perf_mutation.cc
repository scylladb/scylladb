
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "replica/database.hh"
#include "schema/schema_builder.hh"
#include "test/perf/perf.hh"
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>

#include <seastar/testing/linux_perf_event.hh>
#include <stdexcept>

static atomic_cell make_atomic_cell(data_type dt, bytes value) {
    return atomic_cell::make_live(*dt, 0, value);
};

int main(int argc, char* argv[]) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("column-count", bpo::value<size_t>()->default_value(1), "column count")
        ("rows", bpo::value<size_t>()->default_value(1),
            "number of distinct clustering keys to cycle through, round-robin. "
            "Note this varies the size of the memtable's rows b-tree, not the "
            "per-row cell tree; use --column-count for the latter. Default 1.")
        ("sequential-columns", bpo::bool_switch()->default_value(false),
            "instead of the default (repeatedly time_it-timed overwrites of "
            "random existing columns, i.e. the row cell tree's *hit* path), "
            "run a single pass inserting each of --column-count columns into "
            "one fixed row exactly once, in order. Every insert is into a "
            "column that does not yet exist, so this isolates the cell tree's "
            "*miss* (insert) path with no steady-state overwrite phase and no "
            "growth of the memtable's (unrelated) rows b-tree. Ignores --rows.")
        ("merge-mode", bpo::bool_switch()->default_value(false),
            "instead of applying a mutation into a memtable (which always binds "
            "memtable::apply(const mutation&), copying regardless of value "
            "category), merge two freshly-built same-schema mutations via "
            "mutation::apply(mutation&&) -- the rvalue partition-apply path. "
            "Ignores --rows and --sequential-columns.")
        ("merge-rows", bpo::value<size_t>()->default_value(8),
            "rows per mutation in --merge-mode.")
        ("merge-interleave", bpo::bool_switch()->default_value(false),
            "single-pass variant of --merge-mode: target gets --merge-rows even "
            "clustering keys, source gets --merge-rows odd keys interleaved "
            "between them, merged once. Every row is a miss, exercising the "
            "row b-tree's lookup-then-insert path instead of the (identical-keys) "
            "overwrite/hit path --merge-mode alone produces.")
        ("merge-gap", bpo::value<size_t>()->default_value(1),
            "in --merge-interleave, target has --merge-rows dense keys and source "
            "has --merge-rows/gap keys spaced `gap` apart -- each source miss then "
            "skips gap-1 target rows. gap 1 is the default (adjacent) case; a "
            "larger gap tests the lower_bound fallback.");
    return app.run_deprecated(argc, argv, [&] {
        size_t column_count = app.configuration()["column-count"].as<size_t>();
        bool sequential_columns = app.configuration()["sequential-columns"].as<bool>();
        bool merge_mode = app.configuration()["merge-mode"].as<bool>();
        bool merge_interleave = app.configuration()["merge-interleave"].as<bool>();
        size_t rows = app.configuration()["rows"].as<size_t>();
        size_t merge_rows = app.configuration()["merge-rows"].as<size_t>();
        if (column_count == 0) {
            throw std::invalid_argument("--column-count must be greater than zero");
        }
        if (!sequential_columns && !merge_mode && !merge_interleave && rows == 0) {
            throw std::invalid_argument("--rows must be greater than zero");
        }
        if ((merge_mode || merge_interleave) && merge_rows == 0) {
            throw std::invalid_argument("--merge-rows must be greater than zero");
        }
        size_t merge_gap = app.configuration()["merge-gap"].as<size_t>();
        if (merge_interleave && (merge_gap == 0 || merge_gap > merge_rows)) {
            throw std::invalid_argument("--merge-gap must be between 1 and --merge-rows");
        }
        auto builder = schema_builder(this_smp_shard_count(), "ks", "cf")
            .with_column("p1", utf8_type, column_kind::partition_key)
            .with_column("c1", int32_type, column_kind::clustering_key);

        std::vector<sstring> cnames;
        for (size_t i = 0; i < column_count; i++) {
            cnames.push_back(fmt::format("r{}", i + 1));
            builder.with_column(to_bytes(cnames.back()), int32_type);
        }

        auto s = builder.build();
        replica::memtable mt(s);

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        bytes value = int32_type->decompose(3);

        size_t total_ops = 0;
        auto instructions_retired_counter = linux_perf_event::user_instructions_retired();
        auto cpu_cycles_retired_counter = linux_perf_event::user_cpu_cycles_retired();

        if (merge_interleave) {
            size_t gap = merge_gap;
            std::cout << format("Merging {} dense target rows vs {} source rows spaced {} apart (all misses)...\n",
                    merge_rows, merge_rows / gap, gap);
            auto col = *s->get_column_definition(to_bytes(cnames[0]));
            mutation target(s, key);
            for (size_t r = 0; r < merge_rows; r++) {
                auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(int32_t(2 * r))});
                target.set_clustered_cell(c_key, col, make_atomic_cell(col.type, value));
            }
            mutation src(s, key);
            for (size_t r = 0; r < merge_rows / gap; r++) {
                auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(int32_t(2 * gap * r + 1))});
                src.set_clustered_cell(c_key, col, make_atomic_cell(col.type, value));
            }
            instructions_retired_counter.enable();
            cpu_cycles_retired_counter.enable();
            target.apply(std::move(src));
            total_ops = merge_rows / gap;
        } else if (merge_mode) {
            std::cout << format("Merging two freshly-built mutations ({} row(s) x {} column(s) each) "
                    "via mutation::apply(mutation&&)...\n", merge_rows, column_count);
            std::vector<clustering_key> c_keys;
            c_keys.reserve(merge_rows);
            for (size_t i = 0; i < merge_rows; i++) {
                c_keys.push_back(clustering_key::from_exploded(*s, {int32_type->decompose(int32_t(i))}));
            }
            auto build_mutation = [&] {
                mutation m(s, key);
                for (size_t r = 0; r < merge_rows; r++) {
                    for (size_t c = 0; c < column_count; c++) {
                        const column_definition& col = *s->get_column_definition(to_bytes(cnames[c]));
                        m.set_clustered_cell(c_keys[r], col, make_atomic_cell(col.type, value));
                    }
                }
                return m;
            };
            instructions_retired_counter.enable();
            cpu_cycles_retired_counter.enable();
            time_it([&] {
                mutation target = build_mutation();
                mutation src = build_mutation();
                target.apply(std::move(src));
                total_ops++;
            });
        } else if (sequential_columns) {
            std::cout << format("Inserting {} distinct columns into one row, once each (miss path)...\n", column_count);
            auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(2)});
            instructions_retired_counter.enable();
            cpu_cycles_retired_counter.enable();
            for (size_t i = 0; i < column_count; i++) {
                mutation m(s, key);
                const column_definition& col = *s->get_column_definition(to_bytes(cnames[i]));
                m.set_clustered_cell(c_key, col, make_atomic_cell(col.type, value));
                mt.apply(std::move(m));
                total_ops++;
            }
        } else {
            std::cout << format("Timing mutation of a single column, cycling through {} row(s)...\n", rows);

            std::vector<clustering_key> c_keys;
            c_keys.reserve(rows);
            for (size_t i = 0; i < rows; i++) {
                // rows == 1 keeps the original fixed key value (2), so the default
                // workload is unchanged; larger row counts get distinct keys.
                c_keys.push_back(clustering_key::from_exploded(*s, {int32_type->decompose(int32_t(rows == 1 ? 2 : i))}));
            }
            size_t row_idx = 0;

            instructions_retired_counter.enable();
            cpu_cycles_retired_counter.enable();
            time_it([&] {
                mutation m(s, key);
                const column_definition& col = *s->get_column_definition(to_bytes(cnames[size_t(std::rand()) % column_count]));
                m.set_clustered_cell(c_keys[row_idx], col, make_atomic_cell(col.type, value));
                mt.apply(std::move(m));
                row_idx = (row_idx + 1) % rows;
                total_ops++;
            });
        }

        instructions_retired_counter.disable();
        cpu_cycles_retired_counter.disable();
        uint64_t insns = instructions_retired_counter.read();
        uint64_t cycles = cpu_cycles_retired_counter.read();
        auto fmt_per_op = [&] (uint64_t v) {
            return v ? format("{:.1f}", double(v) / total_ops) : sstring("N/A");
        };
        std::cout << format("{} total ops, {} insns/op, {} cycles/op\n",
                total_ops, fmt_per_op(insns), fmt_per_op(cycles));

        engine().exit(0);
    });
}
