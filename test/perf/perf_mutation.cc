
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

#include <linux/perf_event.h>
#include <optional>
#include <stdexcept>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <unistd.h>

static atomic_cell make_atomic_cell(data_type dt, bytes value) {
    return atomic_cell::make_live(*dt, 0, value);
};

// Minimal perf_event_open() wrapper, matching the technique in
// seastar/include/seastar/testing/linux_perf_event.hh, without depending on
// seastar's testing library (which this binary does not otherwise link).
class perf_counter {
    int _fd = -1;
public:
    explicit perf_counter(unsigned config) {
        struct perf_event_attr attr = {};
        attr.type = PERF_TYPE_HARDWARE;
        attr.size = sizeof(attr);
        attr.config = config;
        attr.disabled = 1;
        attr.exclude_kernel = 1;
        attr.exclude_hv = 1;
        attr.read_format = PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING;
#if defined(__x86_64__)
        attr.exclude_idle = 1;
#endif
        int ret = ::syscall(__NR_perf_event_open, &attr, 0 /*pid*/, -1 /*cpu*/, -1 /*group_fd*/, 0 /*flags*/);
        if (ret != -1) {
            _fd = ret; // ignore failures, can happen in constrained environments such as containers
        }
    }
    ~perf_counter() { if (_fd != -1) { ::close(_fd); } }
    perf_counter(const perf_counter&) = delete;
    void enable() { if (_fd != -1) { ::ioctl(_fd, PERF_EVENT_IOC_ENABLE, 0); } }
    void disable() { if (_fd != -1) { ::ioctl(_fd, PERF_EVENT_IOC_DISABLE, 0); } }
    std::optional<double> read() {
        if (_fd == -1) {
            return std::nullopt;
        }
        struct {
            uint64_t value;
            uint64_t time_enabled;
            uint64_t time_running;
        } result;
        auto res = ::read(_fd, &result, sizeof(result));
        if (res != sizeof(result) || result.time_running == 0) {
            return std::nullopt;
        }
        // scale up for time the counter was multiplexed off the PMU
        return static_cast<double>(result.value) * result.time_enabled / result.time_running;
    }
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
            "rows per mutation in --merge-mode.");
    return app.run_deprecated(argc, argv, [&] {
        size_t column_count = app.configuration()["column-count"].as<size_t>();
        bool sequential_columns = app.configuration()["sequential-columns"].as<bool>();
        bool merge_mode = app.configuration()["merge-mode"].as<bool>();
        size_t rows = app.configuration()["rows"].as<size_t>();
        size_t merge_rows = app.configuration()["merge-rows"].as<size_t>();
        if (column_count == 0) {
            throw std::invalid_argument("--column-count must be greater than zero");
        }
        if (!sequential_columns && !merge_mode && rows == 0) {
            throw std::invalid_argument("--rows must be greater than zero");
        }
        if (merge_mode && merge_rows == 0) {
            throw std::invalid_argument("--merge-rows must be greater than zero");
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
        perf_counter instructions_retired_counter(PERF_COUNT_HW_INSTRUCTIONS);
        perf_counter cpu_cycles_retired_counter(PERF_COUNT_HW_CPU_CYCLES);

        if (merge_mode) {
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
            mutation target = build_mutation();
            instructions_retired_counter.enable();
            cpu_cycles_retired_counter.enable();
            time_it([&] {
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
        auto insns = instructions_retired_counter.read();
        auto cycles = cpu_cycles_retired_counter.read();
        auto fmt_per_op = [&] (std::optional<double> v) {
            return v ? format("{:.1f}", *v / total_ops) : sstring("N/A");
        };
        std::cout << format("{} total ops, {} insns/op, {} cycles/op\n",
                total_ops, fmt_per_op(insns), fmt_per_op(cycles));

        engine().exit(0);
    });
}
