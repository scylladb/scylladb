
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
    uint64_t read() {
        if (_fd == -1) { return 0; }
        uint64_t v = 0;
        auto res = ::read(_fd, &v, sizeof(v));
        return res == sizeof(v) ? v : 0;
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
            "growth of the memtable's (unrelated) rows b-tree. Ignores --rows.");
    return app.run_deprecated(argc, argv, [&] {
        size_t column_count = app.configuration()["column-count"].as<size_t>();
        bool sequential_columns = app.configuration()["sequential-columns"].as<bool>();
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

        if (sequential_columns) {
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
            size_t rows = app.configuration()["rows"].as<size_t>();
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
        std::cout << format("{} total ops, {:.1f} insns/op, {:.1f} cycles/op\n",
                total_ops, double(insns) / total_ops, double(cycles) / total_ops);

        engine().exit(0);
    });
}
