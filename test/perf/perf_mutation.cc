
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "replica/database.hh"
#include "schema/schema_builder.hh"
#include "test/perf/perf.hh"
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>

static atomic_cell make_atomic_cell(data_type dt, bytes value) {
    return atomic_cell::make_live(*dt, 0, value);
};

int main(int argc, char* argv[]) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("column-count", bpo::value<size_t>()->default_value(1), "column count");
    return app.run_deprecated(argc, argv, [&] {
        size_t column_count = app.configuration()["column-count"].as<size_t>();
        auto builder = schema_builder("ks", "cf")
            .with_column("p1", utf8_type, column_kind::partition_key)
            .with_column("c1", int32_type, column_kind::clustering_key);

        std::vector<sstring> cnames;
        for (size_t i = 0; i < column_count; i++) {
            cnames.push_back(fmt::format("r{}", i + 1));
            builder.with_column(to_bytes(cnames.back()), int32_type);
        }

        auto s = builder.build();
        replica::memtable mt(s);

        std::cout << "Timing mutation of single column within one row...\n";

        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(2)});
        bytes value = int32_type->decompose(3);

        time_it([&] {
            mutation m(s, key);
            const column_definition& col = *s->get_column_definition(to_bytes(cnames[std::rand() % column_count]));
            m.set_clustered_cell(c_key, col, make_atomic_cell(col.type, value));
            mt.apply(std::move(m));
        });
        engine().exit(0);
    });
}
