/*
 * Copyright 2015 Cloudius Systems
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

#include <core/distributed.hh>
#include <core/app-template.hh>
#include <core/sstring.hh>
#include <core/thread.hh>

#include "utils/managed_bytes.hh"
#include "utils/logalloc.hh"
#include "row_cache.hh"
#include "log.hh"
#include "schema_builder.hh"
#include "memtable.hh"
#include "tests/perf/perf.hh"

static
dht::decorated_key new_key(schema_ptr s) {
    static thread_local int next = 0;
    return dht::global_partitioner().decorate_key(*s,
        partition_key::from_single_value(*s, to_bytes(sprint("key%d", next++))));
}

static
clustering_key new_ckey(schema_ptr s) {
    static thread_local int next = 0;
    return clustering_key::from_single_value(*s, to_bytes(sprint("ckey%d", next++)));
}

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("debug", "enable debug logging")
        ("partitions", bpo::value<unsigned>()->default_value(128), "number of partitions per memtable")
        ("cell-size", bpo::value<unsigned>()->default_value(1024), "cell size in bytes")
        ("rows", bpo::value<unsigned>()->default_value(128), "row count per partition")
        ("no-cache", "do not update cache");

    return app.run(argc, argv, [&app] {
        if (app.configuration().count("debug")) {
            logging::logger_registry().set_all_loggers_level(logging::log_level::debug);
        }

        return seastar::async([&] {
            auto s = schema_builder("ks", "cf")
                .with_column("pk", bytes_type, column_kind::partition_key)
                .with_column("ck", bytes_type, column_kind::clustering_key)
                .with_column("v", bytes_type, column_kind::regular_column)
                .build();

            cache_tracker tracker;
            row_cache cache(s, [] (auto&&) { return make_empty_reader(); },
                [] (auto&&) { return key_reader(); }, tracker);

            size_t partitions = app.configuration()["partitions"].as<unsigned>();
            size_t cell_size = app.configuration()["cell-size"].as<unsigned>();
            size_t row_count = app.configuration()["rows"].as<unsigned>();
            bool update_cache = !app.configuration().count("no-cache");

            std::vector<mutation> mutations;
            for (unsigned i = 0; i < partitions; ++i) {
                mutation m(new_key(s), s);
                for (size_t j = 0; j < row_count; j++) {
                    m.set_clustered_cell(new_ckey(s), "v", bytes(bytes::initialized_later(), cell_size), 2);
                }
                mutations.emplace_back(std::move(m));
            }

            time_it([&] {
                auto mt = make_lw_shared<memtable>(s);
                for (auto&& m : mutations) {
                    mt->apply(m);
                }

                auto checker = [](const partition_key& key) {
                    return partition_presence_checker_result::maybe_exists;
                };

                if (update_cache) {
                    cache.update(*mt, checker).get();
                }
            }, 5, 1);
        });
    });
}
