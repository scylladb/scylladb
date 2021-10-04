/*
 * Copyright (C) 2015-present ScyllaDB
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

#include <boost/range/irange.hpp>

#include <seastar/util/defer.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>

#include "partition_slice_builder.hh"
#include "schema_builder.hh"
#include "memtable.hh"
#include "row_cache.hh"
#include "frozen_mutation.hh"
#include "test/lib/tmpdir.hh"
#include "sstables/sstables.hh"
#include "canonical_mutation.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/test_services.hh"
#include "test/lib/sstable_test_env.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/schema_registry.hh"

class size_calculator {
    using cells_type = row::sparse_array_type;

    class nest {
    public:
        static thread_local int level;
        nest() { ++level; }
        ~nest() { --level; }
    };

    static std::string prefix() {
        std::string s(" ");
        for (int i = 0; i < nest::level; ++i) {
            s += "-- ";
        }
        return s;
    }
public:
    static void print_cache_entry_size() {
        std::cout << prefix() << "sizeof(cache_entry) = " << sizeof(cache_entry) << "\n";
        std::cout << prefix() << "sizeof(memtable_entry) = " << sizeof(memtable_entry) << "\n";
        std::cout << prefix() << "sizeof(bptree::node) = " << sizeof(row_cache::partitions_type::outer_tree::node) << "\n";
        std::cout << prefix() << "sizeof(bptree::data) = " << sizeof(row_cache::partitions_type::outer_tree::data) << "\n";

        {
            nest n;
            std::cout << prefix() << "sizeof(decorated_key) = " << sizeof(dht::decorated_key) << "\n";
            print_mutation_partition_size();
        }

        std::cout << "\n";

        std::cout << prefix() << "sizeof(rows_entry) = " << sizeof(rows_entry) << "\n";
        std::cout << prefix() << "sizeof(evictable) = " << sizeof(evictable) << "\n";
        std::cout << prefix() << "sizeof(deletable_row) = " << sizeof(deletable_row) << "\n";
        std::cout << prefix() << "sizeof(row) = " << sizeof(row) << "\n";
        std::cout << prefix() << "radix_tree::inner_node::node_sizes = ";
        for (int i = 4; i <= 128; i *= 2) {
            std::cout << " " << cells_type::inner_node::node_type::node_size(cells_type::layout::direct_dynamic, i);
        }
        std::cout << "\n";
        std::cout << prefix() << "radix_tree::leaf_node::node_sizes = ";
        std::cout << " " << cells_type::leaf_node::node_type::node_size(cells_type::layout::indirect_tiny, 0);
        std::cout << " " << cells_type::leaf_node::node_type::node_size(cells_type::layout::indirect_small, 0);
        std::cout << " " << cells_type::leaf_node::node_type::node_size(cells_type::layout::indirect_medium, 0);
        std::cout << " " << cells_type::leaf_node::node_type::node_size(cells_type::layout::indirect_large, 0);
        std::cout << " " << cells_type::leaf_node::node_type::node_size(cells_type::layout::direct_static, 0);
        std::cout << "\n";

        std::cout << prefix() << "sizeof(atomic_cell_or_collection) = " << sizeof(atomic_cell_or_collection) << "\n";
        std::cout << prefix() << "btree::linear_node_size(1) = " << mutation_partition::rows_type::node::linear_node_size(1) << "\n";
        std::cout << prefix() << "btree::inner_node_size = " << mutation_partition::rows_type::node::inner_node_size << "\n";
        std::cout << prefix() << "btree::leaf_node_size = " << mutation_partition::rows_type::node::leaf_node_size << "\n";
    }

    static void print_mutation_partition_size() {
        std::cout << prefix() << "sizeof(mutation_partition) = " << sizeof(mutation_partition) << "\n";
        {
            nest n;
            std::cout << prefix() << "sizeof(_static_row) = " << sizeof(mutation_partition::_static_row) << "\n";
            std::cout << prefix() << "sizeof(_rows) = " << sizeof(mutation_partition::_rows) << "\n";
            std::cout << prefix() << "sizeof(_row_tombstones) = " << sizeof(mutation_partition::_row_tombstones) <<
            "\n";
        }
    }
};

thread_local int size_calculator::nest::level = 0;

static schema_ptr cassandra_stress_schema(schema_registry& registry) {
    return schema_builder(registry, "ks", "cf")
        .with_column("KEY", bytes_type, column_kind::partition_key)
        .with_column("C0", bytes_type)
        .with_column("C1", bytes_type)
        .with_column("C2", bytes_type)
        .with_column("C3", bytes_type)
        .with_column("C4", bytes_type)
        .build();
}

[[gnu::unused]]
static mutation make_cs_mutation() {
    tests::schema_registry_wrapper registry;
    auto s = cassandra_stress_schema(registry);
    mutation m(s, partition_key::from_single_value(*s, bytes_type->from_string("4b343050393536353531")));
    for (auto&& col : s->regular_columns()) {
        m.set_clustered_cell(clustering_key::make_empty(), col,
            atomic_cell::make_live(*bytes_type, 1, bytes_type->from_string("8f75da6b3dcec90c8a404fb9a5f6b0621e62d39c69ba5758e5f41b78311fbb26cc7a")));
    }
    return m;
}

bytes random_bytes(size_t size) {
    bytes result(bytes::initialized_later(), size);
    for (size_t i = 0; i < size; ++i) {
        result[i] = std::rand() % std::numeric_limits<uint8_t>::max();
    }
    return result;
}

sstring random_name(size_t size) {
    sstring result = uninitialized_string(size);
    static const char chars[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    for (size_t i = 0; i < size; ++i) {
        result[i] = chars[std::rand() % sizeof(chars)];
    }
    return result;
}

struct mutation_settings {
    size_t column_count;
    size_t column_name_size;
    size_t row_count;
    size_t partition_count;
    size_t partition_key_size;
    size_t clustering_key_size;
    size_t data_size;
};

static schema_ptr make_schema(schema_registry& registry, const mutation_settings& settings) {
    auto builder = schema_builder(registry, "ks", "cf")
        .with_column("pk", bytes_type, column_kind::partition_key)
        .with_column("ck", bytes_type, column_kind::clustering_key);

    for (size_t i = 0; i < settings.column_count; ++i) {
        builder.with_column(to_bytes(random_name(settings.column_name_size)), bytes_type);
    }

    return builder.build();
}

static mutation make_mutation(schema_ptr s, mutation_settings settings) {
    mutation m(s, partition_key::from_single_value(*s, bytes_type->decompose(data_value(random_bytes(settings.partition_key_size)))));

    for (size_t i = 0; i < settings.row_count; ++i) {
        auto ck = clustering_key::from_single_value(*s, bytes_type->decompose(data_value(random_bytes(settings.clustering_key_size))));
        for (auto&& col : s->regular_columns()) {
            m.set_clustered_cell(ck, col,
                atomic_cell::make_live(*bytes_type, 1,
                    bytes_type->decompose(data_value(random_bytes(settings.data_size)))));
        }
    }
    return m;
}

struct sizes {
    size_t memtable;
    size_t cache;
    std::map<sstables::sstable::version_types, size_t> sstable;
    size_t frozen;
    size_t canonical;
    size_t query_result;
};

static sizes calculate_sizes(schema_registry& registry, cache_tracker& tracker, const mutation_settings& settings) {
    sizes result;
    auto s = make_schema(registry, settings);
    auto mt = make_lw_shared<memtable>(s);
    row_cache cache(s, make_empty_snapshot_source(), tracker);

    auto cache_initial_occupancy = tracker.region().occupancy().used_space();

    assert(mt->occupancy().used_space() == 0);

    std::vector<mutation> muts;
    for (size_t i = 0; i < settings.partition_count; ++i) {
        muts.emplace_back(make_mutation(s, settings));
        mt->apply(muts.back());
        cache.populate(muts.back());
    }

    mutation& m = muts[0];
    result.memtable = mt->occupancy().used_space();
    result.cache = tracker.region().occupancy().used_space() - cache_initial_occupancy;
    result.frozen = freeze(m).representation().size();
    result.canonical = canonical_mutation(m).representation().size();
    result.query_result = query_mutation(mutation(m), partition_slice_builder(*s).build()).buf().size();

    tmpdir sstable_dir;
    sstables::test_env::do_with_async([&] (sstables::test_env& env) {
        for (auto v  : sstables::all_sstable_versions) {
            auto sst = env.make_sstable(s,
                sstable_dir.path().string(),
                1 /* generation */,
                v,
                sstables::sstable::format_types::big);
            auto mt2 = make_lw_shared<memtable>(s);
            mt2->apply(*mt, env.make_reader_permit()).get();
            write_memtable_to_sstable_for_test(*mt2, sst).get();
            sst->load().get();
            result.sstable[v] = sst->data_size();
        }
    }).get();

    return result;
}

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("verbose", "Enable info-level logging")
        ("column-count", bpo::value<size_t>()->default_value(5), "column count")
        ("column-name-size", bpo::value<size_t>()->default_value(2), "column name size")
        ("row-count", bpo::value<size_t>()->default_value(1), "row count")
        ("partition-count", bpo::value<size_t>()->default_value(1), "partition count")
        ("partition-key-size", bpo::value<size_t>()->default_value(10), "partition key size")
        ("clustering-key-size", bpo::value<size_t>()->default_value(10), "clustering key size")
        ("data-size", bpo::value<size_t>()->default_value(32), "cell data size");

    return app.run(argc, argv, [&] {
        if (smp::count != 1) {
            throw std::runtime_error("This test has to be run with -c1");
        }

        if (!app.configuration().contains("verbose")) {
            logging::logger_registry().set_all_loggers_level(seastar::log_level::warn);
        }

        return do_with_cql_env_thread([&](cql_test_env& env) {
            mutation_settings settings;
            settings.column_count = app.configuration()["column-count"].as<size_t>();
            settings.column_name_size = app.configuration()["column-name-size"].as<size_t>();
            settings.row_count = app.configuration()["row-count"].as<size_t>();
            settings.partition_count = app.configuration()["partition-count"].as<size_t>();
            settings.partition_key_size = app.configuration()["partition-key-size"].as<size_t>();
            settings.clustering_key_size = app.configuration()["clustering-key-size"].as<size_t>();
            settings.data_size = app.configuration()["data-size"].as<size_t>();

            auto& tracker = env.local_db().find_column_family("system", "local").get_row_cache().get_cache_tracker();
            auto sizes = calculate_sizes(env.local_db().get_schema_registry(), tracker, settings);

            std::cout << "mutation footprint:" << "\n";
            std::cout << " - in cache:     " << sizes.cache << "\n";
            std::cout << " - in memtable:  " << sizes.memtable << "\n";
            std::cout << " - in sstable:\n";
            for (auto v : sizes.sstable) {
                std::cout << "   " << sstables::to_string(v.first) << ":   " << v.second << "\n";
            }
            std::cout << " - frozen:       " << sizes.frozen << "\n";
            std::cout << " - canonical:    " << sizes.canonical << "\n";
            std::cout << " - query result: " << sizes.query_result << "\n";

            std::cout << "\n";
            size_calculator::print_cache_entry_size();
        });
    });
}
