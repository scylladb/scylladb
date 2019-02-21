/*
 * Copyright (C) 2019 ScyllaDB
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

#include "test/lib/cql_test_env.hh"
#include "test/lib/memtable_snapshot_source.hh"
#include "test/lib/random_utils.hh"

#include "schema_builder.hh"
#include "row_cache.hh"
#include "database.hh"
#include "db/config.hh"

#include <boost/range/irange.hpp>
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/units.hh>
#include <seastar/core/future-util.hh>
#include <seastar/util/log.hh>

static seastar::logger testlog("test");

app_template app;

class memory_demand_probe {

};

namespace seastar::testing {

thread_local std::default_random_engine local_random_engine;

}

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;

    app.add_options()
        ("enable-cache", "Enables cache")
        ("with-compression", "Generates compressed sstables")
        ("reads", bpo::value<unsigned>()->default_value(100), "Concurrent reads")
        ("sstables", bpo::value<uint64_t>()->default_value(100), "")
        ("sstable-size", bpo::value<uint64_t>()->default_value(10000000), "")
        ("sstable-format", bpo::value<std::string>()->default_value("mc"), "Sstable format version to use during population")
        ;

    testing::local_random_engine.seed(std::random_device()());

    return app.run(argc, argv, [] {
        cql_test_config test_cfg;

        auto& db_cfg = *test_cfg.db_config;

        db_cfg.enable_cache(app.configuration().count("enable-cache"));
        db_cfg.enable_commitlog(false);
        db_cfg.virtual_dirty_soft_limit(1.0);

        auto sstable_format_name = app.configuration()["sstable-format"].as<std::string>();
        if (sstable_format_name == "mc") {
            db_cfg.enable_sstables_mc_format(true);
        } else if (sstable_format_name == "la") {
            db_cfg.enable_sstables_mc_format(false);
        } else {
            throw std::runtime_error(format("Unsupported sstable format: {}", sstable_format_name));
        }

        return do_with_cql_env_thread([] (cql_test_env& env) {
            bool with_compression = app.configuration().count("with-compression");
            auto compressor = with_compression ? "LZ4Compressor" : "";
            uint64_t sstable_size = app.configuration()["sstable-size"].as<uint64_t>();
            uint64_t sstables = app.configuration()["sstables"].as<uint64_t>();
            auto reads = app.configuration()["reads"].as<unsigned>();

            env.execute_cql(format("{} WITH compression = {{ 'sstable_compression': '{}' }} "
                                   "AND compaction = {{'class' : 'NullCompactionStrategy'}};",
                "create table test (pk int, ck int, value int, primary key (pk,ck))", compressor)).get();

            table& tab = env.local_db().find_column_family("ks", "test");
            auto s = tab.schema();

            auto value = data_value(tests::random::get_bytes(100));
            auto& value_cdef = *s->get_column_definition("value");
            auto pk = partition_key::from_single_value(*s, data_value(0).serialize());
            uint64_t rows = 0;
            auto gen = [s, &rows, ck = 0, pk, &value_cdef, value] () mutable -> mutation {
                auto ts = api::new_timestamp();
                mutation m(s, pk);
                for (int i = 0; i < 1000; ++i) {
                    auto ckey = clustering_key::from_single_value(*s, data_value(ck).serialize());
                    auto& row = m.partition().clustered_row(*s, ckey);
                    row.cells().apply(value_cdef, atomic_cell::make_live(*value_cdef.type, ts, value.serialize()));
                    ++rows;
                    ++ck;
                }
                return m;
            };

            testlog.info("Populating");

            uint64_t i = 0;
            while (i < sstables) {
                auto m = gen();
                env.local_db().apply(s, freeze(m), db::commitlog::force_sync::no).get();
                if (tab.active_memtable().occupancy().used_space() > sstable_size) {
                    tab.flush().get();
                    ++i;
                }
            }

            env.local_db().flush_all_memtables().get();

            testlog.info("Live disk space used: {}", tab.get_stats().live_disk_space_used);
            testlog.info("Live sstables: {}", tab.get_stats().live_sstable_count);

            testlog.info("Preparing dummy cache");
            memtable_snapshot_source underlying(s);
            cache_tracker& tr = env.local_db().row_cache_tracker();
            row_cache c(s, snapshot_source([&] { return underlying(); }), tr, is_continuous::yes);
            auto prev_evictions = tr.get_stats().row_evictions;
            while (tr.get_stats().row_evictions == prev_evictions) {
                auto mt = make_lw_shared<memtable>(s);
                mt->apply(gen());
                c.update([] {}, *mt).get();
            }

            auto prev_occupancy = logalloc::shard_tracker().occupancy();
            testlog.info("Occupancy before: {}", prev_occupancy);

            testlog.info("Reading");
            parallel_for_each(boost::irange(0u, reads), [&] (unsigned i) {
                return env.execute_cql(format("select * from ks.test where pk = 0 and ck > {} limit 100;",
                    tests::random::get_int(rows / 2))).discard_result();
            }).get();

            auto occupancy = logalloc::shard_tracker().occupancy();
            testlog.info("Occupancy after: {}", occupancy);
            testlog.info("Max demand: {}", prev_occupancy.total_space() - occupancy.total_space());
            testlog.info("Max sstables per read: {}", tab.get_stats().estimated_sstable_per_read.max());
        }, test_cfg);
    });
}
