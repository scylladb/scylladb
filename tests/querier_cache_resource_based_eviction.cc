/*
 * Copyright (C) 2018 ScyllaDB
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

#include "querier.hh"
#include "mutation_query.hh"
#include "tests/cql_test_env.hh"
#include "tests/tmpdir.hh"

#include <seastar/core/app-template.hh>

using namespace std::chrono_literals;

int main(int argc, char** argv) {
    app_template app;
    tmpdir tmp;

    app.add_options()
        ("verbose", "Enables more logging")
        ("trace", "Enables trace-level logging")
        ;

    return app.run(argc, argv, [&tmp, &app] {
        db::config db_cfg;

        db_cfg.enable_cache(false);
        db_cfg.enable_commitlog(false);
        db_cfg.data_file_directories({tmp.path}, db::config::config_source::CommandLine);

        if (!app.configuration().count("verbose")) {
            logging::logger_registry().set_all_loggers_level(seastar::log_level::warn);
        }
        if (app.configuration().count("trace")) {
            logging::logger_registry().set_logger_level("sstable", seastar::log_level::trace);
        }

        return do_with_cql_env([] (cql_test_env& env) {
            return seastar::async([&env] {
                using namespace std::chrono_literals;

                auto& db = env.local_db();

                db.set_querier_cache_entry_ttl(24h);

                try {
                    db.find_keyspace("querier_cache");
                    env.execute_cql("drop keyspace querier_cache;").get();
                } catch (const no_such_keyspace&) {
                    // expected
                }

                env.execute_cql("CREATE KEYSPACE querier_cache WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};").get();
                env.execute_cql("CREATE TABLE querier_cache.test (pk int, ck int, value int, primary key (pk, ck));").get();

                env.require_table_exists("querier_cache", "test").get();

                auto insert_id = env.prepare("INSERT INTO querier_cache.test (pk, ck, value) VALUES (?, ?, ?);").get0();
                auto pk = cql3::raw_value::make_value(data_value(0).serialize());
                for (int i = 0; i < 100; ++i) {
                    auto ck = cql3::raw_value::make_value(data_value(i).serialize());
                    env.execute_prepared(insert_id, {{pk, ck, ck}}).get();
                }

                env.require_table_exists("querier_cache", "test").get();

                auto& cf = db.find_column_family("querier_cache", "test");
                auto s = cf.schema();

                cf.flush().get();

                auto cmd1 = query::read_command(s->id(),
                        s->version(),
                        s->full_slice(),
                        1,
                        gc_clock::now(),
                        stdx::nullopt,
                        1,
                        utils::make_random_uuid());

                // Should save the querier in cache.
                db.query_mutations(s,
                        cmd1,
                        query::full_partition_range,
                        db.get_result_memory_limiter().new_mutation_read(1024 * 1024).get0(),
                        nullptr,
                        db::no_timeout).get();

                // Make a fake keyspace just to obtain the configuration and
                // thus the concurrency semaphore.
                const auto dummy_ks_metadata = keyspace_metadata("dummy_ks", "SimpleStrategy", {{"replication_factor", "1"}}, false);
                auto cfg = db.make_keyspace_config(dummy_ks_metadata);

                assert(db.get_querier_cache_stats().resource_based_evictions == 0);

                // Drain all resources of the semaphore
                std::vector<lw_shared_ptr<reader_concurrency_semaphore::reader_permit>> permits;
                const auto resources = cfg.read_concurrency_semaphore->available_resources();
                permits.reserve(resources.count);
                const auto per_permit_memory  = resources.memory / resources.count;

                for (int i = 0; i < resources.count; ++i) {
                    permits.emplace_back(cfg.read_concurrency_semaphore->wait_admission(per_permit_memory).get0());
                }

                assert(cfg.read_concurrency_semaphore->available_resources().count == 0);
                assert(cfg.read_concurrency_semaphore->available_resources().memory < per_permit_memory);

                auto cmd2 = query::read_command(s->id(),
                        s->version(),
                        s->full_slice(),
                        1,
                        gc_clock::now(),
                        stdx::nullopt,
                        1,
                        utils::make_random_uuid());

                // Should evict the already cached querier.
                db.query_mutations(s,
                        cmd2,
                        query::full_partition_range,
                        db.get_result_memory_limiter().new_mutation_read(1024 * 1024).get0(),
                        nullptr,
                        db::no_timeout).get();

                assert(db.get_querier_cache_stats().resource_based_evictions == 1);

                // We want to read the entire partition so that the querier
                // is not saved at the end and thus ensure it is destroyed.
                // We cannot leave scope with the querier still in the cache
                // as that sadly leads to use-after-free as the database's
                // resource_concurrency_semaphore will be destroyed before some
                // of the tracked buffers.
                cmd2.row_limit = query::max_rows;
                cmd2.partition_limit = query::max_partitions;
                db.query_mutations(s,
                        cmd2,
                        query::full_partition_range,
                        db.get_result_memory_limiter().new_mutation_read(1024 * 1024 * 1024 * 1024).get0(),
                        nullptr,
                        db::no_timeout).get();

            });
        }, db_cfg);
    });
}

