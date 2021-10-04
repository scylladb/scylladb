/*
 * Copyright (C) 2020-present ScyllaDB
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


#include <seastar/testing/thread_test_case.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/core/sstring.hh>
#include "sstables/sstable_directory.hh"
#include "distributed_loader.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/tmpdir.hh"
#include "db/config.hh"

#include "fmt/format.h"

class distributed_loader_for_tests {
public:
    static future<> process_sstable_dir(sharded<sstables::sstable_directory>& dir, bool sort_sstables_according_to_owner = true) {
        return distributed_loader::process_sstable_dir(dir, sort_sstables_according_to_owner);
    }
    static future<> lock_table(sharded<sstables::sstable_directory>& dir, sharded<database>& db, sstring ks_name, sstring cf_name) {
        return distributed_loader::lock_table(dir, db, std::move(ks_name), std::move(cf_name));
    }
    static future<> reshard(sharded<sstables::sstable_directory>& dir, sharded<database>& db, sstring ks_name, sstring table_name, sstables::compaction_sstable_creator_fn creator) {
        return distributed_loader::reshard(dir, db, std::move(ks_name), std::move(table_name), std::move(creator));
    }
};

schema_ptr test_table_schema(schema_registry& registry) {
    return registry.get_or_load(db::system_keyspace::generate_schema_version("ks", "cf"), [&registry] (table_schema_version v) {
        schema_builder builder(registry,
                generate_legacy_id("ks", "cf"), "ks", "cf",
        // partition key
        {{"p", bytes_type}},
        // clustering key
        {},
        // regular columns
        {{"c", int32_type}},
        // static columns
        {},
        // regular column name type
        bytes_type,
        // comment
        ""
       );
       builder.with_version(v);
       return builder.build(schema_builder::compact_storage::no);
    });
}

using namespace sstables;

sstables::shared_sstable
make_sstable_for_this_shard(schema_registry& registry, std::function<sstables::shared_sstable()> sst_factory) {
    auto s = test_table_schema(registry);
    auto key_token_pair = token_generation_for_shard(1, this_shard_id(), 12);
    auto key = partition_key::from_exploded(*s, {to_bytes(key_token_pair[0].first)});
    mutation m(s, key);
    m.set_clustered_cell(clustering_key::make_empty(), bytes("c"), data_value(int32_t(0)), api::timestamp_type(0));
    return make_sstable_containing(sst_factory, {m});
}

/// Create a shared SSTable belonging to all shards for the following schema: "create table cf (p text PRIMARY KEY, c int)"
///
/// Arguments passed to the function are passed to table::make_sstable
template <typename... Args>
sstables::shared_sstable
make_sstable_for_all_shards(database& db, table& table, fs::path sstdir, int64_t generation) {
    // Unlike the previous helper, we'll assume we're in a thread here. It's less flexible
    // but the users are usually in a thread, and rewrite_toc_without_scylla_component requires
    // a thread. We could fix that, but deferring that for now.
    auto s = table.schema();
    auto mt = make_lw_shared<memtable>(s);
    auto msb = db.get_config().murmur3_partitioner_ignore_msb_bits();
    for (shard_id shard = 0; shard < smp::count; ++shard) {
        auto key_token_pair = token_generation_for_shard(1, shard, msb);
        auto key = partition_key::from_exploded(*s, {to_bytes(key_token_pair[0].first)});
        mutation m(s, key);
        m.set_clustered_cell(clustering_key::make_empty(), bytes("c"), data_value(int32_t(0)), api::timestamp_type(0));
        mt->apply(std::move(m));
    }
    auto sst = table.make_sstable(sstdir.native(), generation++,
            sstables::get_highest_sstable_version(), sstables::sstable::format_types::big);
    write_memtable_to_sstable(*mt, sst, table.get_sstables_manager().configure_writer("test")).get();
    mt->clear_gently().get();
    // We can't write an SSTable with bad sharding, so pretend
    // it came from Cassandra
    sstables::test(sst).remove_component(sstables::component_type::Scylla).get();
    sstables::test(sst).rewrite_toc_without_scylla_component();
    return sst;
}

class sstable_from_existing_file {
    std::function<schema_registry&()> _get_registry;
    std::function<sstables::sstables_manager* ()> _get_mgr;
public:
    explicit sstable_from_existing_file(sstables::test_env& env)
        : _get_registry([&env] () -> schema_registry& { return env.registry(); })
        , _get_mgr([m = &env.manager()] { return m; })
    { }
    // This variant this transportable across shards
    explicit sstable_from_existing_file(sharded<sstables::test_env>& env)
        : _get_registry([&env] () -> schema_registry& { return env.local().registry(); })
        , _get_mgr([s = &env] { return &s->local().manager(); })
    { }
    // This variant this transportable across shards
    explicit sstable_from_existing_file(cql_test_env& env)
        : _get_registry([&env] () -> schema_registry& { return env.local_db().get_schema_registry(); })
        , _get_mgr([&env] { return &env.db().local().get_user_sstables_manager(); })
    {}
    sstables::shared_sstable operator()(fs::path dir, int64_t gen, sstables::sstable_version_types v, sstables::sstable_format_types f) const {
        return _get_mgr()->make_sstable(test_table_schema(_get_registry()), dir.native(), gen, v, f, gc_clock::now(), default_io_error_handler_gen(), default_sstable_buffer_size);
    }
};

sstables::shared_sstable new_sstable(sstables::test_env& env, fs::path dir, int64_t gen) {
    return env.manager().make_sstable(test_table_schema(env.registry()), dir.native(), gen,
                sstables::sstable_version_types::mc, sstables::sstable_format_types::big,
                gc_clock::now(), default_io_error_handler_gen(), default_sstable_buffer_size);
}

// there is code for this in distributed_loader.cc but this is so simple it is not worth polluting
// the public namespace for it. Repeat it here.
inline future<int64_t>
highest_generation_seen(sharded<sstables::sstable_directory>& dir) {
    return dir.map_reduce0(std::mem_fn(&sstable_directory::highest_generation_seen), int64_t(0), [] (int64_t a, int64_t b) {
        return std::max<int64_t>(a, b);
    });
}

// Called from a seastar thread
static void with_sstable_directory(
    std::filesystem::path path,
    unsigned load_parallelism,
    sstable_directory::need_mutate_level need_mutate,
    sstable_directory::lack_of_toc_fatal fatal_nontoc,
    sstable_directory::enable_dangerous_direct_import_of_cassandra_counters eddiocc,
    sstable_directory::allow_loading_materialized_view almv,
    sstable_directory::sstable_object_from_existing_fn sstable_from_existing,
    noncopyable_function<void (sharded<sstable_directory>&)> func) {

    sharded<semaphore> sstdir_sem;
    sstdir_sem.start(load_parallelism).get();
    auto stop_sstdir_sem = defer([&sstdir_sem] {
        sstdir_sem.stop().get();
    });

    sharded<sstable_directory> sstdir;
    auto stop_sstdir = defer([&sstdir] {
        sstdir.stop().get();
    });

    auto wrapped_sfe = [&sstable_from_existing] (fs::path dir, int64_t gen, sstables::sstable_version_types v, sstables::sstable_format_types f) {
        return sstable_from_existing(std::move(dir), gen, v, f);
    };

    sstdir.start(std::move(path), load_parallelism, std::ref(sstdir_sem), need_mutate, fatal_nontoc, eddiocc, almv, std::move(wrapped_sfe)).get();

    func(sstdir);
}

SEASTAR_TEST_CASE(sstable_directory_test_table_simple_empty_directory_scan) {
  return sstables::test_env::do_with_async([] (test_env& env) {
    auto dir = tmpdir();

    // Write a manifest file to make sure it's ignored
    auto manifest = dir.path() / "manifest.json";
    auto f = open_file_dma(manifest.native(), open_flags::wo | open_flags::create | open_flags::truncate).get0();
    f.close().get();

   with_sstable_directory(dir.path(), 1,
            sstable_directory::need_mutate_level::no,
            sstable_directory::lack_of_toc_fatal::no,
            sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
            sstable_directory::allow_loading_materialized_view::no,
            sstable_from_existing_file(env),
            [] (sharded<sstables::sstable_directory>& sstdir) {
    distributed_loader_for_tests::process_sstable_dir(sstdir).get();
    int64_t max_generation_seen = highest_generation_seen(sstdir).get0();
    // No generation found on empty directory.
    BOOST_REQUIRE_EQUAL(max_generation_seen, 0);
   });
  });
}

// Test unrecoverable SSTable: missing a file that is expected in the TOC.
SEASTAR_TEST_CASE(sstable_directory_test_table_scan_incomplete_sstables) {
  return sstables::test_env::do_with_sharded_async([] (sharded<test_env>& sharded_env) {
    auto& env = sharded_env.local();
    auto dir = tmpdir();
    auto sst = make_sstable_for_this_shard(env.registry(), std::bind(new_sstable, std::ref(env), dir.path(), 1));

    // Now there is one sstable to the upload directory, but it is incomplete and one component is missing.
    // We should fail validation and leave the directory untouched
    remove_file(sst->filename(sstables::component_type::Statistics)).get();

   with_sstable_directory(dir.path(), 1,
            sstable_directory::need_mutate_level::no,
            sstable_directory::lack_of_toc_fatal::no,
            sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
            sstable_directory::allow_loading_materialized_view::no,
            sstable_from_existing_file(sharded_env),
            [] (sharded<sstables::sstable_directory>& sstdir) {
    auto expect_malformed_sstable = distributed_loader_for_tests::process_sstable_dir(sstdir);
    BOOST_REQUIRE_THROW(expect_malformed_sstable.get(), sstables::malformed_sstable_exception);
   });
  });
}

// Test always-benign incomplete SSTable: temporaryTOC found
SEASTAR_TEST_CASE(sstable_directory_test_table_temporary_toc) {
  return sstables::test_env::do_with_async([] (test_env& env) {
    auto dir = tmpdir();
    auto sst = make_sstable_for_this_shard(env.registry(), std::bind(new_sstable, std::ref(env), dir.path(), 1));
    rename_file(sst->filename(sstables::component_type::TOC), sst->filename(sstables::component_type::TemporaryTOC)).get();

   with_sstable_directory(dir.path(), 1,
            sstable_directory::need_mutate_level::no,
            sstable_directory::lack_of_toc_fatal::yes,
            sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
            sstable_directory::allow_loading_materialized_view::no,
            sstable_from_existing_file(env),
            [] (sharded<sstables::sstable_directory>& sstdir) {
    auto expect_ok = distributed_loader_for_tests::process_sstable_dir(sstdir);
    BOOST_REQUIRE_NO_THROW(expect_ok.get());
   });
  });
}

// Test always-benign incomplete SSTable: with extraneous temporaryTOC found
SEASTAR_TEST_CASE(sstable_directory_test_table_extra_temporary_toc) {
    return sstables::test_env::do_with_async([] (test_env& env) {
        auto dir = tmpdir();
        auto sst = make_sstable_for_this_shard(env.registry(), std::bind(new_sstable, std::ref(env), dir.path(), 1));
        link_file(sst->filename(sstables::component_type::TOC), sst->filename(sstables::component_type::TemporaryTOC)).get();

        with_sstable_directory(dir.path(), 1,
                sstable_directory::need_mutate_level::no,
                sstable_directory::lack_of_toc_fatal::yes,
                sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
                sstable_directory::allow_loading_materialized_view::no,
                sstable_from_existing_file(env),
                [] (sharded<sstables::sstable_directory>& sstdir) {
            auto expect_ok = distributed_loader_for_tests::process_sstable_dir(sstdir);
            BOOST_REQUIRE_NO_THROW(expect_ok.get());
        });
    });
}

// Test the absence of TOC. Behavior is controllable by a flag
SEASTAR_TEST_CASE(sstable_directory_test_table_missing_toc) {
  return sstables::test_env::do_with_async([] (test_env& env) {
    auto dir = tmpdir();

    auto sst = make_sstable_for_this_shard(env.registry(), std::bind(new_sstable, std::ref(env), dir.path(), 1));
    remove_file(sst->filename(sstables::component_type::TOC)).get();

   with_sstable_directory(dir.path(), 1,
            sstable_directory::need_mutate_level::no,
            sstable_directory::lack_of_toc_fatal::yes,
            sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
            sstable_directory::allow_loading_materialized_view::no,
            sstable_from_existing_file(env),
            [] (sharded<sstables::sstable_directory>& sstdir_fatal) {
    auto expect_malformed_sstable  = distributed_loader_for_tests::process_sstable_dir(sstdir_fatal);
    BOOST_REQUIRE_THROW(expect_malformed_sstable.get(), sstables::malformed_sstable_exception);
   });

   with_sstable_directory(dir.path(), 1,
            sstable_directory::need_mutate_level::no,
            sstable_directory::lack_of_toc_fatal::no,
            sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
            sstable_directory::allow_loading_materialized_view::no,
            sstable_from_existing_file(env),
            [] (sharded<sstables::sstable_directory>& sstdir_ok) {
    auto expect_ok = distributed_loader_for_tests::process_sstable_dir(sstdir_ok);
    BOOST_REQUIRE_NO_THROW(expect_ok.get());
   });
  });
}

// Test the presence of TemporaryStatistics. If the old Statistics file is around
// this is benign and we'll just delete it and move on. If the old Statistics file
// is not around (but mentioned in the TOC), then this is an error.
SEASTAR_THREAD_TEST_CASE(sstable_directory_test_temporary_statistics) {
  sstables::test_env::do_with_sharded_async([] (sharded<test_env>& env) {
    auto dir = tmpdir();

    auto sst = make_sstable_for_this_shard(env.local().registry(), std::bind(new_sstable, std::ref(env.local()), dir.path(), 1));
    auto tempstr = sst->filename(dir.path().native(), component_type::TemporaryStatistics);
    auto f = open_file_dma(tempstr, open_flags::rw | open_flags::create | open_flags::truncate).get0();
    f.close().get();
    auto tempstat = fs::canonical(fs::path(tempstr));

   with_sstable_directory(dir.path(), 1,
            sstable_directory::need_mutate_level::no,
            sstable_directory::lack_of_toc_fatal::no,
            sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
            sstable_directory::allow_loading_materialized_view::no,
            sstable_from_existing_file(env),
            [&dir, &tempstat] (sharded<sstables::sstable_directory>& sstdir_ok) {
    auto expect_ok = distributed_loader_for_tests::process_sstable_dir(sstdir_ok);
    BOOST_REQUIRE_NO_THROW(expect_ok.get());
    lister::scan_dir(dir.path(), { directory_entry_type::regular }, [tempstat] (fs::path parent_dir, directory_entry de) {
        BOOST_REQUIRE(fs::canonical(parent_dir / fs::path(de.name)) != tempstat);
        return make_ready_future<>();
    }).get();
   });

    remove_file(sst->filename(sstables::component_type::Statistics)).get();

   with_sstable_directory(dir.path(), 1,
            sstable_directory::need_mutate_level::no,
            sstable_directory::lack_of_toc_fatal::no,
            sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
            sstable_directory::allow_loading_materialized_view::no,
            sstable_from_existing_file(env),
            [] (sharded<sstables::sstable_directory>& sstdir_fatal) {
    auto expect_malformed_sstable  = distributed_loader_for_tests::process_sstable_dir(sstdir_fatal);
    BOOST_REQUIRE_THROW(expect_malformed_sstable.get(), sstables::malformed_sstable_exception);
   });
  }).get();
}

// Test that we see the right generation during the scan. Temporary files are skipped
SEASTAR_THREAD_TEST_CASE(sstable_directory_test_generation_sanity) {
  sstables::test_env::do_with_sharded_async([] (sharded<test_env>& env) {
    auto dir = tmpdir();
    make_sstable_for_this_shard(env.local().registry(), std::bind(new_sstable, std::ref(env.local()), dir.path(), 3333));
    auto sst = make_sstable_for_this_shard(env.local().registry(), std::bind(new_sstable, std::ref(env.local()), dir.path(), 6666));
    rename_file(sst->filename(sstables::component_type::TOC), sst->filename(sstables::component_type::TemporaryTOC)).get();

   with_sstable_directory(dir.path(), 1,
            sstable_directory::need_mutate_level::no,
            sstable_directory::lack_of_toc_fatal::yes,
            sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
            sstable_directory::allow_loading_materialized_view::no,
            sstable_from_existing_file(env),
            [] (sharded<sstables::sstable_directory>& sstdir) {
    distributed_loader_for_tests::process_sstable_dir(sstdir).get();
    int64_t max_generation_seen = highest_generation_seen(sstdir).get0();
    BOOST_REQUIRE_EQUAL(max_generation_seen, 3333);
   });
  }).get();
}

future<> verify_that_all_sstables_are_local(sharded<sstable_directory>& sstdir, unsigned expected_sstables) {
    return do_with(std::make_unique<std::atomic<unsigned>>(0), [&sstdir, expected_sstables] (std::unique_ptr<std::atomic<unsigned>>& count) {
        return sstdir.invoke_on_all([count = count.get()] (sstable_directory& d) {
            return d.do_for_each_sstable([count] (sstables::shared_sstable sst) {
                count->fetch_add(1, std::memory_order_relaxed);
                auto shards = sst->get_shards_for_this_sstable();
                BOOST_REQUIRE_EQUAL(shards.size(), 1);
                BOOST_REQUIRE_EQUAL(shards[0], this_shard_id());
                return make_ready_future<>();
            });
         }).then([count = count.get(), expected_sstables] {
            BOOST_REQUIRE_EQUAL(count->load(std::memory_order_relaxed), expected_sstables);
            return make_ready_future<>();
        });
    });
}

// Test that all SSTables are seen as unshared, if the generation numbers match what their
// shard-assignments expect
SEASTAR_THREAD_TEST_CASE(sstable_directory_unshared_sstables_sanity_matched_generations) {
  sstables::test_env::do_with_sharded_async([] (sharded<test_env>& env) {
    auto dir = tmpdir();
    for (shard_id i = 0; i < smp::count; ++i) {
        env.invoke_on(i, [dir = dir.path(), i] (sstables::test_env& env) {
            // this is why it is annoying for the internal functions in the test infrastructure to
            // assume threaded execution
            return seastar::async([dir, i, &env] {
                make_sstable_for_this_shard(env.registry(), std::bind(new_sstable, std::ref(env), dir, i));
            });
        }).get();
    }

   with_sstable_directory(dir.path(), 1,
            sstable_directory::need_mutate_level::no,
            sstable_directory::lack_of_toc_fatal::yes,
            sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
            sstable_directory::allow_loading_materialized_view::no,
            sstable_from_existing_file(env),
            [] (sharded<sstables::sstable_directory>& sstdir) {
    distributed_loader_for_tests::process_sstable_dir(sstdir).get();
    verify_that_all_sstables_are_local(sstdir, smp::count).get();
   });
  }).get();
}

// Test that all SSTables are seen as unshared, even if the generation numbers do not match what their
// shard-assignments expect
SEASTAR_THREAD_TEST_CASE(sstable_directory_unshared_sstables_sanity_unmatched_generations) {
  sstables::test_env::do_with_sharded_async([] (sharded<test_env>& env) {
    auto dir = tmpdir();
    for (shard_id i = 0; i < smp::count; ++i) {
        env.invoke_on(i, [dir = dir.path(), i] (sstables::test_env& env) {
            // this is why it is annoying for the internal functions in the test infrastructure to
            // assume threaded execution
            return seastar::async([dir, i, &env] {
                make_sstable_for_this_shard(env.registry(), std::bind(new_sstable, std::ref(env), dir, i + 1));
            });
        }).get();
    }

   with_sstable_directory(dir.path(), 1,
            sstable_directory::need_mutate_level::no,
            sstable_directory::lack_of_toc_fatal::yes,
            sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
            sstable_directory::allow_loading_materialized_view::no,
            sstable_from_existing_file(env),
            [] (sharded<sstables::sstable_directory>& sstdir) {
    distributed_loader_for_tests::process_sstable_dir(sstdir).get();
    verify_that_all_sstables_are_local(sstdir, smp::count).get();
   });
  }).get();
}

// Test that the sstable_dir object can keep the table alive against a drop
SEASTAR_TEST_CASE(sstable_directory_test_table_lock_works) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p text PRIMARY KEY, c int)").get();
        auto ks_name = "ks";
        auto cf_name = "cf";
        auto& cf = e.local_db().find_column_family(ks_name, cf_name);
        auto path = fs::path(cf.dir());

        sharded<semaphore> sstdir_sem;
        sstdir_sem.start(1).get();
        auto stop_sstdir_sem = defer([&sstdir_sem] {
            sstdir_sem.stop().get();
        });

        sharded<sstable_directory> sstdir;
        sstdir.start(path, 1, std::ref(sstdir_sem),
                sstable_directory::need_mutate_level::no,
                sstable_directory::lack_of_toc_fatal::no,
                sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
                sstable_directory::allow_loading_materialized_view::no,
                sstable_from_existing_file(e)).get();

        // stop cleanly in case we fail early for unexpected reasons
        auto stop = defer([&sstdir] {
            sstdir.stop().get();
        });

        distributed_loader_for_tests::lock_table(sstdir, e.db(), ks_name, cf_name).get();

        auto drop = e.execute_cql("drop table cf");
        later().get();

        auto table_ok = e.db().invoke_on_all([ks_name, cf_name] (database& db) {
            db.find_column_family(ks_name, cf_name);
        });
        BOOST_REQUIRE_NO_THROW(table_ok.get());

        // Stop manually now, to allow for the object to be destroyed and take the
        // phaser with it.
        stop.cancel();
        sstdir.stop().get();
        drop.get();

        auto no_such_table = e.db().invoke_on_all([ks_name, cf_name] (database& db) {
            db.find_column_family(ks_name, cf_name);
            return make_ready_future<>();
        });
        BOOST_REQUIRE_THROW(no_such_table.get(), no_such_column_family);
    });
}

SEASTAR_TEST_CASE(sstable_directory_shared_sstables_reshard_correctly) {
    if (smp::count == 1) {
        fmt::print("Skipping sstable_directory_shared_sstables_reshard_correctly, smp == 1\n");
        return make_ready_future<>();
    }

    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p text PRIMARY KEY, c int)").get();
        auto& cf = e.local_db().find_column_family("ks", "cf");
        auto upload_path = fs::path(cf.dir()) / "upload";

        e.db().invoke_on_all([] (database& db) {
            auto& cf = db.find_column_family("ks", "cf");
            cf.disable_auto_compaction();
        }).get();

        unsigned num_sstables = 10 * smp::count;
        auto generation = 0;
        for (unsigned nr = 0; nr < num_sstables; ++nr) {
            make_sstable_for_all_shards(e.db().local(), cf, upload_path.native(), generation++);
        }

      with_sstable_directory(upload_path, 1,
                sstable_directory::need_mutate_level::no,
                sstable_directory::lack_of_toc_fatal::yes,
                sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
                sstable_directory::allow_loading_materialized_view::no,
                [&e] (fs::path dir, int64_t gen, sstables::sstable_version_types v, sstables::sstable_format_types f) {
                    auto& cf = e.local_db().find_column_family("ks", "cf");
                    return cf.make_sstable(dir.native(), gen, v, f);
                },
                [&e, upload_path] (sharded<sstables::sstable_directory>& sstdir) {
        distributed_loader_for_tests::process_sstable_dir(sstdir).get();
        verify_that_all_sstables_are_local(sstdir, 0).get();

        int64_t max_generation_seen = highest_generation_seen(sstdir).get0();
        std::atomic<int64_t> generation_for_test = {};
        generation_for_test.store(max_generation_seen + 1, std::memory_order_relaxed);

        distributed_loader_for_tests::reshard(sstdir, e.db(), "ks", "cf", [&e, upload_path, &generation_for_test] (shard_id id) {
            auto generation = generation_for_test.fetch_add(1, std::memory_order_relaxed);
            auto& cf = e.local_db().find_column_family("ks", "cf");
            return cf.make_sstable(upload_path.native(), generation, sstables::sstable::version_types::mc, sstables::sstable::format_types::big);
        }).get();
        verify_that_all_sstables_are_local(sstdir, smp::count * smp::count).get();
      });
    });
}

SEASTAR_TEST_CASE(sstable_directory_shared_sstables_reshard_distributes_well_even_if_files_are_not_well_distributed) {
    if (smp::count == 1) {
        fmt::print("Skipping sstable_directory_shared_sstables_reshard_correctly, smp == 1\n");
        return make_ready_future<>();
    }

    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p text PRIMARY KEY, c int)").get();
        auto& cf = e.local_db().find_column_family("ks", "cf");
        auto upload_path = fs::path(cf.dir()) / "upload";

        e.db().invoke_on_all([] (database& db) {
            auto& cf = db.find_column_family("ks", "cf");
            cf.disable_auto_compaction();
        }).get();

        unsigned num_sstables = 10 * smp::count;
        auto generation = 0;
        for (unsigned nr = 0; nr < num_sstables; ++nr) {
            make_sstable_for_all_shards(e.db().local(), cf, upload_path.native(), generation++ * smp::count);
        }

      with_sstable_directory(upload_path, 1,
                sstable_directory::need_mutate_level::no,
                sstable_directory::lack_of_toc_fatal::yes,
                sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
                sstable_directory::allow_loading_materialized_view::no,
                [&e] (fs::path dir, int64_t gen, sstables::sstable_version_types v, sstables::sstable_format_types f) {
                    auto& cf = e.local_db().find_column_family("ks", "cf");
                    return cf.make_sstable(dir.native(), gen, v, f);
                },
                [&e, upload_path] (sharded<sstables::sstable_directory>& sstdir) {
        distributed_loader_for_tests::process_sstable_dir(sstdir).get();
        verify_that_all_sstables_are_local(sstdir, 0).get();

        int64_t max_generation_seen = highest_generation_seen(sstdir).get0();
        std::atomic<int64_t> generation_for_test = {};
        generation_for_test.store(max_generation_seen + 1, std::memory_order_relaxed);

        distributed_loader_for_tests::reshard(sstdir, e.db(), "ks", "cf", [&e, upload_path, &generation_for_test] (shard_id id) {
            auto generation = generation_for_test.fetch_add(1, std::memory_order_relaxed);
            auto& cf = e.local_db().find_column_family("ks", "cf");
            return cf.make_sstable(upload_path.native(), generation, sstables::sstable::version_types::mc, sstables::sstable::format_types::big);
        }).get();
        verify_that_all_sstables_are_local(sstdir, smp::count * smp::count).get();
      });
    });
}

SEASTAR_TEST_CASE(sstable_directory_shared_sstables_reshard_respect_max_threshold) {
    if (smp::count == 1) {
        fmt::print("Skipping sstable_directory_shared_sstables_reshard_correctly, smp == 1\n");
        return make_ready_future<>();
    }

    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p text PRIMARY KEY, c int)").get();
        auto& cf = e.local_db().find_column_family("ks", "cf");
        auto upload_path = fs::path(cf.dir()) / "upload";

        e.db().invoke_on_all([] (database& db) {
            auto& cf = db.find_column_family("ks", "cf");
            cf.disable_auto_compaction();
        }).get();

        unsigned num_sstables = (cf.schema()->max_compaction_threshold() + 1) * smp::count;
        auto generation = 0;
        for (unsigned nr = 0; nr < num_sstables; ++nr) {
            make_sstable_for_all_shards(e.db().local(), cf, upload_path.native(), generation++);
        }

      with_sstable_directory(upload_path, 1,
                sstable_directory::need_mutate_level::no,
                sstable_directory::lack_of_toc_fatal::yes,
                sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
                sstable_directory::allow_loading_materialized_view::no,
                [&e] (fs::path dir, int64_t gen, sstables::sstable_version_types v, sstables::sstable_format_types f) {
                    auto& cf = e.local_db().find_column_family("ks", "cf");
                    return cf.make_sstable(dir.native(), gen, v, f);
                },
                [&, upload_path] (sharded<sstables::sstable_directory>& sstdir) {
        distributed_loader_for_tests::process_sstable_dir(sstdir).get();
        verify_that_all_sstables_are_local(sstdir, 0).get();

        int64_t max_generation_seen = highest_generation_seen(sstdir).get0();
        std::atomic<int64_t> generation_for_test = {};
        generation_for_test.store(max_generation_seen + 1, std::memory_order_relaxed);

        distributed_loader_for_tests::reshard(sstdir, e.db(), "ks", "cf", [&e, upload_path, &generation_for_test] (shard_id id) {
            auto generation = generation_for_test.fetch_add(1, std::memory_order_relaxed);
            auto& cf = e.local_db().find_column_family("ks", "cf");
            return cf.make_sstable(upload_path.native(), generation, sstables::sstable::version_types::mc, sstables::sstable::format_types::big);
        }).get();
        verify_that_all_sstables_are_local(sstdir, 2 * smp::count * smp::count).get();
      });
    });
}
