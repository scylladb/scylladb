/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <fmt/format.h>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/file.hh>
#include "sstables/generation_type.hh"
#include "test/lib/scylla_test_case.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/sstable_directory.hh"
#include "replica/distributed_loader.hh"
#include "replica/global_table_ptr.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/tmpdir.hh"
#include "test/lib/key_utils.hh"
#include "test/lib/test_utils.hh"
#include "test/lib/cql_assertions.hh"
#include "utils/lister.hh"
#include "db/config.hh"

#include <fmt/core.h>
#include <fmt/ranges.h>
#include <boost/algorithm/string/erase.hpp>

class distributed_loader_for_tests {
public:
    static future<> process_sstable_dir(sharded<sstables::sstable_directory>& dir, sstable_directory::process_flags flags) {
        return replica::distributed_loader::process_sstable_dir(dir, flags);
    }
    static future<> lock_table(sharded<sstables::sstable_directory>& dir, sharded<replica::database>& db, sstring ks_name, sstring cf_name) {
        auto gtable = co_await replica::get_table_on_all_shards(db, ks_name, cf_name);
        co_await replica::distributed_loader::lock_table(gtable, dir);
    }
    static future<> reshard(sharded<sstables::sstable_directory>& dir, sharded<replica::database>& db, sstring ks_name, sstring table_name, sstables::compaction_sstable_creator_fn creator, compaction::owned_ranges_ptr owned_ranges_ptr = nullptr) {
        return replica::distributed_loader::reshard(dir, db, std::move(ks_name), std::move(table_name), std::move(creator), std::move(owned_ranges_ptr));
    }
};

schema_ptr test_table_schema() {
    static thread_local auto s = [] {
        schema_builder builder(make_shared_schema(
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
       ));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return s;
}

using namespace sstables;

// Must be called from a seastar thread.
sstables::shared_sstable
make_sstable_for_this_shard(std::function<sstables::shared_sstable()> sst_factory) {
    auto s = test_table_schema();
    auto key = tests::generate_partition_key(s);
    mutation m(s, key);
    m.set_clustered_cell(clustering_key::make_empty(), bytes("c"), data_value(int32_t(0)), api::timestamp_type(0));
    return make_sstable_containing(sst_factory, {m});
}

/// Create a shared SSTable belonging to all shards for the following schema: "create table cf (p text PRIMARY KEY, c int)"
///
/// Arguments passed to the function are passed to table::make_sstable
template <typename... Args>
sstables::shared_sstable
make_sstable_for_all_shards(replica::database& db, replica::table& table, sstables::sstable_state state, sstables::generation_type generation) {
    // Unlike the previous helper, we'll assume we're in a thread here. It's less flexible
    // but the users are usually in a thread, and rewrite_toc_without_scylla_component requires
    // a thread. We could fix that, but deferring that for now.
    auto s = table.schema();
    auto mt = make_lw_shared<replica::memtable>(s);
    for (shard_id shard = 0; shard < smp::count; ++shard) {
        auto key = tests::generate_partition_key(s, shard);
        mutation m(s, key);
        m.set_clustered_cell(clustering_key::make_empty(), bytes("c"), data_value(int32_t(0)), api::timestamp_type(0));
        mt->apply(std::move(m));
    }
    data_dictionary::storage_options local;
    auto sst = table.get_sstables_manager().make_sstable(s, table.dir(), local, generation, state);
    write_memtable_to_sstable(*mt, sst).get();
    mt->clear_gently().get();
    // We can't write an SSTable with bad sharding, so pretend
    // it came from Cassandra
    testlog.debug("make_sstable_for_all_shards: {}: rewriting TOC", sst->get_filename());
    sstables::test(sst).remove_component(sstables::component_type::Scylla).get();
    sstables::test(sst).rewrite_toc_without_scylla_component();
    return sst;
}

sstables::shared_sstable new_sstable(sstables::test_env& env, fs::path dir, sstables::generation_type gen) {
    testlog.debug("new_sstable: dir={} gen={}", dir, gen);
    return env.make_sstable(test_table_schema(), dir.native(), gen);
}

sstables::shared_sstable new_env_sstable(sstables::test_env& env) {
    testlog.debug("new_env_sstable: dir={}", env.tempdir().path());
    return env.make_sstable(test_table_schema());
}

class wrapped_test_env {
    std::function<sstables::sstables_manager* ()> _get_mgr;
    std::optional<tmpdir> tmpdir_opt;
    fs::path _tmpdir_path;
public:
    wrapped_test_env(sstables::test_env& env)
        : _get_mgr([m = &env.manager()] { return m; })
        , _tmpdir_path(env.tempdir().path())
    {}
    // This variant this transportable across shards
    wrapped_test_env(sharded<sstables::test_env>& env)
        : _get_mgr([s = &env] { return &s->local().manager(); })
        , _tmpdir_path(env.local().tempdir().path())
    {}
    // This variant this transportable across shards
    wrapped_test_env(cql_test_env& env)
        : _get_mgr([&env] { return &env.db().local().get_user_sstables_manager(); })
        , tmpdir_opt(tmpdir())
        , _tmpdir_path(tmpdir_opt->path())
    {}
    sstables_manager& get_manager() { return *_get_mgr(); }
    const fs::path& tmpdir_path() noexcept { return _tmpdir_path; }
};

// Called from a seastar thread
static void with_sstable_directory(
    fs::path path,
    sstables::sstable_state state,
    wrapped_test_env env_wrap,
    noncopyable_function<void (sharded<sstable_directory>&)> func) {

    testlog.debug("with_sstable_directory: {}/{}", path, state);

    sharded<sstables::directory_semaphore> sstdir_sem;
    sstdir_sem.start(1).get();
    auto stop_sstdir_sem = defer([&sstdir_sem] {
        sstdir_sem.stop().get();
    });

    sharded<sstable_directory> sstdir;
    auto stop_sstdir = defer([&sstdir] {
        // The func is allowed to stop sstdir, and some tests actually do it
        if (sstdir.local_is_initialized()) {
            sstdir.stop().get();
        }
    });

    sstdir.start(seastar::sharded_parameter([&env_wrap] { return std::ref(env_wrap.get_manager()); }),
            seastar::sharded_parameter([] { return test_table_schema(); }),
            seastar::sharded_parameter([] { return std::ref(test_table_schema()->get_sharder()); }),
            path.native(), state, default_io_error_handler_gen()).get();

    func(sstdir);
}

static void with_sstable_directory(
        wrapped_test_env env_wrap,
        noncopyable_function<void (sharded<sstable_directory>&)> func) {
    with_sstable_directory(env_wrap.tmpdir_path(), sstables::sstable_state::normal, std::move(env_wrap), std::move(func));
}

SEASTAR_TEST_CASE(sstable_directory_test_table_simple_empty_directory_scan) {
    return sstables::test_env::do_with_async([] (test_env& env) {
        auto& dir = env.tempdir();

        // Write a manifest file to make sure it's ignored
        auto manifest = dir.path() / "manifest.json";
        tests::touch_file(manifest.native()).get();

        with_sstable_directory(env, [] (sharded<sstables::sstable_directory>& sstdir) {
            distributed_loader_for_tests::process_sstable_dir(sstdir, {}).get();
            auto max_generation_seen = highest_generation_seen(sstdir).get();
            // No generation found on empty directory.
            BOOST_REQUIRE(!max_generation_seen);
        });
    });
}

// Test unrecoverable SSTable: missing a file that is expected in the TOC.
SEASTAR_TEST_CASE(sstable_directory_test_table_scan_incomplete_sstables) {
    return sstables::test_env::do_with_async([] (test_env& env) {
        auto sst = make_sstable_for_this_shard(std::bind(new_sstable, std::ref(env), env.tempdir().path().native(), generation_type(this_shard_id())));

        // Now there is one sstable to the upload directory, but it is incomplete and one component is missing.
        // We should fail validation and leave the directory untouched
        remove_file(test(sst).filename(sstables::component_type::Statistics).native()).get();

        with_sstable_directory(env, [] (sharded<sstables::sstable_directory>& sstdir) {
            auto expect_malformed_sstable = distributed_loader_for_tests::process_sstable_dir(sstdir, {});
            BOOST_REQUIRE_THROW(expect_malformed_sstable.get(), sstables::malformed_sstable_exception);
        });
    });
}

// Test scanning a directory with unrecognized file
// reproducing https://github.com/scylladb/scylla/issues/10697
SEASTAR_TEST_CASE(sstable_directory_test_table_scan_invalid_file) {
    return sstables::test_env::do_with_async([] (test_env& env) {
        auto& dir = env.tempdir();
        auto sst = make_sstable_for_this_shard(std::bind(new_env_sstable, std::ref(env)));

        // Add a bogus file in the sstables directory
        auto name = dir.path() / "bogus";
        tests::touch_file(name.native()).get();

        with_sstable_directory(env, [] (sharded<sstables::sstable_directory>& sstdir) {
                auto expect_malformed_sstable = distributed_loader_for_tests::process_sstable_dir(sstdir, {});
                BOOST_REQUIRE_THROW(expect_malformed_sstable.get(), sstables::malformed_sstable_exception);
        });
    });
}

// Test always-benign incomplete SSTable: temporaryTOC found
SEASTAR_TEST_CASE(sstable_directory_test_table_temporary_toc) {
    return sstables::test_env::do_with_async([] (test_env& env) {
        auto sst = make_sstable_for_this_shard(std::bind(new_env_sstable, std::ref(env)));
        rename_file(test(sst).filename(sstables::component_type::TOC).native(), test(sst).filename(sstables::component_type::TemporaryTOC).native()).get();

        with_sstable_directory(env, [] (sharded<sstables::sstable_directory>& sstdir) {
            auto expect_ok = distributed_loader_for_tests::process_sstable_dir(sstdir, { .throw_on_missing_toc = true });
            BOOST_REQUIRE_NO_THROW(expect_ok.get());
        });
    });
}

// Test always-benign incomplete SSTable: with extraneous temporaryTOC found
SEASTAR_TEST_CASE(sstable_directory_test_table_extra_temporary_toc) {
    return sstables::test_env::do_with_async([] (test_env& env) {
        auto sst = make_sstable_for_this_shard(std::bind(new_env_sstable, std::ref(env)));
        link_file(test(sst).filename(sstables::component_type::TOC).native(), test(sst).filename(sstables::component_type::TemporaryTOC).native()).get();

        with_sstable_directory(env, [] (sharded<sstables::sstable_directory>& sstdir) {
            auto expect_ok = distributed_loader_for_tests::process_sstable_dir(sstdir, { .throw_on_missing_toc = true });
            BOOST_REQUIRE_NO_THROW(expect_ok.get());
        });
    });
}

// Test the absence of TOC. Behavior is controllable by a flag
SEASTAR_TEST_CASE(sstable_directory_test_table_missing_toc) {
    return sstables::test_env::do_with_async([] (test_env& env) {
        auto sst = make_sstable_for_this_shard(std::bind(new_env_sstable, std::ref(env)));
        remove_file(test(sst).filename(sstables::component_type::TOC).native()).get();

        with_sstable_directory(env, [] (sharded<sstables::sstable_directory>& sstdir_fatal) {
            auto expect_malformed_sstable  = distributed_loader_for_tests::process_sstable_dir(sstdir_fatal, { .throw_on_missing_toc = true });
            BOOST_REQUIRE_THROW(expect_malformed_sstable.get(), sstables::malformed_sstable_exception);
        });

        with_sstable_directory(env, [] (sharded<sstables::sstable_directory>& sstdir_ok) {
            auto expect_ok = distributed_loader_for_tests::process_sstable_dir(sstdir_ok, {});
            BOOST_REQUIRE_NO_THROW(expect_ok.get());
        });
    });
}

// Test the presence of TemporaryStatistics. If the old Statistics file is around
// this is benign and we'll just delete it and move on. If the old Statistics file
// is not around (but mentioned in the TOC), then this is an error.
SEASTAR_THREAD_TEST_CASE(sstable_directory_test_temporary_statistics) {
    sstables::test_env::do_with_sharded_async([] (sharded<test_env>& env) {
        auto sst = make_sstable_for_this_shard(std::bind(new_env_sstable, std::ref(env.local())));
        auto tempstr = test(sst).filename(component_type::TemporaryStatistics);
        tests::touch_file(tempstr.native()).get();
        auto tempstat = fs::canonical(tempstr);

        with_sstable_directory(env, [&] (sharded<sstables::sstable_directory>& sstdir_ok) {
            auto expect_ok = distributed_loader_for_tests::process_sstable_dir(sstdir_ok, {});
            BOOST_REQUIRE_NO_THROW(expect_ok.get());
            const auto& dir = env.local().tempdir();
            lister::scan_dir(dir.path(), lister::dir_entry_types::of<directory_entry_type::regular>(), [tempstat] (fs::path parent_dir, directory_entry de) {
                BOOST_REQUIRE(fs::canonical(parent_dir / fs::path(de.name)) != tempstat);
                return make_ready_future<>();
            }).get();
        });

        remove_file(test(sst).filename(sstables::component_type::Statistics).native()).get();

        with_sstable_directory(env, [] (sharded<sstables::sstable_directory>& sstdir_fatal) {
            auto expect_malformed_sstable  = distributed_loader_for_tests::process_sstable_dir(sstdir_fatal, {});
            BOOST_REQUIRE_THROW(expect_malformed_sstable.get(), sstables::malformed_sstable_exception);
        });
    }).get();
}

// Test that we see the right generation during the scan. Temporary files are skipped
SEASTAR_THREAD_TEST_CASE(sstable_directory_test_generation_sanity) {
    sstables::test_env::do_with_sharded_async([] (sharded<test_env>& env) {
        auto sst1 = make_sstable_for_this_shard(std::bind(new_env_sstable, std::ref(env.local())));
        auto sst2 = make_sstable_for_this_shard(std::bind(new_env_sstable, std::ref(env.local())));
        rename_file(test(sst2).filename(sstables::component_type::TOC).native(), test(sst2).filename(sstables::component_type::TemporaryTOC).native()).get();

        std::vector<bool> gen1_seen;
        gen1_seen.resize(smp::count);
        with_sstable_directory(env, [&] (sharded<sstables::sstable_directory>& sstdir) {
            distributed_loader_for_tests::process_sstable_dir(sstdir, { .throw_on_missing_toc = true }).get();
            sstdir.invoke_on_all([&] (sstables::sstable_directory& sstdir) {
                return seastar::async([&] {
                    sstdir.do_for_each_sstable([&] (const shared_sstable& sst) {
                        THREADSAFE_BOOST_REQUIRE_EQUAL(sst->generation(), sst1->generation());
                        THREADSAFE_BOOST_REQUIRE(!gen1_seen[this_shard_id()]);
                        gen1_seen[this_shard_id()] = true;
                        return make_ready_future<>();
                    }).get();
                });
            }).get();
        });
        BOOST_REQUIRE_EQUAL(std::count(gen1_seen.begin(), gen1_seen.end(), true), 1);
    }).get();
}

future<> verify_that_all_sstables_are_local(sharded<sstable_directory>& sstdir, unsigned expected_sstables) {
    return do_with(std::make_unique<std::atomic<unsigned>>(0), [&sstdir, expected_sstables] (std::unique_ptr<std::atomic<unsigned>>& count) {
        return sstdir.invoke_on_all([count = count.get()] (sstable_directory& d) {
            return d.do_for_each_sstable([count] (sstables::shared_sstable sst) {
                count->fetch_add(1, std::memory_order_relaxed);
                auto shards = sst->get_shards_for_this_sstable();
                THREADSAFE_BOOST_REQUIRE_EQUAL(shards.size(), 1);
                THREADSAFE_BOOST_REQUIRE_EQUAL(shards[0], this_shard_id());
                return make_ready_future<>();
            });
         }).then([count = count.get(), expected_sstables] {
            THREADSAFE_BOOST_REQUIRE_EQUAL(count->load(std::memory_order_relaxed), expected_sstables);
            return make_ready_future<>();
        });
    });
}

// Test that all SSTables are seen as unshared, if the generation numbers match what their
// shard-assignments expect
SEASTAR_THREAD_TEST_CASE(sstable_directory_unshared_sstables_sanity_matched_generations) {
    sstables::test_env::do_with_sharded_async([] (sharded<test_env>& env) {
        // Use the local env.tempdir since each shard has its own
        auto& dir = env.local().tempdir();

        sharded<sstables::sstable_generation_generator> sharded_gen;
        sharded_gen.start(0).get();
        auto stop_generator = deferred_stop(sharded_gen);

        for (shard_id i = 0; i < smp::count; ++i) {
            env.invoke_on(i, [dir = dir.path(), &sharded_gen] (sstables::test_env& env) {
                auto generation = std::invoke(sharded_gen.local());
                // this is why it is annoying for the internal functions in the test infrastructure to
                // assume threaded execution
                return seastar::async([dir, generation, &env] {
                    make_sstable_for_this_shard(std::bind(new_sstable, std::ref(env), dir, generation));
                });
            }).get();
        }

        with_sstable_directory(dir.path(), sstables::sstable_state::normal, env, [] (sharded<sstables::sstable_directory>& sstdir) {
            distributed_loader_for_tests::process_sstable_dir(sstdir, { .throw_on_missing_toc = true }).get();
            verify_that_all_sstables_are_local(sstdir, smp::count).get();
        });
    }).get();
}

// Test that all SSTables are seen as unshared, even if the generation numbers do not match what their
// shard-assignments expect
SEASTAR_THREAD_TEST_CASE(sstable_directory_unshared_sstables_sanity_unmatched_generations) {
    sstables::test_env::do_with_sharded_async([] (sharded<test_env>& env) {
        // Use the local env.tempdir since each shard has its own
        auto& dir = env.local().tempdir();

        sharded<sstables::sstable_generation_generator> sharded_gen;
        sharded_gen.start(0).get();
        auto stop_generator = deferred_stop(sharded_gen);

        for (shard_id i = 0; i < smp::count; ++i) {
            env.invoke_on(i, [dir = dir.path(), &sharded_gen] (sstables::test_env& env) -> future<> {
                // intentionally generate the generation on a different shard
                auto generation = co_await sharded_gen.invoke_on((this_shard_id() + 1) % smp::count, [] (auto& gen) {
                    return gen(sstables::uuid_identifiers::no);
                });
                // this is why it is annoying for the internal functions in the test infrastructure to
                // assume threaded execution
                co_return co_await seastar::async([dir, generation, &env] {
                    make_sstable_for_this_shard(std::bind(new_sstable, std::ref(env), dir, generation));
                });
            }).get();
        }

        with_sstable_directory(dir.path(), sstables::sstable_state::normal, env, [] (sharded<sstables::sstable_directory>& sstdir) {
            distributed_loader_for_tests::process_sstable_dir(sstdir, { .throw_on_missing_toc = true }).get();
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
        auto path = fs::path(e.local_db().find_column_family(ks_name, cf_name).dir());
        std::unordered_map<unsigned, std::vector<sstring>> sstables;

        testlog.debug("Inserting into cf");
        e.execute_cql("insert into cf (p, c) values ('one', 1)").get();

        testlog.debug("Flushing cf");
        e.db().invoke_on_all([&] (replica::database& db) {
            auto& cf = db.find_column_family(ks_name, cf_name);
            return cf.flush();
        }).get();

        with_sstable_directory(path, sstables::sstable_state::normal, e, [&] (sharded<sstable_directory>& sstdir) {
            distributed_loader_for_tests::process_sstable_dir(sstdir, {}).get();

            // Collect all sstable file names
            sstdir.invoke_on_all([&] (sstable_directory& d) {
                return d.do_for_each_sstable([&] (sstables::shared_sstable sst) {
                    sstables[this_shard_id()].push_back(test(sst).filename(sstables::component_type::Data).native());
                    return make_ready_future<>();
                });
            }).get();
            BOOST_REQUIRE_NE(sstables.size(), 0);

            distributed_loader_for_tests::lock_table(sstdir, e.db(), ks_name, cf_name).get();

            auto drop = e.execute_cql("drop table cf");

            auto table_exists = [&] () {
                try {
                    e.db().invoke_on_all([ks_name, cf_name] (replica::database& db) {
                        db.find_column_family(ks_name, cf_name);
                    }).get();
                    return true;
                } catch (replica::no_such_column_family&) {
                    return false;
                }
            };

            testlog.debug("Waiting until {}.{} is unlisted from the database", ks_name, cf_name);
            while (table_exists()) {
                yield().get();
            }

            auto all_sstables_exist = [&] () {
                std::unordered_map<bool, size_t> res;
                for (const auto& [shard, files] : sstables) {
                    for (const auto& f : files) {
                        res[file_exists(f).get()]++;
                    }
                }
                return res;
            };

            auto res = all_sstables_exist();
            BOOST_REQUIRE_EQUAL(res[false], 0);
            BOOST_REQUIRE_EQUAL(res[true], sstables.size());

            // Stop manually now, to allow for the object to be destroyed and take the
            // phaser with it.
            sstdir.stop().get();
            drop.get();

            BOOST_REQUIRE(!table_exists());

            res = all_sstables_exist();
            BOOST_REQUIRE_EQUAL(res[false], sstables.size());
            BOOST_REQUIRE_EQUAL(res[true], 0);
        });
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

        e.db().invoke_on_all([] (replica::database& db) {
            auto& cf = db.find_column_family("ks", "cf");
            return cf.disable_auto_compaction();
        }).get();

        unsigned num_sstables = 10 * smp::count;

        sharded<sstables::sstable_generation_generator> sharded_gen;
        sharded_gen.start(0).get();
        auto stop_generator = deferred_stop(sharded_gen);

        for (unsigned nr = 0; nr < num_sstables; ++nr) {
            auto generation = sharded_gen.invoke_on(nr % smp::count, [] (auto& gen) {
                return gen(sstables::uuid_identifiers::no);
            }).get();
            make_sstable_for_all_shards(e.db().local(), cf, sstables::sstable_state::upload, generation);
        }

      with_sstable_directory(fs::path(cf.dir()), sstables::sstable_state::upload, e, [&] (sharded<sstables::sstable_directory>& sstdir) {
        distributed_loader_for_tests::process_sstable_dir(sstdir, { .throw_on_missing_toc = true }).get();
        verify_that_all_sstables_are_local(sstdir, 0).get();

        sharded<sstables::sstable_generation_generator> sharded_gen;
        auto max_generation_seen = highest_generation_seen(sstdir).get();
        sharded_gen.start(max_generation_seen.as_int()).get();
        auto stop_generator = deferred_stop(sharded_gen);

        auto make_sstable = [&e, &sharded_gen] (shard_id shard) {
            auto generation = sharded_gen.invoke_on(shard, [] (auto& gen) {
                return gen(sstables::uuid_identifiers::no);
            }).get();
            auto& cf = e.local_db().find_column_family("ks", "cf");
            data_dictionary::storage_options local;
            return cf.get_sstables_manager().make_sstable(cf.schema(), cf.dir(), local, generation, sstables::sstable_state::upload);
        };
        distributed_loader_for_tests::reshard(sstdir, e.db(), "ks", "cf", std::move(make_sstable)).get();
        verify_that_all_sstables_are_local(sstdir, smp::count * smp::count).get();
      });
    });
}

// Regression test for #14618 - resharding with non-empty owned_ranges_ptr.
SEASTAR_TEST_CASE(sstable_directory_shared_sstables_reshard_correctly_with_owned_ranges) {
    if (smp::count == 1) {
        fmt::print("Skipping sstable_directory_shared_sstables_reshard_correctly, smp == 1\n");
        return make_ready_future<>();
    }

    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p text PRIMARY KEY, c int)").get();
        auto& cf = e.local_db().find_column_family("ks", "cf");
        auto upload_path = fs::path(cf.dir()) / sstables::upload_dir;

        e.db().invoke_on_all([] (replica::database& db) {
            auto& cf = db.find_column_family("ks", "cf");
            return cf.disable_auto_compaction();
        }).get();

        unsigned num_sstables = 10 * smp::count;

        sharded<sstables::sstable_generation_generator> sharded_gen;
        sharded_gen.start(0).get();
        auto stop_generator = deferred_stop(sharded_gen);

        for (unsigned nr = 0; nr < num_sstables; ++nr) {
            auto generation = sharded_gen.invoke_on(nr % smp::count, [] (auto& gen) {
                return gen(sstables::uuid_identifiers::no);
            }).get();
            make_sstable_for_all_shards(e.db().local(), cf, sstables::sstable_state::upload, generation);
        }

      with_sstable_directory(fs::path(cf.dir()), sstables::sstable_state::upload, e, [&] (sharded<sstables::sstable_directory>& sstdir) {
        distributed_loader_for_tests::process_sstable_dir(sstdir, { .throw_on_missing_toc = true }).get();
        verify_that_all_sstables_are_local(sstdir, 0).get();

        sharded<sstables::sstable_generation_generator> sharded_gen;
        auto max_generation_seen = highest_generation_seen(sstdir).get();
        sharded_gen.start(max_generation_seen.as_int()).get();
        auto stop_generator = deferred_stop(sharded_gen);

        auto make_sstable = [&e, upload_path, &sharded_gen] (shard_id shard) {
            auto generation = sharded_gen.invoke_on(shard, [] (auto& gen) {
                return gen(sstables::uuid_identifiers::no);
            }).get();
            auto& cf = e.local_db().find_column_family("ks", "cf");
            data_dictionary::storage_options local;
            return cf.get_sstables_manager().make_sstable(cf.schema(), cf.dir(), local, generation, sstables::sstable_state::upload);
        };
        const auto& erm = e.db().local().find_keyspace("ks").get_vnode_effective_replication_map();
        auto owned_ranges_ptr = compaction::make_owned_ranges_ptr(e.db().local().get_keyspace_local_ranges(erm).get());
        distributed_loader_for_tests::reshard(sstdir, e.db(), "ks", "cf", std::move(make_sstable), std::move(owned_ranges_ptr)).get();
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

        e.db().invoke_on_all([] (replica::database& db) {
            auto& cf = db.find_column_family("ks", "cf");
            return cf.disable_auto_compaction();
        }).get();

        unsigned num_sstables = 10 * smp::count;

        sharded<sstables::sstable_generation_generator> sharded_gen;
        sharded_gen.start(0).get();
        auto stop_generator = deferred_stop(sharded_gen);

        for (unsigned nr = 0; nr < num_sstables; ++nr) {
            // always generate the generation on shard#0
            auto generation = sharded_gen.invoke_on(0, [] (auto& gen) {
                return gen(sstables::uuid_identifiers::no);
            }).get();
            make_sstable_for_all_shards(e.db().local(), cf, sstables::sstable_state::upload, generation);
        }

      with_sstable_directory(fs::path(cf.dir()), sstables::sstable_state::upload, e, [&e] (sharded<sstables::sstable_directory>& sstdir) {
        distributed_loader_for_tests::process_sstable_dir(sstdir, { .throw_on_missing_toc = true }).get();
        verify_that_all_sstables_are_local(sstdir, 0).get();

        sharded<sstables::sstable_generation_generator> sharded_gen;
        auto max_generation_seen = highest_generation_seen(sstdir).get();
        sharded_gen.start(max_generation_seen.as_int()).get();
        auto stop_generator = deferred_stop(sharded_gen);

        auto make_sstable = [&e, &sharded_gen] (shard_id shard) {
            auto generation = sharded_gen.invoke_on(shard, [] (auto& gen) {
                return gen(sstables::uuid_identifiers::no);
            }).get();
            auto& cf = e.local_db().find_column_family("ks", "cf");
            data_dictionary::storage_options local;
            return cf.get_sstables_manager().make_sstable(cf.schema(), cf.dir(), local, generation, sstables::sstable_state::upload);
        };
        distributed_loader_for_tests::reshard(sstdir, e.db(), "ks", "cf", std::move(make_sstable)).get();
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

        e.db().invoke_on_all([] (replica::database& db) {
            auto& cf = db.find_column_family("ks", "cf");
            return cf.disable_auto_compaction();
        }).get();

        unsigned num_sstables = (cf.schema()->max_compaction_threshold() + 1) * smp::count;

        sharded<sstables::sstable_generation_generator> sharded_gen;
        sharded_gen.start(0).get();
        auto stop_generator = deferred_stop(sharded_gen);

        for (unsigned nr = 0; nr < num_sstables; ++nr) {
            auto generation = sharded_gen.invoke_on(nr % smp::count, [] (auto& gen) {
                return gen(sstables::uuid_identifiers::no);
            }).get();
            make_sstable_for_all_shards(e.db().local(), cf, sstables::sstable_state::upload, generation);
        }

      with_sstable_directory(fs::path(cf.dir()), sstables::sstable_state::upload, e, [&] (sharded<sstables::sstable_directory>& sstdir) {
        distributed_loader_for_tests::process_sstable_dir(sstdir, { .throw_on_missing_toc = true }).get();
        verify_that_all_sstables_are_local(sstdir, 0).get();

        sharded<sstables::sstable_generation_generator> sharded_gen;
        auto max_generation_seen = highest_generation_seen(sstdir).get();
        sharded_gen.start(max_generation_seen.as_int()).get();
        auto stop_generator = deferred_stop(sharded_gen);

        auto make_sstable = [&e, &sharded_gen] (shard_id shard) {
            auto generation = sharded_gen.invoke_on(shard, [] (auto& gen) {
                return gen(sstables::uuid_identifiers::no);
            }).get();
            auto& cf = e.local_db().find_column_family("ks", "cf");
            data_dictionary::storage_options local;
            return cf.get_sstables_manager().make_sstable(cf.schema(), cf.dir(), local, generation, sstables::sstable_state::upload);
        };
        distributed_loader_for_tests::reshard(sstdir, e.db(), "ks", "cf", std::move(make_sstable)).get();
        verify_that_all_sstables_are_local(sstdir, 2 * smp::count * smp::count).get();
      });
    });
}

SEASTAR_THREAD_TEST_CASE(test_multiple_data_dirs) {
    std::vector<tmpdir> data_dirs;
    data_dirs.resize(2);
    sstring ks_name = "ks";
    sstring tbl_name = "test";
    sstring uuid_sstring;
    cql_test_config cfg;
    cfg.db_config->data_file_directories({
        data_dirs[0].path().native(),
        data_dirs[1].path().native()
    }, db::config::config_source::CommandLine);
    do_with_cql_env_thread([&] (cql_test_env& e) {
        e.execute_cql(format("create table {}.{} (p text PRIMARY KEY, c int)", ks_name, tbl_name)).get();
        auto id = e.local_db().find_uuid(ks_name, tbl_name);
        uuid_sstring = id.to_sstring();
        boost::erase_all(uuid_sstring, "-");
        e.execute_cql(format("insert into {}.{} (p, c) values ('one', 1)", ks_name, tbl_name)).get();
        e.execute_cql(format("insert into {}.{} (p, c) values ('two', 2)", ks_name, tbl_name)).get();
    }, cfg).get();

    sstring tbl_dirname = tbl_name + "-" + uuid_sstring;
    BOOST_REQUIRE(file_exists((data_dirs[0].path() / ks_name / tbl_dirname).native()).get());
    BOOST_REQUIRE(file_exists((data_dirs[1].path() / ks_name / tbl_dirname).native()).get());

    do_with_cql_env_thread([&] (cql_test_env& e) {
        auto res = e.execute_cql(format("select * from {}.{}", ks_name, tbl_name)).get();
        assert_that(res).is_rows().with_size(2).with_rows({
            { utf8_type->decompose("one"), int32_type->decompose(1) },
            { utf8_type->decompose("two"), int32_type->decompose(2) }
        });
    }, cfg).get();
}

fs::path table_dirname(cql_test_env& e, const fs::path& root, sstring ks, sstring cf) {
    auto id = e.local_db().find_uuid(ks, cf);
    sstring uuid_sstring = id.to_sstring();
    boost::erase_all(uuid_sstring, "-");
    return root / ks / (cf + "-" + uuid_sstring);
}

SEASTAR_THREAD_TEST_CASE(test_user_datadir_layout) {
    sstring ks = "ks";
    sstring cf = "test";
    tmpdir data_dir;
    fs::path tbl_dirname;
    cql_test_config cfg;
    cfg.db_config->data_file_directories({
        data_dir.path().native(),
    }, db::config::config_source::CommandLine);

    do_with_cql_env_thread([&] (cql_test_env& e) {
        e.execute_cql(format("create table {}.{} (p text PRIMARY KEY, c int)", ks, cf)).get();
        tbl_dirname = table_dirname(e, data_dir.path(), ks, cf);
        testlog.info("Checking {}.{}: {}", ks, cf, tbl_dirname);
    }, cfg).get();

    BOOST_REQUIRE(file_exists(tbl_dirname.native()).get());
    BOOST_REQUIRE(file_exists((tbl_dirname / sstables::upload_dir).native()).get());
    BOOST_REQUIRE(file_exists((tbl_dirname / sstables::staging_dir).native()).get());

    seastar::recursive_remove_directory(tbl_dirname.native()).get();
    do_with_cql_env_thread([&] (cql_test_env& e) { /* nothing -- just populate */ }, cfg).get();

    BOOST_REQUIRE(file_exists(tbl_dirname.native()).get());
    BOOST_REQUIRE(file_exists((tbl_dirname / sstables::upload_dir).native()).get());
    BOOST_REQUIRE(file_exists((tbl_dirname / sstables::staging_dir).native()).get());
}

SEASTAR_THREAD_TEST_CASE(test_system_datadir_layout) {
    tmpdir data_dir;
    cql_test_config cfg;
    cfg.db_config->data_file_directories({
        data_dir.path().native(),
    }, db::config::config_source::CommandLine);

    do_with_cql_env_thread([&] (cql_test_env& e) {
        auto tbl_dirname = table_dirname(e, data_dir.path(), "system", "local");
        testlog.info("Checking system.local: {}", tbl_dirname);

        BOOST_REQUIRE(file_exists(tbl_dirname.native()).get());
        BOOST_REQUIRE(file_exists((tbl_dirname / sstables::upload_dir).native()).get());
        BOOST_REQUIRE(file_exists((tbl_dirname / sstables::staging_dir).native()).get());

        tbl_dirname = table_dirname(e, data_dir.path(), "system", "config");
        testlog.info("Checking system.config: {}", tbl_dirname);

        BOOST_REQUIRE(!file_exists(tbl_dirname.native()).get());
    }, cfg).get();
}

SEASTAR_TEST_CASE(test_pending_log_garbage_collection) {
    return sstables::test_env::do_with_sharded_async([] (auto& env) {
      for (auto state : {sstables::sstable_state::normal, sstables::sstable_state::staging}) {
        auto base = env.local().tempdir().path() / fmt::to_string(table_id::create_random_id());
        auto dir = base / fmt::to_string(state);
        recursive_touch_directory(dir.native()).get();

        auto new_sstable = [&] {
            return env.local().make_sstable(test_table_schema(), dir.native());
        };
        std::vector<shared_sstable> ssts_to_keep;
        for (int i = 0; i < 2; i++) {
            ssts_to_keep.emplace_back(make_sstable_for_this_shard(new_sstable));
        }
        testlog.debug("SSTables to keep: {}", ssts_to_keep);
        std::vector<shared_sstable> ssts_to_remove;
        for (int i = 0; i < 3; i++) {
            ssts_to_remove.emplace_back(make_sstable_for_this_shard(new_sstable));
        }
        testlog.debug("SSTables to remove: {}", ssts_to_remove);

        // Now start atomic deletion -- create the pending deletion log for all
        // three sstables, move TOC file for one of them into temporary-TOC, and 
        // partially delete another
        sstable_directory::create_pending_deletion_log(ssts_to_remove).get();
        rename_file(test(ssts_to_remove[1]).filename(sstables::component_type::TOC).native(), test(ssts_to_remove[1]).filename(sstables::component_type::TemporaryTOC).native()).get();
        rename_file(test(ssts_to_remove[2]).filename(sstables::component_type::TOC).native(), test(ssts_to_remove[2]).filename(sstables::component_type::TemporaryTOC).native()).get();
        remove_file(test(ssts_to_remove[2]).filename(sstables::component_type::Data).native()).get();

        // mimic distributed_loader table_populator::start order
        // as the pending_delete_dir is now shared, at the table base directory
        if (state != sstables::sstable_state::normal) {
            with_sstable_directory(base, sstables::sstable_state::normal, env, [&] (sharded<sstables::sstable_directory>& sstdir) {
                auto expect_ok = distributed_loader_for_tests::process_sstable_dir(sstdir, { .throw_on_missing_toc = true, .garbage_collect = true });
                BOOST_REQUIRE_NO_THROW(expect_ok.get());
            });
        }

        with_sstable_directory(base, state, env, [&] (sharded<sstables::sstable_directory>& sstdir) {
            auto expect_ok = distributed_loader_for_tests::process_sstable_dir(sstdir, { .throw_on_missing_toc = true, .garbage_collect = true });
            BOOST_REQUIRE_NO_THROW(expect_ok.get());

            auto collected = sstdir.map_reduce0(
                [] (auto& sstdir) {
                    return do_with(std::set<sstables::generation_type>(), [&sstdir] (auto& gens) {
                        return sstdir.do_for_each_sstable([&] (const shared_sstable& sst) {
                            gens.emplace(sst->generation());
                            return make_ready_future<>();
                        }).then([&gens] () mutable -> future<std::set<sstables::generation_type>> {
                            return make_ready_future<std::set<sstables::generation_type>>(std::move(gens));;
                        });
                    });
                }, std::set<sstables::generation_type>(),
                [] (auto&& res, auto&& gens) {
                    res.merge(gens);
                    return std::move(res);
                }
            ).get();

            std::set<sstables::generation_type> expected;
            for (auto& sst : ssts_to_keep) {
                expected.insert(sst->generation());
            }

            BOOST_REQUIRE_EQUAL(expected, collected);
        });
      }
    });
}
