/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/do_with.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/defer.hh>

#include "data_dictionary/storage_options.hh"
#include "db/config.hh"
#include "db/large_data_handler.hh"
#include "gms/feature_service.hh"
#include "sstables/version.hh"
#include "sstables/sstable_directory.hh"
#include "replica/database.hh"

#include "test/lib/tmpdir.hh"
#include "test/lib/test_services.hh"
#include "test/lib/log.hh"

namespace sstables {

class test_env_sstables_manager : public sstables_manager {
    using sstables_manager::sstables_manager;
    std::optional<size_t> _promoted_index_block_size;
public:
    virtual sstable_writer_config configure_writer(sstring origin = "test") const override {
        auto ret = sstables_manager::configure_writer(std::move(origin));
        if (_promoted_index_block_size) {
            ret.promoted_index_block_size = *_promoted_index_block_size;
        }
        return ret;
    }

    void set_promoted_index_block_size(size_t promoted_index_block_size) {
        _promoted_index_block_size = promoted_index_block_size;
    }
};

struct test_env_config {
    db::large_data_handler* large_data_handler = nullptr;
    data_dictionary::storage_options storage; // will be local by default
    bool use_uuid = false;
};

data_dictionary::storage_options make_test_object_storage_options();

class test_env {
    struct impl {
        tmpdir dir;
        std::unique_ptr<db::config> db_config;
        directory_semaphore dir_sem;
        ::cache_tracker cache_tracker;
        gms::feature_service feature_service;
        db::nop_large_data_handler nop_ld_handler;
        test_env_sstables_manager mgr;
        reader_concurrency_semaphore semaphore;
        sstables::sstable_generation_generator gen{0};
        sstables::uuid_identifiers use_uuid;
        data_dictionary::storage_options storage;

        impl(test_env_config cfg, sstables::storage_manager* sstm);
        impl(impl&&) = delete;
        impl(const impl&) = delete;

        sstables::generation_type new_generation() noexcept {
            return gen(use_uuid);
        }
    };
    std::unique_ptr<impl> _impl;
public:

    explicit test_env(test_env_config cfg = {}, sstables::storage_manager* sstm = nullptr) : _impl(std::make_unique<impl>(std::move(cfg), sstm)) { }

    future<> stop() {
        return _impl->mgr.close().finally([this] {
            return _impl->semaphore.stop();
        });
    }

    sstables::generation_type new_generation() noexcept {
        return _impl->new_generation();
    }

    shared_sstable make_sstable(schema_ptr schema, sstring dir, sstables::generation_type generation,
            sstable::version_types v = sstables::get_highest_sstable_version(), sstable::format_types f = sstable::format_types::big,
            size_t buffer_size = default_sstable_buffer_size, gc_clock::time_point now = gc_clock::now()) {
        return _impl->mgr.make_sstable(std::move(schema), dir, _impl->storage, generation, sstables::sstable_state::normal, v, f, now, default_io_error_handler_gen(), buffer_size);
    }

    shared_sstable make_sstable(schema_ptr schema, sstring dir, sstable::version_types v = sstables::get_highest_sstable_version()) {
        return make_sstable(std::move(schema), std::move(dir), new_generation(), std::move(v));
    }

    shared_sstable make_sstable(schema_ptr schema, sstables::generation_type generation,
            sstable::version_types v = sstables::get_highest_sstable_version(), sstable::format_types f = sstable::format_types::big,
            size_t buffer_size = default_sstable_buffer_size, gc_clock::time_point now = gc_clock::now()) {
        return make_sstable(std::move(schema), _impl->dir.path().native(), generation, std::move(v), std::move(f), buffer_size, now);
    }

    shared_sstable make_sstable(schema_ptr schema, sstable::version_types v = sstables::get_highest_sstable_version()) {
        return make_sstable(std::move(schema), _impl->dir.path().native(), std::move(v));
    }

    std::function<shared_sstable()> make_sst_factory(schema_ptr s) {
        return [this, s = std::move(s)] {
            return make_sstable(s, new_generation());
        };
    }

    std::function<shared_sstable()> make_sst_factory(schema_ptr s, sstable::version_types version) {
        return [this, s = std::move(s), version] {
            return make_sstable(s, new_generation(), version);
        };
    }

    struct sst_not_found : public std::runtime_error {
        sst_not_found(const sstring& dir, sstables::generation_type generation)
            : std::runtime_error(format("no versions of sstable generation {} found in {}", generation, dir))
        {}
    };

    // reusable_sst() opens the requested sstable for reading only (sstables are
    // immutable, so an existing sstable cannot be opened for writing).
    // It returns a future because opening requires reading from disk, and
    // therefore may block. The future value is a shared sstable - a reference-
    // counting pointer to an sstable - allowing for the returned handle to
    // be passed around until no longer needed.
    future<shared_sstable> reusable_sst(schema_ptr schema, sstring dir, sstables::generation_type generation,
            sstable::version_types version, sstable::format_types f = sstable::format_types::big) {
        auto sst = make_sstable(std::move(schema), dir, generation, version, f);
        sstable_open_config cfg { .load_first_and_last_position_metadata = true };
        return sst->load(sst->get_schema()->get_sharder(), cfg).then([sst = std::move(sst)] {
            return make_ready_future<shared_sstable>(std::move(sst));
        });
    }
    future<shared_sstable> reusable_sst(schema_ptr schema, sstring dir, sstables::generation_type::int_t gen_value,
            sstable::version_types version, sstable::format_types f = sstable::format_types::big) {
        return reusable_sst(std::move(schema), std::move(dir), sstables::generation_type(gen_value), version, f);
    }

    future<shared_sstable> reusable_sst(schema_ptr schema, sstables::generation_type generation,
            sstable::version_types version, sstable::format_types f = sstable::format_types::big) {
        return reusable_sst(std::move(schema), _impl->dir.path().native(), std::move(generation), std::move(version), std::move(f));
    }

    future<shared_sstable> reusable_sst(schema_ptr schema, shared_sstable sst) {
        return reusable_sst(std::move(schema), sst->get_storage().prefix(), sst->generation(), sst->get_version());
    }

    future<shared_sstable> reusable_sst(shared_sstable sst) {
        return reusable_sst(sst->get_schema(), std::move(sst));
    }

    // looks up the sstable in the given dir
    future<shared_sstable> reusable_sst(schema_ptr schema, sstring dir, sstables::generation_type generation = sstables::generation_type{1});

    future<shared_sstable> reusable_sst(schema_ptr schema, sstables::generation_type generation) {
        return reusable_sst(std::move(schema), _impl->dir.path().native(), generation);
    }

    test_env_sstables_manager& manager() { return _impl->mgr; }
    reader_concurrency_semaphore& semaphore() { return _impl->semaphore; }
    db::config& db_config() { return *_impl->db_config; }
    tmpdir& tempdir() noexcept { return _impl->dir; }
    data_dictionary::storage_options get_storage_options() const noexcept { return _impl->storage; }

    reader_permit make_reader_permit(const schema* const s, const char* n, db::timeout_clock::time_point timeout) {
        return _impl->semaphore.make_tracking_only_permit(s, n, timeout, {});
    }
    reader_permit make_reader_permit(db::timeout_clock::time_point timeout = db::no_timeout) {
        return _impl->semaphore.make_tracking_only_permit(nullptr, "test", timeout, {});
    }

    replica::table::config make_table_config() {
        return replica::table::config{.compaction_concurrency_semaphore = &_impl->semaphore};
    }

    template <typename Func>
    static inline auto do_with(Func&& func, test_env_config cfg = {}) {
        return seastar::do_with(test_env(std::move(cfg)), [func = std::move(func)] (test_env& env) mutable {
            return futurize_invoke(func, env).finally([&env] {
                return env.stop();
            });
        });
    }

    static future<> do_with_async(noncopyable_function<void (test_env&)> func, test_env_config cfg = {});

    static inline future<> do_with_sharded_async(noncopyable_function<void (sharded<test_env>&)> func) {
        return seastar::async([func = std::move(func)] {
            sharded<test_env> env;
            env.start().get();
            auto stop = defer([&] { env.stop().get(); });
            func(env);
        });
    }

    template <typename T>
    static future<T> do_with_async_returning(noncopyable_function<T (test_env&)> func) {
        return seastar::async([func = std::move(func)] {
            test_env env;
            auto stop = defer([&] { env.stop().get(); });
            return func(env);
        });
    }

    table_for_tests make_table_for_tests(schema_ptr s, sstring dir) {
        return table_for_tests(manager(), s, std::move(dir), _impl->storage);
    }

    table_for_tests make_table_for_tests(schema_ptr s = nullptr) {
        return table_for_tests(manager(), s, tempdir().path().native(), _impl->storage);
    }
};

}   // namespace sstables
