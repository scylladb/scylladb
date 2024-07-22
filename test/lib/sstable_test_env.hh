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
#include "db/large_data_handler.hh"
#include "sstables/version.hh"
#include "sstables/sstable_directory.hh"
#include "compaction/compaction_manager.hh"

#include "test/lib/tmpdir.hh"
#include "test/lib/test_services.hh"
#include "test/lib/log.hh"

namespace compaction {
class table_state;
class compaction_task_executor;
}

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

    void increment_total_reclaimable_memory_and_maybe_reclaim(sstable *sst) {
        sstables_manager::increment_total_reclaimable_memory_and_maybe_reclaim(sst);
    }

    size_t get_total_memory_reclaimed() {
        return _total_memory_reclaimed;
    }

    size_t get_total_reclaimable_memory() {
        return _total_reclaimable_memory;
    }

    void remove_sst_from_reclaimed(sstable* sst) {
        _reclaimed.erase(*sst);
    }

    auto& get_reclaimed_set() {
        return _reclaimed;
    }
};

class test_env_compaction_manager {
    tasks::task_manager _tm;
    compaction_manager _cm;

public:
    test_env_compaction_manager()
        : _cm(_tm, compaction_manager::for_testing_tag{})
    {}

    compaction_manager& get_compaction_manager() { return _cm; }

    void propagate_replacement(compaction::table_state& table_s, const std::vector<shared_sstable>& removed, const std::vector<shared_sstable>& added);

    future<> perform_compaction(shared_ptr<compaction::compaction_task_executor> task);
};

struct test_env_config {
    db::large_data_handler* large_data_handler = nullptr;
    data_dictionary::storage_options storage; // will be local by default
    bool use_uuid = true;
    size_t available_memory = memory::stats().total_memory();
};

data_dictionary::storage_options make_test_object_storage_options();

class test_env {
    struct impl;
    std::unique_ptr<impl> _impl;
public:

    void maybe_start_compaction_manager(bool enable = true);

    explicit test_env(test_env_config cfg = {}, sstables::storage_manager* sstm = nullptr);
    ~test_env();
    test_env(test_env&&) noexcept;

    future<> stop();

    sstables::generation_type new_generation() noexcept;

    shared_sstable make_sstable(schema_ptr schema, sstring dir, sstables::generation_type generation,
            sstable::version_types v = sstables::get_highest_sstable_version(), sstable::format_types f = sstable::format_types::big,
            size_t buffer_size = default_sstable_buffer_size, gc_clock::time_point now = gc_clock::now());

    shared_sstable make_sstable(schema_ptr schema, sstring dir, sstable::version_types v = sstables::get_highest_sstable_version());

    shared_sstable make_sstable(schema_ptr schema, sstables::generation_type generation,
            sstable::version_types v = sstables::get_highest_sstable_version(), sstable::format_types f = sstable::format_types::big,
            size_t buffer_size = default_sstable_buffer_size, gc_clock::time_point now = gc_clock::now());

    shared_sstable make_sstable(schema_ptr schema, sstable::version_types v = sstables::get_highest_sstable_version());

    std::function<shared_sstable()> make_sst_factory(schema_ptr s);

    std::function<shared_sstable()> make_sst_factory(schema_ptr s, sstable::version_types version);

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
            sstable::version_types version, sstable::format_types f = sstable::format_types::big);

    future<shared_sstable> reusable_sst(schema_ptr schema, sstring dir, sstables::generation_type::int_t gen_value,
            sstable::version_types version, sstable::format_types f = sstable::format_types::big);

    future<shared_sstable> reusable_sst(schema_ptr schema, sstables::generation_type generation,
            sstable::version_types version, sstable::format_types f = sstable::format_types::big);

    future<shared_sstable> reusable_sst(schema_ptr schema, shared_sstable sst);

    future<shared_sstable> reusable_sst(shared_sstable sst);

    // looks up the sstable in the given dir
    future<shared_sstable> reusable_sst(schema_ptr schema, sstring dir, sstables::generation_type generation = sstables::generation_type{1});

    future<shared_sstable> reusable_sst(schema_ptr schema, sstables::generation_type generation);

    test_env_sstables_manager& manager();
    test_env_compaction_manager& test_compaction_manager();
    reader_concurrency_semaphore& semaphore();
    db::config& db_config();
    tmpdir& tempdir() noexcept;
    data_dictionary::storage_options get_storage_options() const noexcept;

    reader_permit make_reader_permit(const schema_ptr &s, const char* n, db::timeout_clock::time_point timeout);
    reader_permit make_reader_permit(db::timeout_clock::time_point timeout = db::no_timeout);

    replica::table::config make_table_config();

    template <typename Func>
    static inline auto do_with(Func&& func, test_env_config cfg = {}) {
        return seastar::do_with(test_env(std::move(cfg)), [func = std::move(func)] (test_env& env) mutable {
            return futurize_invoke(func, env).finally([&env] {
                return env.stop();
            });
        });
    }

    static future<> do_with_async(noncopyable_function<void (test_env&)> func, test_env_config cfg = {});

    static future<> do_with_sharded_async(noncopyable_function<void (sharded<test_env>&)> func);

    template <typename T>
    static future<T> do_with_async_returning(noncopyable_function<T (test_env&)> func) {
        return seastar::async([func = std::move(func)] {
            test_env env;
            auto stop = defer([&] { env.stop().get(); });
            return func(env);
        });
    }

    table_for_tests make_table_for_tests(schema_ptr s, sstring dir);

    table_for_tests make_table_for_tests(schema_ptr s = nullptr);

    void request_abort();
};

}   // namespace sstables
