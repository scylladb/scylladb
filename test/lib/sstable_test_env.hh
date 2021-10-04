/*
 * Copyright (C) 2019-present ScyllaDB
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

#pragma once

#include <seastar/core/do_with.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/defer.hh>

#include "sstables/sstables.hh"
#include "test/lib/tmpdir.hh"
#include "test/lib/test_services.hh"
#include "test/lib/log.hh"
#include "test/lib/schema_registry.hh"

namespace sstables {

class test_env_sstables_manager : public sstables_manager {
    using sstables_manager::sstables_manager;
public:
    sstable_writer_config configure_writer(sstring origin = "test") const {
        return sstables_manager::configure_writer(std::move(origin));
    }
};

class test_env {
    std::unique_ptr<cache_tracker> _cache_tracker;
    std::unique_ptr<test_env_sstables_manager> _mgr;
    std::unique_ptr<reader_concurrency_semaphore> _semaphore;
    std::unique_ptr<tests::schema_registry_wrapper> _registry;
public:
    explicit test_env()
        : _cache_tracker(std::make_unique<cache_tracker>())
        , _mgr(std::make_unique<test_env_sstables_manager>(nop_lp_handler, test_db_config, test_feature_service, *_cache_tracker))
        , _semaphore(std::make_unique<reader_concurrency_semaphore>(reader_concurrency_semaphore::no_limits{}, "sstables::test_env"))
        , _registry(std::make_unique<tests::schema_registry_wrapper>()) { }

    future<> stop() {
        return _mgr->close().finally([this] {
            return _semaphore->stop();
        });
    }

    shared_sstable make_sstable(schema_ptr schema, sstring dir, unsigned long generation,
            sstable::version_types v = sstables::get_highest_sstable_version(), sstable::format_types f = sstable::format_types::big,
            size_t buffer_size = default_sstable_buffer_size, gc_clock::time_point now = gc_clock::now()) {
        return _mgr->make_sstable(std::move(schema), dir, generation, v, f, now, default_io_error_handler_gen(), buffer_size);
    }

    struct sst_not_found : public std::runtime_error {
        sst_not_found(const sstring& dir, unsigned long generation)
            : std::runtime_error(format("no versions of sstable generation {} found in {}", generation, dir))
        {}
    };

    future<shared_sstable> reusable_sst(schema_ptr schema, sstring dir, unsigned long generation,
            sstable::version_types version, sstable::format_types f = sstable::format_types::big) {
        auto sst = make_sstable(std::move(schema), dir, generation, version, f);
        return sst->load().then([sst = std::move(sst)] {
            return make_ready_future<shared_sstable>(std::move(sst));
        });
    }

    // looks up the sstable in the given dir
    future<shared_sstable> reusable_sst(schema_ptr schema, sstring dir, unsigned long generation);

    test_env_sstables_manager& manager() { return *_mgr; }
    reader_concurrency_semaphore& semaphore() { return *_semaphore; }
    reader_permit make_reader_permit(const schema* const s, const char* n, db::timeout_clock::time_point timeout) {
        return _semaphore->make_tracking_only_permit(s, n, timeout);
    }
    reader_permit make_reader_permit(db::timeout_clock::time_point timeout = db::no_timeout) {
        return _semaphore->make_tracking_only_permit(nullptr, "test", timeout);
    }

    schema_registry& registry() { return *_registry; }

    future<> working_sst(schema_ptr schema, sstring dir, unsigned long generation) {
        return reusable_sst(std::move(schema), dir, generation).then([] (auto ptr) { return make_ready_future<>(); });
    }

    template <typename Func>
    static inline auto do_with(Func&& func) {
        return seastar::do_with(test_env(), [func = std::move(func)] (test_env& env) mutable {
            return futurize_invoke(func, env).finally([&env] {
                return env.stop();
            });
        });
    }

    template <typename T, typename Func>
    static inline auto do_with(T&& rval, Func&& func) {
        return seastar::do_with(test_env(), std::forward<T>(rval), [func = std::move(func)] (test_env& env, T& val) mutable {
            return futurize_invoke(func, env, val).finally([&env] {
                return env.stop();
            });
        });
    }

    static inline future<> do_with_async(noncopyable_function<void (test_env&)> func) {
        return seastar::async([func = std::move(func)] {
            test_env env;
            auto close_env = defer([&] { env.stop().get(); });
            func(env);
        });
    }

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
};

}   // namespace sstables
