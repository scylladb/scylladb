/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include <seastar/util/closeable.hh>
#include <seastar/core/file.hh>
#include "reader_concurrency_semaphore.hh"
#include "sstables/sstables_manager.hh"
#include "test/lib/log.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/eventually.hh"
#include "test/lib/key_utils.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/random_schema.hh"
#include "test/lib/test_utils.hh"
#include "test/lib/tmpdir.hh"

#include <fmt/ranges.h>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include <boost/test/unit_test.hpp>
#include "readers/empty_v2.hh"
#include "readers/from_mutations_v2.hh"
#include "replica/database.hh" // new_reader_base_cost is there :(
#include "db/config.hh"

SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_clear_inactive_reads) {
    simple_schema s;
    std::vector<reader_permit> permits;
    std::vector<reader_concurrency_semaphore::inactive_read_handle> handles;

    {
        reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::no_limits{}, get_name(), reader_concurrency_semaphore::register_metrics::no);
        auto stop_sem = deferred_stop(semaphore);
        auto clear_permits = defer([&permits] { permits.clear(); });

        for (int i = 0; i < 10; ++i) {
            permits.emplace_back(semaphore.make_tracking_only_permit(s.schema(), get_name(), db::no_timeout, {}));
            handles.emplace_back(semaphore.register_inactive_read(make_empty_flat_reader_v2(s.schema(), permits.back())));
        }

        BOOST_REQUIRE(std::all_of(handles.begin(), handles.end(), [] (const reader_concurrency_semaphore::inactive_read_handle& handle) { return bool(handle); }));
        BOOST_REQUIRE(std::all_of(permits.begin(), permits.end(), [] (const reader_permit& permit) { return permit.get_state() == reader_permit::state::inactive; }));

        semaphore.clear_inactive_reads();

        BOOST_REQUIRE(std::all_of(handles.begin(), handles.end(), [] (const reader_concurrency_semaphore::inactive_read_handle& handle) { return !bool(handle); }));
        BOOST_REQUIRE(std::all_of(permits.begin(), permits.end(), [] (const reader_permit& permit) { return permit.get_state() == reader_permit::state::evicted; }));

        BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 0);

        permits.clear();
        handles.clear();

        for (int i = 0; i < 10; ++i) {
            handles.emplace_back(semaphore.register_inactive_read(make_empty_flat_reader_v2(s.schema(), semaphore.make_tracking_only_permit(s.schema(), get_name(), db::no_timeout, {}))));
        }

        BOOST_REQUIRE(std::all_of(handles.begin(), handles.end(), [] (const reader_concurrency_semaphore::inactive_read_handle& handle) { return bool(handle); }));
    }

    // Check that the destructor also clears inactive reads.
    BOOST_REQUIRE(std::all_of(handles.begin(), handles.end(), [] (const reader_concurrency_semaphore::inactive_read_handle& handle) { return !bool(handle); }));
}

SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_destroyed_permit_releases_units) {
    simple_schema s;
    const auto initial_resources = reader_concurrency_semaphore::resources{10, 1024 * 1024};
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, get_name(), initial_resources.count, initial_resources.memory);
    auto stop_sem = deferred_stop(semaphore);

    // Not admitted, active
    {
        auto permit = semaphore.make_tracking_only_permit(s.schema(), get_name(), db::no_timeout, {});
        auto units2 = permit.consume_memory(1024);
    }
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources);

    // Not admitted, inactive
    {
        auto permit = semaphore.make_tracking_only_permit(s.schema(), get_name(), db::no_timeout, {});
        auto units2 = permit.consume_memory(1024);

        auto handle = semaphore.register_inactive_read(make_empty_flat_reader_v2(s.schema(), permit));
        BOOST_REQUIRE(semaphore.try_evict_one_inactive_read());
    }
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources);

    // Admitted, active
    {
        auto permit = semaphore.obtain_permit(s.schema(), get_name(), 1024, db::no_timeout, {}).get();
        auto units1 = permit.consume_memory(1024);
    }
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources);

    // Admitted, inactive
    {
        auto permit = semaphore.obtain_permit(s.schema(), get_name(), 1024, db::no_timeout, {}).get();
        auto units1 = permit.consume_memory(1024);

        auto handle = semaphore.register_inactive_read(make_empty_flat_reader_v2(s.schema(), permit));
        BOOST_REQUIRE(semaphore.try_evict_one_inactive_read());
    }
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources);
}

SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_abandoned_handle_closes_reader) {
    simple_schema s;
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::no_limits{}, get_name(), reader_concurrency_semaphore::register_metrics::no);
    auto stop_sem = deferred_stop(semaphore);

    auto permit = semaphore.make_tracking_only_permit(s.schema(), get_name(), db::no_timeout, {});
    {
        auto handle = semaphore.register_inactive_read(make_empty_flat_reader_v2(s.schema(), permit));
        // The handle is destroyed here, triggering the destrution of the inactive read.
        // If the test fails an SCYLLA_ASSERT() is triggered due to the reader being
        // destroyed without having been closed before.
    }
}

// This unit test passes a read through admission again-and-again, just
// like an evictable reader would be during its lifetime. When readmitted
// the read sometimes has to wait and sometimes not. This is to check that
// the readmitting a previously admitted reader doesn't leak any units.
SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_readmission_preserves_units) {
    simple_schema s;
    const auto initial_resources = reader_concurrency_semaphore::resources{10, 1024 * 1024};
    const auto base_resources = reader_concurrency_semaphore::resources{1, 1024};
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, get_name(), initial_resources.count, initial_resources.memory);

    auto stop_sem = deferred_stop(semaphore);

    reader_permit_opt permit = semaphore.obtain_permit(s.schema(), get_name(), 1024, db::no_timeout, {}).get();
    BOOST_REQUIRE_EQUAL(permit->consumed_resources(), base_resources);

    std::optional<reader_permit::resource_units> residue_units;

    for (int i = 0; i < 10; ++i) {
        residue_units.emplace(permit->consume_resources(reader_resources(0, 100)));
        BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources - permit->consumed_resources());

        auto handle = semaphore.register_inactive_read(make_empty_flat_reader_v2(s.schema(), *permit));
        BOOST_REQUIRE(semaphore.try_evict_one_inactive_read());
        BOOST_REQUIRE_EQUAL(permit->consumed_resources(), residue_units->resources());

        BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources - permit->consumed_resources());

        if (i % 2) {
            auto sponge_permit = semaphore.make_tracking_only_permit(s.schema(), get_name(), db::no_timeout, {});
            auto consumed_resources = sponge_permit.consume_resources(semaphore.available_resources());

            auto fut = make_ready_future<>();
            if (permit->needs_readmission()) {
                fut = permit->wait_readmission();
            }
            BOOST_REQUIRE(!fut.available());

            consumed_resources.reset_to_zero();
            fut.get();
        } else {
            if (permit->needs_readmission()) {
                permit->wait_readmission().get();
            }
        }

        BOOST_REQUIRE_EQUAL(permit->consumed_resources(), residue_units->resources() + base_resources);
        BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources - permit->consumed_resources());
    }

    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources - permit->consumed_resources());

    residue_units.reset();

    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources - permit->consumed_resources());

    permit = {};

    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources);
}

// This unit test checks that the semaphore doesn't get into a deadlock
// when contended, in the presence of many memory-only reads (that don't
// wait for admission). This is tested by simulating the 3 kind of reads we
// currently have in the system:
// * memory-only: reads that don't pass admission and only own memory.
// * admitted: reads that pass admission.
// * evictable: admitted reads that are furthermore evictable.
//
// The test creates and runs a large number of these reads in parallel,
// read kinds being selected randomly, then creates a watchdog which
// kills the test if no progress is being made.
SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_forward_progress) {
    class reader {
        class skeleton_reader : public mutation_reader::impl {
            std::optional<reader_permit::resource_units> _resources;
        public:
            skeleton_reader(schema_ptr s, reader_permit permit)
                : impl(std::move(s), std::move(permit)) { }
            virtual future<> fill_buffer() override {
                reader_permit::awaits_guard _{_permit};
                _resources.emplace(_permit.consume_resources(reader_resources(0, tests::random::get_int(1024, 2048))));
                co_await sleep(std::chrono::milliseconds(1));
            }
            virtual future<> next_partition() override { return make_ready_future<>(); }
            virtual future<> fast_forward_to(const dht::partition_range& pr) override { return make_ready_future<>(); }
            virtual future<> fast_forward_to(position_range) override { return make_ready_future<>(); }
            virtual future<> close() noexcept override {
                _resources.reset();
                return make_ready_future<>();
            }
        };
        struct reader_visitor {
            reader& r;
            future<> operator()(std::monostate& ms) { return r.tick(ms); }
            future<> operator()(mutation_reader& reader) { return r.tick(reader); }
            future<> operator()(reader_concurrency_semaphore::inactive_read_handle& handle) { return r.tick(handle); }
        };

    private:
        schema_ptr _schema;
        reader_concurrency_semaphore& _semaphore;
        bool _memory_only = true;
        bool _evictable = false;
        reader_permit_opt _permit;
        std::optional<reader_permit::resource_units> _units;
        std::variant<std::monostate, mutation_reader, reader_concurrency_semaphore::inactive_read_handle> _reader;

    private:
        void make_reader() {
            _reader = make_mutation_reader<skeleton_reader>(_schema, *_permit);
        }
        future<> tick(std::monostate&) {
            make_reader();
            co_await tick(std::get<mutation_reader>(_reader));
        }
        future<> tick(mutation_reader& reader) {
            co_await reader.fill_buffer();
            if (_evictable) {
                _reader = _permit->semaphore().register_inactive_read(std::move(reader));
            }
        }
        future<> tick(reader_concurrency_semaphore::inactive_read_handle& handle) {
            if (auto reader = _permit->semaphore().unregister_inactive_read(std::move(handle)); reader) {
                _reader = std::move(*reader);
            } else {
                if (_permit->needs_readmission()) {
                    co_await _permit->wait_readmission();
                }
                make_reader();
            }
            co_await tick(std::get<mutation_reader>(_reader));
        }

    public:
        reader(schema_ptr s, reader_concurrency_semaphore& semaphore, bool memory_only, bool evictable)
            : _schema(std::move(s))
            , _semaphore(semaphore)
            , _memory_only(memory_only)
            , _evictable(evictable)
        {
        }
        future<> obtain_permit() {
            if (_memory_only) {
                _permit = _semaphore.make_tracking_only_permit(_schema, "reader_m", db::no_timeout, {});
            } else {
                _permit = co_await _semaphore.obtain_permit(_schema, fmt::format("reader_{}", _evictable ? 'e' : 'a'), 1024, db::no_timeout, {});
            }
            _units = _permit->consume_memory(tests::random::get_int(128, 1024));
        }
        future<> tick() {
            return std::visit(reader_visitor{*this}, _reader);
        }
        future<> close() noexcept {
            if (auto reader = std::get_if<mutation_reader>(&_reader)) {
                return reader->close();
            }
            return make_ready_future<>();
        }
    };

#ifdef DEBUG
    const auto count = 10;
    const auto num_readers = 512;
    const auto ticks = 200;
#else
    const auto count = 10;
    const auto num_readers = 128;
    const auto ticks = 10;
#endif

    simple_schema s;
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, get_name(), count, count * 1024);
    auto stop_sem = deferred_stop(semaphore);

    std::vector<std::unique_ptr<reader>> readers;

    unsigned nr_memory_only = 0;
    unsigned nr_admitted = 0;
    unsigned nr_evictable = 0;

    for (auto i = 0; i <  num_readers; ++i) {
        const auto memory_only = tests::random::get_bool();
        const auto evictable = !memory_only && tests::random::get_bool();
        if (memory_only) {
            ++nr_memory_only;
        } else if (evictable) {
            ++nr_evictable;
        } else {
            ++nr_admitted;
        }
        readers.emplace_back(std::make_unique<reader>(s.schema(), semaphore, memory_only, evictable));
    }

    testlog.info("Created {} readers, memory_only={}, admitted={}, evictable={}", readers.size(), nr_memory_only, nr_admitted, nr_evictable);

    bool watchdog_touched = false;
    auto watchdog = timer<db::timeout_clock>([&semaphore, &watchdog_touched] {
        if (!watchdog_touched) {
            testlog.error("Watchdog detected a deadlock, dumping diagnostics before killing the test: {}", semaphore.dump_diagnostics());
            semaphore.broken(std::make_exception_ptr(std::runtime_error("test killed by watchdog")));
        }
        watchdog_touched = false;
    });
    watchdog.arm_periodic(std::chrono::seconds(30));

    parallel_for_each(readers, [&] (std::unique_ptr<reader>& r_) -> future<> {
        auto r = std::move(r_);
        try {
            co_await r->obtain_permit();
        } catch (semaphore_timed_out&) {
            semaphore.broken(std::make_exception_ptr(std::runtime_error("test failed due to read ")));
            co_return;
        }

        for (auto i = 0; i < ticks; ++i) {
            try {
                watchdog_touched = true;
                co_await r->tick();
            } catch (semaphore_timed_out&) {
                semaphore.broken(std::make_exception_ptr(std::runtime_error("test failed due to read ")));
                break;
            }
        }
        co_await r->close();
        watchdog_touched = true;
    }).get();
}

class dummy_file_impl : public file_impl {
    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, io_intent*) override {
        return make_ready_future<size_t>(0);
    }

    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, io_intent*) override {
        return make_ready_future<size_t>(0);
    }

    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, io_intent*) override {
        return make_ready_future<size_t>(0);
    }

    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, io_intent*) override {
        return make_ready_future<size_t>(0);
    }

    virtual future<> flush(void) override {
        return make_ready_future<>();
    }

    virtual future<struct stat> stat(void) override {
        return make_ready_future<struct stat>();
    }

    virtual future<> truncate(uint64_t length) override {
        return make_ready_future<>();
    }

    virtual future<> discard(uint64_t offset, uint64_t length) override {
        return make_ready_future<>();
    }

    virtual future<> allocate(uint64_t position, uint64_t length) override {
        return make_ready_future<>();
    }

    virtual future<uint64_t> size(void) override {
        return make_ready_future<uint64_t>(0);
    }

    virtual future<> close() override {
        return make_ready_future<>();
    }

    virtual subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override {
        throw_with_backtrace<std::bad_function_call>();
    }

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, io_intent*) override {
        temporary_buffer<uint8_t> buf(range_size);

        memset(buf.get_write(), 0xff, buf.size());

        return make_ready_future<temporary_buffer<uint8_t>>(std::move(buf));
    }
};

SEASTAR_TEST_CASE(reader_restriction_file_tracking) {
    return async([&] {
        reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, get_name(), 100, 4 * 1024);
        auto stop_sem = deferred_stop(semaphore);
        auto permit = semaphore.obtain_permit(nullptr, get_name(), 0, db::no_timeout, {}).get();

        {
            auto tracked_file = make_tracked_file(file(shared_ptr<file_impl>(make_shared<dummy_file_impl>())), permit);

            BOOST_REQUIRE_EQUAL(4 * 1024, semaphore.available_resources().memory);

            auto buf1 = tracked_file.dma_read_bulk<char>(0, 1024).get();
            BOOST_REQUIRE_EQUAL(3 * 1024, semaphore.available_resources().memory);

            auto buf2 = tracked_file.dma_read_bulk<char>(0, 1024).get();
            BOOST_REQUIRE_EQUAL(2 * 1024, semaphore.available_resources().memory);

            auto buf3 = tracked_file.dma_read_bulk<char>(0, 1024).get();
            BOOST_REQUIRE_EQUAL(1 * 1024, semaphore.available_resources().memory);

            auto buf4 = tracked_file.dma_read_bulk<char>(0, 1024).get();
            BOOST_REQUIRE_EQUAL(0 * 1024, semaphore.available_resources().memory);

            auto buf5 = tracked_file.dma_read_bulk<char>(0, 1024).get();
            BOOST_REQUIRE_EQUAL(-1 * 1024, semaphore.available_resources().memory);

            // Reassing buf1, should still have the same amount of units.
            buf1 = tracked_file.dma_read_bulk<char>(0, 1024).get();
            BOOST_REQUIRE_EQUAL(-1 * 1024, semaphore.available_resources().memory);

            // Move buf1 to the heap, so that we can safely destroy it
            auto buf1_ptr = std::make_unique<temporary_buffer<char>>(std::move(buf1));
            BOOST_REQUIRE_EQUAL(-1 * 1024, semaphore.available_resources().memory);

            buf1_ptr.reset();
            BOOST_REQUIRE_EQUAL(0 * 1024, semaphore.available_resources().memory);

            // Move tracked_file to the heap, so that we can safely destroy it.
            auto tracked_file_ptr = std::make_unique<file>(std::move(tracked_file));
            tracked_file_ptr.reset();

            // Move buf4 to the heap, so that we can safely destroy it
            auto buf4_ptr = std::make_unique<temporary_buffer<char>>(std::move(buf4));
            BOOST_REQUIRE_EQUAL(0 * 1024, semaphore.available_resources().memory);

            // Releasing buffers that overlived the tracked-file they
            // originated from should succeed.
            buf4_ptr.reset();
            BOOST_REQUIRE_EQUAL(1 * 1024, semaphore.available_resources().memory);
        }

        // All units should have been deposited back.
        REQUIRE_EVENTUALLY_EQUAL(4 * 1024, semaphore.available_resources().memory);
    });
}

SEASTAR_TEST_CASE(reader_concurrency_semaphore_timeout) {
    return async([&] () {
        reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, get_name(), 2, replica::new_reader_base_cost);
        auto stop_sem = deferred_stop(semaphore);

        {
            auto timeout = db::timeout_clock::now() + std::chrono::duration_cast<db::timeout_clock::time_point::duration>(std::chrono::milliseconds{1});

            reader_permit_opt permit1 = semaphore.obtain_permit(nullptr, "permit1", replica::new_reader_base_cost, timeout, {}).get();

            auto permit2_fut = semaphore.obtain_permit(nullptr, "permit2", replica::new_reader_base_cost, timeout, {});

            auto permit3_fut = semaphore.obtain_permit(nullptr, "permit3", replica::new_reader_base_cost, timeout, {});

            BOOST_REQUIRE_EQUAL(semaphore.get_stats().waiters, 2);

            const auto futures_failed = eventually_true([&] { return permit2_fut.failed() && permit3_fut.failed(); });
            BOOST_CHECK(futures_failed);

            if (futures_failed) {
                BOOST_CHECK_THROW(std::rethrow_exception(permit2_fut.get_exception()), semaphore_timed_out);
                BOOST_CHECK_THROW(std::rethrow_exception(permit3_fut.get_exception()), semaphore_timed_out);
            } else {
                // We need special cleanup when the test failed to avoid invalid
                // memory access.
                permit1 = {};

                BOOST_CHECK(eventually_true([&] { return permit2_fut.available(); }));
                {
                    auto res = permit2_fut.get();
                }

                BOOST_CHECK(eventually_true([&] { return permit3_fut.available(); }));
                {
                    auto res = permit3_fut.get();
                }
            }
       }

        // All units should have been deposited back.
        REQUIRE_EVENTUALLY_EQUAL(replica::new_reader_base_cost, semaphore.available_resources().memory);
    });
}

SEASTAR_TEST_CASE(reader_concurrency_semaphore_max_queue_length) {
    return async([&] () {
        reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, get_name(), 1, replica::new_reader_base_cost, 2);
        auto stop_sem = deferred_stop(semaphore);

        {
            reader_permit_opt permit1 = semaphore.obtain_permit(nullptr, "permit1", replica::new_reader_base_cost, db::no_timeout, {}).get();

            auto permit2_fut = semaphore.obtain_permit(nullptr, "permit2", replica::new_reader_base_cost, db::no_timeout, {});

            auto permit3_fut = semaphore.obtain_permit(nullptr, "permit3", replica::new_reader_base_cost, db::no_timeout, {});

            BOOST_REQUIRE_EQUAL(semaphore.get_stats().waiters, 2);

            auto permit4_fut = semaphore.obtain_permit(nullptr, "permit4", replica::new_reader_base_cost, db::no_timeout, {});

            // The queue should now be full.
            BOOST_REQUIRE_THROW(permit4_fut.get(), std::runtime_error);

            permit1 = {};
            {
                auto res = permit2_fut.get();
            }
            {
                auto res = permit3_fut.get();
            }
        }

        REQUIRE_EVENTUALLY_EQUAL(replica::new_reader_base_cost, semaphore.available_resources().memory);
    });
}

SEASTAR_THREAD_TEST_CASE(reader_concurrency_semaphore_dump_reader_diganostics) {
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::no_limits{}, get_name(), reader_concurrency_semaphore::register_metrics::no);
    auto stop_sem = deferred_stop(semaphore);

    const auto nr_tables = tests::random::get_int<unsigned>(2, 4);
    std::vector<schema_ptr> schemas;
    for (unsigned i = 0; i < nr_tables; ++i) {
        schemas.emplace_back(schema_builder("ks", fmt::format("tbl{}", i))
                .with_column("pk", int32_type, column_kind::partition_key)
                .with_column("v", int32_type, column_kind::regular_column).build());
    }

    const auto nr_ops = tests::random::get_int<unsigned>(1, 3);
    std::vector<std::string> op_names;
    for (unsigned i = 0; i < nr_ops; ++i) {
        op_names.emplace_back(fmt::format("op{}", i));
    }

    std::deque<std::pair<reader_permit, reader_permit::resource_units>> permits;
    for (auto& schema : schemas) {
        const auto nr_permits = tests::random::get_int<unsigned>(2, 32);
        for (unsigned i = 0; i < nr_permits; ++i) {
            auto permit = semaphore.make_tracking_only_permit(schema, op_names.at(tests::random::get_int<unsigned>(0, nr_ops - 1)), db::no_timeout, {});
            if (tests::random::get_int<unsigned>(0, 4)) {
                auto units = permit.consume_resources(reader_resources(tests::random::get_int<unsigned>(0, 1), tests::random::get_int<unsigned>(1024, 16 * 1024 * 1024)));
                permits.push_back(std::pair(std::move(permit), std::move(units)));
            } else {
                auto hdnl = semaphore.register_inactive_read(make_empty_flat_reader_v2(schema, permit));
                BOOST_REQUIRE(semaphore.try_evict_one_inactive_read());
                auto units = permit.consume_memory(tests::random::get_int<unsigned>(1024, 2048));
                permits.push_back(std::pair(std::move(permit), std::move(units)));
            }
        }
    }

    testlog.info("With max-lines=4: {}", semaphore.dump_diagnostics(4));
    testlog.info("With no max-lines: {}", semaphore.dump_diagnostics(0));
}
SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_stop_waits_on_permits) {
    BOOST_TEST_MESSAGE("unused");
    {
        reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::no_limits{}, get_name(), reader_concurrency_semaphore::register_metrics::no);
        // Checks for stop() should not be triggered.
    }

    BOOST_TEST_MESSAGE("0 permits");
    {
        reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::no_limits{}, get_name(), reader_concurrency_semaphore::register_metrics::no);
        // Test will fail by timing out.
        semaphore.stop().get();
    }

    BOOST_TEST_MESSAGE("1 permit");
    {
        auto semaphore = std::make_unique<reader_concurrency_semaphore>(reader_concurrency_semaphore::no_limits{}, get_name(),
                reader_concurrency_semaphore::register_metrics::no);
        auto permit = std::make_unique<reader_permit>(semaphore->make_tracking_only_permit(nullptr, "permit1", db::no_timeout, {}));

        // Test will fail via use-after-free
        auto f = semaphore->stop().then([semaphore = std::move(semaphore)] { });

        yield().get();
        BOOST_REQUIRE(!f.available());
        permit.reset();

        // Test will fail by timing out.
        f.get();
    }
}


static void require_can_admit(schema_ptr schema, reader_concurrency_semaphore& semaphore, bool expected_can_admit, const char* description,
        seastar::compat::source_location sl = seastar::compat::source_location::current()) {
    testlog.trace("Running admission scenario {}, with exepcted_can_admit={}", description, expected_can_admit);
    const auto stats_before = semaphore.get_stats();

    auto admit_fut = semaphore.obtain_permit(schema, "require_can_admit", 1024, db::timeout_clock::now(), {});
    admit_fut.wait();
    const bool can_admit = !admit_fut.failed();
    if (can_admit) {
        admit_fut.ignore_ready_future();
    } else {
        // Make sure we have a timeout exception, not something else
        BOOST_REQUIRE_THROW(std::rethrow_exception(admit_fut.get_exception()), semaphore_timed_out);
    }

    const auto stats_after = semaphore.get_stats();
    BOOST_REQUIRE_EQUAL(stats_after.reads_admitted, stats_before.reads_admitted + uint64_t(can_admit));
    // Deliberately not checking `reads_enqueued_for_admission`, a read can be enqueued temporarily during the admission process.

    if (can_admit == expected_can_admit) {
        testlog.trace("admission scenario '{}' with expected_can_admit={} passed at {}:{}", description, expected_can_admit, sl.file_name(),
                sl.line());
    } else {
        BOOST_FAIL(fmt::format("admission scenario '{}'  with expected_can_admit={} failed at {}:{}\ndiagnostics: {}", description,
                expected_can_admit, sl.file_name(), sl.line(), semaphore.dump_diagnostics()));
    }
};

SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_admission) {
    simple_schema s;
    const auto schema = s.schema();
    const auto initial_resources = reader_concurrency_semaphore::resources{2, 2 * 1024};
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, get_name(), initial_resources.count, initial_resources.memory);
    auto stop_sem = deferred_stop(semaphore);

    auto require_can_admit = [&] (bool expected_can_admit, const char* description,
            seastar::compat::source_location sl = seastar::compat::source_location::current()) {
        ::require_can_admit(schema, semaphore, expected_can_admit, description, sl);
    };

    require_can_admit(true, "semaphore in initial state");

    // resources and waitlist
    {
        reader_permit_opt permit = semaphore.obtain_permit(schema, get_name(), 1024, db::timeout_clock::now(), {}).get();

        require_can_admit(true, "enough resources");

        const auto stats_before = semaphore.get_stats();

        auto enqueued_permit_fut = semaphore.obtain_permit(schema, get_name(), 2 * 1024, db::no_timeout, {});
        {
            const auto stats_after = semaphore.get_stats();
            BOOST_REQUIRE(!enqueued_permit_fut.available());
            BOOST_REQUIRE_EQUAL(stats_after.reads_enqueued_for_admission, stats_before.reads_enqueued_for_admission + 1);
            BOOST_REQUIRE_EQUAL(stats_after.reads_admitted, stats_before.reads_admitted);
            BOOST_REQUIRE_EQUAL(semaphore.get_stats().waiters, 1);
        }

        BOOST_REQUIRE(semaphore.available_resources().count >= 1);
        BOOST_REQUIRE(semaphore.available_resources().memory >= 1024);
        require_can_admit(false, "enough resources but waitlist not empty");

        permit = {};

        reader_permit _(enqueued_permit_fut.get());
    }
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources);
    require_can_admit(true, "semaphore in initial state");

    // need_cpu and awaits
    {
        auto permit = semaphore.obtain_permit(schema, get_name(), 1024, db::timeout_clock::now(), {}).get();

        require_can_admit(true, "!need_cpu");
        {
            reader_permit::need_cpu_guard ncpu_guard{permit};

            require_can_admit(false, "need_cpu > awaits");
            {
                reader_permit::awaits_guard awaits_guard{permit};
                require_can_admit(true, "need_cpu == awaits");
            }
            require_can_admit(false, "need_cpu > awaits");
        }
        require_can_admit(true, "!need_cpu");
    }
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources);
    require_can_admit(true, "semaphore in initial state");

    // forward progress -- resources
    {
        auto sponge_permit = semaphore.make_tracking_only_permit(nullptr, "sponge", db::no_timeout, {});
        sponge_permit.consume_resources(reader_resources::with_memory(semaphore.available_resources().memory));
        require_can_admit(true, "semaphore with no memory but all count available");
    }
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources);
    require_can_admit(true, "semaphore in initial state");

    // forward progress -- readmission
    {
        auto permit = semaphore.obtain_permit(schema, get_name(), 1024, db::timeout_clock::now(), {}).get();

        auto irh = semaphore.register_inactive_read(make_empty_flat_reader_v2(s.schema(), permit));
        BOOST_REQUIRE(semaphore.try_evict_one_inactive_read());
        BOOST_REQUIRE(!irh);

        reader_permit::need_cpu_guard _{permit};

        const auto stats_before = semaphore.get_stats();

        auto wait_fut = make_ready_future<>();
        if (permit.needs_readmission()) {
            wait_fut = permit.wait_readmission();
        }
        wait_fut.wait();
        BOOST_REQUIRE(!wait_fut.failed());

        const auto stats_after = semaphore.get_stats();
        BOOST_REQUIRE_EQUAL(stats_after.reads_admitted, stats_before.reads_admitted + 1);
        BOOST_REQUIRE_EQUAL(stats_after.reads_enqueued_for_admission, stats_before.reads_enqueued_for_admission);
    }
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources);
    require_can_admit(true, "semaphore in initial state");

    // inactive readers
    {
        auto permit = semaphore.obtain_permit(schema, get_name(), 1024, db::no_timeout, {}).get();

        require_can_admit(true, "!need_cpu");
        {
            auto irh = semaphore.register_inactive_read(make_empty_flat_reader_v2(s.schema(), permit));
            require_can_admit(true, "inactive");

            reader_permit::need_cpu_guard ncpu_guard{permit};

            require_can_admit(true, "inactive (need_cpu)");

            {
                auto rd = semaphore.unregister_inactive_read(std::move(irh));
                rd->close().get();
            }

            require_can_admit(false, "need_cpu > awaits");

            irh = semaphore.register_inactive_read(make_empty_flat_reader_v2(s.schema(), permit));
            require_can_admit(true, "inactive (need_cpu)");
        }
        require_can_admit(true, "!need_cpu");
    }
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources);
    require_can_admit(true, "semaphore in initial state");

    // evicting inactive readers for admission
    {
        auto permit1 = semaphore.obtain_permit(schema, get_name(), 1024, db::timeout_clock::now(), {}).get();
        auto irh1 = semaphore.register_inactive_read(make_empty_flat_reader_v2(s.schema(), permit1));

        auto permit2 = semaphore.obtain_permit(schema, get_name(), 1024, db::timeout_clock::now(), {}).get();
        auto irh2 = semaphore.register_inactive_read(make_empty_flat_reader_v2(s.schema(), permit2));

        require_can_admit(true, "evictable reads");
    }
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources);
    require_can_admit(true, "semaphore in initial state");

    auto check_admitting_enqueued_read = [&] (auto pre_admission_hook, auto post_enqueue_hook) {
        auto cookie1 = pre_admission_hook();

        require_can_admit(false, "admission awaits");

        const auto stats_before = semaphore.get_stats();

        auto permit2_fut = semaphore.obtain_permit(schema, get_name(), 1024, db::no_timeout, {});

        const auto stats_after = semaphore.get_stats();
        BOOST_REQUIRE_EQUAL(stats_after.reads_admitted, stats_before.reads_admitted);
        BOOST_REQUIRE_EQUAL(stats_after.reads_enqueued_for_admission, stats_before.reads_enqueued_for_admission + 1);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().waiters, 1);

        std::ignore = post_enqueue_hook(cookie1);

        if (!eventually_true([&] { return permit2_fut.available(); })) {
            semaphore.broken();
            permit2_fut.wait();
            permit2_fut.ignore_ready_future();
            BOOST_FAIL("Enqueued permit didn't get admitted as expected");
        }
    };

    // admitting enqueued reads -- permit owning resources destroyed
    {
        check_admitting_enqueued_read(
            [&] {
                return reader_permit_opt(semaphore.obtain_permit(schema, get_name(), 2 * 1024, db::timeout_clock::now(), {}).get());
            },
            [] (reader_permit_opt& permit1) {
                permit1 = {};
                return 0;
            }
        );
    }
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources);
    require_can_admit(true, "semaphore in initial state");

    // admitting enqueued reads -- permit owning resources becomes inactive
    {
        check_admitting_enqueued_read(
            [&] {
                return reader_permit_opt(semaphore.obtain_permit(schema, get_name(), 2 * 1024, db::timeout_clock::now(), {}).get());
            },
            [&] (reader_permit_opt& permit1) {
                return semaphore.register_inactive_read(make_empty_flat_reader_v2(s.schema(), *permit1));
            }
        );
    }
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources);
    require_can_admit(true, "semaphore in initial state");

    // admitting enqueued reads -- permit becomes active
    {
        check_admitting_enqueued_read(
            [&] {
                auto permit = semaphore.obtain_permit(schema, get_name(), 1024, db::timeout_clock::now(), {}).get();
                require_can_admit(true, "enough resources");
                return std::pair(permit, std::optional<reader_permit::need_cpu_guard>{permit});
            }, [&] (std::pair<reader_permit, std::optional<reader_permit::need_cpu_guard>>& permit_and_need_cpu_guard) {
                permit_and_need_cpu_guard.second.reset();
                return 0;
            }
        );
    }
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources);
    require_can_admit(true, "semaphore in initial state");

    // admitting enqueued reads -- permit becomes awaits
    {
        check_admitting_enqueued_read(
            [&] {
                auto permit = semaphore.obtain_permit(schema, get_name(), 1024, db::timeout_clock::now(), {}).get();
                require_can_admit(true, "enough resources");
                return std::pair(permit, reader_permit::need_cpu_guard{permit});
            }, [&] (std::pair<reader_permit, reader_permit::need_cpu_guard>& permit_and_need_cpu_guard) {
                return reader_permit::awaits_guard{permit_and_need_cpu_guard.first};
            }
        );
    }
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources);
    require_can_admit(true, "semaphore in initial state");
}

SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_need_cpu_awaits) {
    const auto initial_resources = reader_concurrency_semaphore::resources{2, 2 * 1024};
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, get_name(), initial_resources.count, initial_resources.memory);
    auto stop_sem = deferred_stop(semaphore);

    BOOST_REQUIRE_EQUAL(semaphore.get_stats().current_permits, 0);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().need_cpu_permits, 0);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().awaits_permits, 0);

    auto permit = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout, {}).get();

    for (auto scenario = 0; scenario < 5; ++scenario) {
        testlog.info("Running scenario {}", scenario);

        std::vector<reader_permit::need_cpu_guard> need_cpu;
        std::vector<reader_permit::awaits_guard> awaits;
        unsigned count;

        switch (scenario) {
            case 0:
                need_cpu.emplace_back(permit);

                BOOST_REQUIRE_EQUAL(semaphore.get_stats().current_permits, 1);
                BOOST_REQUIRE_EQUAL(semaphore.get_stats().need_cpu_permits, 1);
                BOOST_REQUIRE_EQUAL(semaphore.get_stats().awaits_permits, 0);
                break;
            case 1:
                need_cpu.emplace_back(permit);
                awaits.emplace_back(permit);

                BOOST_REQUIRE_EQUAL(semaphore.get_stats().current_permits, 1);
                BOOST_REQUIRE_EQUAL(semaphore.get_stats().need_cpu_permits, 1);
                BOOST_REQUIRE_EQUAL(semaphore.get_stats().awaits_permits, 1);
                break;
            case 2:
                awaits.emplace_back(permit);

                BOOST_REQUIRE_EQUAL(semaphore.get_stats().current_permits, 1);
                BOOST_REQUIRE_EQUAL(semaphore.get_stats().need_cpu_permits, 0);
                BOOST_REQUIRE_EQUAL(semaphore.get_stats().awaits_permits, 0);
                break;
            case 3:
                awaits.emplace_back(permit);
                need_cpu.emplace_back(permit);

                BOOST_REQUIRE_EQUAL(semaphore.get_stats().current_permits, 1);
                BOOST_REQUIRE_EQUAL(semaphore.get_stats().need_cpu_permits, 1);
                BOOST_REQUIRE_EQUAL(semaphore.get_stats().awaits_permits, 1);
                break;
            default:
                count = tests::random::get_int<unsigned>(3, 100);
                for (unsigned i = 0; i < count; ++i) {
                    if (tests::random::get_bool()) {
                        need_cpu.emplace_back(permit);
                    } else {
                        awaits.emplace_back(permit);
                    }
                }
                break;
        }

        while (!need_cpu.empty() && !awaits.empty()) {
            const bool pop_need_cpu = !need_cpu.empty() && tests::random::get_bool();

            if (pop_need_cpu) {
                need_cpu.pop_back();
                if (need_cpu.empty()) {
                    BOOST_REQUIRE_EQUAL(semaphore.get_stats().need_cpu_permits, 0);
                }
            } else {
                awaits.pop_back();
                if (awaits.empty()) {
                    BOOST_REQUIRE_EQUAL(semaphore.get_stats().awaits_permits, 0);
                }
            }
        }
    }
}


SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_evict_inactive_reads_for_table) {
    auto spec = tests::make_random_schema_specification(get_name());

    std::list<tests::random_schema> schemas;
    struct inactive_read {
        reader_concurrency_semaphore::inactive_read_handle handle;
        std::optional<dht::partition_range> range;
        explicit inactive_read(std::optional<dht::partition_range> range = {})
            : range(std::move(range))
        { }
        inactive_read(reader_concurrency_semaphore::inactive_read_handle handle, std::optional<dht::partition_range> range = {})
            : handle(std::move(handle))
            , range(std::move(range))
        { }
        operator bool() const {
            return bool(handle);
        }
    };
    std::unordered_map<tests::random_schema*, std::list<inactive_read>> schema_handles;
    for (unsigned i = 0; i < 4; ++i) {
        auto& s = schemas.emplace_back(tests::random_schema(i, *spec));
        schema_handles.emplace(&s, std::list<inactive_read>{});
    }

    auto make_random_range = [] (tests::random_schema& s) {
        auto keys = s.make_pkeys(2);
        return interval<tests::data_model::mutation_description::key>::make({keys[0]}, {keys[1]}).transform([&s] (const tests::data_model::mutation_description::key& k) -> dht::ring_position {
            return dht::decorate_key(*s.schema(), partition_key::from_exploded(*s.schema(), k));
        });
    };

    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::no_limits{}, get_name(), reader_concurrency_semaphore::register_metrics::no);
    auto stop_sem = deferred_stop(semaphore);

    for (auto& s : schemas) {
        auto& handles = schema_handles[&s];
        for (int i = 0; i < 10; ++i) {
            auto& handle = handles.emplace_back(make_random_range(s));
            handle.handle = semaphore.register_inactive_read(
                    make_empty_flat_reader_v2(s.schema(), semaphore.make_tracking_only_permit(s.schema(), get_name(), db::no_timeout, {})),
                    &*handle.range);
        }
        for (int i = 0; i < 4; ++i) {
            handles.emplace_back(semaphore.register_inactive_read(make_empty_flat_reader_v2(s.schema(), semaphore.make_tracking_only_permit(s.schema(), get_name(), db::no_timeout, {}))));
        }
    }

    for (auto& s : schemas) {
        auto& handles = schema_handles[&s];
        BOOST_REQUIRE(std::all_of(handles.begin(), handles.end(), [] (const inactive_read& ir) { return bool(ir); }));
    }

    for (auto& s : schemas) {
        auto& handles = schema_handles[&s];
        BOOST_REQUIRE(std::all_of(handles.begin(), handles.end(), [] (const inactive_read& ir) { return bool(ir); }));

        std::optional<dht::partition_range> evict_range;
        if (tests::random::get_bool()) {
            evict_range.emplace(make_random_range(s));
        }

        semaphore.evict_inactive_reads_for_table(s.schema()->id(), evict_range ? &*evict_range : nullptr).get();
        for (const auto& [k, v] : schema_handles) {
            if (k == &s) {
                for (const auto& ir : v) {
                    if (ir) {
                        BOOST_REQUIRE(ir.range);
                        BOOST_REQUIRE(evict_range);
                        BOOST_REQUIRE(!ir.range->overlaps(*evict_range, dht::ring_position_comparator(*s.schema())));
                    }
                }
            } else if (!v.empty()) {
                BOOST_REQUIRE(std::all_of(v.begin(), v.end(), [] (const inactive_read& ir) { return bool(ir); }));
            }
        }
        handles.clear();
    }
}

// Reproduces https://github.com/scylladb/scylladb/issues/11770
SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_evict_inactive_reads_when_all_is_awaits) {
    simple_schema ss;
    const auto& s = ss.schema();

    const auto initial_resources = reader_concurrency_semaphore::resources{2, 32 * 1024};
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, get_name(), initial_resources.count, initial_resources.memory);
    auto stop_sem = deferred_stop(semaphore);

    class read {
        reader_permit _permit;
        promise<> _read_started_pr;
        future<> _read_started_fut;
        promise<> _read_done_pr;
        reader_permit::need_cpu_guard _ncpu_guard;
        std::optional<reader_permit::awaits_guard> _awaits_guard;

    public:
        explicit read(reader_permit p) : _permit(std::move(p)), _read_started_fut(_read_started_pr.get_future()), _ncpu_guard(_permit) { }
        future<> wait_read_started() { return std::move(_read_started_fut); }
        void set_read_done() { _read_done_pr.set_value(); }
        void mark_as_awaits() { _awaits_guard.emplace(_permit); }
        void mark_as_not_awaits() { _awaits_guard.reset(); }
        reader_concurrency_semaphore::read_func get_read_func() {
            return [this] (reader_permit permit) -> future<> {
                _read_started_pr.set_value();
                co_await _read_done_pr.get_future();
            };
        }
    };

    auto p1 = semaphore.obtain_permit(s, get_name(), 1024, db::no_timeout, {}).get();
    auto irh1 = semaphore.register_inactive_read(make_empty_flat_reader_v2(ss.schema(), p1));

    auto p2 = semaphore.obtain_permit(s, get_name(), 1024, db::no_timeout, {}).get();
    read rd2(p2);
    auto fut2 = semaphore.with_ready_permit(p2, rd2.get_read_func());

    // At this point we expect to have:
    // * 1 inactive read (not evicted)
    // * 1 need_cpu (but not awaiting) read on the ready list
    // * 1 waiter
    // * no more count resources left
    auto p3_fut = semaphore.obtain_permit(s, get_name(), 1024, db::no_timeout, {});
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().waiters, 2); // (waiters includes _ready_list entries)
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().reads_enqueued_for_admission, 1);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().need_cpu_permits, 0); // permit looses need_cpu status while waiting for execution
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().awaits_permits, 0);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 1);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().permit_based_evictions, 0);
    BOOST_REQUIRE_EQUAL(semaphore.available_resources().count, 0);
    BOOST_REQUIRE(irh1);

    // Start the read emptying the ready list, this should not be enough to admit p3
    rd2.wait_read_started().get();
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().waiters, 1);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().need_cpu_permits, 1);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().awaits_permits, 0);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 1);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().permit_based_evictions, 0);
    BOOST_REQUIRE_EQUAL(semaphore.available_resources().count, 0);
    BOOST_REQUIRE(irh1);

    // Marking p2 as awaits should now allow p3 to be admitted by evicting p1
    rd2.mark_as_awaits();
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().waiters, 0);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().need_cpu_permits, 1);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().awaits_permits, 1);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 0);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().permit_based_evictions, 1);
    BOOST_REQUIRE_EQUAL(semaphore.available_resources().count, 0);
    BOOST_REQUIRE(!irh1);

    p3_fut.get();
    rd2.mark_as_not_awaits();
    rd2.set_read_done();
    fut2.get();
}

SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_set_resources) {
    const auto initial_resources = reader_concurrency_semaphore::resources{4, 4 * 1024};
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, get_name(), initial_resources.count, initial_resources.memory);
    auto stop_sem = deferred_stop(semaphore);

    auto permit1 = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout, {}).get();
    auto permit2 = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout, {}).get();
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), reader_resources(2, 2 * 1024));
    BOOST_REQUIRE_EQUAL(semaphore.initial_resources(), reader_resources(4, 4 * 1024));

    semaphore.set_resources({8, 8 * 1024});
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), reader_resources(6, 6 * 1024));
    BOOST_REQUIRE_EQUAL(semaphore.initial_resources(), reader_resources(8, 8 * 1024));

    semaphore.set_resources({2, 2 * 1024});
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), reader_resources(0, 0));
    BOOST_REQUIRE_EQUAL(semaphore.initial_resources(), reader_resources(2, 2 * 1024));

    semaphore.set_resources({3, 128});
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), reader_resources(1, 128 - 2 * 1024));
    BOOST_REQUIRE_EQUAL(semaphore.initial_resources(), reader_resources(3, 128));

    semaphore.set_resources({1, 3 * 1024});
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), reader_resources(-1, 1024));
    BOOST_REQUIRE_EQUAL(semaphore.initial_resources(), reader_resources(1, 3 * 1024));

    auto permit3_fut = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout, {});
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().reads_enqueued_for_admission, 1);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().waiters, 1);

    semaphore.set_resources({4, 4 * 1024});
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().waiters, 0);
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), reader_resources(1, 1024));
    BOOST_REQUIRE_EQUAL(semaphore.initial_resources(), reader_resources(4, 4 * 1024));
    permit3_fut.get();
}

namespace {

class allocating_reader {
    static constexpr size_t admission_cost = 1024;
    static constexpr size_t buf_size = 1024;
    static constexpr size_t read_iterations = 4;
public:
    enum class state {
        wait_for_admission,
        request_memory,
        wait_for_memory,
        release_memory,
        done,
    };
    const char* to_string(state s) {
        switch (s) {
            case state::wait_for_admission: return "state::wait_for_admission";
            case state::request_memory: return "state::request_memory";
            case state::wait_for_memory: return "state::wait_for_memory";
            case state::release_memory: return "state::release_memory";
            case state::done: return "state::done";
        }
        std::abort();
    };
private:
    reader_concurrency_semaphore& _sem;
    state _state = state::wait_for_admission;
    std::optional<future<>> _admission_fut;
    std::optional<reader_permit> _permit;
    std::list<reader_permit::resource_units> _current_resource_units;
    std::list<future<reader_permit::resource_units>> _pending_resource_units;
    unsigned _read_count = 0;
    bool _success = true;
public:
    explicit allocating_reader(reader_concurrency_semaphore& sem) : _sem(sem) {
        testlog.debug("[{}] allocating_reader created", fmt::ptr(this));
        _admission_fut = sem.obtain_permit(nullptr, "reader", admission_cost, db::no_timeout, {}).then_wrapped([this] (future<reader_permit>&& permit_fut) {
            try {
                _permit = std::move(permit_fut.get());
                _state = state::request_memory;
            } catch (...) {
                _state = state::done;
                _success = false;
            }
        });
    }
    ~allocating_reader() { }
    void operator()() {
        testlog.debug("[{}|p:0x{:x}] allocating_reader(): _state={}, _permit.state={}, _permit.resources={}, _sem.resources={}",
                fmt::ptr(this),
                _permit ? _permit->id() : 0,
                to_string(_state),
                _permit ? format("{}", _permit->get_state()) : "N/A",
                _permit ? _permit->consumed_resources() : reader_resources{},
                _sem.consumed_resources());
        switch (_state) {
            case state::wait_for_admission:
                break;
            case state::request_memory:
            {
                size_t n = 0;
                if (!_read_count) {
                    n = 1;
                } else {
                    n = tests::random::get_int(1, 8);
                }
                ++_read_count;
                try {
                    for (size_t i = 0; i < n; ++i) {
                        _pending_resource_units.emplace_back(_permit->request_memory(buf_size));
                    }
                } catch (std::bad_alloc&) {
                    testlog.debug("[{}|p:{}] read killed", fmt::ptr(this), _permit ? _permit->id() : 0);
                    _read_count = read_iterations;
                }
                _state = state::wait_for_memory;
                break;
            }
            case state::wait_for_memory:
                for (auto it = _pending_resource_units.begin(); it != _pending_resource_units.end();) {
                    if (it->available()) {
                        try {
                            _current_resource_units.push_back(it->get());
                        } catch (std::bad_alloc&) {
                            testlog.debug("[{}|p:{}] read killed", fmt::ptr(this), _permit ? _permit->id() : 0);
                            _read_count = read_iterations;
                        }
                        it = _pending_resource_units.erase(it);
                    } else {
                        ++it;
                    }
                }
                if (_pending_resource_units.empty()) {
                    _state = state::release_memory;
                }
                break;
            case state::release_memory:
                if (_current_resource_units.empty()) {
                    if (_read_count == read_iterations) {
                        _state = state::done;
                    } else if (!tests::random::get_int(0, 7)) {
                        _state = state::done;
                    } else {
                        _state = state::request_memory;
                    }
                } else {
                    _current_resource_units.pop_front();
                }
                break;
            case state::done:
                _permit.reset();
                break;
        }
    }
    bool done() const { return _state == state::done; }
    bool success() const { return _success; }
    reader_resources resources() const { return _permit ? _permit->consumed_resources() : reader_resources{}; }
    future<> close() {
        if (_admission_fut) {
            co_await std::move(_admission_fut).value();
        }
        co_await coroutine::parallel_for_each(_pending_resource_units.begin(), _pending_resource_units.end(), [] (future<reader_permit::resource_units>& fut) {
            return std::move(fut).then_wrapped([] (future<reader_permit::resource_units>&& fut) {
                try {
                    fut.get();
                } catch (...) {
                }
            });
        });
        _current_resource_units.clear();
        _permit.reset();
    }
};

} //anonymous namespace

// Check that the memory consumption limiting mechanism doesn't leak any
// resources or cause any internal consistencies in the semaphore.
SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_memory_limit_no_leaks) {
    const auto initial_resources = reader_concurrency_semaphore::resources{4, 4 * 1024};
    const auto serialize_multiplier = 2;
    const auto kill_multiplier = 3;
    reader_concurrency_semaphore semaphore(initial_resources.count, initial_resources.memory, get_name(), 100,
            utils::updateable_value<uint32_t>(serialize_multiplier), utils::updateable_value<uint32_t>(kill_multiplier), reader_concurrency_semaphore::register_metrics::no);
    auto stop_sem = deferred_stop(semaphore);

    const size_t reader_count_target = 6;
    const size_t iteration_limit = 1000;

    std::list<allocating_reader> readers;

    size_t i = 0;
    bool done = false;
    sstring error = "";
    while (!done) {
        testlog.debug("iteration {}", i);

        for (auto& rd : readers) {
            rd();
            reader_resources all_permit_res;
            semaphore.foreach_permit([&all_permit_res] (const reader_permit& p) { all_permit_res += p.consumed_resources(); });
            if (semaphore.consumed_resources() != all_permit_res) {
                testlog.error("resource mismatch: semaphore.consumed_resources() ({}) != sum of resources in permits ({})", semaphore.consumed_resources(), all_permit_res);
            }
        }

        if (readers.size() < reader_count_target) {
            readers.emplace_back(semaphore);
        }
        done = std::all_of(readers.begin(), readers.end(), std::mem_fn(&allocating_reader::done));

        testlog.debug("{}", semaphore.dump_diagnostics());

        reader_resources all_permit_res;
        semaphore.foreach_permit([&all_permit_res] (const reader_permit& p) { all_permit_res += p.consumed_resources(); });

        if (semaphore.consumed_resources().memory >= (semaphore.initial_resources().memory * kill_multiplier)) {
            error = format("kill limit failed: semaphore.consumed_resources() ({}) >= kill limit ({})", semaphore.consumed_resources().memory, (semaphore.initial_resources().memory * kill_multiplier));
        } else if (semaphore.consumed_resources() != all_permit_res) {
            error = format("resource mismatch: semaphore.consumed_resources() ({}) != sum of resources in permits ({})", semaphore.consumed_resources(), all_permit_res);
        } else if (i >= iteration_limit) {
            error = format("test failed to finish in {} iterations", iteration_limit);
        }

        if (error.empty()) {
            ++i;
        } else {
            testlog.error("stopping test at iteration {}: {}", i, error);
            done = true;
        }

        seastar::thread::yield();
    }
    testlog.info("{}", semaphore.dump_diagnostics());
    parallel_for_each(readers.begin(), readers.end(), [] (allocating_reader& rd) {
        return rd.close();
    }).get();
    if (!error.empty()) {
        BOOST_FAIL(error);
    }
    const bool all_ok = std::all_of(readers.begin(), readers.end(), std::mem_fn(&allocating_reader::success));
    BOOST_REQUIRE(all_ok);
}

struct memory_limit_table {
    schema_ptr schema;
    tmpdir sst_dir;
    partition_key pk;
    clustering_key ck;
    sstring value;
};
memory_limit_table create_memory_limit_table(cql_test_env& env, uint64_t target_num_sstables) {
    auto& db = env.local_db();

    sstring value(256 * 1024, '0');

    env.execute_cql("CREATE TABLE ks.tbl (pk int, ck int, value text, primary key (pk, ck)) WITH compaction = {'class': 'NullCompactionStrategy'};").get();

    BOOST_REQUIRE(env.local_db().has_schema("ks", "tbl"));

    auto& tbl = db.find_column_family("ks", "tbl");
    auto s = tbl.schema();
    auto& sst_man = tbl.get_sstables_manager();
    auto& semaphore = db.get_reader_concurrency_semaphore();

    auto dk = tests::generate_partition_key(s);
    auto ck = tests::generate_clustering_key(s);

    mutation mut(s, dk);
    mut.set_clustered_cell(ck, to_bytes("value"), data_value(value), 0);

    auto sstables_dir = tmpdir();

    const auto sstable_write_concurrency = 16;

    uint64_t num_sstables = 0;
    parallel_for_each(boost::irange(0, sstable_write_concurrency), [&] (int i) {
        return seastar::async([&] {
            while (num_sstables != target_num_sstables) {
                ++num_sstables;
                auto sst = tbl.make_sstable();
                auto writer_cfg = sst_man.configure_writer("test");
                sst->write_components(
                    make_mutation_reader_from_mutations_v2(s, semaphore.make_tracking_only_permit(s, "test", db::no_timeout, {}), mut, s->full_slice()),
                    1,
                    s,
                    writer_cfg,
                    encoding_stats{}).get();
                sst->open_data().get();
                tbl.add_sstable_and_update_cache(std::move(sst)).get();
            }
        });
    }).get();

    return {s, std::move(sstables_dir), std::move(dk.key()), std::move(ck), std::move(value)};
}

#ifndef DEBUG
constexpr uint64_t target_memory = uint64_t(1) << 28; // 256MB
#endif

// Check that the memory consumption limiting mechanism of the semaphore does
// prevent OOM crashes.
// The test fails by OOM crashing.
// This test should be run with 256MB of memory.
SEASTAR_TEST_CASE(test_reader_concurrency_semaphore_memory_limit_no_oom) {
#ifndef DEBUG
    if (memory::stats().total_memory() != target_memory) {
        std::cerr << "Test " << get_name() << " should be run with 256M of memory, make sure you invoke with -m256M" << std::endl;
        return make_ready_future<>();
    }
#endif

    auto db_cfg_ptr = make_shared<db::config>();
    auto& db_cfg = *db_cfg_ptr;

    // Disable the cache altogether, we want all reads to go to disk.
    db_cfg.enable_cache(false);
    db_cfg.enable_commitlog(false);
    db_cfg.reader_concurrency_semaphore_serialize_limit_multiplier.set(2, utils::config_file::config_source::CommandLine);
    db_cfg.reader_concurrency_semaphore_kill_limit_multiplier.set(4, utils::config_file::config_source::CommandLine);

    return do_with_cql_env_thread([] (cql_test_env& env) {
        auto tbl = create_memory_limit_table(env, 256);

#ifdef DEBUG
        const auto num_reads = 16;
#else
        const auto num_reads = 128;
#endif

        auto read_id = env.prepare("SELECT value FROM ks.tbl WHERE pk = ? AND ck = ?").get();

        parallel_for_each(boost::irange(0, num_reads), [&] (int i) {
            return env.execute_prepared(read_id, {cql3::raw_value::make_value(tbl.pk.explode().front()), cql3::raw_value::make_value(tbl.ck.explode().front())}).then_wrapped(
                    [&] (future<shared_ptr<cql_transport::messages::result_message>> fut) {
                if (fut.failed()) {
                    // We expect failed, OOM-killed reads here.
                    // No way to verify why they failed so we swallow all failures.
                    fut.ignore_ready_future();
                    return;
                }
                assert_that(fut.get()).is_rows().with_rows_ignore_order({ {serialized(tbl.value)} });
            });
        }).get();
        return make_ready_future<>();
    }, std::move(db_cfg_ptr));
}

// Check that the memory consumption limiting mechanism of the semaphore does
// prevent reads exhausting memory to the extent that they start to fail due to
// bad alloc (but not necessarily crash the node).
// Instead the limiting mechanism engages and kills reads before they get to that
// point. From the outset, a read failing due to OOM and a read killed by the
// limiting mechanism looks the same. To differentiate, the test checks that all
// failures were caused by the limiting mechanism.
// This test should be run with 256M memory.
SEASTAR_TEST_CASE(test_reader_concurrency_semaphore_memory_limit_engages) {
#ifndef DEBUG
    if (memory::stats().total_memory() != target_memory) {
        std::cerr << "Test " << get_name() << " should be run with 256M of memory, make sure you invoke with -m256M" << std::endl;
        return make_ready_future<>();
    }
#endif
    auto db_cfg_ptr = make_shared<db::config>();
    auto& db_cfg = *db_cfg_ptr;

    // Disable the cache altogether, we want all reads to go to disk.
    db_cfg.enable_cache(false);
    db_cfg.enable_commitlog(false);
    db_cfg.reader_concurrency_semaphore_serialize_limit_multiplier.set(2, utils::config_file::config_source::CommandLine);
    db_cfg.reader_concurrency_semaphore_kill_limit_multiplier.set(4, utils::config_file::config_source::CommandLine);

    return do_with_cql_env_thread([] (cql_test_env& env) {
        auto tbl = create_memory_limit_table(env, 32);

        auto& db = env.local_db();
        auto& semaphore = db.get_reader_concurrency_semaphore();

        const auto num_reads = 128;

        auto read_id = env.prepare("SELECT value FROM ks.tbl WHERE pk = ? AND ck = ?").get();

        // We first check that the test params are not too strict and a single
        // read can finish successfully.
        try {
            auto msg = env.execute_prepared(read_id, {cql3::raw_value::make_value(tbl.pk.explode().front()), cql3::raw_value::make_value(tbl.ck.explode().front())}).get();
            assert_that(msg).is_rows().with_rows_ignore_order({ {serialized(tbl.value)} });
        } catch (...) {
            BOOST_FAIL(fmt::format("canary read failed with: {}", std::current_exception()));
        }

        uint64_t successful_reads = 0;
        uint64_t failed_reads = 0;

        parallel_for_each(boost::irange(0, num_reads), [&] (int i) {
            return env.execute_prepared(read_id, {cql3::raw_value::make_value(tbl.pk.explode().front()), cql3::raw_value::make_value(tbl.ck.explode().front())}).then_wrapped(
                        [&] (future<shared_ptr<cql_transport::messages::result_message>> fut) {
                if (fut.failed()) {
                    // We expect failed, OOM-killed reads here.
                    // No way to verify why they failed so we swallow all failures.
                    fut.ignore_ready_future();
                    ++failed_reads;
                    return;
                }
                assert_that(fut.get()).is_rows().with_rows_ignore_order({ {serialized(tbl.value)} });
                ++successful_reads;
            });
        }).get();

        testlog.info("total reads: {} ({} successful, {} failed)", num_reads, successful_reads, failed_reads);
        testlog.info("{}", semaphore.dump_diagnostics());

        // There should be both successful and failed reads.
        // If there is only one or the other, the test is not testing anything.
        // We also check that the memory limiting mechanism of the semaphore was engaged.
        // The test is meaningless without it.
        // In the slow debug builds we never reach the kill limit for some reason.
#ifndef DEBUG
        BOOST_REQUIRE_GE(failed_reads, 1);
#endif
        BOOST_REQUIRE_GE(successful_reads, 1);

        // Almost each failed read should have been in the memory queue at one point.
        BOOST_REQUIRE_GE(semaphore.get_stats().reads_enqueued_for_memory, semaphore.get_stats().total_reads_killed_due_to_kill_limit);

        // All failures must be caused by the kill limit triggering.
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().total_reads_killed_due_to_kill_limit, failed_reads);

        return make_ready_future<>();
    }, std::move(db_cfg_ptr));
}

SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_request_memory_preserves_state) {
    const auto initial_resources = reader_concurrency_semaphore::resources{2, 2 * 1024};
    const auto serialize_multiplier = 2;
    const auto kill_multiplier = std::numeric_limits<uint32_t>::max(); // we don't want this to interfere with our test
    reader_concurrency_semaphore semaphore(initial_resources.count, initial_resources.memory, get_name(), 100,
            utils::updateable_value<uint32_t>(serialize_multiplier), utils::updateable_value<uint32_t>(kill_multiplier), reader_concurrency_semaphore::register_metrics::no);
    auto stop_sem = deferred_stop(semaphore);

    auto sponge_permit = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout, {}).get();

    uint64_t reads_enqueued_for_memory = 0;

    auto do_check = [&] (reader_permit& permit, uint64_t need_cpu, uint64_t awaits, seastar::compat::source_location sl) {
        testlog.info("do_check() {}:{}", sl.file_name(), sl.line());

        BOOST_REQUIRE_EQUAL(semaphore.get_stats().current_permits, 2);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().need_cpu_permits, need_cpu);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().awaits_permits, awaits);

        auto units1 = permit.request_memory(1024).get();

        BOOST_REQUIRE_EQUAL(semaphore.get_stats().current_permits, 2);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().need_cpu_permits, need_cpu);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().awaits_permits, awaits);

        auto sponge_units = sponge_permit.request_memory(8 * 1024).get();

        // sponge permit is now the blessed one

        auto units2_fut = permit.request_memory(1024);
        BOOST_REQUIRE(!units2_fut.available());
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().reads_enqueued_for_memory, ++reads_enqueued_for_memory);

        sponge_units.reset_to_zero();
        auto units2 = units2_fut.get();

        BOOST_REQUIRE_EQUAL(semaphore.get_stats().current_permits, 2);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().need_cpu_permits, need_cpu);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().awaits_permits, awaits);
    };

    // active
    {
        auto permit = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout, {}).get();
        do_check(permit, 0, 0, seastar::compat::source_location::current());
    }

    // need_cpu
    {
        auto permit = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout, {}).get();
        reader_permit::need_cpu_guard ncpu_guard{permit};
        do_check(permit, 1, 0, seastar::compat::source_location::current());
    }

    // awaits
    {
        auto permit = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout, {}).get();
        reader_permit::need_cpu_guard ncpu_guard{permit};
        reader_permit::awaits_guard awaits_guard{permit};
        do_check(permit, 1, 1, seastar::compat::source_location::current());
    }
}

SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_blessed_read_goes_inactive) {
    const auto initial_resources = reader_concurrency_semaphore::resources{2, 2 * 1024};
    const auto serialize_multiplier = 2;
    const auto kill_multiplier = std::numeric_limits<uint32_t>::max(); // we don't want this to interfere with our test
    reader_concurrency_semaphore semaphore(initial_resources.count, initial_resources.memory, get_name(), 100,
            utils::updateable_value<uint32_t>(serialize_multiplier), utils::updateable_value<uint32_t>(kill_multiplier), reader_concurrency_semaphore::register_metrics::no);
    auto stop_sem = deferred_stop(semaphore);

    simple_schema ss;
    auto s = ss.schema();

    auto permit = semaphore.obtain_permit(s, get_name(), 1024, db::no_timeout, {}).get();

    std::vector<reader_permit::resource_units> permit_res;

    permit_res.emplace_back(permit.request_memory(1024).get());
    permit_res.emplace_back(permit.request_memory(1024).get());
    BOOST_REQUIRE_EQUAL(semaphore.consumed_resources(), reader_resources(1, 3 * 1024));
    BOOST_REQUIRE_EQUAL(semaphore.get_blessed_permit(), 0);

    // permit is the blessed one
    permit_res.emplace_back(permit.request_memory(1024).get());
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().reads_enqueued_for_memory, 0);
    BOOST_REQUIRE_EQUAL(semaphore.get_blessed_permit(), permit.id());

    // register the blessed permit (permit) as inactive
    permit_res.clear();
    auto handle = semaphore.register_inactive_read(make_empty_flat_reader_v2(s, permit));

    // Upon being registered as inactive, the permit should loose the blessed status
    BOOST_REQUIRE_EQUAL(semaphore.get_blessed_permit(), 0);
}

// Check that `stop()` correctly evicts all inactive reads.
SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_stop_with_inactive_reads) {
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::no_limits{}, get_name(), reader_concurrency_semaphore::register_metrics::no);

    simple_schema ss;
    auto s = ss.schema();

    auto permit = reader_permit_opt(semaphore.obtain_permit(s, get_name(), 1024, db::no_timeout, {}).get());

    auto handle = semaphore.register_inactive_read(make_empty_flat_reader_v2(s, *permit));

    BOOST_REQUIRE(handle);
    BOOST_REQUIRE_EQUAL(permit->get_state(), reader_permit::state::inactive);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 1);

    // Using BOOST_CHECK_* because an exception thrown here causes a segfault,
    // due to the stop future not being waited for.
    auto stop_f = semaphore.stop();
    BOOST_CHECK(!stop_f.available());
    BOOST_CHECK(eventually_true([&] { return !semaphore.get_stats().inactive_reads; }));
    BOOST_CHECK(!handle);
    BOOST_CHECK_EQUAL(permit->get_state(), reader_permit::state::evicted);

    // Stop waits on all permits, so we need to destroy the permit before we can
    // wait on the stop future.
    permit = {};
    stop_f.get();
}

SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_permit_waiting_for_memory_goes_inactive) {
    const auto initial_resources = reader_concurrency_semaphore::resources{2, 2 * 1024};
    const auto serialize_multiplier = 2;
    const auto kill_multiplier = std::numeric_limits<uint32_t>::max(); // we don't want this to interfere with our test
    reader_concurrency_semaphore semaphore(initial_resources.count, initial_resources.memory, get_name(), 100,
            utils::updateable_value<uint32_t>(serialize_multiplier), utils::updateable_value<uint32_t>(kill_multiplier), reader_concurrency_semaphore::register_metrics::no);
    auto stop_sem = deferred_stop(semaphore);

    auto permit1 = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout, {}).get();
    auto permit2 = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout, {}).get();

    std::vector<reader_permit::resource_units> res;

    res.emplace_back(permit1.consume_memory(2048));
    res.emplace_back(permit2.consume_memory(2048));

    res.emplace_back(permit1.request_memory(1024).get());
    BOOST_REQUIRE_EQUAL(semaphore.get_blessed_permit(), permit1.id());
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().reads_enqueued_for_memory, 0);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().waiters, 0);

    auto res_fut = permit2.request_memory(1024);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().reads_enqueued_for_memory, 1);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().waiters, 1);

    simple_schema ss;
    auto s = ss.schema();
    auto handle = semaphore.register_inactive_read(make_empty_flat_reader_v2(s, permit2));

    // permit2 should have been evicted, its memory requests killed with std::bad_alloc
    BOOST_REQUIRE(!handle);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().permit_based_evictions, 1);
    BOOST_REQUIRE_EQUAL(permit2.get_state(), reader_permit::state::evicted);
    BOOST_REQUIRE_THROW(res_fut.get(), std::bad_alloc);

    res.clear();

    // Reproduce #13539: successful request for memory, should not include
    // amounts of failed request in the past
    BOOST_REQUIRE(permit2.needs_readmission());
    permit2.wait_readmission().get();
    permit2.request_memory(1024).get();
}

// Check that inactive reads are not needlessly evicted when admission is not
// blocked on resources.
// This test covers all the cases where eviction should **not** happen.
SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_no_unnecessary_evicting) {
    const auto initial_resources = reader_concurrency_semaphore::resources{2, 4 * 1024};
    const auto serialize_multiplier = std::numeric_limits<uint32_t>::max();
    const auto kill_multiplier = std::numeric_limits<uint32_t>::max();
    reader_concurrency_semaphore semaphore(initial_resources.count, initial_resources.memory, get_name(), 100,
            utils::updateable_value<uint32_t>(serialize_multiplier), utils::updateable_value<uint32_t>(kill_multiplier), reader_concurrency_semaphore::register_metrics::no);
    auto stop_sem = deferred_stop(semaphore);

    simple_schema ss;
    auto s = ss.schema();

    auto permit1 = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout, {}).get();

    // There are available resources
    {
        BOOST_REQUIRE_EQUAL(semaphore.available_resources().count, 1);
        BOOST_REQUIRE_EQUAL(semaphore.available_resources().memory, 3 * 1024);

        auto handle = semaphore.register_inactive_read(make_empty_flat_reader_v2(s, permit1));
        BOOST_REQUIRE(handle);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 1);

        semaphore.set_resources(initial_resources);
        BOOST_REQUIRE(handle);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 1);

        BOOST_REQUIRE(semaphore.unregister_inactive_read(std::move(handle)));
    }

    // Count resources are on the limit but no one wants more
    {
        auto permit2 = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout, {}).get();

        BOOST_REQUIRE_EQUAL(semaphore.available_resources().count, 0);
        BOOST_REQUIRE_EQUAL(semaphore.available_resources().memory, 2 * 1024);

        auto handle = semaphore.register_inactive_read(make_empty_flat_reader_v2(s, permit1));
        BOOST_REQUIRE(handle);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 1);

        semaphore.set_resources(initial_resources);
        BOOST_REQUIRE(handle);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 1);

        BOOST_REQUIRE(semaphore.unregister_inactive_read(std::move(handle)));
    }

    // Memory resources are on the limit but no one wants more
    {
        auto units = permit1.consume_memory(3 * 1024);

        BOOST_REQUIRE_EQUAL(semaphore.available_resources().count, 1);
        BOOST_REQUIRE_EQUAL(semaphore.available_resources().memory, 0);

        auto handle = semaphore.register_inactive_read(make_empty_flat_reader_v2(s, permit1));
        BOOST_REQUIRE(handle);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 1);
        BOOST_REQUIRE(semaphore.unregister_inactive_read(std::move(handle)));
    }

    // Up the resource count, we need more permits to check the rest of the scenarios
    semaphore.set_resources({4, 4 * 1024});

    // There are waiters but they are not blocked on resources
    {
        auto permit2 = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout, {}).get();
        auto permit3 = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout, {}).get();

        std::optional<reader_permit::need_cpu_guard> ncpu_guard1{permit1};
        std::optional<reader_permit::need_cpu_guard> ncpu_guard2{permit2};

        auto permit4_fut = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout, {});
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().waiters, 1);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().reads_queued_because_need_cpu_permits, 1);

        // First check the register path.
        auto handle = semaphore.register_inactive_read(make_empty_flat_reader_v2(s, permit3));

        BOOST_REQUIRE(handle);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().permit_based_evictions, 0);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 1);
        BOOST_REQUIRE_EQUAL(permit3.get_state(), reader_permit::state::inactive);

        // Now check the callback admission path (admission check on resources being freed).
        ncpu_guard2.reset();
        BOOST_REQUIRE(handle);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().permit_based_evictions, 0);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 1);
        BOOST_REQUIRE_EQUAL(permit3.get_state(), reader_permit::state::inactive);
    }
}

// Check that inactive reads are evicted when they are blocking admission
SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_necessary_evicting) {
    const auto initial_resources = reader_concurrency_semaphore::resources{2, 4 * 1024};
    const auto serialize_multiplier = std::numeric_limits<uint32_t>::max();
    const auto kill_multiplier = std::numeric_limits<uint32_t>::max();
    reader_concurrency_semaphore semaphore(initial_resources.count, initial_resources.memory, get_name(), 100,
            utils::updateable_value<uint32_t>(serialize_multiplier), utils::updateable_value<uint32_t>(kill_multiplier), reader_concurrency_semaphore::register_metrics::no);
    auto stop_sem = deferred_stop(semaphore);

    simple_schema ss;
    auto s = ss.schema();

    uint64_t evicted_reads = 0;

    auto permit1 = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout, {}).get();

    // No count resources - obtaining new permit
    {
        auto permit2 = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout, {}).get();

        BOOST_REQUIRE_EQUAL(semaphore.available_resources().count, 0);
        BOOST_REQUIRE_EQUAL(semaphore.available_resources().memory, 2 * 1024);

        auto handle = semaphore.register_inactive_read(make_empty_flat_reader_v2(s, permit1));
        BOOST_REQUIRE(handle);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 1);

        auto new_permit = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout, {}).get();
        BOOST_REQUIRE(!handle);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 0);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().permit_based_evictions, ++evicted_reads);
    }

    BOOST_REQUIRE(permit1.needs_readmission());
    permit1.wait_readmission().get();

    // No count resources - waiter
    {
        auto permit2 = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout, {}).get();

        BOOST_REQUIRE_EQUAL(semaphore.available_resources().count, 0);
        BOOST_REQUIRE_EQUAL(semaphore.available_resources().memory, 2 * 1024);

        auto new_permit_fut = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout, {});
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().waiters, 1);

        auto handle = semaphore.register_inactive_read(make_empty_flat_reader_v2(s, permit1));
        BOOST_REQUIRE(!handle);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 0);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().permit_based_evictions, ++evicted_reads);

        new_permit_fut.get();
    }

    BOOST_REQUIRE(permit1.needs_readmission());
    permit1.wait_readmission().get();

    // No memory resources
    {
        auto units = permit1.consume_memory(3 * 1024);

        BOOST_REQUIRE_EQUAL(semaphore.available_resources().count, 1);
        BOOST_REQUIRE_EQUAL(semaphore.available_resources().memory, 0);

        auto handle = semaphore.register_inactive_read(make_empty_flat_reader_v2(s, permit1));
        BOOST_REQUIRE(handle);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 1);

        auto new_permit = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout, {}).get();
        BOOST_REQUIRE(!handle);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 0);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().permit_based_evictions, ++evicted_reads);
    }

    BOOST_REQUIRE(permit1.needs_readmission());
    permit1.wait_readmission().get();

    // No memory resources - waiter
    {
        auto units = permit1.consume_memory(3 * 1024);

        BOOST_REQUIRE_EQUAL(semaphore.available_resources().count, 1);
        BOOST_REQUIRE_EQUAL(semaphore.available_resources().memory, 0);

        auto new_permit_fut = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout, {});
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().waiters, 1);

        auto handle = semaphore.register_inactive_read(make_empty_flat_reader_v2(s, permit1));
        BOOST_REQUIRE(!handle);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 0);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().permit_based_evictions, ++evicted_reads);

        new_permit_fut.get();
    }

    BOOST_REQUIRE(permit1.needs_readmission());
    permit1.wait_readmission().get();

    // No count resources - waiter blocked on something else too
    {
        auto permit2 = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout, {}).get();

        BOOST_REQUIRE_EQUAL(semaphore.available_resources().count, 0);
        BOOST_REQUIRE_EQUAL(semaphore.available_resources().memory, 2 * 1024);

        std::optional<reader_permit::need_cpu_guard> ncpu_guard{permit2};

        auto new_permit_fut = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout, {});
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().waiters, 1);

        auto handle = semaphore.register_inactive_read(make_empty_flat_reader_v2(s, permit1));
        BOOST_REQUIRE(handle);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 1);

        ncpu_guard.reset();
        BOOST_REQUIRE(!handle);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 0);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().permit_based_evictions, ++evicted_reads);

        new_permit_fut.get();
    }

    BOOST_REQUIRE(permit1.needs_readmission());
    permit1.wait_readmission().get();

    // No memory resources - waiter blocked on something else too
    {
        semaphore.set_resources({initial_resources.count + 1, initial_resources.memory});
        auto permit2 = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout, {}).get();
        auto units = permit1.consume_memory(2 * 1024);

        BOOST_REQUIRE_EQUAL(semaphore.available_resources().count, 1);
        BOOST_REQUIRE_EQUAL(semaphore.available_resources().memory, 0);

        std::optional<reader_permit::need_cpu_guard> ncpu_guard{permit2};

        auto new_permit_fut = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout, {});
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().waiters, 1);

        auto handle = semaphore.register_inactive_read(make_empty_flat_reader_v2(s, permit1));
        BOOST_REQUIRE(handle);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 1);

        ncpu_guard.reset();
        thread::yield(); // allow debug builds to schedule the fiber evicting the reads again
        BOOST_REQUIRE(!handle);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 0);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().permit_based_evictions, ++evicted_reads);

        new_permit_fut.get();

        semaphore.set_resources(initial_resources);
    }
}

// Check that a waiter permit which was queued due to the _ready_list not being
// empty, will be executed right after the previous read in _ready_list is
// executed, even if said read doesn't trigger admission checks via releasing
// resources.
SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_execution_stage_wakeup) {
    const auto initial_resources = reader_concurrency_semaphore::resources{2, 4 * 1024};
    const auto serialize_multiplier = std::numeric_limits<uint32_t>::max();
    const auto kill_multiplier = std::numeric_limits<uint32_t>::max();
    reader_concurrency_semaphore semaphore(initial_resources.count, initial_resources.memory, get_name(), 100,
            utils::updateable_value<uint32_t>(serialize_multiplier), utils::updateable_value<uint32_t>(kill_multiplier), reader_concurrency_semaphore::register_metrics::no);
    auto stop_sem = deferred_stop(semaphore);

    auto permit1 = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout, {}).get();

    BOOST_REQUIRE_EQUAL(semaphore.get_stats().reads_admitted, 1);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().reads_admitted_immediately, 1);

    bool func_called = false;
    auto func_fut = semaphore.with_ready_permit(permit1, [&] (reader_permit permit) {
        func_called = true;
        return sleep(std::chrono::milliseconds(1));
    });
    // permit1 should be on the ready list, not executed yet
    BOOST_REQUIRE(!func_called);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().waiters, 1);

    // trying to obtain a second permit should block on the _ready_list
    auto permit2_fut = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout, {});
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().reads_queued_because_ready_list, 1);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().waiters, 2);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().reads_admitted, 1);

    // After func runs, the _ready_list becomes empty and the waiting permit should be admitted
    func_fut.get();
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().waiters, 0);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().reads_admitted, 2);

    permit2_fut.get();
}

SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_live_update_count) {
    utils::updateable_value_source<int> count{1};
    const uint32_t initial_memory = 4 * 1024;
    const auto serialize_multiplier = std::numeric_limits<uint32_t>::max();
    const auto kill_multiplier = std::numeric_limits<uint32_t>::max();

    reader_concurrency_semaphore semaphore(
            utils::updateable_value(count),
            initial_memory,
            get_name(),
            100,
            utils::updateable_value<uint32_t>(serialize_multiplier),
            utils::updateable_value<uint32_t>(kill_multiplier),
            utils::updateable_value<uint32_t>(1),
            reader_concurrency_semaphore::register_metrics::no);
    auto stop_sem = deferred_stop(semaphore);

    BOOST_REQUIRE_EQUAL(semaphore.initial_resources(), reader_resources(count(), initial_memory));

    count.set(10);

    BOOST_REQUIRE_EQUAL(semaphore.initial_resources(), reader_resources(count(), initial_memory));
}

SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_live_update_cpu_concurrency) {
    simple_schema s;
    const auto schema = s.schema();

    utils::updateable_value_source<uint32_t> cpu_concurrency{2};
    const int32_t initial_count = 4;
    const uint32_t initial_memory = 4 * 1024;
    const auto serialize_multiplier = std::numeric_limits<uint32_t>::max();
    const auto kill_multiplier = std::numeric_limits<uint32_t>::max();

    reader_concurrency_semaphore semaphore(
            utils::updateable_value<int>(initial_count),
            initial_memory,
            get_name(),
            100,
            utils::updateable_value<uint32_t>(serialize_multiplier),
            utils::updateable_value<uint32_t>(kill_multiplier),
            utils::updateable_value(cpu_concurrency),
            reader_concurrency_semaphore::register_metrics::no);
    auto stop_sem = deferred_stop(semaphore);

    auto require_can_admit = [&] (bool expected_can_admit, const char* description,
            seastar::compat::source_location sl = seastar::compat::source_location::current()) {
        ::require_can_admit(schema, semaphore, expected_can_admit, description, sl);
    };

    auto permit1 = semaphore.obtain_permit(schema, get_name(), 1024, db::timeout_clock::now(), {}).get();

    require_can_admit(true, "!need_cpu");
    {
        reader_permit::need_cpu_guard ncpu_guard{permit1};

        require_can_admit(true, "need_cpu < cpu_concurrency");

        auto permit2 = semaphore.obtain_permit(schema, get_name(), 1024, db::timeout_clock::now(), {}).get();

        // no change
        require_can_admit(true, "need_cpu < cpu_concurrency");
        {
            reader_permit::need_cpu_guard ncpu_guard{permit2};
            require_can_admit(false, "need_cpu == cpu_concurrency");

            cpu_concurrency.set(3);

            require_can_admit(true, "after set(3): need_cpu < cpu_concurrency");

            cpu_concurrency.set(2);

            require_can_admit(false, "after set(2): need_cpu == cpu_concurrency");
        }
        require_can_admit(true, "need_cpu < cpu_concurrency");
    }
    require_can_admit(true, "!need_cpu");
}
