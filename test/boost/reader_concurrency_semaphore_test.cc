/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/util/closeable.hh>
#include <seastar/core/file.hh>
#include "reader_concurrency_semaphore.hh"
#include "test/lib/log.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/eventually.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/random_schema.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <boost/test/unit_test.hpp>
#include "readers/empty_v2.hh"
#include "replica/database.hh" // new_reader_base_cost is there :(

SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_clear_inactive_reads) {
    simple_schema s;
    std::vector<reader_permit> permits;
    std::vector<reader_concurrency_semaphore::inactive_read_handle> handles;

    {
        reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::no_limits{}, get_name());
        auto stop_sem = deferred_stop(semaphore);
        auto clear_permits = defer([&permits] { permits.clear(); });

        for (int i = 0; i < 10; ++i) {
            permits.emplace_back(semaphore.make_tracking_only_permit(s.schema(), get_name(), db::no_timeout));
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
            handles.emplace_back(semaphore.register_inactive_read(make_empty_flat_reader_v2(s.schema(), semaphore.make_tracking_only_permit(s.schema(), get_name(), db::no_timeout))));
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
        auto permit = semaphore.make_tracking_only_permit(s.schema(), get_name(), db::no_timeout);
        auto units2 = permit.consume_memory(1024);
    }
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources);

    // Not admitted, inactive
    {
        auto permit = semaphore.make_tracking_only_permit(s.schema(), get_name(), db::no_timeout);
        auto units2 = permit.consume_memory(1024);

        auto handle = semaphore.register_inactive_read(make_empty_flat_reader_v2(s.schema(), permit));
        BOOST_REQUIRE(semaphore.try_evict_one_inactive_read());
    }
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources);

    // Admitted, active
    {
        auto permit = semaphore.obtain_permit(s.schema(), get_name(), 1024, db::no_timeout).get0();
        auto units1 = permit.consume_memory(1024);
    }
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources);

    // Admitted, inactive
    {
        auto permit = semaphore.obtain_permit(s.schema(), get_name(), 1024, db::no_timeout).get0();
        auto units1 = permit.consume_memory(1024);

        auto handle = semaphore.register_inactive_read(make_empty_flat_reader_v2(s.schema(), permit));
        BOOST_REQUIRE(semaphore.try_evict_one_inactive_read());
    }
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources);
}

SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_abandoned_handle_closes_reader) {
    simple_schema s;
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::no_limits{}, get_name());
    auto stop_sem = deferred_stop(semaphore);

    auto permit = semaphore.make_tracking_only_permit(s.schema(), get_name(), db::no_timeout);
    {
        auto handle = semaphore.register_inactive_read(make_empty_flat_reader_v2(s.schema(), permit));
        // The handle is destroyed here, triggering the destrution of the inactive read.
        // If the test fails an assert() is triggered due to the reader being
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

    reader_permit_opt permit = semaphore.obtain_permit(s.schema(), get_name(), 1024, db::no_timeout).get();
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
            const auto consumed_resources = semaphore.available_resources();
            semaphore.consume(consumed_resources);

            auto fut = make_ready_future<>();
            if (permit->needs_readmission()) {
                fut = permit->wait_readmission();
            }
            BOOST_REQUIRE(!fut.available());

            semaphore.signal(consumed_resources);
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
        class skeleton_reader : public flat_mutation_reader_v2::impl {
            std::optional<reader_permit::resource_units> _resources;
        public:
            skeleton_reader(schema_ptr s, reader_permit permit)
                : impl(std::move(s), std::move(permit)) { }
            virtual future<> fill_buffer() override {
                reader_permit::blocked_guard _{_permit};
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
            future<> operator()(flat_mutation_reader_v2& reader) { return r.tick(reader); }
            future<> operator()(reader_concurrency_semaphore::inactive_read_handle& handle) { return r.tick(handle); }
        };

    private:
        schema_ptr _schema;
        reader_concurrency_semaphore& _semaphore;
        bool _memory_only = true;
        bool _evictable = false;
        reader_permit_opt _permit;
        std::optional<reader_permit::resource_units> _units;
        std::variant<std::monostate, flat_mutation_reader_v2, reader_concurrency_semaphore::inactive_read_handle> _reader;

    private:
        void make_reader() {
            _reader = make_flat_mutation_reader_v2<skeleton_reader>(_schema, *_permit);
        }
        future<> tick(std::monostate&) {
            make_reader();
            co_await tick(std::get<flat_mutation_reader_v2>(_reader));
        }
        future<> tick(flat_mutation_reader_v2& reader) {
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
            co_await tick(std::get<flat_mutation_reader_v2>(_reader));
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
                _permit = _semaphore.make_tracking_only_permit(_schema, "reader_m", db::no_timeout);
            } else {
                _permit = co_await _semaphore.obtain_permit(_schema, fmt::format("reader_{}", _evictable ? 'e' : 'a'), 1024, db::no_timeout);
            }
            _units = _permit->consume_memory(tests::random::get_int(128, 1024));
        }
        future<> tick() {
            return std::visit(reader_visitor{*this}, _reader);
        }
        future<> close() noexcept {
            if (auto reader = std::get_if<flat_mutation_reader_v2>(&_reader)) {
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
    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) override {
        return make_ready_future<size_t>(0);
    }

    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        return make_ready_future<size_t>(0);
    }

    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) override {
        return make_ready_future<size_t>(0);
    }

    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
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

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, const io_priority_class& pc) override {
        temporary_buffer<uint8_t> buf(1024);

        memset(buf.get_write(), 0xff, buf.size());

        return make_ready_future<temporary_buffer<uint8_t>>(std::move(buf));
    }
};

SEASTAR_TEST_CASE(reader_restriction_file_tracking) {
    return async([&] {
        reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, get_name(), 100, 4 * 1024);
        auto stop_sem = deferred_stop(semaphore);
        auto permit = semaphore.obtain_permit(nullptr, get_name(), 0, db::no_timeout).get();

        {
            auto tracked_file = make_tracked_file(file(shared_ptr<file_impl>(make_shared<dummy_file_impl>())), permit);

            BOOST_REQUIRE_EQUAL(4 * 1024, semaphore.available_resources().memory);

            auto buf1 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(3 * 1024, semaphore.available_resources().memory);

            auto buf2 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(2 * 1024, semaphore.available_resources().memory);

            auto buf3 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(1 * 1024, semaphore.available_resources().memory);

            auto buf4 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(0 * 1024, semaphore.available_resources().memory);

            auto buf5 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(-1 * 1024, semaphore.available_resources().memory);

            // Reassing buf1, should still have the same amount of units.
            buf1 = tracked_file.dma_read_bulk<char>(0, 0).get0();
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

            reader_permit_opt permit1 = semaphore.obtain_permit(nullptr, "permit1", replica::new_reader_base_cost, timeout).get();

            auto permit2_fut = semaphore.obtain_permit(nullptr, "permit2", replica::new_reader_base_cost, timeout);

            auto permit3_fut = semaphore.obtain_permit(nullptr, "permit3", replica::new_reader_base_cost, timeout);

            BOOST_REQUIRE_EQUAL(semaphore.waiters(), 2);

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
            reader_permit_opt permit1 = semaphore.obtain_permit(nullptr, "permit1", replica::new_reader_base_cost, db::no_timeout).get();

            auto permit2_fut = semaphore.obtain_permit(nullptr, "permit2", replica::new_reader_base_cost, db::no_timeout);

            auto permit3_fut = semaphore.obtain_permit(nullptr, "permit3", replica::new_reader_base_cost, db::no_timeout);

            BOOST_REQUIRE_EQUAL(semaphore.waiters(), 2);

            auto permit4_fut = semaphore.obtain_permit(nullptr, "permit4", replica::new_reader_base_cost, db::no_timeout);

            // The queue should now be full.
            BOOST_REQUIRE_THROW(permit4_fut.get(), std::runtime_error);

            permit1 = {};
            {
                auto res = permit2_fut.get0();
            }
            {
                auto res = permit3_fut.get();
            }
        }

        REQUIRE_EVENTUALLY_EQUAL(replica::new_reader_base_cost, semaphore.available_resources().memory);
    });
}

SEASTAR_THREAD_TEST_CASE(reader_concurrency_semaphore_dump_reader_diganostics) {
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::no_limits{}, get_name());
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
            auto permit = semaphore.make_tracking_only_permit(schema, op_names.at(tests::random::get_int<unsigned>(0, nr_ops - 1)), db::no_timeout);
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
        reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::no_limits{}, get_name());
        // Checks for stop() should not be triggered.
    }

    BOOST_TEST_MESSAGE("0 permits");
    {
        reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::no_limits{}, get_name());
        // Test will fail by timing out.
        semaphore.stop().get();
    }

    BOOST_TEST_MESSAGE("1 permit");
    {
        auto semaphore = std::make_unique<reader_concurrency_semaphore>(reader_concurrency_semaphore::no_limits{}, get_name());
        auto permit = std::make_unique<reader_permit>(semaphore->make_tracking_only_permit(nullptr, "permit1", db::no_timeout));

        // Test will fail via use-after-free
        auto f = semaphore->stop().then([semaphore = std::move(semaphore)] { });

        yield().get();
        BOOST_REQUIRE(!f.available());
        permit.reset();

        // Test will fail by timing out.
        f.get();
    }
}

SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_admission) {
    simple_schema s;
    const auto schema = s.schema();
    const auto initial_resources = reader_concurrency_semaphore::resources{2, 2 * 1024};
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, get_name(), initial_resources.count, initial_resources.memory);
    auto stop_sem = deferred_stop(semaphore);

    auto require_can_admit = [&] (bool expected_can_admit, const char* description,
            seastar::compat::source_location sl = seastar::compat::source_location::current()) {
        testlog.trace("Running admission scenario {}, with exepcted_can_admit={}", description, expected_can_admit);
        const auto stats_before = semaphore.get_stats();

        auto admit_fut = semaphore.obtain_permit(schema, get_name(), 1024, db::timeout_clock::now());
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
        // Deliberately not checking `reads_enqueued`, a read can be enqueued temporarily during the admission process.

        if (can_admit == expected_can_admit) {
            testlog.trace("admission scenario '{}' with expected_can_admit={} passed at {}:{}", description, expected_can_admit, sl.file_name(),
                    sl.line());
        } else {
            BOOST_FAIL(fmt::format("admission scenario '{}'  with expected_can_admit={} failed at {}:{}\ndiagnostics: {}", description,
                    expected_can_admit, sl.file_name(), sl.line(), semaphore.dump_diagnostics()));
        }
    };

    require_can_admit(true, "semaphore in initial state");

    // resources and waitlist
    {
        reader_permit_opt permit = semaphore.obtain_permit(schema, get_name(), 1024, db::timeout_clock::now()).get();

        require_can_admit(true, "enough resources");

        const auto stats_before = semaphore.get_stats();

        auto enqueued_permit_fut = semaphore.obtain_permit(schema, get_name(), 2 * 1024, db::no_timeout);
        {
            const auto stats_after = semaphore.get_stats();
            BOOST_REQUIRE(!enqueued_permit_fut.available());
            BOOST_REQUIRE_EQUAL(stats_after.reads_enqueued, stats_before.reads_enqueued + 1);
            BOOST_REQUIRE_EQUAL(stats_after.reads_admitted, stats_before.reads_admitted);
            BOOST_REQUIRE_EQUAL(semaphore.waiters(), 1);
        }

        BOOST_REQUIRE(semaphore.available_resources().count >= 1);
        BOOST_REQUIRE(semaphore.available_resources().memory >= 1024);
        require_can_admit(false, "enough resources but waitlist not empty");

        permit = {};

        reader_permit _(enqueued_permit_fut.get());
    }
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources);
    require_can_admit(true, "semaphore in initial state");

    // used and blocked
    {
        auto permit = semaphore.obtain_permit(schema, get_name(), 1024, db::timeout_clock::now()).get();

        require_can_admit(true, "!used");
        {
            reader_permit::used_guard ug{permit};

            require_can_admit(false, "used > blocked");
            {
                reader_permit::blocked_guard bg{permit};
                require_can_admit(true, "used == blocked");
            }
            require_can_admit(false, "used > blocked");
        }
        require_can_admit(true, "!used");
    }
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources);
    require_can_admit(true, "semaphore in initial state");

    // forward progress -- resources
    {
        const auto resources = reader_resources::with_memory(semaphore.available_resources().memory);
        semaphore.consume(resources);
        require_can_admit(true, "semaphore with no memory but all count available");
        semaphore.signal(resources);
    }
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources);
    require_can_admit(true, "semaphore in initial state");

    // forward progress -- readmission
    {
        auto permit = semaphore.obtain_permit(schema, get_name(), 1024, db::timeout_clock::now()).get();

        auto irh = semaphore.register_inactive_read(make_empty_flat_reader_v2(s.schema(), permit));
        BOOST_REQUIRE(semaphore.try_evict_one_inactive_read());
        BOOST_REQUIRE(!irh);

        reader_permit::used_guard _{permit};

        const auto stats_before = semaphore.get_stats();

        auto wait_fut = make_ready_future<>();
        if (permit.needs_readmission()) {
            wait_fut = permit.wait_readmission();
        }
        wait_fut.wait();
        BOOST_REQUIRE(!wait_fut.failed());

        const auto stats_after = semaphore.get_stats();
        BOOST_REQUIRE_EQUAL(stats_after.reads_admitted, stats_before.reads_admitted + 1);
        BOOST_REQUIRE_EQUAL(stats_after.reads_enqueued, stats_before.reads_enqueued);
    }
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources);
    require_can_admit(true, "semaphore in initial state");

    // inactive readers
    {
        auto permit = semaphore.obtain_permit(schema, get_name(), 1024, db::timeout_clock::now()).get();

        require_can_admit(true, "!used");
        {
            auto irh = semaphore.register_inactive_read(make_empty_flat_reader_v2(s.schema(), permit));
            require_can_admit(true, "inactive");

            reader_permit::used_guard ug{permit};

            require_can_admit(true, "inactive (used)");

            {
                auto rd = semaphore.unregister_inactive_read(std::move(irh));
                rd->close().get();
            }

            require_can_admit(false, "used > blocked");

            irh = semaphore.register_inactive_read(make_empty_flat_reader_v2(s.schema(), permit));
            require_can_admit(true, "inactive (used)");
        }
        require_can_admit(true, "!used");
    }
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources);
    require_can_admit(true, "semaphore in initial state");

    // evicting inactive readers for admission
    {
        auto permit1 = semaphore.obtain_permit(schema, get_name(), 1024, db::timeout_clock::now()).get();
        auto irh1 = semaphore.register_inactive_read(make_empty_flat_reader_v2(s.schema(), permit1));

        auto permit2 = semaphore.obtain_permit(schema, get_name(), 1024, db::timeout_clock::now()).get();
        auto irh2 = semaphore.register_inactive_read(make_empty_flat_reader_v2(s.schema(), permit2));

        require_can_admit(true, "evictable reads");
    }
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources);
    require_can_admit(true, "semaphore in initial state");

    auto check_admitting_enqueued_read = [&] (auto pre_admission_hook, auto post_enqueue_hook) {
        auto cookie1 = pre_admission_hook();

        require_can_admit(false, "admission blocked");

        const auto stats_before = semaphore.get_stats();

        auto permit2_fut = semaphore.obtain_permit(schema, get_name(), 1024, db::no_timeout);

        const auto stats_after = semaphore.get_stats();
        BOOST_REQUIRE_EQUAL(stats_after.reads_admitted, stats_before.reads_admitted);
        BOOST_REQUIRE_EQUAL(stats_after.reads_enqueued, stats_before.reads_enqueued + 1);
        BOOST_REQUIRE_EQUAL(semaphore.waiters(), 1);

        auto cookie2 = post_enqueue_hook(cookie1);

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
                return reader_permit_opt(semaphore.obtain_permit(schema, get_name(), 2 * 1024, db::timeout_clock::now()).get());
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
                return reader_permit_opt(semaphore.obtain_permit(schema, get_name(), 2 * 1024, db::timeout_clock::now()).get());
            },
            [&] (reader_permit_opt& permit1) {
                return semaphore.register_inactive_read(make_empty_flat_reader_v2(s.schema(), *permit1));
            }
        );
    }
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources);
    require_can_admit(true, "semaphore in initial state");

    // admitting enqueued reads -- permit becomes unused
    {
        check_admitting_enqueued_read(
            [&] {
                auto permit = semaphore.obtain_permit(schema, get_name(), 1024, db::timeout_clock::now()).get();
                require_can_admit(true, "enough resources");
                return std::pair(permit, std::optional<reader_permit::used_guard>{permit});
            }, [&] (std::pair<reader_permit, std::optional<reader_permit::used_guard>>& permit_and_used_guard) {
                permit_and_used_guard.second.reset();
                return 0;
            }
        );
    }
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources);
    require_can_admit(true, "semaphore in initial state");

    // admitting enqueued reads -- permit becomes blocked
    {
        check_admitting_enqueued_read(
            [&] {
                auto permit = semaphore.obtain_permit(schema, get_name(), 1024, db::timeout_clock::now()).get();
                require_can_admit(true, "enough resources");
                return std::pair(permit, reader_permit::used_guard{permit});
            }, [&] (std::pair<reader_permit, reader_permit::used_guard>& permit_and_used_guard) {
                return reader_permit::blocked_guard{permit_and_used_guard.first};
            }
        );
    }
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), initial_resources);
    require_can_admit(true, "semaphore in initial state");
}

SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_used_blocked) {
    const auto initial_resources = reader_concurrency_semaphore::resources{2, 2 * 1024};
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, get_name(), initial_resources.count, initial_resources.memory);
    auto stop_sem = deferred_stop(semaphore);

    BOOST_REQUIRE_EQUAL(semaphore.get_stats().current_permits, 0);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().used_permits, 0);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().blocked_permits, 0);

    auto permit = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout).get0();

    for (auto scenario = 0; scenario < 5; ++scenario) {
        testlog.info("Running scenario {}", scenario);

        std::vector<reader_permit::used_guard> used;
        std::vector<reader_permit::blocked_guard> blocked;
        unsigned count;

        switch (scenario) {
            case 0:
                used.emplace_back(permit);

                BOOST_REQUIRE_EQUAL(semaphore.get_stats().current_permits, 1);
                BOOST_REQUIRE_EQUAL(semaphore.get_stats().used_permits, 1);
                BOOST_REQUIRE_EQUAL(semaphore.get_stats().blocked_permits, 0);
                break;
            case 1:
                used.emplace_back(permit);
                blocked.emplace_back(permit);

                BOOST_REQUIRE_EQUAL(semaphore.get_stats().current_permits, 1);
                BOOST_REQUIRE_EQUAL(semaphore.get_stats().used_permits, 1);
                BOOST_REQUIRE_EQUAL(semaphore.get_stats().blocked_permits, 1);
                break;
            case 2:
                blocked.emplace_back(permit);

                BOOST_REQUIRE_EQUAL(semaphore.get_stats().current_permits, 1);
                BOOST_REQUIRE_EQUAL(semaphore.get_stats().used_permits, 0);
                BOOST_REQUIRE_EQUAL(semaphore.get_stats().blocked_permits, 0);
                break;
            case 3:
                blocked.emplace_back(permit);
                used.emplace_back(permit);

                BOOST_REQUIRE_EQUAL(semaphore.get_stats().current_permits, 1);
                BOOST_REQUIRE_EQUAL(semaphore.get_stats().used_permits, 1);
                BOOST_REQUIRE_EQUAL(semaphore.get_stats().blocked_permits, 1);
                break;
            default:
                count = tests::random::get_int<unsigned>(3, 100);
                for (unsigned i = 0; i < count; ++i) {
                    if (tests::random::get_bool()) {
                        used.emplace_back(permit);
                    } else {
                        blocked.emplace_back(permit);
                    }
                }
                break;
        }

        while (!used.empty() && !blocked.empty()) {
            const bool pop_used = !used.empty() && tests::random::get_bool();

            if (pop_used) {
                used.pop_back();
                if (used.empty()) {
                    BOOST_REQUIRE_EQUAL(semaphore.get_stats().used_permits, 0);
                }
            } else {
                blocked.pop_back();
                if (blocked.empty()) {
                    BOOST_REQUIRE_EQUAL(semaphore.get_stats().blocked_permits, 0);
                }
            }
        }
    }
}


SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_evict_inactive_reads_for_table) {
    auto spec = tests::make_random_schema_specification(get_name());

    std::list<tests::random_schema> schemas;
    std::unordered_map<tests::random_schema*, std::vector<reader_concurrency_semaphore::inactive_read_handle>> schema_handles;
    for (unsigned i = 0; i < 4; ++i) {
        auto& s = schemas.emplace_back(tests::random_schema(i, *spec));
        schema_handles.emplace(&s, std::vector<reader_concurrency_semaphore::inactive_read_handle>{});
    }

    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::no_limits{}, get_name());
    auto stop_sem = deferred_stop(semaphore);

    for (auto& s : schemas) {
        auto& handles = schema_handles[&s];
        for (int i = 0; i < 10; ++i) {
            handles.emplace_back(semaphore.register_inactive_read(make_empty_flat_reader_v2(s.schema(), semaphore.make_tracking_only_permit(s.schema(), get_name(), db::no_timeout))));
        }
    }

    for (auto& s : schemas) {
        auto& handles = schema_handles[&s];
        BOOST_REQUIRE(std::all_of(handles.begin(), handles.end(), [] (const reader_concurrency_semaphore::inactive_read_handle& handle) { return bool(handle); }));
    }

    for (auto& s : schemas) {
        auto& handles = schema_handles[&s];
        BOOST_REQUIRE(std::all_of(handles.begin(), handles.end(), [] (const reader_concurrency_semaphore::inactive_read_handle& handle) { return bool(handle); }));
        semaphore.evict_inactive_reads_for_table(s.schema()->id()).get();
        for (const auto& [k, v] : schema_handles) {
            if (k == &s) {
                BOOST_REQUIRE(std::all_of(v.begin(), v.end(), [] (const reader_concurrency_semaphore::inactive_read_handle& handle) { return !bool(handle); }));
            } else if (!v.empty()) {
                BOOST_REQUIRE(std::all_of(v.begin(), v.end(), [] (const reader_concurrency_semaphore::inactive_read_handle& handle) { return bool(handle); }));
            }
        }
        handles.clear();
    }
}

// Reproduces https://github.com/scylladb/scylladb/issues/11770
SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_evict_inactive_reads_when_all_is_blocked) {
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
        reader_permit::used_guard _ug;
        std::optional<reader_permit::blocked_guard> _bg;

    public:
        explicit read(reader_permit p) : _permit(std::move(p)), _read_started_fut(_read_started_pr.get_future()), _ug(_permit) { }
        future<> wait_read_started() { return std::move(_read_started_fut); }
        void set_read_done() { _read_done_pr.set_value(); }
        void mark_as_blocked() { _bg.emplace(_permit); }
        void mark_as_unblocked() { _bg.reset(); }
        reader_concurrency_semaphore::read_func get_read_func() {
            return [this] (reader_permit permit) -> future<> {
                _read_started_pr.set_value();
                co_await _read_done_pr.get_future();
            };
        }
    };

    auto p1 = semaphore.obtain_permit(s, get_name(), 1024, db::no_timeout).get();
    auto irh1 = semaphore.register_inactive_read(make_empty_flat_reader_v2(ss.schema(), p1));

    auto p2 = semaphore.obtain_permit(s, get_name(), 1024, db::no_timeout).get();
    read rd2(p2);
    auto fut2 = semaphore.with_ready_permit(p2, rd2.get_read_func());

    // At this point we expect to have:
    // * 1 inactive read (not evicted)
    // * 1 used (but not blocked) read on the ready list
    // * 1 waiter
    // * no more count resources left
    auto p3_fut = semaphore.obtain_permit(s, get_name(), 1024, db::no_timeout);
    BOOST_REQUIRE_EQUAL(semaphore.waiters(), 1);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().reads_enqueued, 1);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().used_permits, 1);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().blocked_permits, 0);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 1);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().permit_based_evictions, 0);
    BOOST_REQUIRE_EQUAL(semaphore.available_resources().count, 0);
    BOOST_REQUIRE(irh1);

    // Start the read emptying the ready list, this should not be enough to admit p3
    rd2.wait_read_started().get();
    BOOST_REQUIRE_EQUAL(semaphore.waiters(), 1);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().used_permits, 1);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().blocked_permits, 0);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 1);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().permit_based_evictions, 0);
    BOOST_REQUIRE_EQUAL(semaphore.available_resources().count, 0);
    BOOST_REQUIRE(irh1);

    // Marking p2 as blocked should now allow p3 to be admitted by evicting p1
    rd2.mark_as_blocked();
    BOOST_REQUIRE_EQUAL(semaphore.waiters(), 0);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().used_permits, 1);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().blocked_permits, 1);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 0);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().permit_based_evictions, 1);
    BOOST_REQUIRE_EQUAL(semaphore.available_resources().count, 0);
    BOOST_REQUIRE(!irh1);

    p3_fut.get();
    rd2.mark_as_unblocked();
    rd2.set_read_done();
    fut2.get();
}

SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_set_resources) {
    const auto initial_resources = reader_concurrency_semaphore::resources{4, 4 * 1024};
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::for_tests{}, get_name(), initial_resources.count, initial_resources.memory);
    auto stop_sem = deferred_stop(semaphore);

    auto permit1 = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout).get0();
    auto permit2 = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout).get0();
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

    auto permit3_fut = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().reads_enqueued, 1);
    BOOST_REQUIRE_EQUAL(semaphore.waiters(), 1);

    semaphore.set_resources({4, 4 * 1024});
    BOOST_REQUIRE_EQUAL(semaphore.waiters(), 0);
    BOOST_REQUIRE_EQUAL(semaphore.available_resources(), reader_resources(1, 1024));
    BOOST_REQUIRE_EQUAL(semaphore.initial_resources(), reader_resources(4, 4 * 1024));
    permit3_fut.get();
}


// Check that `stop()` correctly evicts all inactive reads.
SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_stop_with_inactive_reads) {
    reader_concurrency_semaphore semaphore(reader_concurrency_semaphore::no_limits{}, get_name());

    simple_schema ss;
    auto s = ss.schema();

    auto permit = reader_permit_opt(semaphore.obtain_permit(s, get_name(), 1024, db::no_timeout).get());

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

// Check that inactive reads are not needlessly evicted when admission is not
// blocked on resources.
// This test covers all the cases where eviction should **not** happen.
SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_no_unnecessary_evicting) {
    const auto initial_resources = reader_concurrency_semaphore::resources{2, 4 * 1024};
    reader_concurrency_semaphore semaphore(initial_resources.count, initial_resources.memory, get_name(), 100);
    auto stop_sem = deferred_stop(semaphore);

    simple_schema ss;
    auto s = ss.schema();

    auto permit1 = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout).get();

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
        auto permit2 = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout).get();

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
        auto permit2 = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout).get();
        auto permit3 = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout).get();

        std::optional<reader_permit::used_guard> ug1{permit1};
        std::optional<reader_permit::used_guard> ug2{permit2};

        auto permit4_fut = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout);
        BOOST_REQUIRE_EQUAL(semaphore.waiters(), 1);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().reads_queued_because_used_permits, 1);

        // First check the register path.
        auto handle = semaphore.register_inactive_read(make_empty_flat_reader_v2(s, permit3));

        BOOST_REQUIRE(handle);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().permit_based_evictions, 0);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 1);
        BOOST_REQUIRE_EQUAL(permit3.get_state(), reader_permit::state::inactive);

        // Now check the callback admission path (admission check on resources being freed).
        ug2.reset();
        BOOST_REQUIRE(handle);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().permit_based_evictions, 0);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 1);
        BOOST_REQUIRE_EQUAL(permit3.get_state(), reader_permit::state::inactive);
    }
}

// Check that inactive reads are evicted when they are blocking admission
SEASTAR_THREAD_TEST_CASE(test_reader_concurrency_semaphore_necessary_evicting) {
    const auto initial_resources = reader_concurrency_semaphore::resources{2, 4 * 1024};
    reader_concurrency_semaphore semaphore(initial_resources.count, initial_resources.memory, get_name(), 100);
    auto stop_sem = deferred_stop(semaphore);

    simple_schema ss;
    auto s = ss.schema();

    uint64_t evicted_reads = 0;

    auto permit1 = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout).get();

    // No count resources - obtaining new permit
    {
        auto permit2 = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout).get();

        BOOST_REQUIRE_EQUAL(semaphore.available_resources().count, 0);
        BOOST_REQUIRE_EQUAL(semaphore.available_resources().memory, 2 * 1024);

        auto handle = semaphore.register_inactive_read(make_empty_flat_reader_v2(s, permit1));
        BOOST_REQUIRE(handle);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 1);

        auto new_permit = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout).get();
        BOOST_REQUIRE(!handle);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 0);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().permit_based_evictions, ++evicted_reads);
    }

    BOOST_REQUIRE(permit1.needs_readmission());
    permit1.wait_readmission().get();

    // No count resources - waiter
    {
        auto permit2 = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout).get();

        BOOST_REQUIRE_EQUAL(semaphore.available_resources().count, 0);
        BOOST_REQUIRE_EQUAL(semaphore.available_resources().memory, 2 * 1024);

        auto new_permit_fut = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout);
        BOOST_REQUIRE_EQUAL(semaphore.waiters(), 1);

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

        auto new_permit = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout).get();
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

        auto new_permit_fut = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout);
        BOOST_REQUIRE_EQUAL(semaphore.waiters(), 1);

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
        auto permit2 = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout).get();

        BOOST_REQUIRE_EQUAL(semaphore.available_resources().count, 0);
        BOOST_REQUIRE_EQUAL(semaphore.available_resources().memory, 2 * 1024);

        std::optional<reader_permit::used_guard> ug{permit2};

        auto new_permit_fut = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout);
        BOOST_REQUIRE_EQUAL(semaphore.waiters(), 1);

        auto handle = semaphore.register_inactive_read(make_empty_flat_reader_v2(s, permit1));
        BOOST_REQUIRE(handle);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 1);

        ug.reset();
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
        auto permit2 = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout).get();
        auto units = permit1.consume_memory(2 * 1024);

        BOOST_REQUIRE_EQUAL(semaphore.available_resources().count, 1);
        BOOST_REQUIRE_EQUAL(semaphore.available_resources().memory, 0);

        std::optional<reader_permit::used_guard> ug{permit2};

        auto new_permit_fut = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout);
        BOOST_REQUIRE_EQUAL(semaphore.waiters(), 1);

        auto handle = semaphore.register_inactive_read(make_empty_flat_reader_v2(s, permit1));
        BOOST_REQUIRE(handle);
        BOOST_REQUIRE_EQUAL(semaphore.get_stats().inactive_reads, 1);

        ug.reset();
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
    reader_concurrency_semaphore semaphore(initial_resources.count, initial_resources.memory, get_name(), 100);
    auto stop_sem = deferred_stop(semaphore);

    auto permit1 = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout).get();

    BOOST_REQUIRE_EQUAL(semaphore.get_stats().reads_admitted, 1);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().reads_admitted_immediately, 1);

    bool func_called = false;
    auto func_fut = semaphore.with_ready_permit(permit1, [&] (reader_permit permit) {
        func_called = true;
        return sleep(std::chrono::milliseconds(1));
    });
    // permit1 should be on the ready list, not executed yet
    BOOST_REQUIRE(!func_called);
    BOOST_REQUIRE_EQUAL(semaphore.waiters(), 0);

    // trying to obtain a second permit should block on the _ready_list
    auto permit2_fut = semaphore.obtain_permit(nullptr, get_name(), 1024, db::no_timeout);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().reads_queued_because_ready_list, 1);
    BOOST_REQUIRE_EQUAL(semaphore.waiters(), 1);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().reads_admitted, 1);

    // After func runs, the _ready_list becomes empty and the waiting permit should be admitted
    func_fut.get();
    BOOST_REQUIRE_EQUAL(semaphore.waiters(), 0);
    BOOST_REQUIRE_EQUAL(semaphore.get_stats().reads_admitted, 2);

    permit2_fut.get();
}
