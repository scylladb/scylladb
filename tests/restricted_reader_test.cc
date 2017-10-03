/*
 * Copyright (C) 2017 ScyllaDB
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

#include "core/thread.hh"
#include "core/sleep.hh"

#include "cell_locking.hh"
#include "mutation_reader.hh"
#include "sstables/sstables.hh"
#include "sstable_mutation_readers.hh"

#include "tests/simple_schema.hh"
#include "tests/sstable_utils.hh"
#include "tests/test-utils.hh"
#include "tests/tmpdir.hh"


thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;


static const std::size_t new_reader_base_cost{16 * 1024};


using namespace seastar;

template<typename EventuallySucceedingFunction>
static bool eventually_true(EventuallySucceedingFunction&& f) {
    const unsigned max_attempts = 10;
    unsigned attempts = 0;
    while (true) {
        if (f()) {
            return true;
        }

        if (++attempts < max_attempts) {
            sleep(std::chrono::milliseconds(1 << attempts)).get0();
        } else {
            return false;
        }
    }

    return false;
}

#define REQUIRE_EVENTUALLY_EQUAL(a, b) BOOST_REQUIRE(eventually_true([&] { return a == b; }))


sstables::shared_sstable create_sstable(simple_schema& sschema, const sstring& path) {
    std::vector<mutation> mutations;
    mutations.reserve(1 << 14);

    for (std::size_t p = 0; p < (1 << 10); ++p) {
        mutation m(sschema.make_pkey(p), sschema.schema());
        sschema.add_static_row(m, sprint("%i_static_val", p));

        for (std::size_t c = 0; c < (1 << 4); ++c) {
            sschema.add_row(m, sschema.make_ckey(c), sprint("val_%i", c));
        }

        mutations.emplace_back(std::move(m));
        thread::yield();
    }

    return make_sstable_containing([&] {
            return make_lw_shared<sstables::sstable>(sschema.schema(), path, 0, sstables::sstable::version_types::la, sstables::sstable::format_types::big);
        }
        , mutations);
}


class tracking_reader : public mutation_reader::impl {
    mutation_reader _reader;
    std::size_t _call_count{0};
    std::size_t _ff_count{0};
public:
    tracking_reader(semaphore* resources_sem, schema_ptr schema, lw_shared_ptr<sstables::sstable> sst)
        : _reader(make_mutation_reader<sstable_range_wrapping_reader>(
                        std::move(sst),
                        std::move(schema),
                        query::full_partition_range,
                        query::full_slice,
                        default_priority_class(),
                        reader_resource_tracker(resources_sem),
                        streamed_mutation::forwarding::no,
                        mutation_reader::forwarding::yes)) {
    }

    virtual future<streamed_mutation_opt> operator()() override {
        ++_call_count;
        return _reader();
    }

    virtual future<> fast_forward_to(const dht::partition_range& pr) override {
        ++_ff_count;
        // Don't forward this to the underlying reader, it will force us
        // to come up with meaningful partition-ranges which is hard and
        // unecessary for these tests.
        return make_ready_future<>();
    }

    std::size_t call_count() const {
        return _call_count;
    }

    std::size_t ff_count() const {
        return _ff_count;
    }
};

class reader_wrapper {
    mutation_reader _reader;
    tracking_reader* _tracker{nullptr};

public:
    reader_wrapper(
            const restricted_mutation_reader_config& config,
            schema_ptr schema,
            lw_shared_ptr<sstables::sstable> sst) {
        auto ms = mutation_source([this, &config, sst=std::move(sst)] (schema_ptr schema, const dht::partition_range&) {
            auto tracker_ptr = std::make_unique<tracking_reader>(config.resources_sem, std::move(schema), std::move(sst));
            _tracker = tracker_ptr.get();
            return mutation_reader(std::move(tracker_ptr));
        });

        _reader = make_restricted_reader(config, std::move(ms), std::move(schema));
    }

    future<streamed_mutation_opt> operator()() {
        return _reader();
    }

    future<> fast_forward_to(const dht::partition_range& pr) {
        return _reader.fast_forward_to(pr);
    }

    std::size_t call_count() const {
        return _tracker ? _tracker->call_count() : 0;
    }

    std::size_t ff_count() const {
        return _tracker ? _tracker->ff_count() : 0;
    }

    bool created() const {
        return bool(_tracker);
    }
};

struct restriction_data {
    std::unique_ptr<semaphore> reader_semaphore;
    restricted_mutation_reader_config config;

    restriction_data(std::size_t units,
            std::chrono::nanoseconds timeout = {},
            std::size_t max_queue_length = std::numeric_limits<std::size_t>::max())
        : reader_semaphore(std::make_unique<semaphore>(units)) {
        config.resources_sem = reader_semaphore.get();
        config.timeout = timeout;
        config.max_queue_length = max_queue_length;
    }
};


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
        throw std::bad_function_call();
    }

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, const io_priority_class& pc) override {
        temporary_buffer<uint8_t> buf(1024);

        memset(buf.get_write(), 0xff, buf.size());

        return make_ready_future<temporary_buffer<uint8_t>>(std::move(buf));
    }
};


SEASTAR_TEST_CASE(reader_restriction_file_tracking) {
    return async([&] {
        restriction_data rd(4 * 1024);

        {
            reader_resource_tracker resource_tracker(rd.config.resources_sem);

            auto tracked_file = resource_tracker.track(
                    file(shared_ptr<file_impl>(make_shared<dummy_file_impl>())));

            BOOST_REQUIRE_EQUAL(4 * 1024, rd.reader_semaphore->available_units());

            auto buf1 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(3 * 1024, rd.reader_semaphore->available_units());

            auto buf2 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(2 * 1024, rd.reader_semaphore->available_units());

            auto buf3 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(1 * 1024, rd.reader_semaphore->available_units());

            auto buf4 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(0 * 1024, rd.reader_semaphore->available_units());

            auto buf5 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(-1 * 1024, rd.reader_semaphore->available_units());

            // Reassing buf1, should still have the same amount of units.
            buf1 = tracked_file.dma_read_bulk<char>(0, 0).get0();
            BOOST_REQUIRE_EQUAL(-1 * 1024, rd.reader_semaphore->available_units());

            // Move buf1 to the heap, so that we can safely destroy it
            auto buf1_ptr = std::make_unique<temporary_buffer<char>>(std::move(buf1));
            BOOST_REQUIRE_EQUAL(-1 * 1024, rd.reader_semaphore->available_units());

            buf1_ptr.reset();
            BOOST_REQUIRE_EQUAL(0 * 1024, rd.reader_semaphore->available_units());

            // Move tracked_file to the heap, so that we can safely destroy it.
            auto tracked_file_ptr = std::make_unique<file>(std::move(tracked_file));
            tracked_file_ptr.reset();

            // Move buf4 to the heap, so that we can safely destroy it
            auto buf4_ptr = std::make_unique<temporary_buffer<char>>(std::move(buf4));
            BOOST_REQUIRE_EQUAL(0 * 1024, rd.reader_semaphore->available_units());

            // Releasing buffers that overlived the tracked-file they
            // originated from should succeed.
            buf4_ptr.reset();
            BOOST_REQUIRE_EQUAL(1 * 1024, rd.reader_semaphore->available_units());
        }

        // All units should have been deposited back.
        REQUIRE_EVENTUALLY_EQUAL(4 * 1024, rd.reader_semaphore->available_units());
    });
}

SEASTAR_TEST_CASE(restricted_reader_reading) {
    return async([&] {
        restriction_data rd(new_reader_base_cost);

        {
            simple_schema s;
            auto tmp = make_lw_shared<tmpdir>();
            auto sst = create_sstable(s, tmp->path);

            auto reader1 = reader_wrapper(rd.config, s.schema(), sst);

            reader1().get();

            BOOST_REQUIRE_LE(rd.reader_semaphore->available_units(), 0);
            BOOST_REQUIRE_EQUAL(reader1.call_count(), 1);

            auto reader2 = reader_wrapper(rd.config, s.schema(), sst);
            auto read_fut = reader2();

            // reader2 shouldn't be allowed just yet.
            BOOST_REQUIRE_EQUAL(reader2.call_count(), 0);

            // Move reader1 to the heap, so that we can safely destroy it.
            auto reader1_ptr = std::make_unique<reader_wrapper>(std::move(reader1));
            reader1_ptr.reset();

            // reader1's destruction should've made some space for reader2 by now.
            REQUIRE_EVENTUALLY_EQUAL(reader2.call_count(), 1);
            read_fut.get();

            {
                // Consume all available units.
                const auto consume_guard = consume_units(*rd.reader_semaphore, rd.reader_semaphore->current());

                // Already allowed readers should not be blocked anymore even if
                // there are no more units available.
                read_fut = reader2();
                BOOST_REQUIRE_EQUAL(reader2.call_count(), 2);
                read_fut.get();
            }
        }

        // All units should have been deposited back.
        REQUIRE_EVENTUALLY_EQUAL(new_reader_base_cost, rd.reader_semaphore->available_units());
    });
}

SEASTAR_TEST_CASE(restricted_reader_timeout) {
    return async([&] {
        restriction_data rd(new_reader_base_cost, std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::milliseconds{10}));

        {
            simple_schema s;
            auto tmp = make_lw_shared<tmpdir>();
            auto sst = create_sstable(s, tmp->path);

            auto reader1 = reader_wrapper(rd.config, s.schema(), sst);
            reader1().get();

            auto reader2 = reader_wrapper(rd.config, s.schema(), sst);
            auto read_fut = reader2();

            seastar::sleep(std::chrono::milliseconds(20)).get();

            // The read should have timed out.
            BOOST_REQUIRE(read_fut.failed());
            BOOST_REQUIRE_THROW(std::rethrow_exception(read_fut.get_exception()), semaphore_timed_out);
        }

        // All units should have been deposited back.
        REQUIRE_EVENTUALLY_EQUAL(new_reader_base_cost, rd.reader_semaphore->available_units());
    });
}

SEASTAR_TEST_CASE(restricted_reader_max_queue_length) {
    return async([&] {
        restriction_data rd(new_reader_base_cost, {}, 1);

        {
            simple_schema s;
            auto tmp = make_lw_shared<tmpdir>();
            auto sst = create_sstable(s, tmp->path);

            auto reader1_ptr = std::make_unique<reader_wrapper>(rd.config, s.schema(), sst);
            (*reader1_ptr)().get();

            auto reader2_ptr = std::make_unique<reader_wrapper>(rd.config, s.schema(), sst);
            auto read_fut = (*reader2_ptr)();

            // The queue should now be full.
            BOOST_REQUIRE_THROW(reader_wrapper(rd.config, s.schema(), sst), std::runtime_error);

            reader1_ptr.reset();
            read_fut.get();
        }

        REQUIRE_EVENTUALLY_EQUAL(new_reader_base_cost, rd.reader_semaphore->available_units());
    });
}

SEASTAR_TEST_CASE(restricted_reader_create_reader) {
    return async([&] {
        restriction_data rd(new_reader_base_cost);

        {
            simple_schema s;
            auto tmp = make_lw_shared<tmpdir>();
            auto sst = create_sstable(s, tmp->path);

            {
                auto reader = reader_wrapper(rd.config, s.schema(), sst);
                // This fast-forward is stupid, I know but the
                // underlying dummy reader won't care, so it's fine.
                reader.fast_forward_to(query::full_partition_range).get();

                BOOST_REQUIRE(reader.created());
                BOOST_REQUIRE_EQUAL(reader.call_count(), 0);
                BOOST_REQUIRE_EQUAL(reader.ff_count(), 1);
            }

            {
                auto reader = reader_wrapper(rd.config, s.schema(), sst);
                reader().get();

                BOOST_REQUIRE(reader.created());
                BOOST_REQUIRE_EQUAL(reader.call_count(), 1);
                BOOST_REQUIRE_EQUAL(reader.ff_count(), 0);
            }
        }

        REQUIRE_EVENTUALLY_EQUAL(new_reader_base_cost, rd.reader_semaphore->available_units());
    });
}
