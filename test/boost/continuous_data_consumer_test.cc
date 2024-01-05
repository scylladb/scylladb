/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "vint-serialization.hh"
#include "sstables/consumer.hh"

#include "bytes.hh"
#include "utils/buffer_input_stream.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "test/lib/random_utils.hh"
#include "schema/schema.hh"
#include "sstables/processing_result_generator.hh"

#include <boost/test/unit_test.hpp>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include <random>

namespace {

class test_consumer final : public data_consumer::continuous_data_consumer<test_consumer> {
    static const int MULTIPLIER = 10;
    uint64_t _tested_value;
    int _state = 0;
    int _count = 0;
    reader_permit::need_cpu_guard _need_cpu_guard;

    void check(uint64_t got) {
        BOOST_REQUIRE_EQUAL(_tested_value, got);
    }

    static uint64_t calculate_length(uint64_t tested_value) {
        return MULTIPLIER * unsigned_vint::serialized_size(tested_value);
    }

    static input_stream<char> prepare_stream(uint64_t tested_value) {
        temporary_buffer<char> buf(calculate_length(tested_value));
        int pos = 0;
        bytes::value_type* out = reinterpret_cast<bytes::value_type*>(buf.get_write());
        for (int i = 0; i < MULTIPLIER; ++i) {
            pos += unsigned_vint::serialize(tested_value, out + pos);
        }
        return make_buffer_input_stream(std::move(buf), [] {return 1;});
    }

public:
    test_consumer(reader_permit permit, uint64_t tested_value)
        : continuous_data_consumer(std::move(permit), prepare_stream(tested_value), 0, calculate_length(tested_value))
        , _tested_value(tested_value)
        , _need_cpu_guard(_permit)
    { }

    bool non_consuming() { return false; }

    void verify_end_state() {}

    data_consumer::processing_result process_state(temporary_buffer<char>& data) {
        switch (_state) {
        case 0:
            if (read_unsigned_vint(data) != read_status::ready) {
                _state = 1;
                break;
            }
            [[fallthrough]];
        case 1:
            check(_u64);
            ++_count;
            _state = _count < MULTIPLIER ? 0 : 2;
            break;
        default:
            BOOST_FAIL("wrong consumer state");
        }
        return _state == 2 ? data_consumer::proceed::no : data_consumer::proceed::yes;
    }

    void run() {
        consume_input().get();
    }
};

}

SEASTAR_THREAD_TEST_CASE(test_read_unsigned_vint) {
    tests::reader_concurrency_semaphore_wrapper semaphore;

    auto nr_tests =
#ifdef SEASTAR_DEBUG
            10
#else
            1000
#endif
            ;
    test_consumer(semaphore.make_permit(), 0).run();
    for (int highest_bit = 0; highest_bit < 64; ++highest_bit) {
        uint64_t tested_value = uint64_t{1} << highest_bit;
        for (int i = 0; i < nr_tests; ++i) {
            test_consumer(semaphore.make_permit(), tested_value + tests::random::get_int<uint64_t>(tested_value - 1)).run();
        }
    }
}

class skipping_consumer final : public data_consumer::continuous_data_consumer<skipping_consumer> {
    int _initial_data_size;
    int _to_skip;
    int _next_data_size;
    processing_result_generator _gen;
    temporary_buffer<char>* _processing_data;

    // stream starting with initial_data_size 'a's, followed by to_skip 'b's,
    // ending with next_data_size 'a's, returning chunks of size 1
    static input_stream<char> prepare_stream(int initial_data_size, int to_skip, int next_data_size) {
        temporary_buffer<char> buf(initial_data_size + to_skip + next_data_size);
        std::memset(buf.get_write(), 'a', initial_data_size);
        std::memset(buf.get_write() + initial_data_size, 'b', to_skip);
        std::memset(buf.get_write() + initial_data_size + to_skip, 'a', next_data_size);
        return make_buffer_input_stream(std::move(buf), [] {return 1;});
    }
    static size_t prepare_initial_consumer_length(int initial_data_size, int to_skip) {
        // some bytes that we want to skip may end up even after the initial consumer range
        return initial_data_size + tests::random::get_int<int>(0, to_skip);
    }

public:
    skipping_consumer(reader_permit permit, int initial_data_size, int to_skip, int next_data_size)
        : continuous_data_consumer(std::move(permit), prepare_stream(initial_data_size, to_skip, next_data_size),
                                    0, prepare_initial_consumer_length(initial_data_size, to_skip))
        , _initial_data_size(initial_data_size)
        , _to_skip(to_skip)
        , _next_data_size(next_data_size)
        , _gen(do_process_state())
    { }

    bool non_consuming() { return false; }

    void verify_end_state() {}

    data_consumer::processing_result process_state(temporary_buffer<char>& data) {
        _processing_data = &data;
        return _gen.generate();
    }

    processing_result_generator do_process_state() {
        while (_initial_data_size--) {
            co_yield read_8(*_processing_data);
            if (_u8 != 'a') {
                BOOST_FAIL("wrong data read");
            }
        }
        auto skipped_by_trimming = _processing_data->size();
        _processing_data->trim(0);
        co_yield skip_bytes{_to_skip - skipped_by_trimming};
        while (_next_data_size--) {
            co_yield read_8(*_processing_data);
            if (_u8 != 'a') {
                BOOST_FAIL("wrong data read");
            }
        }
        co_yield data_consumer::proceed::no;
    }

    void run() {
        consume_input().get();
    }
};

// Make sure that we can correctly fast forward to the next position with useful data,
// in a case when the previous consumer range ends with bytes that we want to
// skip using skip_bytes (instead of simply trimming the received data buffer)
SEASTAR_THREAD_TEST_CASE(test_skip_at_end) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    for (int i = 0; i < 1000; i++) {
        int initial_data_size = tests::random::get_int<int>(1, 50);
        int to_skip = tests::random::get_int<int>(1, 50);
        int next_data_size = tests::random::get_int<int>(1, 50);
        skipping_consumer consumer(semaphore.make_permit(), initial_data_size, to_skip, next_data_size);
        consumer.run();
        consumer.fast_forward_to(initial_data_size + to_skip, initial_data_size + to_skip + next_data_size).get();
        consumer.run();
    }
}
