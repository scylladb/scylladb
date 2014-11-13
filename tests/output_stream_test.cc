/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "core/app-template.hh"
#include "core/shared_ptr.hh"
#include "core/reactor.hh"
#include "core/vector-data-sink.hh"
#include "core/future-util.hh"
#include "core/sstring.hh"
#include "test-utils.hh"
#include <vector>

#define REQUIRE_TMPBUF_HOLDS(buf, text) BOOST_REQUIRE(strncmp((buf).get(), (text), (buf).size()) == 0)

future<> assert_split(std::vector<std::string> write_calls,
        std::vector<std::string> expected_split, int size) {
    static int i = 0;
    BOOST_TEST_MESSAGE("checking split: " << i++);
    auto sh_write_calls = make_shared(std::move(write_calls));
    auto sh_expected_splits = make_shared(std::move(expected_split));
    auto v = make_shared<std::vector<temporary_buffer<char>>>();
    auto out = make_shared<output_stream<char>>(
        data_sink(std::make_unique<vector_data_sink>(*v)), size);

    return do_for_each(sh_write_calls->begin(), sh_write_calls->end(), [out, sh_write_calls] (auto&& chunk) {
        return out->write(chunk);
    }).then([out, v, sh_expected_splits] {
        return out->flush().then([out, v, sh_expected_splits] {
            BOOST_REQUIRE_EQUAL(v->size(), sh_expected_splits->size());
            int i = 0;
            for (auto&& chunk : *sh_expected_splits) {
                REQUIRE_TMPBUF_HOLDS((*v)[i], chunk.c_str());
                i++;
            }
        });
    });
}

SEASTAR_TEST_CASE(test_splitting) {
    return assert_split({"1"}, {"1"}, 4)
        .then([] { return assert_split({"12", "3"}, {"123"}, 4); })
        .then([] { return assert_split({"12", "34"}, {"1234"}, 4); })
        .then([] { return assert_split({"12", "345"}, {"1234", "5"}, 4); })
        .then([] { return assert_split({"1234"}, {"1234"}, 4); })
        .then([] { return assert_split({"12345"}, {"12345"}, 4); })
        .then([] { return assert_split({"1234567890"}, {"1234567890"}, 4); })
        .then([] { return assert_split({"1", "23456"}, {"1234", "56"}, 4); })
        .then([] { return assert_split({"123", "4567"}, {"1234", "567"}, 4); })
        .then([] { return assert_split({"123", "45678"}, {"1234", "5678"}, 4); })
        .then([] { return assert_split({"123", "4567890"}, {"1234", "567890"}, 4); })
        .then([] { return assert_split({"1234", "567"}, {"1234", "567"}, 4); })

        .then([] { return assert_split({"1", "234567", "89"}, {"123", "4567", "89"}, 3); })
        .then([] { return assert_split({"1", "2345", "67"}, {"123", "456", "7"}, 3); })
        ;
}
