/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include <iostream>
#include <limits>
#include "tests/test-utils.hh"
#include "core/shared_ptr.hh"
#include "net/packet-data-source.hh"
#include "apps/memcache/ascii.hh"

using namespace net;

using parser_type = memcache_ascii_parser;

static packet make_packet(std::vector<std::string> chunks) {
    packet p;
    for (auto&& chunk : chunks) {
        size_t size = chunk.size();
        char* b = new char[size];
        memcpy(b, chunk.c_str(), size);
        p = packet(std::move(p), fragment{b, size}, [b] { delete[] b; });
    }
    return p;
}

static auto make_input_stream(packet&& p) {
    return input_stream<char>(data_source(
            std::make_unique<packet_data_source>(std::move(p))));
}

static auto parse(packet&& p) {
    auto is = make_input_stream(std::move(p));
    auto parser = make_shared<parser_type>();
    parser->init();
    return is.consume(*parser).then([parser] {
        return make_ready_future<shared_ptr<parser_type>>(parser);
    });
}

SEASTAR_TEST_CASE(test_set_command_is_parsed) {
    return parse(make_packet({"set key 1 2 3\r\nabc\r\n"}))
        .then([] (auto p) {
            BOOST_REQUIRE(p->_state == parser_type::state::cmd_set);
            BOOST_REQUIRE(p->_flags == 1);
            BOOST_REQUIRE(p->_expiration == 2);
            BOOST_REQUIRE(p->_size == 3);
            BOOST_REQUIRE(p->_key == "key");
            BOOST_REQUIRE(p->_blob == "abc");
        });
}

SEASTAR_TEST_CASE(test_empty_data_is_parsed) {
    return parse(make_packet({"set key 1 2 0\r\n\r\n"}))
        .then([] (auto p) {
            BOOST_REQUIRE(p->_state == parser_type::state::cmd_set);
            BOOST_REQUIRE(p->_flags == 1);
            BOOST_REQUIRE(p->_expiration == 2);
            BOOST_REQUIRE(p->_size == 0);
            BOOST_REQUIRE(p->_key == "key");
            BOOST_REQUIRE(p->_blob == "");
        });
}

SEASTAR_TEST_CASE(test_superflous_data_is_an_error) {
    return parse(make_packet({"set key 0 0 0\r\nasd\r\n"}))
        .then([] (auto p) {
            BOOST_REQUIRE(p->_state == parser_type::state::error);
        });
}

SEASTAR_TEST_CASE(test_not_enough_data_is_an_error) {
    return parse(make_packet({"set key 0 0 3\r\n"}))
        .then([] (auto p) {
            BOOST_REQUIRE(p->_state == parser_type::state::error);
        });
}

SEASTAR_TEST_CASE(test_u32_parsing) {
    return make_ready_future<>()
        .then([] {
            return parse(make_packet({"set key 0 0 0\r\n\r\n"}))
                .then([] (auto p) {
                    BOOST_REQUIRE(p->_state == parser_type::state::cmd_set);
                    BOOST_REQUIRE(p->_flags == 0);
                });
        }).then([] {
            return parse(make_packet({"set key 12345 0 0\r\n\r\n"}))
                .then([] (auto p) {
                    BOOST_REQUIRE(p->_state == parser_type::state::cmd_set);
                    BOOST_REQUIRE(p->_flags == 12345);
                });
        }).then([] {
            return parse(make_packet({"set key -1 0 0\r\n\r\n"}))
                .then([] (auto p) {
                    BOOST_REQUIRE(p->_state == parser_type::state::error);
                });
        }).then([] {
            return parse(make_packet({"set key 1-1 0 0\r\n\r\n"}))
                .then([] (auto p) {
                    BOOST_REQUIRE(p->_state == parser_type::state::error);
                });
        }).then([] {
            return parse(make_packet({"set key " + std::to_string(std::numeric_limits<uint32_t>::max()) + " 0 0\r\n\r\n"}))
                .then([] (auto p) {
                    BOOST_REQUIRE(p->_state == parser_type::state::cmd_set);
                    BOOST_REQUIRE(p->_flags == std::numeric_limits<uint32_t>::max());
                });
        });
}

SEASTAR_TEST_CASE(test_parsing_of_split_data) {
    return make_ready_future<>()
        .then([] {
            return parse(make_packet({"set key 11", "1 222 3\r\nasd\r\n"}))
                .then([] (auto p) {
                    BOOST_REQUIRE(p->_state == parser_type::state::cmd_set);
                    BOOST_REQUIRE(p->_key == "key");
                    BOOST_REQUIRE(p->_flags == 111);
                    BOOST_REQUIRE(p->_expiration == 222);
                    BOOST_REQUIRE(p->_size == 3);
                    BOOST_REQUIRE(p->_blob == "asd");
                });
        }).then([] {
            return parse(make_packet({"set key 11", "1 22", "2 3", "\r\nasd\r\n"}))
                .then([] (auto p) {
                    BOOST_REQUIRE(p->_state == parser_type::state::cmd_set);
                    BOOST_REQUIRE(p->_key == "key");
                    BOOST_REQUIRE(p->_flags == 111);
                    BOOST_REQUIRE(p->_expiration == 222);
                    BOOST_REQUIRE(p->_size == 3);
                    BOOST_REQUIRE(p->_blob == "asd");
                });
        }).then([] {
            return parse(make_packet({"set k", "ey 11", "1 2", "2", "2 3", "\r\nasd\r\n"}))
                .then([] (auto p) {
                    BOOST_REQUIRE(p->_state == parser_type::state::cmd_set);
                    BOOST_REQUIRE(p->_key == "key");
                    BOOST_REQUIRE(p->_flags == 111);
                    BOOST_REQUIRE(p->_expiration == 222);
                    BOOST_REQUIRE(p->_size == 3);
                    BOOST_REQUIRE(p->_blob == "asd");
                });
        }).then([] {
            return parse(make_packet({"set key 111 222 3\r\n", "asd\r\n"}))
                .then([] (auto p) {
                    BOOST_REQUIRE(p->_state == parser_type::state::cmd_set);
                    BOOST_REQUIRE(p->_key == "key");
                    BOOST_REQUIRE(p->_flags == 111);
                    BOOST_REQUIRE(p->_expiration == 222);
                    BOOST_REQUIRE(p->_size == 3);
                    BOOST_REQUIRE(p->_blob == "asd");
                });
        }).then([] {
            return parse(make_packet({"set key 111 222 3\r\na", "sd\r\n"}))
                .then([] (auto p) {
                    BOOST_REQUIRE(p->_state == parser_type::state::cmd_set);
                    BOOST_REQUIRE(p->_key == "key");
                    BOOST_REQUIRE(p->_flags == 111);
                    BOOST_REQUIRE(p->_expiration == 222);
                    BOOST_REQUIRE(p->_size == 3);
                    BOOST_REQUIRE(p->_blob == "asd");
                });
        }).then([] {
            return parse(make_packet({"set key 111 222 3\r\nasd", "\r\n"}))
                .then([] (auto p) {
                    BOOST_REQUIRE(p->_state == parser_type::state::cmd_set);
                    BOOST_REQUIRE(p->_key == "key");
                    BOOST_REQUIRE(p->_flags == 111);
                    BOOST_REQUIRE(p->_expiration == 222);
                    BOOST_REQUIRE(p->_size == 3);
                    BOOST_REQUIRE(p->_blob == "asd");
                });
        }).then([] {
            return parse(make_packet({"set key 111 222 3\r\nasd\r", "\n"}))
                .then([] (auto p) {
                    BOOST_REQUIRE(p->_state == parser_type::state::cmd_set);
                    BOOST_REQUIRE(p->_key == "key");
                    BOOST_REQUIRE(p->_flags == 111);
                    BOOST_REQUIRE(p->_expiration == 222);
                    BOOST_REQUIRE(p->_size == 3);
                    BOOST_REQUIRE(p->_blob == "asd");
                });
        });
}

SEASTAR_TEST_CASE(test_get_parsing) {
    return make_ready_future<>()
        .then([] {
            return parse(make_packet({"get key1\r\n"}))
                .then([] (auto p) {
                    BOOST_REQUIRE(p->_state == parser_type::state::cmd_get);
                    BOOST_REQUIRE_EQUAL(p->_keys, std::vector<sstring>({"key1"}));
                });
        }).then([] {
            return parse(make_packet({"get key1 key2\r\n"}))
                .then([] (auto p) {
                    BOOST_REQUIRE(p->_state == parser_type::state::cmd_get);
                    BOOST_REQUIRE_EQUAL(p->_keys, std::vector<sstring>({"key1", "key2"}));
                });
        }).then([] {
            return parse(make_packet({"get key1 key2 key3\r\n"}))
                .then([] (auto p) {
                    BOOST_REQUIRE(p->_state == parser_type::state::cmd_get);
                    BOOST_REQUIRE_EQUAL(p->_keys, std::vector<sstring>({"key1", "key2", "key3"}));
                });
        });
}

SEASTAR_TEST_CASE(test_multiple_requests_in_one_stream) {
    auto p = make_shared<parser_type>();
    auto is = make_shared(make_input_stream(make_packet({"set key1 1 1 5\r\ndata1\r\nset key2 2 2 6\r\ndata2+\r\n"})));
    p->init();
    return is->consume(*p).then([p] {
            BOOST_REQUIRE(p->_state == parser_type::state::cmd_set);
            BOOST_REQUIRE(p->_key == "key1");
            BOOST_REQUIRE(p->_flags == 1);
            BOOST_REQUIRE(p->_expiration == 1);
            BOOST_REQUIRE(p->_size == 5);
            BOOST_REQUIRE(p->_blob == "data1");
        }).then([is, p] {
            p->init();
            return is->consume(*p).then([p] {
                BOOST_REQUIRE(p->_state == parser_type::state::cmd_set);
                BOOST_REQUIRE(p->_key == "key2");
                BOOST_REQUIRE(p->_flags == 2);
                BOOST_REQUIRE(p->_expiration == 2);
                BOOST_REQUIRE(p->_size == 6);
                BOOST_REQUIRE(p->_blob == "data2+");
            });
        });
}
