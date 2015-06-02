/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include <iostream>
#include <limits>
#include "tests/test-utils.hh"
#include "core/shared_ptr.hh"
#include "net/packet-data-source.hh"
#include "apps/memcached/ascii.hh"
#include "core/future-util.hh"

using namespace net;
using namespace memcache;

using parser_type = memcache_ascii_parser;

static packet make_packet(std::vector<std::string> chunks, size_t buffer_size) {
    packet p;
    for (auto&& chunk : chunks) {
        size_t size = chunk.size();
        for (size_t pos = 0; pos < size; pos += buffer_size) {
            auto now = std::min(pos + buffer_size, chunk.size()) - pos;
            p.append(packet(chunk.data() + pos, now));
        }
    }
    return p;
}

static auto make_input_stream(packet&& p) {
    return input_stream<char>(data_source(
            std::make_unique<packet_data_source>(std::move(p))));
}

static auto parse(packet&& p) {
    auto is = make_lw_shared<input_stream<char>>(make_input_stream(std::move(p)));
    auto parser = make_lw_shared<parser_type>();
    parser->init();
    return is->consume(*parser).then([is, parser] {
        return make_ready_future<lw_shared_ptr<parser_type>>(parser);
    });
}

auto for_each_fragment_size = [] (auto&& func) {
    auto buffer_sizes = { 100000, 1000, 100, 10, 5, 2, 1 };
    return do_for_each(buffer_sizes.begin(), buffer_sizes.end(), [func] (size_t buffer_size) {
        return func([buffer_size] (std::vector<std::string> chunks) {
            return make_packet(chunks, buffer_size);
        });
    });
};

SEASTAR_TEST_CASE(test_set_command_is_parsed) {
    return for_each_fragment_size([] (auto make_packet) {
        return parse(make_packet({"set key 1 2 3\r\nabc\r\n"})).then([] (auto p) {
            BOOST_REQUIRE(p->_state == parser_type::state::cmd_set);
            BOOST_REQUIRE(p->_flags_str == "1");
            BOOST_REQUIRE(p->_expiration == 2);
            BOOST_REQUIRE(p->_size == 3);
            BOOST_REQUIRE(p->_size_str == "3");
            BOOST_REQUIRE(p->_key.key() == "key");
            BOOST_REQUIRE(p->_blob == "abc");
        });
    });
}

SEASTAR_TEST_CASE(test_empty_data_is_parsed) {
    return for_each_fragment_size([] (auto make_packet) {
        return parse(make_packet({"set key 1 2 0\r\n\r\n"})).then([] (auto p) {
            BOOST_REQUIRE(p->_state == parser_type::state::cmd_set);
            BOOST_REQUIRE(p->_flags_str == "1");
            BOOST_REQUIRE(p->_expiration == 2);
            BOOST_REQUIRE(p->_size == 0);
            BOOST_REQUIRE(p->_size_str == "0");
            BOOST_REQUIRE(p->_key.key() == "key");
            BOOST_REQUIRE(p->_blob == "");
        });
    });
}

SEASTAR_TEST_CASE(test_superflous_data_is_an_error) {
    return for_each_fragment_size([] (auto make_packet) {
        return parse(make_packet({"set key 0 0 0\r\nasd\r\n"})).then([] (auto p) {
            BOOST_REQUIRE(p->_state == parser_type::state::error);
        });
    });
}

SEASTAR_TEST_CASE(test_not_enough_data_is_an_error) {
    return for_each_fragment_size([] (auto make_packet) {
        return parse(make_packet({"set key 0 0 3\r\n"})).then([] (auto p) {
            BOOST_REQUIRE(p->_state == parser_type::state::error);
        });
    });
}

SEASTAR_TEST_CASE(test_u32_parsing) {
    return for_each_fragment_size([] (auto make_packet) {
        return make_ready_future<>().then([make_packet] {
            return parse(make_packet({"set key 0 0 0\r\n\r\n"})).then([] (auto p) {
                BOOST_REQUIRE(p->_state == parser_type::state::cmd_set);
                BOOST_REQUIRE(p->_flags_str == "0");
            });
        }).then([make_packet] {
            return parse(make_packet({"set key 12345 0 0\r\n\r\n"}))
                    .then([] (auto p) {
                BOOST_REQUIRE(p->_state == parser_type::state::cmd_set);
                BOOST_REQUIRE(p->_flags_str == "12345");
            });
        }).then([make_packet] {
            return parse(make_packet({"set key -1 0 0\r\n\r\n"}))
                    .then([] (auto p) {
                BOOST_REQUIRE(p->_state == parser_type::state::error);
            });
        }).then([make_packet] {
            return parse(make_packet({"set key 1-1 0 0\r\n\r\n"}))
                    .then([] (auto p) {
                BOOST_REQUIRE(p->_state == parser_type::state::error);
            });
        }).then([make_packet] {
            return parse(make_packet({"set key " + std::to_string(std::numeric_limits<uint32_t>::max()) + " 0 0\r\n\r\n"}))
                    .then([] (auto p) {
                BOOST_REQUIRE(p->_state == parser_type::state::cmd_set);
                BOOST_REQUIRE(p->_flags_str == to_sstring(std::numeric_limits<uint32_t>::max()));
            });
        });
    });
}

SEASTAR_TEST_CASE(test_parsing_of_split_data) {
    return for_each_fragment_size([] (auto make_packet) {
        return make_ready_future<>()
                .then([make_packet] {
            return parse(make_packet({"set key 11", "1 222 3\r\nasd\r\n"}))
                    .then([] (auto p) {
                BOOST_REQUIRE(p->_state == parser_type::state::cmd_set);
                BOOST_REQUIRE(p->_key.key() == "key");
                BOOST_REQUIRE(p->_flags_str == "111");
                BOOST_REQUIRE(p->_expiration == 222);
                BOOST_REQUIRE(p->_size == 3);
                BOOST_REQUIRE(p->_size_str == "3");
                BOOST_REQUIRE(p->_blob == "asd");
            });
        }).then([make_packet] {
            return parse(make_packet({"set key 11", "1 22", "2 3", "\r\nasd\r\n"}))
                    .then([] (auto p) {
                BOOST_REQUIRE(p->_state == parser_type::state::cmd_set);
                BOOST_REQUIRE(p->_key.key() == "key");
                BOOST_REQUIRE(p->_flags_str == "111");
                BOOST_REQUIRE(p->_expiration == 222);
                BOOST_REQUIRE(p->_size == 3);
                BOOST_REQUIRE(p->_size_str == "3");
                BOOST_REQUIRE(p->_blob == "asd");
            });
        }).then([make_packet] {
            return parse(make_packet({"set k", "ey 11", "1 2", "2", "2 3", "\r\nasd\r\n"}))
                    .then([] (auto p) {
                BOOST_REQUIRE(p->_state == parser_type::state::cmd_set);
                BOOST_REQUIRE(p->_key.key() == "key");
                BOOST_REQUIRE(p->_flags_str == "111");
                BOOST_REQUIRE(p->_expiration == 222);
                BOOST_REQUIRE(p->_size == 3);
                BOOST_REQUIRE(p->_size_str == "3");
                BOOST_REQUIRE(p->_blob == "asd");
            });
        }).then([make_packet] {
            return parse(make_packet({"set key 111 222 3\r\n", "asd\r\n"}))
                    .then([] (auto p) {
                BOOST_REQUIRE(p->_state == parser_type::state::cmd_set);
                BOOST_REQUIRE(p->_key.key() == "key");
                BOOST_REQUIRE(p->_flags_str == "111");
                BOOST_REQUIRE(p->_expiration == 222);
                BOOST_REQUIRE(p->_size == 3);
                BOOST_REQUIRE(p->_size_str == "3");
                BOOST_REQUIRE(p->_blob == "asd");
            });
        }).then([make_packet] {
            return parse(make_packet({"set key 111 222 3\r\na", "sd\r\n"}))
                    .then([] (auto p) {
                BOOST_REQUIRE(p->_state == parser_type::state::cmd_set);
                BOOST_REQUIRE(p->_key.key() == "key");
                BOOST_REQUIRE(p->_flags_str == "111");
                BOOST_REQUIRE(p->_expiration == 222);
                BOOST_REQUIRE(p->_size == 3);
                BOOST_REQUIRE(p->_size_str == "3");
                BOOST_REQUIRE(p->_blob == "asd");
            });
        }).then([make_packet] {
            return parse(make_packet({"set key 111 222 3\r\nasd", "\r\n"}))
                    .then([] (auto p) {
                BOOST_REQUIRE(p->_state == parser_type::state::cmd_set);
                BOOST_REQUIRE(p->_key.key() == "key");
                BOOST_REQUIRE(p->_flags_str == "111");
                BOOST_REQUIRE(p->_expiration == 222);
                BOOST_REQUIRE(p->_size == 3);
                BOOST_REQUIRE(p->_size_str == "3");
                BOOST_REQUIRE(p->_blob == "asd");
            });
        }).then([make_packet] {
            return parse(make_packet({"set key 111 222 3\r\nasd\r", "\n"}))
                    .then([] (auto p) {
                BOOST_REQUIRE(p->_state == parser_type::state::cmd_set);
                BOOST_REQUIRE(p->_key.key() == "key");
                BOOST_REQUIRE(p->_flags_str == "111");
                BOOST_REQUIRE(p->_expiration == 222);
                BOOST_REQUIRE(p->_size == 3);
                BOOST_REQUIRE(p->_size_str == "3");
                BOOST_REQUIRE(p->_blob == "asd");
            });
        });
    });
}

static std::vector<sstring> as_strings(std::vector<item_key>& keys) {
    std::vector<sstring> v;
    for (auto&& key : keys) {
        v.push_back(key.key());
    }
    return v;
}

SEASTAR_TEST_CASE(test_get_parsing) {
    return for_each_fragment_size([] (auto make_packet) {
        return make_ready_future<>()
                .then([make_packet] {
            return parse(make_packet({"get key1\r\n"}))
                    .then([] (auto p) {
                BOOST_REQUIRE(p->_state == parser_type::state::cmd_get);
                BOOST_REQUIRE_EQUAL(as_strings(p->_keys), std::vector<sstring>({"key1"}));
            });
        }).then([make_packet] {
            return parse(make_packet({"get key1 key2\r\n"}))
                    .then([] (auto p) {
                BOOST_REQUIRE(p->_state == parser_type::state::cmd_get);
                BOOST_REQUIRE_EQUAL(as_strings(p->_keys), std::vector<sstring>({"key1", "key2"}));
            });
        }).then([make_packet] {
            return parse(make_packet({"get key1 key2 key3\r\n"}))
                    .then([] (auto p) {
                BOOST_REQUIRE(p->_state == parser_type::state::cmd_get);
                BOOST_REQUIRE_EQUAL(as_strings(p->_keys), std::vector<sstring>({"key1", "key2", "key3"}));
            });
        });
    });
}

SEASTAR_TEST_CASE(test_catches_errors_in_get) {
    return for_each_fragment_size([] (auto make_packet) {
        return make_ready_future<>()
                .then([make_packet] {
            return parse(make_packet({"get\r\n"}))
                    .then([] (auto p) {
                BOOST_REQUIRE(p->_state == parser_type::state::error);
            });
        });
    });
}

SEASTAR_TEST_CASE(test_parser_returns_eof_state_when_no_command_follows) {
    return for_each_fragment_size([] (auto make_packet) {
        auto p = make_shared<parser_type>();
        auto is = make_shared<input_stream<char>>(make_input_stream(make_packet({"get key\r\n"})));
        p->init();
        return is->consume(*p).then([p] {
            BOOST_REQUIRE(p->_state == parser_type::state::cmd_get);
        }).then([is, p] {
            p->init();
            return is->consume(*p).then([p, is] {
                BOOST_REQUIRE(p->_state == parser_type::state::eof);
            });
        });
    });
}

SEASTAR_TEST_CASE(test_incomplete_command_is_an_error) {
    return for_each_fragment_size([] (auto make_packet) {
        auto p = make_shared<parser_type>();
        auto is = make_shared<input_stream<char>>(make_input_stream(make_packet({"get"})));
        p->init();
        return is->consume(*p).then([p] {
            BOOST_REQUIRE(p->_state == parser_type::state::error);
        }).then([is, p] {
            p->init();
            return is->consume(*p).then([p, is] {
                BOOST_REQUIRE(p->_state == parser_type::state::eof);
            });
        });
    });
}

SEASTAR_TEST_CASE(test_multiple_requests_in_one_stream) {
    return for_each_fragment_size([] (auto make_packet) {
        auto p = make_shared<parser_type>();
        auto is = make_shared<input_stream<char>>(make_input_stream(make_packet({"set key1 1 1 5\r\ndata1\r\nset key2 2 2 6\r\ndata2+\r\n"})));
        p->init();
        return is->consume(*p).then([p] {
            BOOST_REQUIRE(p->_state == parser_type::state::cmd_set);
            BOOST_REQUIRE(p->_key.key() == "key1");
            BOOST_REQUIRE(p->_flags_str == "1");
            BOOST_REQUIRE(p->_expiration == 1);
            BOOST_REQUIRE(p->_size == 5);
            BOOST_REQUIRE(p->_size_str == "5");
            BOOST_REQUIRE(p->_blob == "data1");
        }).then([is, p] {
            p->init();
            return is->consume(*p).then([p, is] {
                BOOST_REQUIRE(p->_state == parser_type::state::cmd_set);
                BOOST_REQUIRE(p->_key.key() == "key2");
                BOOST_REQUIRE(p->_flags_str == "2");
                BOOST_REQUIRE(p->_expiration == 2);
                BOOST_REQUIRE(p->_size == 6);
                BOOST_REQUIRE(p->_size_str == "6");
                BOOST_REQUIRE(p->_blob == "data2+");
            });
        });
    });
}
