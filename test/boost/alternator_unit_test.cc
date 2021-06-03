/*
 * Copyright (C) 2020-present ScyllaDB
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

#define BOOST_TEST_MODULE alternator
#include <boost/test/included/unit_test.hpp>

#include <seastar/util/defer.hh>
#include <seastar/core/memory.hh>
#include "alternator/base64.hh"

static bytes_view to_bytes_view(const std::string& s) {
    return bytes_view(reinterpret_cast<const signed char*>(s.c_str()), s.size());
}

static std::map<std::string, std::string> strings {
    {"", ""},
    {"a", "YQ=="},
    {"ab", "YWI="},
    {"abc", "YWJj"},
    {"abcd", "YWJjZA=="},
    {"abcde", "YWJjZGU="},
    {"abcdef", "YWJjZGVm"},
    {"abcdefg", "YWJjZGVmZw=="},
    {"abcdefgh", "YWJjZGVmZ2g="},
};

BOOST_AUTO_TEST_CASE(test_base64_encode_decode) {
    for (auto& [str, encoded] : strings) {
        BOOST_REQUIRE_EQUAL(base64_encode(to_bytes_view(str)), encoded);
        auto decoded = base64_decode(encoded);
        BOOST_REQUIRE_EQUAL(to_bytes_view(str), bytes_view(decoded));
    }
}

BOOST_AUTO_TEST_CASE(test_base64_decoded_len) {
    for (auto& [str, encoded] : strings) {
        BOOST_REQUIRE_EQUAL(str.size(), base64_decoded_len(encoded));
    }
}

BOOST_AUTO_TEST_CASE(test_base64_begins_with) {
    for (auto& [str, encoded] : strings) {
        for (size_t i = 0; i < str.size(); ++i) {
            std::string prefix(str.c_str(), i);
            std::string encoded_prefix = base64_encode(to_bytes_view(prefix));
            BOOST_REQUIRE(base64_begins_with(encoded, encoded_prefix));
        }
    }
    std::string str1 = "ABCDEFGHIJKL123456";
    std::string str2 = "ABCDEFGHIJKL1234567";
    std::string str3 = "ABCDEFGHIJKL12345678";
    std::string encoded_str1 = base64_encode(to_bytes_view(str1));
    std::string encoded_str2 = base64_encode(to_bytes_view(str2));
    std::string encoded_str3 = base64_encode(to_bytes_view(str3));
    std::vector<std::string> non_prefixes = {
        "B", "AC", "ABD", "ACD", "ABCE", "ABCEG", "ABCDEFGHIJKLM", "ABCDEFGHIJKL123456789"
    };
    for (auto& non_prefix : non_prefixes) {
        std::string encoded_non_prefix = base64_encode(to_bytes_view(non_prefix));
        BOOST_REQUIRE(!base64_begins_with(encoded_str1, encoded_non_prefix));
        BOOST_REQUIRE(!base64_begins_with(encoded_str2, encoded_non_prefix));
        BOOST_REQUIRE(!base64_begins_with(encoded_str3, encoded_non_prefix));
    }
}

BOOST_AUTO_TEST_CASE(test_allocator_fail_gracefully) {
    // Allocation size is set to a ridiculously high value to ensure
    // that it will immediately fail - trying to lazily allocate just
    // a little more than total memory may still succeed.
    static size_t too_large_alloc_size = memory::stats().total_memory() * 1024 * 1024;
    rjson::allocator allocator;
    // Impossible allocation should throw
    BOOST_REQUIRE_THROW(allocator.Malloc(too_large_alloc_size), rjson::error);
    // So should impossible reallocation
    void* memory = allocator.Malloc(1);
    auto release = defer([memory] { rjson::allocator::Free(memory); });
    BOOST_REQUIRE_THROW(allocator.Realloc(memory, 1, too_large_alloc_size), rjson::error);
    // Internal rapidjson stack should also throw
    // and also be destroyed gracefully later
    rapidjson::internal::Stack stack(&allocator, 0);
    BOOST_REQUIRE_THROW(stack.Push<char>(too_large_alloc_size), rjson::error);
}
