/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "test/lib/scylla_test_case.hh"

#include <seastar/util/defer.hh>
#include <seastar/core/memory.hh>
#include "utils/base64.hh"
#include "utils/rjson.hh"
#include "alternator/serialization.hh"

#include "cdc/generation.hh"
#include "alternator/expressions.hh"
#include "alternator/streams.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>


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

// Test the alternator::internal::magnitude_and_precision() function which we
// use to used to check if a number exceeds DynamoDB's limits on magnitude and
// precision (for issue #6794). This just tests the internal implementation -
// we also have end-to-end tests trying to insert various numbers with bad
// magnitude and precision to the database in test/alternator/test_number.py.
BOOST_AUTO_TEST_CASE(test_magnitude_and_precision) {
    struct expected {
        const char* number;
        int magnitude;
        int precision;
    };
    std::vector<expected> tests = {
        // number     magnitude, precision
        {"0",         0, 0},
        {"0e10",      0, 0},
        {"0e-10",     0, 0},
        {"0e+10",     0, 0},
        {"0.0",       0, 0},
        {"0.00e10",   0, 0},
        {"1",         0, 1},
        {"12.",       1, 2},
        {"1.1",       0, 2},
        {"12.3",      1, 3},
        {"12.300",    1, 3},
        {"0.3",       -1, 1},
        {".3",        -1, 1},
        {"3e-1",      -1, 1},
        {"0.00012",   -4, 2},
        {"1.2e-4",    -4, 2},
        {"1.2E-4",    -4, 2},
        {"12.345e50", 51, 5},
        {"12.345e-50",-49, 5},
        {"123000000", 8, 3},
        {"123000000.000e+5", 13, 3},
        {"10.01",     1, 4},
        {"1.001e1",   1, 4},
        {"1e5",       5, 1},
        {"1e+5",      5, 1},
        {"1e-5",      -5, 1},
        {"123e-7",    -5, 3},
        // These are important edge cases: DynamoDB considers 1e126 to be
        // overflowing but 9.9999e125 is considered to have magnitude 125
        // and ok. Conversely, 1e-131 is underflowing and 0.9e-130 is too.
        {"9.99999e125", 125, 6},
        {"0.99999e-130", -131, 5},
        {"0.9e-130", -131, 1},
        // Although 1e1000 is not allowed, 0e0000 is allowed - it's just 0.
        {"0e1000",    0, 0},
    };
    // prefixes that should do nothing to a number
    std::vector<std::string> prefixes = {
        "",
        "0",
        "+",
        "-",
        "+0000",
        "-0000"
    };
    for (expected test : tests) {
        for (std::string prefix : prefixes) {
            std::string number = prefix + test.number;
            auto res = alternator::internal::get_magnitude_and_precision(number);
            BOOST_CHECK_MESSAGE(res.magnitude == test.magnitude,
                seastar::format("{}: expected magnitude {}, got {}", number, test.magnitude, res.magnitude));
            BOOST_CHECK_MESSAGE(res.precision == test.precision,
                seastar::format("{}: expected precision {}, got {}", number, test.precision, res.precision));
        }
    }
    // Huge exponents like 1e1000000 are not guaranteed to return that
    // specific number as magnitude, but is guaranteed to return some
    // other high magnitude that the caller can complain is excessive.
    auto res = alternator::internal::get_magnitude_and_precision("1e1000000");
    BOOST_CHECK(res.magnitude > 1000);
    res = alternator::internal::get_magnitude_and_precision("1e-1000000");
    BOOST_CHECK(res.magnitude < -1000);
    // Even if an exponent so huge that it doesn't even fit in a 32-bit
    // integer, we shouldn't fail to recognize its excessive magnitude:
    res = alternator::internal::get_magnitude_and_precision("1e1000000000000");
    BOOST_CHECK(res.magnitude > 1000);
    res = alternator::internal::get_magnitude_and_precision("1e-1000000000000");
    BOOST_CHECK(res.magnitude < -1000);
}

// parsed expression cache tests:

// ANTLR3 leaks memory when it tries to recover from missing token.
// - it creates a "fake" token, if it allows to continue parsing.
// Leak was reported by ASAN, when running this test in debug mode - 
// the test passed but the leak is discovered when the test file exits.
// Reproduces #25878
BOOST_AUTO_TEST_CASE(missing_tokens_memory_leak) {
    BOOST_REQUIRE_THROW(alternator::parse_update_expression("SET a :v"), alternator::expressions_syntax_error); // missing '='
    BOOST_REQUIRE_THROW(alternator::parse_update_expression("DELETE a v"), alternator::expressions_syntax_error); // missing ':'
    BOOST_REQUIRE_THROW(alternator::parse_update_expression("ADD a v"), alternator::expressions_syntax_error); // missing ':'
    
    BOOST_REQUIRE_THROW(alternator::parse_condition_expression("size(a < 5", "Test"), alternator::expressions_syntax_error); // missing ')'
    BOOST_REQUIRE_THROW(alternator::parse_condition_expression("a IN :x)", "Test"), alternator::expressions_syntax_error); // missing '('
    BOOST_REQUIRE_THROW(alternator::parse_condition_expression("a IN (:x", "Test"), alternator::expressions_syntax_error); // missing ')'
    BOOST_REQUIRE_THROW(alternator::parse_condition_expression("a BETWEEN :x AN :y", "Test"), alternator::expressions_syntax_error); // missing 'AND'
    BOOST_REQUIRE_THROW(alternator::parse_condition_expression("a BETWEEN :x :y", "Test"), alternator::expressions_syntax_error); // missing 'AND'

    BOOST_REQUIRE_THROW(alternator::parse_projection_expression("a[0.b"), alternator::expressions_syntax_error); // missing ']'
}

// Tests of inputs that cause exceptions inside the expression parser.
// ANTR3 itself doesn't use exceptions, but we do in additional checks.
// Apart from correct response, which may be tested in Python tests,
// main concern here is if this can cause memory leaks
// similar to issue in the above test.
BOOST_AUTO_TEST_CASE(exception_at_expression_parsing) {
    // std::stoi throws std::out_of_range if the number is too big
    BOOST_REQUIRE_THROW(alternator::parse_projection_expression("a[99999999999999999]") , alternator::expressions_syntax_error);

    // Path depth limit exceeded should throw expressions_syntax_error
    // alternator::parsed::path::depth_limit is private, so try with some arbitrary long path:
    std::string long_path = "a";
    for (int i = 0; i < 100; ++i) {
        long_path += ".a";
    }
    BOOST_REQUIRE_THROW(alternator::parse_projection_expression(long_path), alternator::expressions_syntax_error);

    // Appending duplicate update actions throws expressions_syntax_error
    BOOST_REQUIRE_THROW(alternator::parse_update_expression("SET a = :v SET b = :w"), alternator::expressions_syntax_error);

    // Single non-function condition throws expressions_syntax_error
    BOOST_REQUIRE_THROW(alternator::parse_condition_expression("a OR b", "TEST"), alternator::expressions_syntax_error);
}

using exp_type = alternator::stats::expression_types;
static int exp_type_i(exp_type type) {
    if (static_cast<int>(type) >= exp_type::NUM_EXPRESSION_TYPES)
        BOOST_FAIL("Invalid expression type");
    return static_cast<int>(type);
}
static std::string_view str(exp_type type) {
    constexpr static std::string_view exp_type_s[exp_type::NUM_EXPRESSION_TYPES] = { "projection", "update", "condition" };
    return exp_type_s[exp_type_i(type)];
};
static uint64_t& hits_counter(alternator::stats& stats, exp_type type) {
    return stats.expression_cache.requests[exp_type_i(type)].hits;
}
static uint64_t& misses_counter(alternator::stats& stats, exp_type type) {
    return stats.expression_cache.requests[exp_type_i(type)].misses;
}
enum class expecting_exception { yes, no };
static expecting_exception hit(alternator::stats& stats, exp_type type) {
    hits_counter(stats, type)++;
    return expecting_exception::no;
}
static expecting_exception miss(alternator::stats& stats, exp_type type) {
    misses_counter(stats, type)++;
    return expecting_exception::no;
}
static expecting_exception eviction_miss(alternator::stats& stats, exp_type type) {
    stats.expression_cache.evictions++;
    return miss(stats, type);
}
static expecting_exception invalid(alternator::stats& stats, exp_type type) {
    return expecting_exception::yes;
}
struct test_cache {
    alternator::stats stats;
    alternator::stats expected_stats;
    utils::updateable_value_source<uint32_t> max_cache_entries;
    std::unique_ptr<alternator::parsed::expression_cache> cache;
    test_cache(int size) : max_cache_entries(size), cache(std::make_unique<alternator::parsed::expression_cache>(alternator::parsed::expression_cache::config{
        .max_cache_entries = utils::updateable_value<uint32_t>(max_cache_entries)
    }, stats)) {}

    std::string validate_stats(const std::string& msg) {
        for (int t = 0; t < exp_type::NUM_EXPRESSION_TYPES; t++) {
            exp_type type = static_cast<exp_type>(t);
            if(hits_counter(stats, type) != hits_counter(expected_stats, type)) {
                return format("{}: expected {} {} hits, got {}", msg, hits_counter(expected_stats, type), str(type), hits_counter(stats, type));
            }
            if(misses_counter(stats, type) != misses_counter(expected_stats, type)) {
                return format("{}: expected {} {} misses, got {}", msg, misses_counter(expected_stats, type), str(type), misses_counter(stats, type));
            }
        }
        if(stats.expression_cache.evictions != expected_stats.expression_cache.evictions) {
            return format("{}: expected {} evictions, got {}", msg, expected_stats.expression_cache.evictions, stats.expression_cache.evictions);
        }
        return std::string();
    }
    void check_stats(const std::string& msg) {
        std::string v = validate_stats(msg);
        BOOST_REQUIRE_MESSAGE(v.empty(), v);
    }
    seastar::future<> wait_check_stats(const std::string& msg) {
        for (int attempt = 0; attempt < 100; attempt++) {
            std::string v = validate_stats(msg);
            if (v.empty()) {
                co_return;
            }
            co_await seastar::sleep(std::chrono::milliseconds(10));
        }
        check_stats(msg); // Final check after all attempts
    }
    void try_parse(const std::string& expr, exp_type type, expecting_exception (*expected_cache_behavior)(alternator::stats&, exp_type)) {
        try {
            switch (type) {
            case exp_type::PROJECTION_EXPRESSION:
                (void)(cache->parse_projection_expression(expr));
                break;
            case exp_type::UPDATE_EXPRESSION:
                (void)(cache->parse_update_expression(expr));
                break;
            case exp_type::CONDITION_EXPRESSION:
                (void)(cache->parse_condition_expression(expr, "Test"));
                break;
            default:
                BOOST_FAIL("Invalid expression type");
            }
            if (expected_cache_behavior(expected_stats, type) == expecting_exception::yes) {
                BOOST_FAIL(format("Expected exception for {} expression: {}, but none was thrown.", str(type), expr));
            }
        } catch (const alternator::expressions_syntax_error& ex) {
            if (expected_cache_behavior(expected_stats, type) == expecting_exception::no) {
                BOOST_FAIL(format("Unexpected syntax exception for {} expression: {}, {}", str(type), expr, ex.what()));
            }
        } catch (const std::exception& ex) {
            BOOST_FAIL(format("Unexpected exception for {} expression: {}, {}", str(type), expr, ex.what()));
        }
        check_stats(format("after parsing {} expression: {}", str(type), expr));
    }
};

// Basic cache functionality test: hits, misses, evictions.
SEASTAR_TEST_CASE(test_parsed_expression_cache) {
    test_cache cache(3);

    // New entries
    cache.try_parse("a", exp_type::PROJECTION_EXPRESSION, miss);
    cache.try_parse("a", exp_type::PROJECTION_EXPRESSION, hit);
    cache.try_parse("SET a=:v", exp_type::UPDATE_EXPRESSION, miss);
    cache.try_parse("SET a=:v", exp_type::UPDATE_EXPRESSION, hit);
    cache.try_parse("a=:v", exp_type::CONDITION_EXPRESSION, miss);
    cache.try_parse("a=:v", exp_type::CONDITION_EXPRESSION, hit);

    // Cache full - evicting old entrires
    cache.try_parse("b", exp_type::PROJECTION_EXPRESSION, eviction_miss);
    cache.try_parse("b", exp_type::PROJECTION_EXPRESSION, hit);
    cache.try_parse("SET b=:v", exp_type::UPDATE_EXPRESSION, eviction_miss);
    cache.try_parse("SET b=:v", exp_type::UPDATE_EXPRESSION, hit);
    cache.try_parse("b=:v", exp_type::CONDITION_EXPRESSION, eviction_miss);
    cache.try_parse("b=:v", exp_type::CONDITION_EXPRESSION, hit);

    // Keys existing in cache, but invalid (for a given type) - raise exception
    cache.try_parse("b", exp_type::UPDATE_EXPRESSION, invalid);
    cache.try_parse("b", exp_type::CONDITION_EXPRESSION, invalid);
    cache.try_parse("SET b=:v", exp_type::PROJECTION_EXPRESSION, invalid);
    cache.try_parse("SET b=:v", exp_type::CONDITION_EXPRESSION, invalid);
    cache.try_parse("b=:v", exp_type::PROJECTION_EXPRESSION, invalid);
    cache.try_parse("b=:v", exp_type::UPDATE_EXPRESSION, invalid);

    // Invalid expressions should not affect cache state
    cache.try_parse("b", exp_type::PROJECTION_EXPRESSION, hit);
    cache.try_parse("SET b=:v", exp_type::UPDATE_EXPRESSION, hit);
    cache.try_parse("b=:v", exp_type::CONDITION_EXPRESSION, hit);

    co_return;
}

// Test that same strings can't be parsed to different expression types.
SEASTAR_TEST_CASE(test_parsed_expression_cache_invalid_requests) {
    test_cache cache(2000);

    auto inv_expr = {"", " ", "SET", "set", ":v", "1"};
    for (auto expr : inv_expr) {
        cache.try_parse(expr, exp_type::PROJECTION_EXPRESSION, invalid);
        cache.try_parse(expr, exp_type::UPDATE_EXPRESSION, invalid);
        cache.try_parse(expr, exp_type::CONDITION_EXPRESSION, invalid);
    }
    auto projection = {"a", "a, b", "a.b", "a.#b", "#a[1]", "a[1].b"};
    for (auto expr : projection) {
        cache.try_parse(expr, exp_type::UPDATE_EXPRESSION, invalid);
        cache.try_parse(expr, exp_type::CONDITION_EXPRESSION, invalid);
        cache.try_parse(expr, exp_type::PROJECTION_EXPRESSION, miss);
    }
    auto condition = {"a=:v", "size(a)", "a IN (:v)", "a > :v", "a = :v AND b = :w", "a = :v OR b = :w", "NOT a = :v", "(a = :v)"};
    for (auto expr : condition) {
        cache.try_parse(expr, exp_type::PROJECTION_EXPRESSION, invalid);
        cache.try_parse(expr, exp_type::UPDATE_EXPRESSION, invalid);
        cache.try_parse(expr, exp_type::CONDITION_EXPRESSION, miss);
    }
    auto update = {"SET a=:v", "SET a=:v, b = :1", "ADD a[1] :v", "REMOVE a[1]", "DELETE a :v", "DELETE a :v, b :w REMOVE c", "SET a=:v REMOVE b ADD c :w"};
    for (auto expr : update) {
        cache.try_parse(expr, exp_type::PROJECTION_EXPRESSION, invalid);
        cache.try_parse(expr, exp_type::CONDITION_EXPRESSION, invalid);
        cache.try_parse(expr, exp_type::UPDATE_EXPRESSION, miss);
    }
    co_return;
}

// Test resizing the cache at runtime.
SEASTAR_TEST_CASE(test_parsed_expression_cache_resize) {
    test_cache cache(3);

    cache.try_parse("a", exp_type::PROJECTION_EXPRESSION, miss);
    cache.try_parse("b", exp_type::PROJECTION_EXPRESSION, miss);
    cache.try_parse("c", exp_type::PROJECTION_EXPRESSION, miss);
    cache.try_parse("d", exp_type::PROJECTION_EXPRESSION, eviction_miss);

    cache.max_cache_entries.set(4);
    cache.try_parse("e", exp_type::PROJECTION_EXPRESSION, miss);

    cache.max_cache_entries.set(2);
    cache.expected_stats.expression_cache.evictions += 2;
    cache.check_stats("after resizing cache to 2 entries");

    cache.max_cache_entries.set(0);
    cache.expected_stats.expression_cache.evictions += 2;
    cache.check_stats("after disabling cache");

    // for resizes down with more then 3000 evictions the change may be asynchronous
    size_t large_size = 30000;
    size_t first_reduce = 75*large_size/100;
    cache.max_cache_entries.set(large_size);
    for (size_t i = 0; i < large_size; i++) {
        cache.try_parse(seastar::format("expr{}", i), exp_type::PROJECTION_EXPRESSION, miss);
        co_await maybe_yield();
    }
    cache.max_cache_entries.set(first_reduce);
    cache.expected_stats.expression_cache.evictions += (large_size - first_reduce);
    co_await cache.wait_check_stats("async, after resizing cache");
    for (size_t i = 0; i < first_reduce; i++) {
        cache.try_parse(seastar::format("expr{}", i), exp_type::PROJECTION_EXPRESSION, eviction_miss);
        co_await maybe_yield();
    }
    cache.max_cache_entries.set(0);
    cache.expected_stats.expression_cache.evictions += first_reduce;
    co_await cache.wait_check_stats("async, after disabling cache");

    cache.max_cache_entries.set(large_size);
    for (size_t i = 0; i < large_size; i++) {
        cache.try_parse(seastar::format("expr{}", i), exp_type::PROJECTION_EXPRESSION, miss);
        co_await maybe_yield();
    }
    cache.max_cache_entries.set(1000);
    co_await cache.cache->stop();
    cache.cache.reset();

    co_return;
}

namespace {
    auto sid(std::int64_t token) {
        return cdc::stream_id{ dht::token{ token }, 0 };
    }
    auto sids(std::initializer_list<std::int64_t> tokens) {
        utils::chunked_vector<cdc::stream_id> result;
        for (auto t : tokens) {
            result.push_back(sid(t));
        }
        return result;
    }
    utils::chunked_vector<cdc::stream_id> to_sids(alternator::stream_id_range range) {
        utils::chunked_vector<cdc::stream_id> result;
        for (auto &sid : range.iterate()) {
            result.push_back(sid);
        }
        return result;
    }
    std::vector<cdc::stream_id> vec(utils::chunked_vector<cdc::stream_id>& vec, int start = 0, int end = 0x7fffffff, int start2 = 0, int end2 = 0) {
        auto update_start_end = [&](int &start, int &end) {
            if (start < 0) start += (int)vec.size();
            if (end < 0) end += (int)vec.size();
            if (start < 0) start = 0;
            if (start > (int)vec.size()) start = (int)vec.size();
            if (end < 0) end = 0;
            if (end > (int)vec.size()) end = (int)vec.size();
        };
        update_start_end(start, end);
        update_start_end(start2, end2);
        std::vector<cdc::stream_id> result;
        while(start < end) {
            result.push_back(vec[start++]);
        }
        while(start2 < end2) {
            result.push_back(vec[start2++]);
        }
        return result;
    }
    std::vector<cdc::stream_id> sorted_vec(utils::chunked_vector<cdc::stream_id>& v, int start = 0, int end = 0x7fffffff, int start2 = 0, int end2 = 0) {
        std::sort(v.begin(), v.end(), [](const cdc::stream_id &a, const cdc::stream_id &b) {
            return a.token() < b.token();
        });
        auto v2 = vec(v, start, end, start2, end2);
        std::sort(v2.begin(), v2.end(), [](const cdc::stream_id &a, const cdc::stream_id &b) {
            return compare_unsigned(a.to_bytes(), b.to_bytes()) < 0;
        });
        return v2;
    }
}

namespace cdc {
    // must be in cdc namespace so ADL could work and BOOST could find it
    std::ostream & operator <<(std::ostream &os, const std::vector<stream_id> &vec) {
        os << "[";
        bool first = true;
        for (auto &sid : vec) {
            if (!first) {
                os << ", ";
            }
            first = false;
            os << sid.token();
        }
        os << "]";
        return os;
    }
}
namespace utils {
    std::ostream & operator <<(std::ostream &os, const utils::chunked_vector<cdc::stream_id> &vec) {
        os << "[";
        bool first = true;
        for (auto &sid : vec) {
            if (!first) {
                os << ", ";
            }
            first = false;
            os << sid.token();
        }
        os << "]";
        return os;
    }
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_tablets_simple) {
    auto parent_streams = sids({ -50, 50, std::numeric_limits<std::int64_t>::max() });
    auto current_streams = sids({ -50, 50, std::numeric_limits<std::int64_t>::max() });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 2, 3));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_tablets_merge_1) {
    auto parent_streams = sids({ 0, 50, std::numeric_limits<std::int64_t>::max() });
    auto current_streams = sids({ 0, std::numeric_limits<std::int64_t>::max() });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_tablets_merge_2) {
    auto parent_streams = sids({ 0, 50, std::numeric_limits<std::int64_t>::max() });
    auto current_streams = sids({ 50, std::numeric_limits<std::int64_t>::max() });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_tablets_merge_into_one) {
    auto parent_streams = sids({ -100, -50, 25, 50, std::numeric_limits<std::int64_t>::max() });
    auto current_streams = sids({ std::numeric_limits<std::int64_t>::max() });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[3], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[4], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_tablets_split_1) {
    auto parent_streams = sids({ 0, 50, std::numeric_limits<std::int64_t>::max() });
    auto current_streams = sids({ 0, 25, 50, std::numeric_limits<std::int64_t>::max() });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 3));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 3, 4));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_tablets_split_2) {
    auto parent_streams = sids({ 0, 50, std::numeric_limits<std::int64_t>::max() });
    auto current_streams = sids({ -25, 0, 50, std::numeric_limits<std::int64_t>::max() });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 2, 3));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 3, 4));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_tablets_split_3) {
    auto parent_streams = sids({ 0, 50, std::numeric_limits<std::int64_t>::max() });
    auto current_streams = sids({ 0, 50, 75, std::numeric_limits<std::int64_t>::max() });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 2, 4));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_tablets_split_from_one) {
    auto parent_streams = sids({ std::numeric_limits<std::int64_t>::max() });
    auto current_streams = sids({ -100, -50, 50, 75, std::numeric_limits<std::int64_t>::max() });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 5));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_tablets_split_and_merge) {
    auto parent_streams = sids({ 0, 50, 100, std::numeric_limits<std::int64_t>::max() });
    auto current_streams = sids({ 25, 75, std::numeric_limits<std::int64_t>::max() });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 3));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[3], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 2, 3));
}







BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_simple) {
    auto parent_streams = sids({ -50, 50 });
    auto current_streams = sids({ -50, 50 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_merge_1) {
    auto parent_streams = sids({ 0, 25, 50, 75 });
    auto current_streams = sids({ 25, 50, 75 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[3], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 2, 3));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_merge_2) {
    auto parent_streams = sids({ 0, 25, 50, 75 });
    auto current_streams = sids({ 0, 50, 75 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[3], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 2, 3));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_merge_3) {
    auto parent_streams = sids({ 0, 25, 50, 75 });
    auto current_streams = sids({ 0, 25, 75 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 2, 3));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[3], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 2, 3));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_merge_4) {
    auto parent_streams = sids({ 0, 25, 50, 75 });
    auto current_streams = sids({ 0, 25, 50 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 2, 3));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[3], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_merge_into_one_1) {
    auto parent_streams = sids({ 0, 25, 50 });
    auto current_streams = sids({ -50 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_merge_into_one_2) {
    auto parent_streams = sids({ 0, 25, 50 });
    auto current_streams = sids({ 0 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_merge_into_one_3) {
    auto parent_streams = sids({ 0, 25, 50 });
    auto current_streams = sids({ 10 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_merge_into_one_4) {
    auto parent_streams = sids({ 0, 25, 50 });
    auto current_streams = sids({ 25 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_merge_into_one_5) {
    auto parent_streams = sids({ 0, 25, 50 });
    auto current_streams = sids({ 50 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_merge_into_one_6) {
    auto parent_streams = sids({ 0, 25, 50 });
    auto current_streams = sids({ 110 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));
}


BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_1) {
    auto parent_streams = sids({ 0, 50, 100 });
    auto current_streams = sids({ -25, 0, 50, 100 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 2, 3));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 3, 4));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_2) {
    auto parent_streams = sids({ 0, 50, 100 });
    auto current_streams = sids({ 0, 25, 50, 100 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 3));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 3, 4));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_3) {
    auto parent_streams = sids({ 0, 50, 100 });
    auto current_streams = sids({ 0, 50, 75, 100 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 2, 4));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_4) {
    auto parent_streams = sids({ 0, 50, 100 });
    auto current_streams = sids({ 0, 50, 100, 125 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1, 3, 4));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 2, 3));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_from_one_1) {
    auto parent_streams = sids({ -10 });
    auto current_streams = sids({ 0, 50, 75 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 3));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_from_one_2) {
    auto parent_streams = sids({ 0 });
    auto current_streams = sids({ 0, 50, 75 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 3));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_from_one_3) {
    auto parent_streams = sids({ 25 });
    auto current_streams = sids({ 0, 50, 75 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 3));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_from_one_4) {
    auto parent_streams = sids({ 50 });
    auto current_streams = sids({ 0, 50, 75 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 3));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_from_one_5) {
    auto parent_streams = sids({ 60 });
    auto current_streams = sids({ 0, 50, 75 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 3));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_from_one_6) {
    auto parent_streams = sids({ 75 });
    auto current_streams = sids({ 0, 50, 75 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 3));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_from_one_7) {
    auto parent_streams = sids({ 100 });
    auto current_streams = sids({ 0, 50, 75 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 3));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_and_merge_1) {
    auto parent_streams = sids({ 0, 50, 100 });
    auto current_streams = sids({ 25, 75 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 2));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_and_merge_2) {
    auto parent_streams = sids({ -100, -50, 25, 50, 100, 200 });
    auto current_streams = sids({ 25, 75 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[3], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[4], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[5], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_and_merge_3) {
    auto parent_streams = sids({ -275, -75 });
    auto current_streams = sids({ -400, -300, -200, -100, -50, -10, 0, 10, 50, 100, 200, 300, 400 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    auto tmp1 = vec(range);
    auto tmp2 = sorted_vec(current_streams, 0, 3, 4, 13);
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 3, 4, 13));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 2, 5));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_and_merge_4) {
    auto parent_streams = sids({ 75, 275 });
    auto current_streams = sids({ -100, -50, -10, 0, 10, 50, 100, 200, 300, 400 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 7, 8, 10));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 6, 9));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_and_merge_5) {
    auto parent_streams = sids({ 0, 10 });
    auto current_streams = sids({ -20, -10 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_and_merge_6) {
    auto parent_streams = sids({ -20 });
    auto current_streams = sids({ -20, 0 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 2));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_and_merge_7) {
    auto parent_streams = sids({ -20, -10 });
    auto current_streams = sids({ -20, 0 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));
}

namespace {
    struct encapsulated_range {
        const int from, to;

        explicit operator bool () const {
            return from < to;
        }
        encapsulated_range operator & (encapsulated_range other) const {
            auto f = std::max(from, other.from);
            auto t = std::min(to, other.to);
            if (f >= t) {
                return {0, 0};
            }
            return {f, t};
        }
        // friend std::ostream &operator << (std::ostream &o, const encapsulated_range &r) {
        //     o << "[" << r.from << ", " << r.to << ")";
        //     return o;
        // }
    };
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_brute_force_all_combinations) {
    constexpr int N = 8;
    constexpr int S = -(N / 2);
    std::vector<int> parent_streams_values, current_streams_values;
    utils::chunked_vector<cdc::stream_id> parent_streams, current_streams;
    std::vector<cdc::stream_id> expected;
    size_t prepare_iteration = std::numeric_limits<size_t>::max();

    auto get_encapsulated_range = [](const std::vector<int>& v, size_t index) -> std::pair<encapsulated_range, encapsulated_range>{
        auto end = v[index];
        if (index > 0) {
            auto start = v[index - 1];
            return { encapsulated_range{start, end} , encapsulated_range{end, end} };
        }
        auto start = v.back();
        return { encapsulated_range{start, std::numeric_limits<int>::max()}, encapsulated_range{std::numeric_limits<int>::min(), end} };
    };
    auto prepare = [&](size_t iteration) {
        if (prepare_iteration != iteration) {
            parent_streams_values.clear();
            current_streams_values.clear();
            parent_streams.clear();
            current_streams.clear();
            for(auto i = 0; i < N; ++i) {
                if (iteration & (1 << i)) {
                    parent_streams_values.push_back((S + i) * 10);
                    parent_streams.push_back(sid(parent_streams_values.back()));
                }
                if (iteration & (1 << (N + i))) {
                    current_streams_values.push_back((S + i) * 10);
                    current_streams.push_back(sid(current_streams_values.back()));
                }
            }
            prepare_iteration = iteration;
        }
    };

    auto run = [&](size_t iteration, size_t parent_index) {
        prepare(iteration);
        if (current_streams.empty() || parent_streams.empty()) {
            return;
        }
        // std::cout << "running iteration " << iteration << " parent_index " << parent_index << std::endl;
        // std::cout << "parents [";
        // for(auto v : parent_streams_values) {
        //     if (v != parent_streams_values.front()) {
        //         std::cout << ", ";
        //     }
        //     std::cout << v;
        // }
        // std::cout << "]" << std::endl;
        // std::cout << "currents [";
        // for(auto v : current_streams_values) {
        //     if (v != current_streams_values.front()) {
        //         std::cout << ", ";
        //     }
        //     std::cout << v;
        // }
        //std::cout << "]" << std::endl;
        auto [ range1, range2 ] = get_encapsulated_range(parent_streams_values, parent_index);
        
        expected.clear();
        for(auto i = 0u; i < current_streams_values.size(); ++i) {
            auto [ range3, range4 ] = get_encapsulated_range(current_streams_values, i);

            if (range1 & range3 || range1 & range4 || range2 & range3 || range2 & range4) {
                expected.push_back(current_streams[i]);
                // std::cout << "range1 " << range1 << " range2 " << range2 << " range3 " << range3 << " range4 " << range4 << " range1 & range3 " << (range1 & range3) << " range1 & range4 " << (range1 & range4) << " range2 & range3 " << (range2 & range3) << " range2 & range4 " << (range2 & range4) << std::endl;
            }
        }
        std::sort(expected.begin(), expected.end(), [](const cdc::stream_id &a, const cdc::stream_id &b) {
            return compare_unsigned(a.to_bytes(), b.to_bytes()) < 0;
        });
        assert(!expected.empty());

        auto old_current_streams = current_streams;
        auto old_parent_streams = parent_streams;
        utils::chunked_vector<cdc::stream_id> range;
        try {
        range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[parent_index], false));
        }
        catch(...) {

            BOOST_REQUIRE_MESSAGE(false,
                                "iteration " << iteration << " parent_index " << parent_index
                            );
        }
        auto produced = vec(range);

        if (produced != expected) {
            std::cout << "produced " << produced << std::endl;
            std::cout << "expected " << expected << std::endl;

            BOOST_REQUIRE_MESSAGE(produced == expected,
                                "produced " << produced << "\n" <<
                                "expected " << expected << "\n" <<
                                "iteration " << iteration << " parent_index " << parent_index
                            );
        }

        std::sort(parent_streams.begin(), parent_streams.end(), [](const cdc::stream_id &a, const cdc::stream_id &b) {
            return a.token() < b.token();
        });
        std::sort(current_streams.begin(), current_streams.end(), [](const cdc::stream_id &a, const cdc::stream_id &b) {
            return a.token() < b.token();
        });
        BOOST_REQUIRE_MESSAGE(parent_streams == old_parent_streams,
                            "parent streams modified!\n" <<
                            "produced " << parent_streams << "\n" <<
                            "expected " << old_parent_streams << "\n" <<
                            "iteration " << iteration << " parent_index " << parent_index
                        );
        BOOST_REQUIRE_MESSAGE(current_streams == old_current_streams,
                            "current streams modified!\n" <<
                            "produced " << current_streams << "\n" <<
                            "expected " << old_current_streams << "\n" <<
                            "iteration " << iteration << " parent_index " << parent_index
                        );
    };

    run(2310, 0);
    for(auto iteration = 0u; iteration < (2 << (2 * N)); ++iteration) {
        prepare(iteration);
        for(auto parent_index = 0u; parent_index < parent_streams.size(); ++parent_index) {
            run(iteration, parent_index);
        }
    }
}
