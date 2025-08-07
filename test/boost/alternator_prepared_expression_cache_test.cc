/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <boost/test/unit_test.hpp>
#include <seastar/testing/test_case.hh>
#include <seastar/core/coroutine.hh>
#include "alternator/expressions.hh"
#include <ranges>

namespace alternator {
std::ostream& operator<<(std::ostream& os, const stats& s) {
    os << "{ evictions: " << s.expression_cache.evictions;
    for (int type = 0; type < stats::expression_types::NUM_EXPRESSION_TYPES; type++) {
        os << ", type" << type << ": " << s.expression_cache.requests[type].hits << " / " << s.expression_cache.requests[type].misses;
    }
    os << "}";
    return os;
}
bool operator==(const stats& a, const stats& b) {
    for (int type = 0; type < stats::expression_types::NUM_EXPRESSION_TYPES; type++) {
        if (a.expression_cache.requests[type].hits != b.expression_cache.requests[type].hits
        || a.expression_cache.requests[type].misses != b.expression_cache.requests[type].misses) {
            return false;
        }
    }
    return a.expression_cache.evictions == b.expression_cache.evictions;
}
}

SEASTAR_TEST_CASE(test_prepared_expression_cache) {
    alternator::stats stats;
    auto max_cache_entries = utils::updateable_value<uint32_t>(3);
    alternator::prepared_expression_cache::config cfg{
        .max_cache_entries = max_cache_entries
    };
    alternator::prepared_expression_cache cache(cfg, stats);
    alternator::stats expected_stats;

    using et=alternator::stats::expression_types;
    static std::vector<std::tuple<std::string, et, bool, bool, bool>> expr = {
        // expr, type, is_valid, is_hit, causes_eviction
        {"a", et::PROJECTION_EXPRESSION, true, false, false},
        {"a", et::PROJECTION_EXPRESSION, true, true, false},
        {"SET a=:v", et::UPDATE_EXPRESSION, true, false, false},
        {"SET a=:v", et::UPDATE_EXPRESSION, true, true, false},
        {"a=:v", et::CONDITION_EXPRESSION, true, false, false},
        {"a=:v", et::CONDITION_EXPRESSION, true, true, false},

        {"b", et::PROJECTION_EXPRESSION, true, false, true},
        {"b", et::PROJECTION_EXPRESSION, true, true, false},
        {"SET b=:v", et::UPDATE_EXPRESSION, true, false, true},
        {"SET b=:v", et::UPDATE_EXPRESSION, true, true, false},
        {"b=:v", et::CONDITION_EXPRESSION, true, false, true},
        {"b=:v", et::CONDITION_EXPRESSION, true, true, false},

        {"b", et::UPDATE_EXPRESSION, false, false, false},
        {"b", et::CONDITION_EXPRESSION, false, false, false},
        {"SET b=:v", et::PROJECTION_EXPRESSION, false, false, false},
        {"SET b=:v", et::CONDITION_EXPRESSION, false, false, false},
        {"b=:v", et::PROJECTION_EXPRESSION, false, false, false},
        {"b=:v", et::UPDATE_EXPRESSION, false, false, false},

        {"b", et::PROJECTION_EXPRESSION, true, true, false},
        {"SET b=:v", et::UPDATE_EXPRESSION, true, true, false},
        {"b=:v", et::CONDITION_EXPRESSION, true, true, false},
    };

    for (auto [e, t, is_valid, is_hit, causes_eviction] : expr) {
        switch (t) {
        case et::PROJECTION_EXPRESSION:
            if (is_valid) {
                cache.parse_projection_expression(e);
            } else {
                BOOST_REQUIRE_THROW(cache.parse_projection_expression(e), alternator::expressions_syntax_error);
            }
            break;
        case et::UPDATE_EXPRESSION:
            if (is_valid) {
                cache.parse_update_expression(e);
            } else {
                BOOST_REQUIRE_THROW(cache.parse_update_expression(e), alternator::expressions_syntax_error);
            }
            break;
        case et::CONDITION_EXPRESSION:
            if (is_valid) {
                cache.parse_condition_expression(e, "Test");
            } else {
                BOOST_REQUIRE_THROW(cache.parse_condition_expression(e, "Test"), alternator::expressions_syntax_error);
            }
            break;
        default:
            BOOST_FAIL("Invalid test setup");
        }
        if (is_valid) {
            if (is_hit) {
                expected_stats.expression_cache.requests[t].hits++;
                BOOST_REQUIRE_MESSAGE(!causes_eviction, "Invalid test setup");
            } else {
                expected_stats.expression_cache.requests[t].misses++;
                expected_stats.expression_cache.evictions += causes_eviction ? 1 : 0;
            }
        } else {
            BOOST_REQUIRE_MESSAGE(!is_hit || !causes_eviction, "Invalid test setup");
        }
        BOOST_REQUIRE_EQUAL(stats, expected_stats);
    }

    co_return;
}

// Test that same strings can't be parsed to different expression types.
SEASTAR_TEST_CASE(test_prepared_expression_cache_invalid_requests) {
    alternator::stats stats;
    auto max_cache_entries = utils::updateable_value<uint32_t>(3);
    alternator::prepared_expression_cache::config cfg{
        .max_cache_entries = max_cache_entries
    };
    alternator::prepared_expression_cache cache(cfg, stats);

    auto inv_expr = {"", " ", "SET", ":v", "1"};
    auto projection = {"a", "a, b", "a..b", "a.#b", "#a[1]", "a[1..2]", "a[1].b"};
    auto condition = {"a=:v", "size(a)", "a IN (:v)", "a > :v", "a = :v AND b = :w", "a = :v OR b = :w", "NOT a = :v", "(a = :v)"};
    auto update = {"SET a=:v", "SET a=:v, b = :1", "ADD a[1] :v", "REMOVE a[1]", "DELETE a :v", "DELETE a :v, b :w REMOVE c", "SET a=:v REMOVE b ADD c :w"};
    for (auto expr : std::array{inv_expr, condition, update} | std::views::join) {
        try {
            cache.parse_projection_expression(expr);
            BOOST_FAIL(format("Expected exception for projection expression: {}", expr));
        } catch (const alternator::expressions_syntax_error&) { }
    }
    for (auto expr : std::array{inv_expr, projection, update} | std::views::join) {
        try {
            cache.parse_condition_expression(expr, "Test");
            BOOST_FAIL(format("Expected exception for condition expression: {}", expr));
        } catch (const alternator::expressions_syntax_error&) { }
    }
    for (auto expr : std::array{inv_expr, projection, condition} | std::views::join) {
        try {
            cache.parse_update_expression(expr);
            BOOST_FAIL(format("Expected exception for update expression: {}", expr));
        } catch (const alternator::expressions_syntax_error&) { }
    }
    alternator::stats expected_stats;
    BOOST_REQUIRE_EQUAL(stats, expected_stats);
    co_return;
}

SEASTAR_TEST_CASE(test_prepared_expression_cache_resize) {
    alternator::stats stats;
    utils::updateable_value_source<uint32_t> max_cache_entries(3);
    alternator::prepared_expression_cache::config cfg{
        .max_cache_entries = utils::updateable_value<uint32_t>(max_cache_entries)
    };
    alternator::prepared_expression_cache cache(std::move(cfg), stats);
    alternator::stats expected_stats;

    cache.parse_projection_expression("a");
    cache.parse_projection_expression("b");
    cache.parse_projection_expression("c");
    cache.parse_projection_expression("d");
    expected_stats.expression_cache.requests[alternator::stats::expression_types::PROJECTION_EXPRESSION].misses = 4;
    expected_stats.expression_cache.evictions = 1;
    BOOST_REQUIRE_EQUAL(stats, expected_stats);

    max_cache_entries.set(4);
    cache.parse_projection_expression("e");
    expected_stats.expression_cache.requests[alternator::stats::expression_types::PROJECTION_EXPRESSION].misses++;
    BOOST_REQUIRE_EQUAL(stats, expected_stats);

    max_cache_entries.set(2);
    expected_stats.expression_cache.evictions += 2;
    BOOST_REQUIRE_EQUAL(stats, expected_stats);

    max_cache_entries.set(0);
    expected_stats.expression_cache.evictions += 2;
    BOOST_REQUIRE_EQUAL(stats, expected_stats);

    co_return;
}
