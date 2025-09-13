/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "expressions.hh"
#include "utils/log.hh"

#include "utils/lru_string_map.hh"
#include <variant>
#include <seastar/core/on_internal_error.hh>

static logging::logger logger_("prepared-expression-cache");

namespace alternator {

struct prepared_expression_cache_impl {
    prepared_expression_cache::config _cfg;
    size_t _max_cache_entries;
    stats& _stats;

    using cached_expressions_types = std::variant<
        parsed::update_expression,
        parsed::condition_expression,
        std::vector<parsed::path>
    >;
    lru_string_map<cached_expressions_types> _cached_entries;
    utils::observable<uint32_t>::observer _max_cache_entries_observer;

    prepared_expression_cache_impl(prepared_expression_cache::config cfg, stats& stats);

    size_t evict_until_size(size_t n) {
        try { // Very defensive approach - if impossible happens, we purge the cache
            return _cached_entries.evict_until_size<lru_string_map<cached_expressions_types>::sanity_check::basic>(n);
        } catch (const std::logic_error& ex) {
            on_internal_error(logger_, format("Purging cache due to: {}", ex.what()));
            return _cached_entries.clear();
        }
    }

    // to define the specialized return type of `get_or_create()`
    template <typename Func, typename... Args>
    using ParseResult = std::invoke_result_t<Func, std::string_view, Args...>;

    // Caching layer for parsed expressions
    // The expression type is determined by the type of the parsing function passed as a parameter,
    // and the return type is exactly the same as the return type of this parsing function.
    // StatsType is used only to update appropriate statistics - currently it is aligned with the expression type,
    // but it could be extended in the future if needed, e.g. split per operation.
    template <stats::expression_types StatsType, typename Func, typename... Args>
    ParseResult<Func, Args...> get_or_create(std::string_view query, Func&& parse_func, Args&&... other_args) {
        if (_max_cache_entries == 0) {
            return parse_func(query, std::forward<Args>(other_args)...);
        }
        auto value = _cached_entries.find(query);
        if (value) {
            logger_.trace("Cache hit for query: {}", query);
            _stats.expression_cache.requests[StatsType].hits++;
            try {
                return std::get<ParseResult<Func, Args...>>(value->get());
            } catch (const std::bad_variant_access&) {
                // User can reach this code, by sending the same query string as a different expression type.
                // In practice valid queries are different enough to not collide.
                // Entries in cache are only valid queries.
                // This request will fail at parsing below.
                // If, by any chance this is a valid query, `push` below will update the cache with the new value.
                logger_.error("Cache hit for '{}', but type mismatch.", query);
                _stats.expression_cache.requests[StatsType].hits--;
            }
        } else {
            logger_.trace("Cache miss for query: {}", query);
        }
        ParseResult<Func, Args...> expr = parse_func(query, std::forward<Args>(other_args)...);
        // Invalid query will throw here ^

        _stats.expression_cache.requests[StatsType].misses++;
        _stats.expression_cache.evictions += evict_until_size(_max_cache_entries - 1);
        cached_expressions_types new_value = expr;
        _cached_entries.push(query, std::move(new_value));
        return expr;
    }
};

prepared_expression_cache_impl::prepared_expression_cache_impl(prepared_expression_cache::config cfg, stats& stats) : 
    _cfg(std::move(cfg)), _max_cache_entries(_cfg.max_cache_entries), _stats(stats),
    _max_cache_entries_observer(_cfg.max_cache_entries.observe([this] (uint32_t max_value) {
        logger_.trace("Expression cache max entries changed to {}", max_value);
        _max_cache_entries = max_value;
        if (_max_cache_entries > 0) {
            _stats.expression_cache.evictions += evict_until_size(_max_cache_entries);
            _cached_entries.reserve(_max_cache_entries); //it never shrinks container
        } else { // when disabled it releases container memory
            _stats.expression_cache.evictions += _cached_entries.clear(true);
        }
    })) {
    _cached_entries.reserve(_max_cache_entries);
}

prepared_expression_cache::prepared_expression_cache(prepared_expression_cache::config cfg, stats& stats) : 
    _impl(std::make_unique<prepared_expression_cache_impl>(std::move(cfg), stats)) {
}
prepared_expression_cache::~prepared_expression_cache() = default;

parsed::update_expression prepared_expression_cache::parse_update_expression(std::string_view query) {
    return _impl->get_or_create<stats::expression_types::UPDATE_EXPRESSION>(query, alternator::parse_update_expression);
}

std::vector<parsed::path> prepared_expression_cache::parse_projection_expression(std::string_view query) {
    return _impl->get_or_create<stats::expression_types::PROJECTION_EXPRESSION>(query, alternator::parse_projection_expression);
}

parsed::condition_expression prepared_expression_cache::parse_condition_expression(std::string_view query, const char* caller) {
    return _impl->get_or_create<stats::expression_types::CONDITION_EXPRESSION>(query, alternator::parse_condition_expression, caller);
}

} // namespace alternator
