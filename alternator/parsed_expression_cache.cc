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

static logging::logger logger_("parsed-expression-cache");

namespace alternator::parsed {

struct expression_cache_impl {
    stats& _stats;

    using cached_expressions_types = std::variant<
        update_expression,
        condition_expression,
        std::vector<path>
    >;
    sized_lru_string_map<cached_expressions_types> _cached_entries;
    utils::observable<uint32_t>::observer _max_cache_entries_observer;

    expression_cache_impl(expression_cache::config cfg, stats& stats);

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
        if (_cached_entries.disabled()) {
            return parse_func(query, std::forward<Args>(other_args)...);
        }
        if (!_cached_entries.sanity_check()) {
            _stats.expression_cache.requests[StatsType].misses++;
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
                // If, by any chance this is a valid query, it will be updated below with the new value.
                logger_.trace("Cache hit for '{}', but type mismatch.", query);
                _stats.expression_cache.requests[StatsType].hits--;
            }
        } else {
            logger_.trace("Cache miss for query: {}", query);
        }
        ParseResult<Func, Args...> expr = parse_func(query, std::forward<Args>(other_args)...);
        // Invalid query will throw here ^

        _stats.expression_cache.requests[StatsType].misses++;
        if (value) [[unlikely]] {
            value->get() = cached_expressions_types{expr};
        } else {
            _cached_entries.insert(query, cached_expressions_types{expr});
        }
        return expr;
    }
};

expression_cache_impl::expression_cache_impl(expression_cache::config cfg, stats& stats) : 
    _stats(stats), _cached_entries(logger_, _stats.expression_cache.evictions),
    _max_cache_entries_observer(cfg.max_cache_entries.observe([this] (uint32_t max_value) {
        _cached_entries.set_max_size(max_value);
    })) {
    _cached_entries.set_max_size(cfg.max_cache_entries());
}

expression_cache::expression_cache(expression_cache::config cfg, stats& stats) : 
    _impl(std::make_unique<expression_cache_impl>(std::move(cfg), stats)) {
}
expression_cache::~expression_cache() = default;
future<> expression_cache::stop() {
    return _impl->_cached_entries.stop();
}

update_expression expression_cache::parse_update_expression(std::string_view query) {
    return _impl->get_or_create<stats::expression_types::UPDATE_EXPRESSION>(query, alternator::parse_update_expression);
}

std::vector<path> expression_cache::parse_projection_expression(std::string_view query) {
    return _impl->get_or_create<stats::expression_types::PROJECTION_EXPRESSION>(query, alternator::parse_projection_expression);
}

condition_expression expression_cache::parse_condition_expression(std::string_view query, const char* caller) {
    return _impl->get_or_create<stats::expression_types::CONDITION_EXPRESSION>(query, alternator::parse_condition_expression, caller);
}

} // namespace alternator::parsed
