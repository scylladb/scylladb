/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "expressions.hh"
#include "utils/log.hh"

#include <boost/intrusive/list.hpp>
#include <absl/container/node_hash_map.h>
#include <optional>
#include <seastar/core/on_internal_error.hh>

static logging::logger logger_("parsed-expression-factory");

namespace alternator {
namespace {

template<typename Value>
class lru_string_map {
    struct key {
        mutable boost::intrusive::list_member_hook<> list_hook;
        std::string str;
        explicit key(std::string_view s) : str(std::move(s)) {}
        // We link keys in intrusive::list, so make sure main container provides stable address for keys (e.g in rehashing).
        key() = delete;
        key(const key& k) = delete;
        key(const key&& k) = delete;
    };
    struct key_hash {
        using is_transparent = void;
        size_t operator()(const key& k) const noexcept { return std::hash<std::string_view>{}(k.str); }
        size_t operator()(std::string_view sv) const noexcept { return std::hash<std::string_view>{}(sv); }
    };
    struct key_equal {
        using is_transparent = void;
        bool operator()(const key& k, std::string_view sv) const noexcept { return k.str == sv; }
        bool operator()(std::string_view sv, const key& k) const noexcept { return sv == k.str; }
        bool operator()(const key& a, const key& b) const noexcept { return a.str == b.str; }
    };

    using string_map = absl::node_hash_map<key, Value, key_hash, key_equal>;
    string_map map;
    using lru_key_list_hook = boost::intrusive::member_hook<key, boost::intrusive::list_member_hook<>,&key::list_hook>;
    using lru_key_list = boost::intrusive::list<key, lru_key_list_hook>;
    lru_key_list lru_list;

    // Move the accessed key to the back of the LRU list
    template<bool IsNew=false> 
    void touch(string_map::iterator it) {
        // key is const, but it has mutable list_hook, so we remove constness
        // to allow moving it in the list.
        // This is safe because we only use this for moving keys in the LRU list,
        // and we never modify the key itself.
        key& key_ref = const_cast<key&>(it->first);
        if constexpr (!IsNew) {
            lru_list.erase(lru_list.iterator_to(key_ref));
        }
        lru_list.push_back(key_ref);
    }
public:
    auto size() { return map.size(); }
    void reserve(size_t n) { return map.reserve(n); }

    // Lookup by string_view, move key to back, and return value
    std::optional<std::reference_wrapper<Value>> find(std::string_view key) {
        auto it = map.find(key);
        if (it == map.end()) {
            return std::nullopt;
        }
        touch(it);
        return std::ref(it->second);
    }

    // Removes the least recently used item from the cache
    // Returns true if an item was removed, false if the cache was empty
    bool pop() {
        if (!lru_list.empty()) {
            auto it = map.find(lru_list.front());
            lru_list.pop_front();
            if (it != map.end()) { // This should be always true
                map.erase(it);
            } else {
                on_internal_error(logger_, "LRU list contained object that is not in the cache");
            }
            return true;
        }
        return false;
    }

    // Insert or update the value for the given key.
    void push(std::string_view key, Value&& value) {
        auto ret = map.insert_or_assign(key, std::move(value));
        (ret.second ? touch<true>(ret.first) : touch<false>(ret.first));
    }
};
}

// Actual parsing functions are defined in alternator/expressions.cc
parsed::update_expression parse_update_expression(std::string_view query);
std::vector<parsed::path> parse_projection_expression(std::string_view query);
parsed::condition_expression parse_condition_expression(std::string_view query, const char* caller);

struct expression_factory_impl {
    expression_factory::config _cfg;
    size_t _max_cache_entries;
    stats& _stats;

    using cached_expressions_types = std::variant<
        parsed::update_expression,
        parsed::condition_expression,
        std::vector<parsed::path>
    >;
    lru_string_map<cached_expressions_types> _cached_entries;
    utils::observable<uint32_t>::observer _max_cache_entries_observer;

    expression_factory_impl(expression_factory::config cfg, stats& stats);

    template <typename Func, typename... Args>
    using ParseResult = std::invoke_result_t<Func, std::string_view, Args...>;

    // Caching layer for parsed expressions
    // The expression type is determined only from the parsing function (ParseResult<Func, Args...>)
    // StatsType is used only to update appropriate statistics - currently it is aligned with the expression type,
    // but it could be extended in the future if needed, e.g. split per operation.
    template <stats::expression_types StatsType, typename Func, typename... Args>
    ParseResult<Func, Args...> get_or_create(std::string_view query, Func&& parse_func, Args&&... other_args) {
        if (_cfg.max_cache_entries == 0) {
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
                logger_.error("Cache hit for '{}' but type mismatch.", query);
                _stats.expression_cache.requests[StatsType].hits--;
            }
        } else {
            logger_.trace("Cache miss for query: {}", query);
        }
        ParseResult<Func, Args...> expr = parse_func(query, std::forward<Args>(other_args)...);
        // Invalid query will throw here ^

        _stats.expression_cache.requests[StatsType].misses++;
        if (_cached_entries.size() >= _cfg.max_cache_entries && _cached_entries.pop()) {
            _stats.expression_cache.evictions++;
        }
        cached_expressions_types new_value = expr;
        _cached_entries.push(query, std::move(new_value));
        return expr;
    }
};

expression_factory_impl::expression_factory_impl(expression_factory::config cfg, stats& stats) : 
    _cfg(cfg), _max_cache_entries(_cfg.max_cache_entries), _stats(stats),
    _max_cache_entries_observer(_cfg.max_cache_entries.observe([this] (const uint32_t &max_value) {
        logger_.trace("Expression cache max entries changed to {}", max_value);
        _max_cache_entries = max_value;
        while (_cached_entries.size() > _max_cache_entries && _cached_entries.pop()) {
            _stats.expression_cache.evictions++;
        }
        _cached_entries.reserve(_max_cache_entries); //but it never shrinks container
    })) {
    _cached_entries.reserve(_max_cache_entries);
}

expression_factory::expression_factory(expression_factory::config cfg, stats& stats) : 
    _impl(std::make_unique<expression_factory_impl>(std::move(cfg), stats)) {
}
expression_factory::~expression_factory() = default;

parsed::update_expression expression_factory::parse_update_expression(std::string_view query) {
    return _impl->get_or_create<stats::expression_types::UPDATE_EXPRESSION>(query, alternator::parse_update_expression);
}

std::vector<parsed::path> expression_factory::parse_projection_expression(std::string_view query) {
    return _impl->get_or_create<stats::expression_types::PROJECTION_EXPRESSION>(query, alternator::parse_projection_expression);
}

parsed::condition_expression expression_factory::parse_condition_expression(std::string_view query, const char* caller) {
    return _impl->get_or_create<stats::expression_types::CONDITION_EXPRESSION>(query, alternator::parse_condition_expression, caller);
}

} // namespace alternator
