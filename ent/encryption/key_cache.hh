/*
 * Copyright (C) 2026-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "utils/loading_cache.hh"
#include "encryption_exceptions.hh"
#include "encryption.hh"

namespace seastar::metrics {
class metric_groups;
}

namespace encryption {

enum class key_cache_type : uint8_t {
    id_cache, attr_cache
};

namespace detail {

template<key_cache_type Type> 
struct key_cache_stats_updater {
    static void inc_hits() noexcept;
    static void inc_misses() noexcept;
    static void inc_blocks() noexcept;
    static void inc_evictions() noexcept;
    static void inc_privileged_on_cache_size_eviction() noexcept;
    static void inc_unprivileged_on_cache_size_eviction() noexcept;
};

}

void register_key_cache_metrics(seastar::metrics::metric_groups&);

template<key_cache_type Type,
     typename Key,
     typename Tp,
     typename Hash = std::hash<Key>,
     typename EqualPred = std::equal_to<Key>>
class key_cache : public utils::loading_cache<Key, Tp, 2
        , utils::loading_cache_reload_enabled::yes //_no_expiry
        , utils::simple_entry_size<Tp>
        , Hash
        , EqualPred
        , detail::key_cache_stats_updater<Type>
        , detail::key_cache_stats_updater<Type>
        > 
{
public:
    using mybase = utils::loading_cache<Key, Tp, 2
        , utils::loading_cache_reload_enabled::yes
        , utils::simple_entry_size<Tp>
        , Hash
        , EqualPred
        , detail::key_cache_stats_updater<Type>
        , detail::key_cache_stats_updater<Type>
        >;
    using load_func = std::function<future<Tp>(const Key&)>;

    key_cache(const encryption::encryption_context& ctxt, utils::loading_cache_config cfg, logging::logger& logger, load_func load)
        : mybase(std::move(cfg), logger, load, [this, load, &logger](const Key& k) -> future<Tp> {
            // Do reload via same load, but treat different exceptions 
            // differently. Server/network errors
            // are treated as non-fatal (not caused by key rotation/deletion or bad creds)
            // but any "real" errors are propagated to the calling cache, and counts
            // towards eviction
            // This means we will try our best to keep a cached key alive as long
            // as it is being used regularly, until we can actually reach the key
            // server and detect it being deleted or whatnot
            try {
                co_return co_await load(k);
            } catch (missing_resource_error&) {
                logger.warn("Reload failure: missing_resource_error: {}", std::current_exception());
                throw; 
            } catch (permission_error&) {
                logger.warn("Reload failure: permission_error: {}", std::current_exception());
                throw;
            } catch (malformed_response_error&) {
                logger.warn("Reload failure: malformed_response_error: {}", std::current_exception());
                throw;
            } catch (service_error&) {
                // treat this as non-fatal in reload. fake success by just
                // doing a lookup. The value should still be in cache
            } catch (...) {
                // just for logging
                logger.warn("Reload failure: {}", std::current_exception());
                throw;
            }
            try {
                logger.debug("Reload unsuccessful. Attempt cache refresh of {}", k);
                co_return co_await this->get(k);
            } catch (...) {
                logger.debug("Cache refresh of {} failed: {}", k, std::current_exception());
                throw;
            }
        }, ctxt.cache_maintainance_group())
    {}
};

template<typename Key,
     typename Tp,
     typename Hash = std::hash<Key>,
     typename EqualPred = std::equal_to<Key>>
using id_cache = key_cache<key_cache_type::id_cache, Key, Tp, Hash, EqualPred>;

template<typename Key,
     typename Tp,
     typename Hash = std::hash<Key>,
     typename EqualPred = std::equal_to<Key>>
using attr_cache = key_cache<key_cache_type::attr_cache, Key, Tp, Hash, EqualPred>;

struct cache_expiry {
    std::chrono::milliseconds key_cache_expiry;
    std::chrono::milliseconds key_cache_refresh;
};

cache_expiry adjust_expiry(std::chrono::milliseconds exp, std::chrono::milliseconds ref);

utils::loading_cache_config make_key_cache_config(std::chrono::milliseconds exp, std::chrono::milliseconds ref);

int adjust_service_call_retries(const encryption_context&, int max_retries);

}
