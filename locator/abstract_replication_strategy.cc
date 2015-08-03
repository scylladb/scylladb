/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "locator/abstract_replication_strategy.hh"
#include "utils/class_registrator.hh"
#include "exceptions/exceptions.hh"

namespace locator {

logging::logger abstract_replication_strategy::logger("replication_strategy_logger");

abstract_replication_strategy::abstract_replication_strategy(
    const sstring& ks_name,
    token_metadata& token_metadata,
    snitch_ptr& snitch,
    const std::map<sstring, sstring>& config_options,
    replication_strategy_type my_type)
        : _ks_name(ks_name)
        , _config_options(config_options)
        , _token_metadata(token_metadata)
        , _snitch(snitch)
        , _my_type(my_type) {}

std::unique_ptr<abstract_replication_strategy> abstract_replication_strategy::create_replication_strategy(const sstring& ks_name, const sstring& strategy_name, token_metadata& tk_metadata, const std::map<sstring, sstring>& config_options) {
    assert(locator::i_endpoint_snitch::get_local_snitch_ptr());

    return create_object<abstract_replication_strategy,
                         const sstring&,
                         token_metadata&,
                         snitch_ptr&,
                         const std::map<sstring, sstring>&>
        (strategy_name, ks_name, tk_metadata,
         locator::i_endpoint_snitch::get_local_snitch_ptr(), config_options);
}

std::vector<inet_address> abstract_replication_strategy::get_natural_endpoints(const token& search_token) {
    const token& key_token = _token_metadata.first_token(search_token);
    auto& cached_endpoints = get_cached_endpoints();
    auto res = cached_endpoints.find(key_token);

    if (res == cached_endpoints.end()) {
        auto endpoints = calculate_natural_endpoints(search_token);
        cached_endpoints.emplace(key_token, endpoints);

        return std::move(endpoints);
    }

    ++_cache_hits_count;
    return res->second;
}

void abstract_replication_strategy::validate_replication_factor(sstring rf)
{
    try {
        if (std::stol(rf) < 0) {
            throw exceptions::configuration_exception(
               sstring("Replication factor must be non-negative; found ") + rf);
        }
    } catch (...) {
        throw exceptions::configuration_exception(
            sstring("Replication factor must be numeric; found ") + rf);
    }
}

inline std::unordered_map<token, std::vector<inet_address>>&
abstract_replication_strategy::get_cached_endpoints() {
    if (_last_invalidated_ring_version != _token_metadata.get_ring_version()) {
        _cached_endpoints.clear();
        _last_invalidated_ring_version = _token_metadata.get_ring_version();
    }

    return _cached_endpoints;
}

} // namespace locator
