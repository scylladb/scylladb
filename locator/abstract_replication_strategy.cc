/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "abstract_replication_strategy.hh"

namespace locator {

abstract_replication_strategy::abstract_replication_strategy(const sstring& ks_name, token_metadata& token_metadata, std::unordered_map<sstring, sstring>& config_options) :
       _ks_name(ks_name),  _config_options(config_options), _token_metadata(token_metadata)  {}

std::unique_ptr<abstract_replication_strategy> abstract_replication_strategy::create_replication_strategy(const sstring& ks_name, const sstring& strategy_name, token_metadata& token_metadata, std::unordered_map<sstring, sstring>& config_options) {
    return replication_strategy_registry::create(strategy_name, ks_name, token_metadata, config_options);
}

std::vector<inet_address> abstract_replication_strategy::get_natural_endpoints(token& search_token) {
    const token& key_token = _token_metadata.first_token(search_token);
    return calculate_natural_endpoints(key_token);
}

std::unordered_map<sstring, strategy_creator_type> replication_strategy_registry::_strategies;

void replication_strategy_registry::register_strategy(sstring name, strategy_creator_type creator) {
    _strategies.emplace(name, std::move(creator));
}

std::unique_ptr<abstract_replication_strategy> replication_strategy_registry::create(const sstring& name, const sstring& keyspace_name, token_metadata& token_metadata, std::unordered_map<sstring, sstring>& config_options) {
    return _strategies[name](keyspace_name, token_metadata, config_options);
}

replication_strategy_registrator::replication_strategy_registrator(sstring name, strategy_creator_type creator) {
    replication_strategy_registry::register_strategy(name, creator);
}

}
