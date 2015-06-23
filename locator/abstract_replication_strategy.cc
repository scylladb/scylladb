/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "abstract_replication_strategy.hh"
#include "utils/class_registrator.hh"
#include "locator/snitch_base.hh"

namespace locator {

abstract_replication_strategy::abstract_replication_strategy(const sstring& ks_name, token_metadata& token_metadata, snitch_ptr& snitch, const std::map<sstring, sstring>& config_options) :
       _ks_name(ks_name),  _config_options(config_options), _token_metadata(token_metadata), _snitch(snitch)  {}

std::unique_ptr<abstract_replication_strategy> abstract_replication_strategy::create_replication_strategy(const sstring& ks_name, const sstring& strategy_name, token_metadata& tk_metadata, const std::map<sstring, sstring>& config_options) {
    assert(locator::i_endpoint_snitch::get_local_snitch_ptr());

    sstring class_name = strategy_name.find(".") != sstring::npos ? strategy_name : "org.apache.cassandra.locator." + strategy_name;
    return create_object<abstract_replication_strategy,
                         const sstring&,
                         token_metadata&,
                         snitch_ptr&,
                         const std::map<sstring, sstring>&>
        (class_name, ks_name, tk_metadata,
         locator::i_endpoint_snitch::get_local_snitch_ptr(), config_options);
}

std::vector<inet_address> abstract_replication_strategy::get_natural_endpoints(const token& search_token) {
    const token& key_token = _token_metadata.first_token(search_token);
    return calculate_natural_endpoints(key_token);
}

}
