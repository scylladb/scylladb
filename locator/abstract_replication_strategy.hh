/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include <memory>
#include <functional>
#include <unordered_map>
#include "gms/inet_address.hh"
#include "dht/i_partitioner.hh"
#include "token_metadata.hh"
#include "snitch_base.hh"

// forward declaration since database.hh includes this file
class keyspace;

namespace locator {

using inet_address = gms::inet_address;
using token = dht::token;

class abstract_replication_strategy {
protected:
    sstring _ks_name;
    keyspace* _keyspace = nullptr;
    std::map<sstring, sstring> _config_options;
    token_metadata& _token_metadata;
    snitch_ptr _snitch;
    virtual std::vector<inet_address> calculate_natural_endpoints(const token& search_token) = 0;
public:
    abstract_replication_strategy(const sstring& keyspace_name, token_metadata& token_metadata, snitch_ptr&& snitch, const std::map<sstring, sstring>& config_options);
    virtual ~abstract_replication_strategy() {}
    static std::unique_ptr<abstract_replication_strategy> create_replication_strategy(const sstring& ks_name, const sstring& strategy_name, token_metadata& token_metadata, snitch_ptr&& snitch, const std::map<sstring, sstring>& config_options);
    std::vector<inet_address> get_natural_endpoints(const token& search_token);
    future<> stop() {
        if (_snitch) {
            return _snitch->stop();
        } else {
            return make_ready_future<>();
        }
    }
    virtual size_t get_replication_factor() const = 0;
};

}
