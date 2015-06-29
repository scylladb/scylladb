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
private:
    long _last_invalidated_ring_version = 0;
    std::unordered_map<token, std::vector<inet_address>> _cached_endpoints;

    static logging::logger& logger() {
        static thread_local logging::logger lgr("replication_strategy_logger");

        return lgr;
    }

    std::unordered_map<token, std::vector<inet_address>>&
    get_cached_endpoints();
protected:
    sstring _ks_name;
    // TODO: Do we need this member at all?
    //keyspace* _keyspace = nullptr;
    std::map<sstring, sstring> _config_options;
    token_metadata& _token_metadata;
    snitch_ptr& _snitch;

    template <typename... Args>
    void err(const char* fmt, Args&&... args) const {
        logger().error(fmt, std::forward<Args>(args)...);
    }

    template <typename... Args>
    void warn(const char* fmt, Args&&... args) const {
        logger().warn(fmt, std::forward<Args>(args)...);
    }

    template <typename... Args>
    void debug(const char* fmt, Args&&... args) const {
        logger().debug(fmt, std::forward<Args>(args)...);
    }

    void validate_replication_factor(sstring rf);

    virtual std::vector<inet_address> calculate_natural_endpoints(const token& search_token) = 0;

public:
    abstract_replication_strategy(const sstring& keyspace_name, token_metadata& token_metadata, snitch_ptr& snitch, const std::map<sstring, sstring>& config_options);
    virtual ~abstract_replication_strategy() {}
    static std::unique_ptr<abstract_replication_strategy> create_replication_strategy(const sstring& ks_name, const sstring& strategy_name, token_metadata& token_metadata, const std::map<sstring, sstring>& config_options);
    std::vector<inet_address> get_natural_endpoints(const token& search_token);
    virtual size_t get_replication_factor() const = 0;
};

}
