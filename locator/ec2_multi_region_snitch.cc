/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "locator/ec2_multi_region_snitch.hh"
#include "exceptions/exceptions.hh"
#include "gms/gossiper.hh"
#include "utils/class_registrator.hh"

static constexpr const char* PUBLIC_IP_QUERY_REQ  = "/latest/meta-data/public-ipv4";
static constexpr const char* PRIVATE_IP_QUERY_REQ = "/latest/meta-data/local-ipv4";

static constexpr const char* PUBLIC_IPV6_QUERY_REQ  = "/latest/meta-data/network/interfaces/macs/{}/ipv6s";
static constexpr const char* PRIVATE_MAC_QUERY = "/latest/meta-data/network/interfaces/macs";

namespace locator {
ec2_multi_region_snitch::ec2_multi_region_snitch(const snitch_config& cfg)
    : ec2_snitch(cfg)
{}

future<> ec2_multi_region_snitch::start() {
    _state = snitch_state::initializing;

    co_await ec2_snitch::load_config(true);
    if (this_shard_id() == io_cpu_id()) {
        auto token = co_await aws_api_call(AWS_QUERY_SERVER_ADDR, AWS_QUERY_SERVER_PORT, TOKEN_REQ_ENDPOINT, std::nullopt);

        try {
            auto broadcast = _cfg.broadcast_address;
            if (broadcast.addr().is_ipv6()) {
                auto macs = co_await aws_api_call(AWS_QUERY_SERVER_ADDR, AWS_QUERY_SERVER_PORT, PRIVATE_MAC_QUERY, token);
                // we should just get a single line, ending in '/'. If there are more than one mac, we should
                // maybe try to loop the addresses and exclude local/link-local etc, but these addresses typically 
                // are already filtered by aws, so probably does not help. For now, just warn and pick first address.
                auto i = macs.find('/');
                auto mac = macs.substr(0, i);
                if (i != std::string::npos && ++i != macs.size()) {
                    logger().warn("Ec2MultiRegionSnitch (ipv6): more than one MAC address listed ({}). Will use first.", macs);
                }
                auto ipv6 = co_await aws_api_call(AWS_QUERY_SERVER_ADDR, AWS_QUERY_SERVER_PORT, format(PUBLIC_IPV6_QUERY_REQ, mac), token);
                _local_public_address = inet_address(ipv6);
                _local_private_address = ipv6;
            } else {
                auto pub_addr = co_await aws_api_call(AWS_QUERY_SERVER_ADDR, AWS_QUERY_SERVER_PORT, PUBLIC_IP_QUERY_REQ, token);
                _local_public_address = inet_address(pub_addr);
            }
        } catch (...) {
            std::throw_with_nested(exceptions::configuration_exception("Failed to get a Public IP. Public IP is a requirement for Ec2MultiRegionSnitch. Consider using a different snitch if your instance doesn't have it"));
        }
        logger().info("Ec2MultiRegionSnitch using publicIP as identifier: {}", _local_public_address);

        if (!_local_public_address.addr().is_ipv6()) {
            sstring priv_addr = co_await aws_api_call(AWS_QUERY_SERVER_ADDR, AWS_QUERY_SERVER_PORT, PRIVATE_IP_QUERY_REQ, token);
            _local_private_address = priv_addr;
        }

        //
        // Gossiper main instance is currently running on CPU0 -
        // therefore we need to make sure the _local_private_address is
        // set on the shard0 so that it may be used when Gossiper is
        // going to invoke gossiper_starting() method.
        //
        co_await container().invoke_on(0, [this] (snitch_ptr& local_s) {
            if (this_shard_id() != io_cpu_id()) {
                local_s->set_local_private_addr(_local_private_address);
            }
        });
    }

    set_snitch_ready();
}

void ec2_multi_region_snitch::set_local_private_addr(const sstring& addr_str) {
    _local_private_address = addr_str;
}

gms::application_state_map ec2_multi_region_snitch::get_app_states() const {
    return {
        {gms::application_state::DC, gms::versioned_value::datacenter(_my_dc)},
        {gms::application_state::RACK, gms::versioned_value::rack(_my_rack)},
        {gms::application_state::INTERNAL_IP, gms::versioned_value::internal_ip(_local_private_address)},
    };
}

using registry_default = class_registrator<i_endpoint_snitch, ec2_multi_region_snitch, const snitch_config&>;
static registry_default registrator_default("org.apache.cassandra.locator.Ec2MultiRegionSnitch");
static registry_default registrator_default_short_name("Ec2MultiRegionSnitch");

} // namespace locator
