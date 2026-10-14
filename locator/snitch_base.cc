/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#include "locator/snitch_base.hh"
#include "gms/application_state.hh"
#include "locator/simple_snitch.hh"
#include "locator/rack_inferring_snitch.hh"
#include "locator/gossiping_property_file_snitch.hh"
#include "locator/ec2_snitch.hh"
#include "locator/ec2_multi_region_snitch.hh"
#include "locator/gce_snitch.hh"
#include "locator/azure_snitch.hh"

namespace locator {

gms::application_state_map snitch_base::get_app_states() const {
    return {
        {gms::application_state::DC, gms::versioned_value::datacenter(_my_dc)},
        {gms::application_state::RACK, gms::versioned_value::rack(_my_rack)},
    };
}

template <typename Snitch>
static i_endpoint_snitch::ptr_type construct_snitch(const snitch_config& cfg) {
    return std::make_unique<Snitch>(cfg);
}

struct snitch_class {
    std::string_view qualified_name;
    std::string_view short_name;
    i_endpoint_snitch::ptr_type (*construct)(const snitch_config&);
};

static constexpr snitch_class snitch_classes[] = {
    {"org.apache.cassandra.locator.SimpleSnitch", "SimpleSnitch", construct_snitch<simple_snitch>},
    {"org.apache.cassandra.locator.RackInferringSnitch", "RackInferringSnitch", construct_snitch<rack_inferring_snitch>},
    {"org.apache.cassandra.locator.GossipingPropertyFileSnitch", "GossipingPropertyFileSnitch", construct_snitch<gossiping_property_file_snitch>},
    {"org.apache.cassandra.locator.Ec2Snitch", "Ec2Snitch", construct_snitch<ec2_snitch>},
    {"org.apache.cassandra.locator.Ec2MultiRegionSnitch", "Ec2MultiRegionSnitch", construct_snitch<ec2_multi_region_snitch>},
    {"org.apache.cassandra.locator.GoogleCloudSnitch", "GoogleCloudSnitch", construct_snitch<gce_snitch>},
    {"org.apache.cassandra.locator.AzureSnitch", "AzureSnitch", construct_snitch<azure_snitch>},
};

snitch_ptr::snitch_ptr(const snitch_config cfg)
{
    auto it = std::ranges::find_if(snitch_classes, [&cfg] (const snitch_class& snitch_class) {
        return cfg.name == snitch_class.qualified_name || cfg.name == snitch_class.short_name;
    });
    if (it == std::ranges::end(snitch_classes)) {
        i_endpoint_snitch::logger().error("Can't create snitch {}: not supported", cfg.name);
        throw std::invalid_argument(fmt::format("Snitch {} is not supported", cfg.name));
    }
    auto s = it->construct(cfg);
    s->set_backreference(*this);
    _ptr = std::move(s);
}

} // namespace locator
