/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <vector>
#include <algorithm>
#include "locator/snitch_base.hh"
#include "gms/application_state.hh"

namespace locator {

std::list<std::pair<gms::application_state, gms::versioned_value>> snitch_base::get_app_states() const {
    return {
        {gms::application_state::DC, gms::versioned_value::datacenter(_my_dc)},
        {gms::application_state::RACK, gms::versioned_value::rack(_my_rack)},
    };
}

snitch_ptr::snitch_ptr(const snitch_config cfg)
{
    static std::vector<sstring> cloud_snitches {"Ec2Snitch", "Ec2MultiRegionSnitch", "GoogleCloudSnitch", "AzureSnitch"};
    i_endpoint_snitch::ptr_type s;
    try {
        bool is_cloud_snitch = std::find(cloud_snitches.begin(), cloud_snitches.end(), cfg.name) != cloud_snitches.end();

        if (is_cloud_snitch) {
            sstring name = "GossipingPropertyFileSnitch";
            s = create_object<i_endpoint_snitch>(name, cfg);
        } else {
            s = create_object<i_endpoint_snitch>(cfg.name, cfg);
        }
    } catch (no_such_class& e) {
        i_endpoint_snitch::logger().error("Can't create snitch {}: not supported", cfg.name);
        throw;
    } catch (...) {
        throw;
    }
    s->set_backreference(*this);
    _ptr = std::move(s);
}

} // namespace locator
