/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "locator/ec2_snitch.hh"

namespace locator {
class ec2_multi_region_snitch : public ec2_snitch {
public:
    ec2_multi_region_snitch(const snitch_config&);
    virtual gms::application_state_map get_app_states() const override;
    virtual future<> start() override;
    virtual void set_local_private_addr(const sstring& addr_str) override;
    virtual sstring get_name() const override {
        return "org.apache.cassandra.locator.Ec2MultiRegionSnitch";
    }
    virtual std::optional<inet_address> get_public_address() const noexcept override {
        return _local_public_address;
    }
private:
    inet_address _local_public_address;
    sstring _local_private_address;
};
} // namespace locator
