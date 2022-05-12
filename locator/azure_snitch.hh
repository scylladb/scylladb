/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

/*
 * Modified by ScyllaDB
 * Copyright (C) 2021-present ScyllaDB
 */
#pragma once

#include "locator/production_snitch_base.hh"

namespace locator {

class azure_snitch : public production_snitch_base {
public:
    static constexpr auto AZURE_SERVER_ADDR = "169.254.169.254";
    static constexpr auto AZURE_QUERY_PATH_TEMPLATE = "/metadata/instance/compute/{}?api-version=2020-09-01&format=text";

    static const std::string REGION_NAME_QUERY_PATH;
    static const std::string ZONE_NAME_QUERY_PATH;

    explicit azure_snitch(const snitch_config&);
    virtual future<> start() override;
    virtual sstring get_name() const override {
        return "org.apache.cassandra.locator.AzureSnitch";
    }
protected:
    future<> load_config();
    future<sstring> azure_api_call(sstring path);
    future<sstring> read_property_file();
};

} // namespace locator
