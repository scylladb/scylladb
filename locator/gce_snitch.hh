/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

/*
 * Modified by ScyllaDB
 * Copyright (C) 2018-present ScyllaDB
 */
#pragma once

#include "locator/production_snitch_base.hh"
#include <seastar/http/response_parser.hh>
#include "utils/exponential_backoff_retry.hh"

namespace locator {

class gce_snitch : public production_snitch_base {
public:
    static constexpr const char* ZONE_NAME_QUERY_REQ = "/computeMetadata/v1/instance/zone";
    static constexpr const char* GCE_QUERY_SERVER_ADDR = "metadata.google.internal";
    static constexpr int GCE_API_CALL_RETRIES = 10;

    explicit gce_snitch(const snitch_config&);
    virtual future<> start() override;
    virtual sstring get_name() const override {
        return "org.apache.cassandra.locator.GoogleCloudSnitch";
    }
protected:
    future<> load_config();
    future<sstring> gce_api_call(sstring addr, const sstring cmd);
    future<sstring> read_property_file();

private:
    sstring _meta_server_url;
    exponential_backoff_retry _gce_api_retry = exponential_backoff_retry(std::chrono::seconds(5), std::chrono::seconds(2560));
    future<sstring> gce_api_call_once(sstring addr, const sstring cmd);
};

} // namespace locator
