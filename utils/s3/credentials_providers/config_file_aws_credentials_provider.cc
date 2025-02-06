/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "config_file_aws_credentials_provider.hh"

#include <iostream>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <yaml-cpp/yaml.h>


using namespace seastar;

namespace aws {
config_file_aws_credentials_provider::config_file_aws_credentials_provider(const std::string& _creds_file) : creds_file(_creds_file) {
}

future<> config_file_aws_credentials_provider::reload() {
    if (creds)
        co_return;
    auto cfg_file = co_await open_file_dma(creds_file, open_flags::ro);
    sstring data;
    std::exception_ptr ex;

    try {
        auto sz = co_await cfg_file.size();
        data = to_sstring(co_await cfg_file.dma_read_exactly<char>(0, sz));
    } catch (...) {
        ex = std::current_exception();
    }
    co_await cfg_file.close();
    if (ex) {
        co_await coroutine::return_exception_ptr(ex);
    }

    std::unordered_map<sstring, s3::endpoint_config> cfg;
    YAML::Node doc = YAML::Load(data.c_str());
    for (auto&& section : doc) {
        auto sec_name = section.first.as<std::string>();
        if (sec_name != "endpoints") {
            co_await coroutine::return_exception(
                std::runtime_error(fmt::format("Failed to parse object_storage config: section {} currently unsupported.", sec_name)));
        }
        for (auto&& endpoint : section.second) {
            creds.access_key_id = endpoint["aws_access_key_id"].as<std::string>("");
            creds.secret_access_key = endpoint["aws_secret_access_key"].as<std::string>("");
            creds.session_token = endpoint["aws_session_token"].as<std::string>("");
            break;
        }
    }
    creds.expires_at = lowres_clock::time_point::max();
}

} // namespace aws
