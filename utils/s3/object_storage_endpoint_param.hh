/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <yaml-cpp/yaml.h>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>
#include "utils/s3/creds.hh"
#include "seastarx.hh"

struct object_storage_endpoint_param {
    sstring endpoint;
    s3::endpoint_config config;
};

namespace YAML {
template<>
struct convert<::object_storage_endpoint_param> {
    static bool decode(const Node& node, ::object_storage_endpoint_param& ep) {
        ep.endpoint = node["name"].as<std::string>();
        ep.config.port = node["port"].as<unsigned>();
        ep.config.use_https = node["https"].as<bool>(false);
        if (node["aws_region"] || std::getenv("AWS_DEFAULT_REGION")) {
            ep.config.aws.emplace();

            // https://github.com/scylladb/scylla-pkg/issues/3845
            // Allow picking up aws values via standard env vars as well.
            // Value in config has prio, but fall back to env.
            // This has the added benefit of potentially reducing the amount of
            // sensitive data in config files (i.e. credentials)
            auto get_node_value_or_env = [&](const char* key, const char* var) {
                auto child = node[key];
                if (child) {
                    return child.as<std::string>();
                }
                auto val = std::getenv(var);
                if (val) {
                    return std::string(val);
                }
                return std::string{};
            };
            ep.config.aws->region = get_node_value_or_env("aws_region", "AWS_DEFAULT_REGION");
            ep.config.aws->access_key_id = get_node_value_or_env("aws_access_key_id", "AWS_ACCESS_KEY_ID");
            ep.config.aws->secret_access_key = get_node_value_or_env("aws_secret_access_key", "AWS_SECRET_ACCESS_KEY");
            ep.config.aws->session_token = get_node_value_or_env("aws_session_token", "AWS_SESSION_TOKEN");
        }
        return true;
    }
};
}

inline future<std::unordered_map<sstring, s3::endpoint_config>>
read_endpoints_config(std::string_view fn) {
    sstring data;
    std::exception_ptr ex;

    auto file = co_await open_file_dma(fn, open_flags::ro);
    try {
        auto size = co_await file.size();
        data = seastar::to_sstring(co_await file.dma_read_exactly<char>(0, size));
    } catch (...) {
        ex = std::current_exception();
    }
    co_await file.close();
    if (ex) {
        co_await coroutine::return_exception_ptr(ex);
    }

    std::unordered_map<sstring, s3::endpoint_config> cfg;
    YAML::Node doc = YAML::Load(data.c_str());
    for (auto&& section : doc) {
        auto sec_name = section.first.as<std::string>();
        if (sec_name != "endpoints") {
            throw std::runtime_error(fmt::format("While parsing object_storage config: section {} currently unsupported.", sec_name));
        }

        auto endpoints = section.second.as<std::vector<object_storage_endpoint_param>>();
        for (auto&& ep : endpoints) {
            cfg.emplace(ep.endpoint, std::move(ep.config));
        }
    }
    co_return std::move(cfg);
}
