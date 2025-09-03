/*
 * Copyright (C) 2025-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
 
#include <string>
#include <variant>
#include <yaml-cpp/yaml.h>

#include <boost/lexical_cast.hpp>

#include "utils/s3/creds.hh"
#include "object_storage_endpoint_param.hh"

db::object_storage_endpoint_param::object_storage_endpoint_param(s3_storage s)
    : _data(std::move(s))
{}
db::object_storage_endpoint_param::object_storage_endpoint_param(std::string endpoint, s3::endpoint_config config)
    : object_storage_endpoint_param(s3_storage{std::move(endpoint), std::move(config)})
{}

db::object_storage_endpoint_param::object_storage_endpoint_param() = default;
db::object_storage_endpoint_param::object_storage_endpoint_param(const object_storage_endpoint_param&) = default;

std::string db::object_storage_endpoint_param::s3_storage::to_json_string() const {
    return fmt::format("{{ \"port\": {}, \"use_https\": {}, \"aws_region\": \"{}\", \"iam_role_arn\": \"{}\" }}",
        config.port, config.use_https, config.region, config.role_arn
    );
}

std::string db::object_storage_endpoint_param::s3_storage::key() const {
    return endpoint;
}

bool db::object_storage_endpoint_param::is_s3_storage() const {
    return std::holds_alternative<s3_storage>(_data);
}

const db::object_storage_endpoint_param::s3_storage& db::object_storage_endpoint_param::get_s3_storage() const {
    return std::get<db::object_storage_endpoint_param::s3_storage>(_data);
}

std::strong_ordering db::object_storage_endpoint_param::operator<=>(const object_storage_endpoint_param&) const = default;
bool db::object_storage_endpoint_param::operator==(const object_storage_endpoint_param&) const = default;

std::string db::object_storage_endpoint_param::to_json_string() const {
    return std::visit([](auto& o) { return o.to_json_string(); }, _data);
}

std::string db::object_storage_endpoint_param::key() const {
    return std::visit([](auto& o) { return o.key(); }, _data);
}

db::object_storage_endpoint_param db::object_storage_endpoint_param::decode(const YAML::Node& node) {
    auto name = node["name"];
    auto aws_region = node["aws_region"];
    auto iam_role_arn = node["iam_role_arn"];
    auto type = node["type"];
    // aws s3 endpoint. 
    if (!type || type.as<std::string>() == "s3" || aws_region || iam_role_arn) {
        s3_storage ep;
        ep.endpoint = name.as<std::string>();
        ep.config.port = node["port"].as<unsigned>();
        ep.config.use_https = node["https"].as<bool>(false);
        ep.config.region = aws_region ? aws_region.as<std::string>() : std::getenv("AWS_DEFAULT_REGION");
        ep.config.role_arn = iam_role_arn ? iam_role_arn.as<std::string>() : "";

        return object_storage_endpoint_param{std::move(ep)};
    }
    // TODO: other types
    throw std::invalid_argument(fmt::format("Could not decode object_storage_endpoint_param: {}", boost::lexical_cast<std::string>(node)));
}

auto fmt::formatter<db::object_storage_endpoint_param>::format(const db::object_storage_endpoint_param& e, fmt::format_context& ctx) const
    -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "object_storage_endpoint_param{{}}", e.to_json_string());
}
