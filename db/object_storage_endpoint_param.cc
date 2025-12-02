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
#include "object_storage_endpoint_param.hh"

using namespace std::string_literals;

db::object_storage_endpoint_param::object_storage_endpoint_param(s3_storage s)
    : _data(std::move(s))
{}
db::object_storage_endpoint_param::object_storage_endpoint_param(gs_storage s)
    : _data(std::move(s))
{}

db::object_storage_endpoint_param::object_storage_endpoint_param() = default;
db::object_storage_endpoint_param::object_storage_endpoint_param(const object_storage_endpoint_param&) = default;

std::string db::object_storage_endpoint_param::s3_storage::to_json_string() const {
    return fmt::format("{{ \"type\": \"s3\", \"aws_region\": \"{}\", \"iam_role_arn\": \"{}\" }}",
        region, iam_role_arn
    );
}

std::string db::object_storage_endpoint_param::s3_storage::key() const {
    return endpoint;
}

std::string db::object_storage_endpoint_param::gs_storage::to_json_string() const {
    return fmt::format("{{ \"type\": \"gs\", \"credentials_file\": \"{}\" }}",
        credentials_file
    );
}

std::string db::object_storage_endpoint_param::gs_storage::key() const {
    return endpoint;
}

bool db::object_storage_endpoint_param::is_s3_storage() const {
    return std::holds_alternative<s3_storage>(_data);
}

bool db::object_storage_endpoint_param::is_gs_storage() const {
    return std::holds_alternative<gs_storage>(_data);
}

bool db::object_storage_endpoint_param::is_storage_of_type(std::string_view type) const {
    if (type == s3_type) {
        return is_s3_storage();
    }
    if (type == gs_type) {
        return is_gs_storage();
    }
    return false;
}

const db::object_storage_endpoint_param::s3_storage& db::object_storage_endpoint_param::get_s3_storage() const {
    return std::get<db::object_storage_endpoint_param::s3_storage>(_data);
}

const db::object_storage_endpoint_param::gs_storage& db::object_storage_endpoint_param::get_gs_storage() const {
    return std::get<db::object_storage_endpoint_param::gs_storage>(_data);
}

std::strong_ordering db::object_storage_endpoint_param::operator<=>(const object_storage_endpoint_param&) const = default;
bool db::object_storage_endpoint_param::operator==(const object_storage_endpoint_param&) const = default;

std::string db::object_storage_endpoint_param::to_json_string() const {
    return std::visit([](auto& o) { return o.to_json_string(); }, _data);
}

std::string db::object_storage_endpoint_param::key() const {
    return std::visit([](auto& o) { return o.key(); }, _data);
}

const std::string& db::object_storage_endpoint_param::type() const {
    if (is_s3_storage()) {
        return s3_type;
    } else if (is_gs_storage()) {
        return gs_type;
    }
    throw std::runtime_error("Should not reach");
}

db::object_storage_endpoint_param db::object_storage_endpoint_param::decode(const YAML::Node& node) {
    auto name = node["name"];
    auto type = node["type"];

    auto get_opt = [](auto& node, const std::string& key, auto def) {
        auto tmp = node[key];
        return tmp ? tmp.template as<std::decay_t<decltype(def)>>() : def;
    };
    // aws s3 endpoint. 
    if (!type || type.as<std::string>() == s3_type ) {
        s3_storage ep;
        ep.endpoint = name.as<std::string>();
        auto aws_region = node["aws_region"];
        ep.region = aws_region ? aws_region.as<std::string>() : std::getenv("AWS_DEFAULT_REGION");
        ep.iam_role_arn = get_opt(node, "iam_role_arn", ""s);

        if (maybe_legacy_endpoint_name(ep.endpoint)) {
            // Support legacy config for a while
            auto port = node["port"].as<unsigned>();
            auto use_https = node["https"].as<bool>(false);
            ep.endpoint = fmt::format("http{}://{}:{}", use_https ? "s" : "", ep.endpoint, port);
        }

        return object_storage_endpoint_param{std::move(ep)};
    }
    // GCS endpoint
    if (type.as<std::string>() == gs_type) {
        gs_storage ep;
        ep.endpoint = name.as<std::string>();
        ep.credentials_file = get_opt(node, "credentials_file", ""s);

        return object_storage_endpoint_param(std::move(ep));
    }
    // TODO: other types
    throw std::invalid_argument(fmt::format("Could not decode object_storage_endpoint_param: {}", boost::lexical_cast<std::string>(node)));
}

const std::string db::object_storage_endpoint_param::s3_type = "s3";
const std::string db::object_storage_endpoint_param::gs_type = "gs";

auto fmt::formatter<db::object_storage_endpoint_param>::format(const db::object_storage_endpoint_param& e, fmt::format_context& ctx) const
    -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "object_storage_endpoint_param{{}}", e.to_json_string());
}
