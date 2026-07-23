/*
 * Copyright (C) 2025-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
 
#include <string>
#include <variant>
#include <filesystem>
#include <yaml-cpp/yaml.h>

#include <boost/lexical_cast.hpp>

#include "utils/s3/creds.hh"
#include "utils/http.hh"
#include "utils/rjson.hh"
#include "object_storage_endpoint_param.hh"

using namespace std::string_literals;

static auto format_url(std::string_view host, unsigned port, bool use_https) {
    return fmt::format("{}://{}:{}", use_https ? "https" : "http", host, port);
}

db::object_storage_endpoint_param::object_storage_endpoint_param(s3_storage s)
    : _data(std::move(s))
{}
db::object_storage_endpoint_param::object_storage_endpoint_param(std::string endpoint, s3::endpoint_config config)
    : object_storage_endpoint_param(s3_storage{format_url(endpoint, config.port, config.use_https), std::move(config.region), std::move(config.role_arn), true /* legacy_format */})
{}
db::object_storage_endpoint_param::object_storage_endpoint_param(gs_storage s)
    : _data(std::move(s))
{}
db::object_storage_endpoint_param::object_storage_endpoint_param(posix_storage s)
    : _data(std::move(s))
{}

db::object_storage_endpoint_param::object_storage_endpoint_param() = default;
db::object_storage_endpoint_param::object_storage_endpoint_param(const object_storage_endpoint_param&) = default;

std::string db::object_storage_endpoint_param::s3_storage::to_json_string() const {
    if (!legacy_format) {
        return fmt::format("{{ \"type\": \"s3\", \"aws_region\": \"{}\", \"iam_role_arn\": \"{}\" }}",
            region, iam_role_arn
        );
    }

    auto url = utils::http::parse_simple_url(endpoint);
    return fmt::format("{{ \"port\": {}, \"use_https\": {}, \"aws_region\": \"{}\", \"iam_role_arn\": \"{}\" }}",
        url.port, url.is_https(), region, iam_role_arn
    );
}

std::string db::object_storage_endpoint_param::s3_storage::key() const {
    // The `endpoint` is full URL all the time, so only return it as a key
    // if it wasn't configured "the old way". In the latter case, split the
    // URL and return its host part to mimic the old behavior.

    if (!legacy_format) {
        return endpoint;
    }

    auto url = utils::http::parse_simple_url(endpoint);
    return url.host;
}

std::string db::object_storage_endpoint_param::gs_storage::to_json_string() const {
    return fmt::format("{{ \"type\": \"gs\", \"credentials_file\": \"{}\" }}",
        credentials_file
    );
}

std::string db::object_storage_endpoint_param::gs_storage::key() const {
    return endpoint;
}

std::string db::object_storage_endpoint_param::posix_storage::to_json_string() const {
    // path.native() may contain characters that are valid in a POSIX path but
    // must be escaped in JSON (e.g. '"' or '\\'), so quote it via rjson rather
    // than embedding it verbatim.
    return fmt::format("{{ \"type\": \"posix\", \"path\": {} }}",
        rjson::quote_json_string(sstring(path.native()))
    );
}

std::string db::object_storage_endpoint_param::posix_storage::key() const {
    // The path is normalized when the endpoint is decoded, so different
    // spellings of the same location (trailing slash, "."/"..") already
    // resolve to the same string and can be used directly as a key.
    return path.native();
}

bool db::object_storage_endpoint_param::is_s3_storage() const {
    return std::holds_alternative<s3_storage>(_data);
}

bool db::object_storage_endpoint_param::is_gs_storage() const {
    return std::holds_alternative<gs_storage>(_data);
}

bool db::object_storage_endpoint_param::is_posix_storage() const {
    return std::holds_alternative<posix_storage>(_data);
}

bool db::object_storage_endpoint_param::is_storage_of_type(std::string_view type) const {
    if (type == s3_type) {
        return is_s3_storage();
    }
    if (type == gs_type) {
        return is_gs_storage();
    }
    if (type == posix_type) {
        return is_posix_storage();
    }
    return false;
}

const db::object_storage_endpoint_param::s3_storage& db::object_storage_endpoint_param::get_s3_storage() const {
    return std::get<db::object_storage_endpoint_param::s3_storage>(_data);
}

const db::object_storage_endpoint_param::gs_storage& db::object_storage_endpoint_param::get_gs_storage() const {
    return std::get<db::object_storage_endpoint_param::gs_storage>(_data);
}

const db::object_storage_endpoint_param::posix_storage& db::object_storage_endpoint_param::get_posix_storage() const {
    return std::get<db::object_storage_endpoint_param::posix_storage>(_data);
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
    } else if (is_posix_storage()) {
        return posix_type;
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
    if (!type || type.as<std::string>() == s3_type) {
        s3_storage ep;
        auto endpoint = name.as<std::string>();
        ep.legacy_format = (!endpoint.starts_with("http://") && !endpoint.starts_with("https://"));

        if (!ep.legacy_format) {
            ep.endpoint = std::move(endpoint);
        } else {
            ep.endpoint = format_url(endpoint, node["port"].as<unsigned>(), node["https"].as<bool>(false));
        }

        auto aws_region = node["aws_region"];
        ep.region = aws_region ? aws_region.as<std::string>() : std::getenv("AWS_DEFAULT_REGION");
        ep.iam_role_arn = get_opt(node, "iam_role_arn", ""s);

        return object_storage_endpoint_param{std::move(ep)};
    }
    // GCS endpoint
    if (type.as<std::string>() == gs_type) {
        gs_storage ep;
        ep.endpoint = name.as<std::string>();
        ep.credentials_file = get_opt(node, "credentials_file", ""s);

        return object_storage_endpoint_param(std::move(ep));
    }
    // locally-mounted POSIX path endpoint
    if (type.as<std::string>() == posix_type) {
        std::filesystem::path path = name.as<std::string>();
        if (!path.is_absolute()) {
            throw std::invalid_argument(fmt::format(
                "posix object storage endpoint requires an absolute path, got '{}'", path.native()));
        }
        // Normalize the path lexically (collapsing redundant separators,
        // trailing slashes and "."/".." components) so that different spellings
        // of the same location map to one endpoint and key(). Unlike
        // canonical() this does not touch the filesystem: symlinks are not
        // resolved and the path need not exist. Whether the mount actually
        // exists is validated later, when the object storage client is created.
        auto normalized = path.lexically_normal();
        // lexically_normal() keeps a trailing separator for inputs like
        // "/mnt/backup/" or "/mnt/backup/."; drop it so those map to the same
        // key as "/mnt/backup".
        if (!normalized.has_filename()) {
            normalized = normalized.parent_path();
        }
        return object_storage_endpoint_param(posix_storage{std::move(normalized)});
    }
    // TODO: other types
    throw std::invalid_argument(fmt::format("Could not decode object_storage_endpoint_param: {}", boost::lexical_cast<std::string>(node)));
}

const std::string db::object_storage_endpoint_param::s3_type = "s3";
const std::string db::object_storage_endpoint_param::gs_type = "gs";
const std::string db::object_storage_endpoint_param::posix_type = "posix";

auto fmt::formatter<db::object_storage_endpoint_param>::format(const db::object_storage_endpoint_param& e, fmt::format_context& ctx) const
    -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "object_storage_endpoint_param{}", e.to_json_string());
}
