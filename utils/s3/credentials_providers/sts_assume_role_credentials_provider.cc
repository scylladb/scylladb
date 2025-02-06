/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "sts_assume_role_credentials_provider.hh"

#include "utils/UUID.hh"
#include "utils/http.hh"
#include "utils/s3/client.hh"
#include <rapidxml.h>
#include <seastar/core/coroutine.hh>
#include <seastar/http/request.hh>
#include <seastar/util/short_streams.hh>

namespace aws {

static logging::logger sts_logger("sts");

sts_assume_role_credentials_provider::sts_assume_role_credentials_provider(const std::string& _host, unsigned _port, bool _is_secured)
    : sts_host(_host), port(_port), is_secured(_is_secured) {
}

sts_assume_role_credentials_provider::sts_assume_role_credentials_provider(const std::string& _region, const std::string& _role_arn)
    : sts_host(seastar::format("sts.{}.amazonaws.com", _region)), role_arn(_role_arn) {
}

bool sts_assume_role_credentials_provider::is_time_to_refresh() const {
    return seastar::lowres_clock::now() >= creds.expires_at;
}

future<> sts_assume_role_credentials_provider::reload() {
    if (is_time_to_refresh() || !creds) {
        co_await update_credentials();
    }
}

future<> sts_assume_role_credentials_provider::update_credentials() {
    auto req = http::request::make("POST", sts_host, "/");
    // Just set this version
    // https://github.com/aws/aws-sdk-cpp/blob/8d68be52dcad85095753e069a4355e241f1edb1c/generated/src/aws-cpp-sdk-sts/source/model/AssumeRoleRequest.cpp#L143
    req.query_parameters["Version"] = "2011-06-15";
    req.query_parameters["DurationSeconds"] = format("{}", session_duration);
    req.query_parameters["Action"] = "AssumeRole";
    req.query_parameters["RoleSessionName"] = format("{}", utils::make_random_uuid());
    req.query_parameters["RoleArn"] = role_arn;
    auto factory = std::make_unique<utils::http::dns_connection_factory>(sts_host, port, is_secured, sts_logger);
    http::experimental::client http_client(std::move(factory), 1, http::experimental::client::retry_requests::yes);
    co_await http_client.make_request(
        std::move(req),
        [this](const http::reply&, input_stream<char>&& in) -> future<> {
            auto input = std::move(in);
            auto body = co_await util::read_entire_stream_contiguous(input);
            creds = parse_creds(body);
        },
        http::reply::status_type::ok);
}

s3::aws_credentials sts_assume_role_credentials_provider::parse_creds(sstring& body) {
    auto get_node_safe = []<typename T>(const T* node, const std::string_view node_name) {
        auto child = node->first_node(node_name.data());
        if (!child) {
            throw std::runtime_error(seastar::format("'{}' node is missing in AssumeRole response", node_name));
        }
        return child;
    };
    auto doc = std::make_unique<rapidxml::xml_document<>>();
    try {
        doc->parse<0>(body.data());
    } catch (const rapidxml::parse_error& e) {
        throw std::runtime_error("Cannot parse AssumeRole response");
    }

    const auto root = get_node_safe(doc.get(), "AssumeRoleResponse");
    const auto result = get_node_safe(root, "AssumeRoleResult");
    const auto assumed_role_user = get_node_safe(result, "AssumedRoleUser");
    sts_logger.info("Assuming role for ARN: {}, Assumed Role ID: {}",
                    assumed_role_user->first_node("Arn")->value(),
                    assumed_role_user->first_node("AssumedRoleId")->value());

    // Access the Credentials node
    const auto credentials = get_node_safe(result, "Credentials");
    return {.access_key_id = get_node_safe(credentials, "AccessKeyId")->value(),
            .secret_access_key = get_node_safe(credentials, "SecretAccessKey")->value(),
            .session_token = get_node_safe(credentials, "SessionToken")->value(),
            // Set the expiration to one minute earlier to ensure credentials are renewed slightly before they expire
            .expires_at = seastar::lowres_clock::now() + std::chrono::seconds(session_duration - 60)};
}

} // namespace aws
