/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "instance_profile_credentials_provider.hh"
#include "utils/http.hh"
#include "utils/s3/client.hh"
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <seastar/http/request.hh>
#include <seastar/util/short_streams.hh>


namespace aws {

static logging::logger ec2_md_logger("ec2_metadata");

instance_profile_credentials_provider::instance_profile_credentials_provider(const std::string& _host, unsigned _port) : ec2_metadata_ip(_host), port(_port) {
}

bool instance_profile_credentials_provider::is_time_to_refresh() const {
    return seastar::lowres_clock::now() >= creds.expires_at;
}

future<> instance_profile_credentials_provider::reload() {
    if (is_time_to_refresh() || !creds) {
        co_await update_credentials();
    }
}

future<> instance_profile_credentials_provider::update_credentials() {
    auto factory = std::make_unique<utils::http::dns_connection_factory>(ec2_metadata_ip, port, false, ec2_md_logger);
    http::experimental::client http_client(std::move(factory), 1, http::experimental::client::retry_requests::yes);

    auto req = http::request::make("PUT", ec2_metadata_ip, "/latest/api/token");
    req._headers["x-aws-ec2-metadata-token-ttl-seconds"] = format("{}", session_duration);

    std::string token;
    co_await http_client.make_request(
        std::move(req),
        [&token](const http::reply&, input_stream<char>&& in) -> future<> {
            auto input = std::move(in);
            token = co_await util::read_entire_stream_contiguous(input);
        },
        http::reply::status_type::ok);

    std::string role;
    req = http::request::make("GET", ec2_metadata_ip, "/latest/meta-data/iam/security-credentials/");
    req._headers["x-aws-ec2-metadata-token"] = token;
    co_await http_client.make_request(
        std::move(req),
        [&role](const http::reply&, input_stream<char>&& in) -> future<> {
            auto input = std::move(in);
            role = co_await util::read_entire_stream_contiguous(input);
        },
        http::reply::status_type::ok);

    req = http::request::make("GET", ec2_metadata_ip, seastar::format("/latest/meta-data/iam/security-credentials/{}", role));
    req._headers["x-aws-ec2-metadata-token"] = token;
    co_await http_client.make_request(
        std::move(req),
        [this](const http::reply&, input_stream<char>&& in) -> future<> {
            auto input = std::move(in);
            auto creds_response = co_await util::read_entire_stream_contiguous(input);
            creds = parse_creds(creds_response);
        },
        http::reply::status_type::ok);
    ec2_md_logger.info("Retrieved temporary credentials for IAM role: {}", role);
}

s3::aws_credentials instance_profile_credentials_provider::parse_creds(const sstring& creds_response) {
    rapidjson::Document document;
    document.Parse(creds_response.data());

    if (document.HasParseError()) {
        throw std::runtime_error(
            format("Failed to parse EC2 Metadata credentials. Reason: {} (offset: {})", GetParseError_En(document.GetParseError()), document.GetErrorOffset()));
    }

    // Retrieve credentials
    return {.access_key_id = document["AccessKeyId"].GetString(),
            .secret_access_key = document["SecretAccessKey"].GetString(),
            .session_token = document["Token"].GetString(),
            // Set the expiration to one minute earlier to ensure credentials are renewed slightly before they expire
            .expires_at = seastar::lowres_clock::now() + std::chrono::seconds(session_duration - 60)};
}

} // namespace aws
