/*
 * Copyright (C) 2025 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#define CPP_JWT_USE_VENDORED_NLOHMANN_JSON
#include <jwt/jwt.hpp>

#include <seastar/net/dns.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/http/client.hh>
#include <seastar/http/request.hh>
#include <seastar/http/connection_factory.hh>
#include <seastar/util/file.hh>
#include <seastar/util/short_streams.hh>

#include "db/config.hh"
#include "types/types.hh"
#include "utils/base64.hh"
#include "utils/hashers.hh"
#include "request.hh"
#include "exceptions.hh"
#include "service_principal_credentials.hh"

namespace azure {

static constexpr char REDACTED_VALUE[] = "[REDACTED]";

static body_filter make_request_filter() {
    return [](std::string_view body) -> std::optional<std::string> {
        for (const auto& param : {"client_secret=", "client_assertion="}) {
            size_t start_pos = body.find(param);
            if (start_pos == sstring::npos) {
                continue;
            }
            size_t value_start_pos = start_pos + sstring(param).length();
            size_t end_pos = body.find('&', value_start_pos);
            if (end_pos == sstring::npos) {
                end_pos = body.length();
            }
            return fmt::format("{}{}{}",
                    body.substr(0, start_pos + sstring(param).length()),
                    REDACTED_VALUE,
                    body.substr(end_pos));
        }
        return std::nullopt;
    };
}

static body_filter make_response_filter() {
    return [](std::string_view body) -> std::optional<std::string> {
        if (!body.empty()) {
            auto j = rjson::parse(body);
            auto val = rjson::find(j, "access_token");
            if (val) {
                val->SetString(REDACTED_VALUE);
                return rjson::print(j);
            }
        }
        return std::nullopt;
    };
}

// A very simple tls connection factory.
//
// Differences from `seastar::http::experimental::tls_connection_factory`:
// 1. Accepts external certificate credentials.
// 2. Does not support non-TLS connections.
// 3. Does not wait after TLS close_notify alerts (necessary for Azure Entra).
class tls_connection_factory : public http::experimental::connection_factory {
    static constexpr bool TLS_NOWAIT_ON_CLOSE = false;
    socket_address _addr;
    shared_ptr<tls::certificate_credentials> _creds;
    sstring _host;
public:
    tls_connection_factory(socket_address addr, shared_ptr<tls::certificate_credentials> creds, sstring host)
        : _addr(std::move(addr))
        , _creds(std::move(creds))
        , _host(std::move(host))
    {}
    future<connected_socket> make(abort_source* as) override {
        co_return co_await tls::connect(_creds, _addr, tls::tls_options{ .wait_for_eof_on_shutdown = TLS_NOWAIT_ON_CLOSE, .server_name = _host});
     }
 };

service_principal_credentials::service_principal_credentials(const sstring& tenant_id,
        const sstring& client_id, const sstring& client_secret, const sstring& client_cert,
        const sstring& truststore, const sstring& priority_string, const sstring& logctx)
    : credentials(logctx)
    , _tenant_id(tenant_id)
    , _client_id(client_id)
    , _client_secret(client_secret)
    , _client_cert(client_cert)
    , _truststore(truststore)
    , _priority_string(priority_string)
{}

static future<::shared_ptr<tls::certificate_credentials>> make_creds(const sstring& truststore, const sstring& priority_string) {
    auto creds = seastar::make_shared<tls::certificate_credentials>();
    if (!priority_string.empty()) {
        creds->set_priority_string(priority_string);
    } else {
        creds->set_priority_string(db::config::default_tls_priority);
    }
    if (!truststore.empty()) {
        co_await creds->set_x509_trust_file(truststore, seastar::tls::x509_crt_format::PEM);
    } else {
        co_await creds->set_system_trust();
    }
    co_return creds;
}

future<sstring> service_principal_credentials::post(const sstring& body) {
    const auto op = httpd::operation_type::POST;
    const auto host = AZURE_ENTRA_ID_HOST;
    const auto port = 443;
    const auto path = seastar::format(AZURE_ENTRA_ID_TOKEN_PATH_TEMPLATE, _tenant_id);
    const auto mime_type = MIME_TYPE;

    auto req = http::request::make(op, host, path);
    req._version = "1.1";
    req.write_body("", std::move(body));
    req.set_mime_type(mime_type);

    if (az_creds_logger.is_enabled(log_level::trace)) {
        log_trace("Sending request: {}", format_request(req, make_request_filter()));
    }

    auto addr = co_await net::dns::resolve_name(host, net::inet_address::family::INET);
    auto factory = std::make_unique<azure::tls_connection_factory>(socket_address(addr, port), co_await make_creds(_truststore, _priority_string), host);
    http::experimental::client http_client{std::move(factory)};

    sstring resp;
    co_await http_client.make_request(std::move(req), [&](const http::reply& rep, input_stream<char>&& in) -> future<> {
        auto lin = std::move(in);
        resp = co_await util::read_entire_stream_contiguous(lin);
        if (rep._status == http::reply::status_type::ok) {
            if (az_creds_logger.is_enabled(log_level::trace)) {
                log_trace("Got response: {}", format_reply(rep, resp, make_response_filter()));
            }
        } else {
            if (az_creds_logger.is_enabled(log_level::trace)) {
                log_trace("Got unexpected response: {}", format_reply(rep, resp));
            }
            throw creds_auth_error::make_error(rep._status, resp);
        }
    }).finally([&] -> future<> { co_await http_client.close(); });
    co_return resp;
}

access_token service_principal_credentials::make_token(const rjson::value& json, const resource_type& resource_uri) {
    auto token = rjson::get<std::string>(json, "access_token");
    auto expires_in = timeout_clock::now() + std::chrono::seconds(rjson::get<int>(json, "expires_in"));
    return { token, expires_in, resource_uri };
}

future<> service_principal_credentials::refresh(const resource_type& resource_uri) {
    log_debug("Refreshing token for principal: {}", _client_id);
    if (_client_secret != "") {
        co_await refresh_with_secret(resource_uri);
    } else {
        co_await refresh_with_certificate(resource_uri);
    }
}

// Token request with secret.
// Based on: https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-client-creds-grant-flow#first-case-access-token-request-with-a-shared-secret
future<> service_principal_credentials::refresh_with_secret(const resource_type& resource_uri) {
    // Scopes for the client credentials flow must contain only one resource
    // identifier and only the .default scope.
    auto scope = seastar::format("{}/.default", resource_uri);
    sstring grant_type = "client_credentials";
    sstring body = seastar::format(
            "client_id={}&scope={}&client_secret={}&grant_type={}",
            _client_id,
            seastar::http::internal::url_encode(scope),
            seastar::http::internal::url_encode(_client_secret),
            grant_type);
    auto resp = co_await post(body);
    token = make_token(rjson::parse(resp), resource_uri);
}

static future<std::string> read_pem_artifact(const sstring& key_file, const sstring& header) {
    auto contents = co_await util::read_entire_file_contiguous(std::filesystem::path(key_file.c_str()));
    std::istringstream stream(contents);
    std::string line;
    std::string key_data;
    bool in_key = false;
    while (std::getline(stream, line)) {
        if (line.find(header) != std::string::npos) {
            in_key = !in_key;
            key_data += line + "\n";
        } else if (in_key) {
            key_data += line + "\n";
        }
    }
    co_return key_data;
}

// Argument is expected to be a PEM encoded certificate.
// Thumbprint computed as the base64url-encoded SHA-256 hash of the certificate's DER encoding.
// https://learn.microsoft.com/en-us/entra/identity-platform/certificate-credentials#header
// https://datatracker.ietf.org/doc/html/rfc7517?section-4.9
std::string compute_thumbprint(const std::string& pem_cert) {
    BIO* bio = BIO_new_mem_buf(pem_cert.data(), pem_cert.size());
    if (!bio) {
        on_internal_error(az_creds_logger, "Error creating BIO object");
    }

    X509* cert = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr);
    BIO_free(bio);
    if (!cert) {
        on_internal_error(az_creds_logger, "Error reading certificate from memory");
    }

    // Convert to DER format
    unsigned char* der = nullptr;
    int der_len = i2d_X509(cert, &der);
    X509_free(cert);
    if (der_len < 0) {
        on_internal_error(az_creds_logger, "Error converting certificate to DER");
    }

    std::string thumbprint(reinterpret_cast<char*>(der), der_len);
    bytes sha = [&thumbprint] {
        sha256_hasher hasher;
        hasher.update(thumbprint.data(), thumbprint.size());
        return hasher.finalize();
    }();
    return base64url_encode(sha);
}

// Token request with certificate.
// Based on: https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-client-creds-grant-flow#second-case-access-token-request-with-a-certificate
future<> service_principal_credentials::refresh_with_certificate(const resource_type& resource_uri) {
    // Scopes for the client credentials flow must contain only one resource
    // identifier and only the .default scope.
    auto scope = seastar::format("{}/.default", resource_uri);
    sstring grant_type = "client_credentials";
    sstring client_assertion_type = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";
    std::string private_key;
    try {
        private_key = co_await read_pem_artifact(_client_cert, PEM_STRING_PKCS8INF);
    } catch (const std::exception& e) {
        throw std::runtime_error(seastar::format("Failed to read private key from certificate file {}: {}", _client_cert, e.what()));
    }
    if (private_key.empty()) {
        throw std::invalid_argument(seastar::format("Private key not found in certificate file {}", _client_cert));
    }
    std::string cert = co_await read_pem_artifact(_client_cert, PEM_STRING_X509);
    if (cert.empty()) {
        throw std::invalid_argument(seastar::format("Certificate not found in certificate file {}", _client_cert));
    }
    auto alg = "RS256"; // docs suggest PS256, but it's not supported by jwt-cpp
    auto thumbprint = compute_thumbprint(cert);

    using namespace jwt::params;
    jwt::jwt_object obj{algorithm(alg), secret(private_key), headers({{"x5t#S256", thumbprint }})};

    const auto host = AZURE_ENTRA_ID_HOST;
    const auto path = seastar::format(AZURE_ENTRA_ID_TOKEN_PATH_TEMPLATE, _tenant_id);
    auto uri = seastar::format("https://{}{}", host, path);
    using jwt_id = utils::tagged_uuid<struct jwt_id_tag>;
    obj.add_claim("aud", uri)
        .add_claim("exp", timeout_clock::now() + std::chrono::minutes(10))
        .add_claim("iss", _client_id)
        .add_claim("jti", jwt_id::create_random_id().to_sstring())
        .add_claim("nbf", timeout_clock::now())
        .add_claim("sub", _client_id)
        .add_claim("iat", timeout_clock::now())
    ;
    auto sign = obj.signature();
    sstring body = seastar::format(
            "client_id={}&scope={}&client_assertion_type={}&client_assertion={}&grant_type={}",
            _client_id,
            seastar::http::internal::url_encode(scope),
            seastar::http::internal::url_encode(client_assertion_type),
            sign,
            grant_type);
    auto resp = co_await post(body);
    token = make_token(rjson::parse(resp), resource_uri);
}

}