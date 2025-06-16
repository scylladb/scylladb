/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */


#include "gcp_credentials.hh"

#include <fmt/chrono.h>
#include <fmt/ranges.h>
#include <fmt/std.h>

#include <seastar/core/reactor.hh>
#include <seastar/core/fstream.hh>
#include <seastar/util/log.hh>

#define CPP_JWT_USE_VENDORED_NLOHMANN_JSON
#include <jwt/jwt.hpp>

#include "utils/overloaded_functor.hh"
#include "utils/to_string.hh"
#include "utils/rest/client.hh"

static logger gcp_cred_log("gcp_credentials");

static const char CREDENTIAL_ENV_VAR[] = "GOOGLE_APPLICATION_CREDENTIALS";
static const char WELL_KNOWN_CREDENTIALS_FILE[] = "application_default_credentials.json";
static const char CLOUDSDK_CONFIG_DIRECTORY[] = "gcloud";

static const char USER_FILE_TYPE[] = "authorized_user";
static const char SERVICE_ACCOUNT_FILE_TYPE[] = "service_account";
static const char IMPERSONATED_SERVICE_ACCOUNT_FILE_TYPE[] = "impersonated_service_account";

static const char GCE_METADATA_HOST_ENV_VAR[] = "GCE_METADATA_HOST";

static const char DEFAULT_METADATA_SERVER_URL[] = "http://metadata.google.internal";

static const char METADATA_FLAVOR[] = "Metadata-Flavor";
static const char GOOGLE[] = "Google";

static const char TOKEN_SERVER_URI[] = "https://oauth2.googleapis.com/token";

const char utils::gcp::AUTHORIZATION[] = "Authorization";

static const char CLOUD_PLATFORM_SCOPE[] = "https://www.googleapis.com/auth/cloud-platform";

//static const char[] CLOUD_SHELL_ENV_VAR = "DEVSHELL_CLIENT_PORT";
//static const char[] SKIP_APP_ENGINE_ENV_VAR = "GOOGLE_APPLICATION_CREDENTIALS_SKIP_APP_ENGINE";
//static const char[] NO_GCE_CHECK_ENV_VAR = "NO_GCE_CHECK";
//static const char[] GCE_METADATA_HOST_ENV_VAR = "GCE_METADATA_HOST";

utils::gcp::access_token::access_token(const rjson::value& json)
    : token(rjson::get<std::string>(json, "access_token"))
    , expiry(timeout_clock::now() + std::chrono::seconds(rjson::get<int>(json, "expires_in")))
    , scopes(rjson::get_opt<std::string>(json, "scope").value_or(""))
{}

bool utils::gcp::access_token::empty() const {
    return token.empty();
}

bool utils::gcp::access_token::expired() const {
    if (empty()) {
        return true;
    }
    return timeout_clock::now() >= this->expiry;
}

utils::gcp::user_credentials::user_credentials(const rjson::value& v) 
    : client_id(rjson::get<std::string>(v, "client_id"))
    , client_secret(rjson::get<std::string>(v, "client_secret"))
    , refresh_token(rjson::get<std::string>(v, "refresh_token"))
    , quota_project_id(rjson::get_opt<std::string>(v, "refresh_token").value_or(""))
{}

utils::gcp::service_account_credentials::service_account_credentials(const rjson::value& v)
    : client_id(rjson::get<std::string>(v, "client_id"))
    , client_email(rjson::get<std::string>(v, "client_email"))
    , private_key_id(rjson::get<std::string>(v, "private_key_id"))
    , private_key_pkcs8(rjson::get<std::string>(v, "private_key"))
    , token_server_uri([&] {
        auto token_uri = rjson::get_opt<std::string>(v, "token_uri");
        if (token_uri) {
            // TODO: verify uri
            return *token_uri;
        }
        return std::string{};
    }())
    , project_id(rjson::get_opt<std::string>(v, "project_id").value_or(""))
    , quota_project_id(rjson::get_opt<std::string>(v, "refresh_token").value_or(""))
{}

utils::gcp::impersonated_service_account_credentials::impersonated_service_account_credentials(std::string principal, google_credentials&& c)
    : target_principal(std::move(principal))
    , source_credentials(std::make_unique<google_credentials>(std::move(c))) 
{}

utils::gcp::impersonated_service_account_credentials::impersonated_service_account_credentials(const rjson::value& v)
    : delegates([&] {
        std::vector<std::string> res;
        auto tmp = rjson::find(v, "delegates");
        if (tmp) {
            if (!tmp->IsArray()) {
                throw bad_configuration("Malformed json");
            }

            for (const auto& d : tmp->GetArray()) {
                res.emplace_back(std::string(rjson::to_string_view(d)));
            }
        }
        return res;
    }())
    , quota_project_id(rjson::get_opt<std::string>(v, "quota_project_id").value_or(""))
    , target_principal([&] {
        auto url = rjson::get<std::string>(v, "service_account_impersonation_url");

        auto si = url.find_last_of('/');
        auto ei = url.find(":generateAccessToken");

        if (si != std::string::npos && ei != std::string::npos && si < ei) {
            return url.substr(si + 1, ei - si - 1);
        }
        throw bad_configuration( "Unable to determine target principal from service account impersonation URL.");
    }())
    , source_credentials([&]() -> decltype(source_credentials) {
        auto& scjson = rjson::get(v, "source_credentials");
        auto type = rjson::get<std::string>(scjson, "type");

        if (type == USER_FILE_TYPE) {
            return std::make_unique<google_credentials>(user_credentials(scjson));
        } else if (type == SERVICE_ACCOUNT_FILE_TYPE) {
            return std::make_unique<google_credentials>(service_account_credentials(scjson));
        }
        throw bad_configuration(fmt::format("A credential of type {} is not supported as source credential for impersonation.", type));
    }())
{}

// TODO: copied code. Place somewhere sharable, and name better (?)
static future<temporary_buffer<char>> read_text_file_fully(const std::string& filename) {
    return open_file_dma(filename, open_flags::ro).then([](file f) {
        return f.size().then([f](size_t s) {
            return do_with(make_file_input_stream(f), [s](input_stream<char>& in) {
                return in.read_exactly(s).then([](temporary_buffer<char> buf) {
                    return make_ready_future<temporary_buffer<char>>(std::move(buf));
                }).finally([&in] {
                    return in.close();
                });
            });
        });
    });
}

future<utils::gcp::google_credentials> utils::gcp::google_credentials::from_file(const std::string& path) {
    auto buf = co_await read_text_file_fully(path);
    co_return from_data(std::string_view(buf.get(), buf.size()));
}

utils::gcp::google_credentials
utils::gcp::google_credentials::from_data(std::string_view content) {
    auto json = rjson::parse(content);
    auto type = rjson::get_opt<std::string>(json, "type");

    if (!type) {
        throw bad_configuration("Error reading credentials from stream, 'type' field not specified.");
    }
    if (type == USER_FILE_TYPE) {
        return google_credentials(user_credentials(json));
    }
    if (type == SERVICE_ACCOUNT_FILE_TYPE) {
        return google_credentials(service_account_credentials(json));
    }
    if (type == IMPERSONATED_SERVICE_ACCOUNT_FILE_TYPE) {
        return google_credentials(impersonated_service_account_credentials(json));
    }
    throw bad_configuration(fmt::format(
        "Error reading credentials from stream, 'type' value '{}' not recognized. Expecting '{}', '{}' or '{}'."
        , type, USER_FILE_TYPE, SERVICE_ACCOUNT_FILE_TYPE, IMPERSONATED_SERVICE_ACCOUNT_FILE_TYPE));
}

static std::string get_metadata_server_url() {
    auto meta_host = std::getenv(GCE_METADATA_HOST_ENV_VAR);
    auto token_uri = meta_host ? std::string("http://") + meta_host : DEFAULT_METADATA_SERVER_URL;
    return token_uri;
}

future<utils::gcp::google_credentials>
utils::gcp::google_credentials::get_default_credentials() {
    auto credentials_path = std::getenv(CREDENTIAL_ENV_VAR);

    if (credentials_path != nullptr && strlen(credentials_path)) {
        gcp_cred_log.debug("Attempting to load credentials from file: {}", credentials_path);

        try {
            co_return co_await from_file(credentials_path);
        } catch (...) {
            std::throw_with_nested(bad_configuration(fmt::format(
                "Error reading credential file from environment variable {}, value '{}'"
                , CREDENTIAL_ENV_VAR
                , credentials_path
                ))
            );
        }
    }

    {
        auto home = std::getenv("HOME");
        if (home) {
            std::string well_known_file;
            auto env_path = std::getenv("CLOUDSDK_CONFIG");
            if (env_path) {
                well_known_file = fmt::format("{}/{}/{}", home, env_path, WELL_KNOWN_CREDENTIALS_FILE);
            } else {
                well_known_file = fmt::format("{}/.config/{}/{}", home, CLOUDSDK_CONFIG_DIRECTORY, WELL_KNOWN_CREDENTIALS_FILE);
            }

            if (co_await seastar::file_exists(well_known_file)) {
                gcp_cred_log.debug("Attempting to load credentials from well known file: {}", well_known_file);
                try {
                    co_return co_await from_file(well_known_file);
                } catch (...) {
                    std::throw_with_nested(bad_configuration(fmt::format(
                        "Error reading credential file from location {}"
                        , well_known_file
                        ))
                    );
                }
            }
        }
    }

    {
        // Then try Compute Engine and GAE 8 standard environment
        gcp_cred_log.debug("Attempting to load credentials from GCE");

        auto is_on_gce = []() -> future<bool> {
            static bool checked_is_on_gce = false;
            static bool is_on_gce = false;


            if (checked_is_on_gce) {
                co_return is_on_gce;
            }

            auto token_uri = get_metadata_server_url();

            for (int i = 1; i <= 3; ++i) {
                try {
                    rest::key_value headers[] = {
                        { METADATA_FLAVOR, GOOGLE },
                    };
                    co_await rest::send_request(token_uri, {}, std::string{}, "", [&](const rest::httpclient::reply_type& rep, std::string_view) {
                        checked_is_on_gce = true;
                        is_on_gce = rep.get_header(METADATA_FLAVOR) == GOOGLE;
                    }, httpd::operation_type::GET, headers);
                    if (checked_is_on_gce) {
                        co_return is_on_gce;;
                    }
                } catch (...) {
                    // TODO: handle timeout
                    break;
                }
            }

            auto linux_path = "/sys/class/dmi/id/product_name";
            if (co_await seastar::file_exists(linux_path)) {
                auto f = file_desc::open(linux_path, O_RDONLY | O_CLOEXEC);
                char buf[128] = {};
                f.read(buf, 128);
                is_on_gce = std::string_view(buf).find(GOOGLE) == 0;
            }

            checked_is_on_gce = true;
            co_return is_on_gce;
        };

        if (co_await is_on_gce()) {
            co_return compute_engine_credentials{};
        }
    }

    throw bad_configuration("Could not determine initial credentials");
}

template<typename Func>
static void for_each_scope(const utils::gcp::scopes_type& s, Func&& f) {
    size_t i = 0;
    while(i < s.size()) {
        auto j = s.find(' ', i + 1);
        f(s.substr(i, j - i));
        i = j;
    }
}

using namespace utils::gcp;
using namespace rest;

static std::string body(key_values kv) {
    std::ostringstream ss;
    std::string_view sep = "";
    for (auto& [k, v] : kv) {
        ss << sep << k << "=" << http::internal::url_encode(v);
        sep = "&";
    }
    return ss.str();
}

static future<utils::gcp::access_token> 
get_access_token(const google_credentials& creds, const scopes_type& scope, shared_ptr<tls::certificate_credentials> certs) {
    co_return co_await std::visit(overloaded_functor {
        [&](const user_credentials& c) -> future<access_token> {
            assert(!c.refresh_token.empty());
            auto json = co_await send_request(TOKEN_SERVER_URI, certs, body(key_values({
                { "client_id", c.client_id },
                { "client_secret", c.client_secret },
                { "refresh_token", c.refresh_token },
                { "grant_type", "refresh_token" },
            })), "", httpd::operation_type::POST);

            co_return access_token{ json };
        },
        [&](const service_account_credentials& c) -> future<access_token> {
            using namespace jwt::params;

            jwt::jwt_object obj{algorithm("RS256"), secret(c.private_key_pkcs8), headers({{"kid", c.private_key_id }})};

            auto uri = c.token_server_uri.empty() ? TOKEN_SERVER_URI : c.token_server_uri;
            obj.add_claim("iss", c.client_email)
                .add_claim("iat", timeout_clock::now())
                .add_claim("exp", timeout_clock::now() + std::chrono::seconds(3600))
                .add_claim("scope", scope)
                .add_claim("aud", uri)
            ;
            auto sign = obj.signature();

            auto json = co_await send_request(uri, certs, body(key_values({
                { "grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer" },
                { "assertion", sign }
            })), "", httpd::operation_type::POST);
            co_return access_token{ json };
        },
        [&](const impersonated_service_account_credentials& c) -> future<access_token> {
            auto json_body = rjson::empty_object();
            auto scopes = rjson::empty_array();
            for_each_scope(scope, [&](std::string s) {
                rjson::push_back(scopes, rjson::from_string(s));
            });

            rjson::add(json_body, "scope", std::move(scopes));

            if (!c.delegates.empty()) {
                auto delegates = rjson::empty_array();
                for (auto& d : c.delegates) {
                    rjson::push_back(delegates, rjson::from_string(d));
                }
                rjson::add(json_body, "delegates", std::move(delegates));
            }

            rjson::add(json_body, "lifetime", "3600s");

            co_await c.source_credentials->refresh(CLOUD_PLATFORM_SCOPE, certs);

            auto endpoint = c.iam_endpoint_override.empty()
                ? fmt::format("https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{}:generateAccessToken", c.target_principal)
                : c.iam_endpoint_override
                ;
            auto json = co_await send_request(endpoint, certs, json_body, httpd::operation_type::POST, key_values({
                { AUTHORIZATION, fmt::format("Bearer {}", c.source_credentials->token.token) },
            }));

            struct tm tmp;
            ::strptime(rjson::get<std::string>(json, "expireTime").data(), "%FT%TZ", &tmp);

            access_token a;

            a.expiry = timeout_clock::from_time_t(::mktime(&tmp));
            a.scopes = scope;
            a.token = rjson::get<std::string>(json, "accessToken");

            co_return a;
        },
        [&](const compute_engine_credentials& c) -> future<access_token> {
            auto meta_uri = get_metadata_server_url();
            auto token_uri = meta_uri + "/computeMetadata/v1/instance/service-accounts/default/token";
            try {
                 auto json = co_await send_request(token_uri, certs, std::string{}, "", httpd::operation_type::GET, key_values({ { METADATA_FLAVOR, GOOGLE } }));
                 co_return access_token{ json };
            } catch (...) {
                std::throw_with_nested(bad_configuration("Unexpected error code trying to get security access token from Compute Engine metadata for the default service account"));
            }
        }
    }, creds.credentials);
}

bool utils::gcp::default_scopes_implies_other_scope(const scopes_type& scopes, const scopes_type& check_for) {
    if (scopes.find(' ') == std::string::npos && check_for.find(' ') == std::string::npos) {
        return scopes == check_for;
    }
    for (const auto word : std::views::split(check_for, " ")) {
        if (!scopes_contains_scope(scopes, std::string_view(word))) {
            return false;
        }
    }
    return true;
}

bool utils::gcp::scopes_contains_scope(const scopes_type& scopes, std::string_view scope) {
    for (const auto word : std::views::split(scope, " ")) {
        if (std::string_view(word) == scope) {
            return true;
        }
    }
    return false;
}

future<> utils::gcp::google_credentials::refresh(const scopes_type& scopes, scope_implies_other_scope_pred pred, shared_ptr<tls::certificate_credentials> certs) {
    if (!token.expired() && pred(token.scopes, scopes)) {
        co_return;
    }
    token = co_await get_access_token(*this, scopes, std::move(certs));
}

future<> utils::gcp::google_credentials::refresh(const scopes_type& scopes, shared_ptr<tls::certificate_credentials> certs) {
    co_await refresh(scopes, &default_scopes_implies_other_scope, std::move(certs));
}

utils::gcp::bad_configuration::bad_configuration(const std::string& msg)
    : std::runtime_error(msg)
{}

std::string utils::gcp::format_bearer(const access_token& token) {
    return fmt::format("Bearer {}", token.token);
}

const rest::http_log_filter& utils::gcp::bearer_filter() {
    class bearer_filter : public rest::nop_log_filter {
        string_opt filter_header(std::string_view name, std::string_view value) const override { 
            if (name == AUTHORIZATION && value.starts_with("Bearer ")) {
                std::string res = std::string(value.substr(9)) + REDACTED_VALUE + std::string(value.substr(value.size() - 2));
                return res;
            }
            return std::nullopt; 
        }
    };

    static const bearer_filter filter;

    return filter;
}
