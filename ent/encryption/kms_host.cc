/*
 * Copyright (C) 2022 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#include <deque>
#include <unordered_map>
#include <regex>
#include <algorithm>

#include <seastar/net/dns.hh>
#include <seastar/net/api.hh>
#include <seastar/net/tls.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/reactor.hh>
#include <seastar/json/formatter.hh>
#include <seastar/http/url.hh>

#include <boost/beast/http.hpp>
#include <rapidxml.h>

#include <fmt/chrono.h>
#include <fmt/ranges.h>
#include <fmt/std.h>
#include "utils/to_string.hh"

#include "kms_host.hh"
#include "encryption.hh"
#include "encryption_exceptions.hh"
#include "symmetric_key.hh"
#include "utils/hash.hh"
#include "utils/loading_cache.hh"
#include "utils/UUID.hh"
#include "utils/UUID_gen.hh"
#include "utils/rjson.hh"
#include "marshal_exception.hh"
#include "db/config.hh"

template <bool B, typename T> struct fmt::formatter<boost::beast::http::message<B, T>> : fmt::ostream_formatter {};
template <> struct fmt::formatter<boost::beast::http::status> : fmt::ostream_formatter {};

using namespace std::chrono_literals;
using namespace std::string_literals;

logger kms_log("kms");

class kms_error : public std::exception {
    std::string _type, _msg;
public:
    kms_error(std::string_view type, std::string_view msg)
        : _type(type)
        , _msg(fmt::format("{}: {}", type, msg))
    {}
    const std::string& type() const {
        return _type;
    }
    const char* what() const noexcept override {
        return _msg.c_str();
    }
};

namespace kms_errors {
    [[maybe_unused]] static const char* AccessDeniedException = "AccessDeniedException";
    [[maybe_unused]] static const char* IncompleteSignature = "IncompleteSignature";
    [[maybe_unused]] static const char* InternalFailure = "InternalFailure";
    [[maybe_unused]] static const char* InvalidAction = "InvalidAction";
    [[maybe_unused]] static const char* InvalidClientTokenId = "InvalidClientTokenId";
    [[maybe_unused]] static const char* InvalidParameterCombination = "InvalidParameterCombination";
    [[maybe_unused]] static const char* InvalidParameterValue = "InvalidParameterValue";
    [[maybe_unused]] static const char* InvalidQueryParameter = "InvalidQueryParameter";
    [[maybe_unused]] static const char* MalformedQueryString = "MalformedQueryString";
    [[maybe_unused]] static const char* MissingAction = "MissingAction";
    [[maybe_unused]] static const char* MissingAuthenticationToken = "MissingAuthenticationToken";
    [[maybe_unused]] static const char* MissingParameter = "MissingParameter";
    [[maybe_unused]] static const char* NotAuthorized = "NotAuthorized";
    [[maybe_unused]] static const char* OptInRequired = "OptInRequired";
    [[maybe_unused]] static const char* RequestExpired = "RequestExpired";
    [[maybe_unused]] static const char* ServiceUnavailable = "ServiceUnavailable";
    [[maybe_unused]] static const char* ThrottlingException = "ThrottlingException";
    [[maybe_unused]] static const char* ValidationError = "ValidationError";
    [[maybe_unused]] static const char* DependencyTimeoutException = "DependencyTimeoutException";
    [[maybe_unused]] static const char* InvalidArnExceptio = "InvalidArnException";
    [[maybe_unused]] static const char* KMSInternalException = "KMSInternalException";
    [[maybe_unused]] static const char* NotFoundException = "NotFoundException";
    [[maybe_unused]] static const char* AlreadyExistsException = "AlreadyExistsException";
}

namespace beast = boost::beast;     // from <boost/beast.hpp>
// Note: switch http -> bhttp to deal with namespace ambiguity.
namespace bhttp = beast::http;       // from <boost/beast/http.hpp>
namespace shttp = seastar::http;

static std::string to_lower(std::string_view s) {
    std::string tmp(s.size(), 0);
    std::transform(s.begin(), s.end(), tmp.begin(), ::tolower);
    return tmp;
}

static bool is_true(std::string_view s) {
    auto tmp = to_lower(s);
    return tmp == "true" || tmp == "1" || tmp == "yes" || tmp == "on";
}

class encryption::kms_host::impl {
public:
    // set a rather long expiry. normal KMS policies are 365-day rotation of keys.
    // we can do with 10 minutes. CMH. maybe even longer.
    // (see comments below on what keys are here)
    static inline constexpr std::chrono::milliseconds default_expiry = 600s;
    static inline constexpr std::chrono::milliseconds default_refresh = 1200s;

    impl(encryption_context& ctxt, const std::string& name, const host_options& options)
        : _ctxt(ctxt)
        , _name(name)
        , _options(options)
        , _attr_cache(utils::loading_cache_config{
            .max_size = std::numeric_limits<size_t>::max(),
            .expiry = options.key_cache_expiry.value_or(default_expiry),
            .refresh = options.key_cache_refresh.value_or(default_refresh)}, kms_log, std::bind(&impl::create_key, this, std::placeholders::_1))
        , _id_cache(utils::loading_cache_config{
            .max_size = std::numeric_limits<size_t>::max(),
            .expiry = options.key_cache_expiry.value_or(default_expiry),
            .refresh = options.key_cache_refresh.value_or(default_refresh)}, kms_log, std::bind(&impl::find_key, this, std::placeholders::_1))
    {
        // check if we have an explicit endpoint set.
        if (!_options.endpoint.empty()) {
            static std::regex simple_url(R"foo((https?):\/\/(?:([\w\.]+)|\[([\w:]+)\]):?(\d+)?\/?)foo");
            std::transform(_options.endpoint.begin(), _options.endpoint.end(), _options.endpoint.begin(), ::tolower);
            std::smatch m;
            if (!std::regex_match(_options.endpoint, m, simple_url)) {
                throw std::invalid_argument(fmt::format("Could not parse URL: {}", _options.endpoint));
            }
            _options.https = m[1].str() == "https";
            _options.host = m[2].length() > 0 ? m[2].str() : m[3].str();
            _options.port = m[4].length() > 0 ? std::stoi(m[4].str()) : 0;
        }
        if (_options.endpoint.empty() && _options.host.empty() && _options.aws_region.empty() && !_options.aws_use_ec2_region) {
            throw std::invalid_argument("No AWS region or endpoint specified");
        }
        if (_options.port == 0) {
            _options.port = _options.https ? 443 : 80; 
        }
        if (_options.aws_profile.empty()) {
            auto profile = std::getenv("AWS_PROFILE");
            if (profile) {
                _options.aws_profile = profile;
            } else {
                _options.aws_profile = "default";
            }
        }
        kms_log.trace("Added KMS node {}={}", name, _options.endpoint.empty() 
            ? (_options.host.empty() ? _options.aws_region : _options.host)
            : _options.endpoint 
        );
    }
    ~impl() = default;

    future<> init();
    const host_options& options() const {
        return _options;
    }

    future<std::tuple<shared_ptr<symmetric_key>, id_type>> get_or_create_key(const key_info&, const option_override* = nullptr);
    future<shared_ptr<symmetric_key>> get_key_by_id(const id_type&, const key_info&, const option_override* = nullptr);
private:
    class httpclient;
    using key_and_id_type = std::tuple<shared_ptr<symmetric_key>, id_type>;

    struct attr_cache_key {
        std::string master_key;
        std::string aws_assume_role_arn;
        key_info info;

        bool operator==(const attr_cache_key& v) const = default;
        friend std::ostream& operator<<(std::ostream& os, const attr_cache_key& k) {
            fmt::print(os, "{}", std::tie(k.master_key, k.aws_assume_role_arn, k.info));
            return os;
        }
    };

    struct attr_cache_key_hash {
        size_t operator()(const attr_cache_key& k) const {
            return utils::tuple_hash()(std::tie(k.master_key, k.aws_assume_role_arn, k.info.len));
        }
    };

    struct id_cache_key {
        id_type id;
        std::string aws_assume_role_arn;
        bool operator==(const id_cache_key& v) const = default;
        friend std::ostream& operator<<(std::ostream& os, const id_cache_key& k) {
            fmt::print(os, "{{{}, {}}}", k.id, k.aws_assume_role_arn);
            return os;
        }
    };

    struct id_cache_key_hash {
        size_t operator()(const id_cache_key& k) const {
            return utils::tuple_hash()(std::tie(k.id, k.aws_assume_role_arn));
        }
    };

    struct aws_query;
    using result_type = bhttp::response<bhttp::string_body>;

    future<result_type> post(aws_query);
    future<rjson::value> post(std::string_view target, std::string_view aws_assume_role_arn, const rjson::value& query);

    future<key_and_id_type> create_key(const attr_cache_key&);
    future<bytes> find_key(const id_cache_key&);

    encryption_context& _ctxt;
    std::string _name;
    host_options _options;
    utils::loading_cache<attr_cache_key, key_and_id_type, 2, utils::loading_cache_reload_enabled::yes,
        utils::simple_entry_size<key_and_id_type>, attr_cache_key_hash> _attr_cache;
    utils::loading_cache<id_cache_key, bytes, 2, utils::loading_cache_reload_enabled::yes, 
        utils::simple_entry_size<bytes>, id_cache_key_hash> _id_cache;
    shared_ptr<seastar::tls::certificate_credentials> _creds;
    std::unordered_map<bytes, shared_ptr<symmetric_key>> _cache;
    bool _initialized = false;
};

template <> struct fmt::formatter<encryption::kms_host::impl::attr_cache_key> : fmt::ostream_formatter {};
template <> struct fmt::formatter<encryption::kms_host::impl::id_cache_key> : fmt::ostream_formatter {};

/**
 * Not in seastar. Because nowhere near complete, thought through or
 * capable of dealing with anything but tiny aws messages.
 * 
 * TODO: formalize and move to seastar
 */
class encryption::kms_host::impl::httpclient {
public:
    httpclient(std::string host, uint16_t port, shared_ptr<seastar::tls::certificate_credentials> = {});

    httpclient& add_header(std::string_view key, std::string_view value);
    void clear_headers();

    using result_type = kms_host::impl::result_type;
    using request_type = bhttp::request<bhttp::string_body>;

    future<result_type> send();

    using method_type = bhttp::verb;

    void method(method_type);
    void content(std::string_view);
    void target(std::string_view);

    request_type& request() {
        return _req;
    }
    const request_type& request() const {
        return _req;
    }
    const std::string& host() const {
        return _host;
    }
    uint16_t port() const {
        return _port;
    }
private:

    std::string _host;
    uint16_t _port;
    shared_ptr<seastar::tls::certificate_credentials> _creds;
    request_type _req;
};

encryption::kms_host::impl::httpclient::httpclient(std::string host, uint16_t port, shared_ptr<seastar::tls::certificate_credentials> creds)
    : _host(std::move(host))
    , _port(port)
    , _creds(std::move(creds))
{}

encryption::kms_host::impl::httpclient& encryption::kms_host::impl::httpclient::add_header(std::string_view key, std::string_view value) {
    _req.set(beast::string_view(key.data(), key.size()), beast::string_view(value.data(), value.size()));
    return *this;
}

void encryption::kms_host::impl::httpclient::clear_headers() {
    _req.clear();
}

future<encryption::kms_host::impl::httpclient::result_type> encryption::kms_host::impl::httpclient::send() {
    auto addr = co_await net::dns::resolve_name(_host);
    socket_address sa(addr, _port);
    connected_socket s = co_await (_creds 
        ? tls::connect(_creds, sa)
        : seastar::connect(sa)
    );

    s.set_keepalive(true);
    s.set_nodelay(true);

    auto out = s.output();
    auto in = s.input();

    bhttp::serializer<true, bhttp::string_body, typename decltype(_req)::fields_type> ser(_req);

    beast::error_code ec;
    std::exception_ptr ex;

    bhttp::parser<false, bhttp::string_body> p(result_type{});

    try {
        while (!ser.is_done()) {
            future<> f = make_ready_future<>();
            ser.next(ec, [&](beast::error_code& ec, auto&& buffers) {
                for (auto const buffer : beast::buffers_range (buffers)) {
                    f = f.then([&out, data = buffer.data(), size = buffer.size()] {
                        return out.write(static_cast<const char*>(data), size);
                    });
                }
                ser.consume(beast::buffer_bytes(buffers));
            });

            co_await std::move(f);

            if (ec.failed()) {
                break;
            }
        }

        co_await out.flush();

        p.eager(true);
        p.skip(false);

        if (!ec.failed()) {
            while (!p.is_done()) {
                auto buf = co_await in.read();
                if (buf.empty()) {
                    break;
                }
                // parse
                boost::asio::const_buffer wrap(buf.get(), buf.size());
                p.put(wrap, ec);
                if (ec.failed() && ec != bhttp::error::need_more) {
                    break;
                }
                ec.clear();
            }
        }
    } catch (...) {
        ex = std::current_exception();
    }

    try {
        co_await out.close();
    } catch (...) {
        if (!ex) {
            ex = std::current_exception();
        }
    }
    try {
        co_await in.close();
    } catch (...) {
        if (!ex) {
            ex = std::current_exception();
        }
    }

    if (ec.failed()) {
        throw std::system_error(ec);
    }
    if (ex) {
        std::rethrow_exception(ex);
    }

    co_return p.release();
}

void encryption::kms_host::impl::httpclient::method(method_type m) {
    _req.method(m);
}

void encryption::kms_host::impl::httpclient::content(std::string_view body) {
    _req.body().assign(body.begin(), body.end());
    _req.set(bhttp::field::content_length, std::to_string(_req.body().size()));
}

void encryption::kms_host::impl::httpclient::target(std::string_view target) {
    _req.target(std::string(target));
}

static std::string get_option(const encryption::kms_host::option_override* oov, std::optional<std::string> encryption::kms_host::option_override::* f, const std::string& def) {
    if (oov) {
        return (oov->*f).value_or(def);
    }
    return {};
};

[[noreturn]]
static void translate_kms_error(const kms_error& e) {
    using namespace kms_errors;
    using namespace encryption;

    if (e.type() == AccessDeniedException || e.type() == MissingAuthenticationToken || e.type() == NotAuthorized) {
        std::throw_with_nested(permission_error(e.what()));
    }
    if (e.type() == OptInRequired || e.type() == InvalidClientTokenId || e.type() == InvalidAction) {
        std::throw_with_nested(configuration_error(e.what()));
    }
    if (e.type() == NotFoundException || e.type() == DependencyTimeoutException) {
        std::throw_with_nested(missing_resource_error(e.what()));
    }
    std::throw_with_nested(service_error(e.what()));
}

future<std::tuple<shared_ptr<encryption::symmetric_key>, encryption::kms_host::id_type>> encryption::kms_host::impl::get_or_create_key(const key_info& info, const option_override* oov) {
    attr_cache_key key {
        .master_key = get_option(oov, &option_override::master_key, _options.master_key),
        .aws_assume_role_arn = get_option(oov, &option_override::aws_assume_role_arn, _options.aws_assume_role_arn),
        .info = info,
    };

    if (key.master_key.empty() && _options.master_key.empty()) {
        throw configuration_error("No master key set in kms host config or encryption attributes");
    }
    try {
        co_return co_await _attr_cache.get(key);
    } catch (kms_error& e) {
        translate_kms_error(e);
    } catch (base_error&) {
        throw;
    } catch (std::system_error& e) {
        std::throw_with_nested(network_error(e.what()));
    } catch (rjson::malformed_value& e) {
        std::throw_with_nested(malformed_response_error(e.what()));
    } catch (...) {
        std::throw_with_nested(service_error(fmt::format("get_key_by_id: {}", std::current_exception())));
    }
}

future<shared_ptr<encryption::symmetric_key>> encryption::kms_host::impl::get_key_by_id(const id_type& id, const key_info& info, const option_override* oov) {
    // note: since KMS does not really have any actual "key" associtation of id -> key,
    // we only cache/query raw bytes of some length. (See below).
    // Thus keys returned are always new objects. But they are not huge...
    id_cache_key key {
        .id = id,
        .aws_assume_role_arn = get_option(oov, &option_override::aws_assume_role_arn, _options.aws_assume_role_arn),
    };
    try {
        auto data = co_await _id_cache.get(key);
        co_return make_shared<symmetric_key>(info, data);
    } catch (kms_error& e) {
        translate_kms_error(e);
    } catch (base_error&) {
        throw;
    } catch (std::system_error& e) {
        std::throw_with_nested(network_error(e.what()));
    } catch (std::invalid_argument& e) {
        std::throw_with_nested(configuration_error(fmt::format("get_key_by_id: {}", e.what())));
    } catch (rjson::malformed_value& e) {
        std::throw_with_nested(malformed_response_error(e.what()));
    } catch (...) {
        std::throw_with_nested(service_error(fmt::format("get_key_by_id: {}", std::current_exception())));
    }
}

std::string make_aws_host(std::string_view aws_region, std::string_view service) {
    static const char AWS_GLOBAL[] = "aws-global";
    static const char US_EAST_1[] = "us-east-1"; // US East (N. Virginia)
    static const char CN_NORTH_1[] = "cn-north-1"; // China (Beijing)
    static const char CN_NORTHWEST_1[] = "cn-northwest-1"; // China (Ningxia)
    static const char US_ISO_EAST_1[] = "us-iso-east-1";  // US ISO East
    static const char US_ISOB_EAST_1[] = "us-isob-east-1"; // US ISOB East (Ohio)

    // Fallback to us-east-1 if global endpoint does not exists.
    auto region = aws_region == AWS_GLOBAL ? US_EAST_1 : aws_region;

    std::stringstream ss;
    ss << service << "." << region;

    if (region == CN_NORTH_1 || region == CN_NORTHWEST_1) {
        ss << ".amazonaws.com.cn";
    } else if (region == US_ISO_EAST_1) {
        ss << ".c2s.ic.gov";
    } else if (region == US_ISOB_EAST_1) {
        ss << ".sc2s.sgov.gov";
    } else {
        ss << ".amazonaws.com";
    }
    return ss.str();
}

struct encryption::kms_host::impl::aws_query {
    std::string_view host; 

    std::string_view service;
    std::string_view target;
    std::string_view content_type;
    std::string_view content; 

    std::string_view aws_access_key_id; 
    std::string_view aws_secret_access_key;
    std::string_view security_token;

    uint16_t port;
};

future<rjson::value> encryption::kms_host::impl::post(std::string_view target, std::string_view aws_assume_role_arn, const rjson::value& query) {
    static auto get_response_error = [](const result_type& res) -> std::string {
        switch (res.result()) {
        case bhttp::status::unauthorized: case bhttp::status::forbidden: return "AccessDenied";
        case bhttp::status::not_found: return "ResourceNotFound";
        case bhttp::status::too_many_requests: return "SlowDown";
        case bhttp::status::internal_server_error: return "InternalError";
        case bhttp::status::service_unavailable: return "ServiceUnavailable";
        case bhttp::status::request_timeout: case bhttp::status::gateway_timeout:
        case bhttp::status::network_connect_timeout_error:
            return "RequestTimeout";
        default:
            return format("{}", res.result());
        }
    };

    static auto query_ec2_meta = [](std::string_view target, std::string token = {}) -> future<std::tuple<httpclient::result_type, std::string>> {
        static auto get_env_def = [](std::string_view var, std::string_view def) {
            auto val = std::getenv(var.data());
            return val ? std::string_view(val) : def;
        };

        static const std::string ec2_meta_host(get_env_def("AWS_EC2_METADATA_ADDRESS", "169.254.169.254"));
        static const int ec2_meta_port = std::stoi(get_env_def("AWS_EC2_METADATA_PORT", "80").data());

        kms_log.debug("Query ec2 metadata");

        httpclient client(ec2_meta_host, ec2_meta_port);

        static constexpr auto X_AWS_EC2_METADATA_TOKEN_TTL_SECONDS = "X-aws-ec2-metadata-token-ttl-seconds";
        static constexpr auto X_AWS_EC2_METADATA_TOKEN = "X-aws-ec2-metadata-token";
        static constexpr const char* HOST_HEADER = "host";

        static auto logged_send = [](httpclient& client) -> future<result_type> {
            kms_log.trace("Request: {}", client.request());
            result_type res;
            try {
                res = co_await client.send();
            } catch (std::system_error& e) {
                std::throw_with_nested(network_error(fmt::format("Error sending to host {}:{}: {}", client.host(), client.port(), e.what())));
            } catch (std::exception& e) {
                std::throw_with_nested(service_error(fmt::format("Error sending to host {}:{}: {}", client.host(), client.port(), e.what())));
            }
            kms_log.trace("Result: status={}, response={}", res.result_int(), res);
            if (res.result() != bhttp::status::ok) {
                throw kms_error(get_response_error(res), "EC2 metadata query");
            }
            co_return res;
        };

        client.add_header(HOST_HEADER, ec2_meta_host);

        if (token.empty()) {
            client.add_header(X_AWS_EC2_METADATA_TOKEN_TTL_SECONDS, "21600");
            client.method(httpclient::method_type::put);
            client.target("/latest/api/token");


            auto res = co_await logged_send(client);

            if (res.result() != bhttp::status::ok) {
                throw kms_error(get_response_error(res), "EC2 metadata token query");
            }

            token = res.body();
            client.clear_headers();
        }

        client.add_header(X_AWS_EC2_METADATA_TOKEN, token);
        client.add_header(HOST_HEADER, ec2_meta_host);
        client.method(httpclient::method_type::get);
        client.target(target);

        auto res = co_await logged_send(client);
        co_return std::make_tuple(std::move(res), token);
    };

    std::string gtoken;

    if (_options.aws_region.empty() && _options.host.empty()) {
        assert(_options.aws_use_ec2_region);
        httpclient::result_type res;
        std::tie(res, gtoken) = co_await query_ec2_meta("/latest/meta-data/placement/region");
        _options.aws_region = res.body();
    }

    if (_options.host.empty()) {
        // resolve region -> endpoint
        assert(!_options.aws_region.empty());
        _options.host = make_aws_host(_options.aws_region, "kms");
    }

    auto should_resolve_options_credentials = [this] {
        if (_options.aws_use_ec2_credentials) {
            return false;
        }
        return _options.aws_access_key_id.empty() || _options.aws_secret_access_key.empty();
    };

    // if we did not get full auth info in config, we can try to 
    // retrieve it from environment
    if (should_resolve_options_credentials()) {
        auto key_id = std::getenv("AWS_ACCESS_KEY_ID");
        auto key = std::getenv("AWS_SECRET_ACCESS_KEY");
        if (_options.aws_access_key_id.empty() && key_id) {
            kms_log.debug("No aws id specified. Using environment AWS_ACCESS_KEY_ID");
            _options.aws_access_key_id = key_id;
        }
        if (_options.aws_secret_access_key.empty() && key) {
            kms_log.debug("No aws secret specified. Using environment AWS_SECRET_ACCESS_KEY");
            _options.aws_secret_access_key = key;
        }
    }

    // if we did not get full auth info in config or env, we can try to 
    // retrieve it from ~/.aws/credentials
    if (should_resolve_options_credentials()) {
        auto home = std::getenv("HOME");
        if (home) {
            auto credentials = std::string(home) + "/.aws/credentials";
            auto credentials_exists = co_await seastar::file_exists(credentials);
            if (credentials_exists) {
                kms_log.debug("No aws id/secret specified. Trying to read credentials from {}", credentials);
                try {
                    auto buf = co_await read_text_file_fully(credentials);
                    std::string profile;

                    static std::regex cred_line(R"foo(\s*\[(?:profile\s+)?(\w+)\]|([^\s]+)\s*=\s*([^\s]+)\s*\n)foo");
                    std::cregex_iterator i(buf.get(), buf.get() + buf.size(), cred_line), e;

                    std::string id, secret;
                    while (i != e) {
                        if ((*i)[1].length() > 0) {
                            profile = (*i)[1].str();
                            kms_log.trace("Found profile {} ({})", profile, credentials);
                        } else if (profile == _options.aws_profile) {
                            std::string key((*i)[2].str());
                            std::string val((*i)[3].str());
                            if (key == "aws_access_key_id") {
                                id = val;
                            } else if (key == "aws_secret_access_key") {
                                secret = val;
                            }
                        }
                        ++i;
                    }

                    if (!id.empty() && !_options.aws_access_key_id.empty() && id != _options.aws_access_key_id) {
                        throw configuration_error(fmt::format("Mismatched aws id: {} != {}", id, _options.aws_access_key_id));
                    }
                    if (!id.empty() && _options.aws_access_key_id.empty()) {
                        _options.aws_access_key_id = id;
                    }
                    if (!secret.empty() && _options.aws_secret_access_key.empty()) {
                        _options.aws_secret_access_key = secret;
                    }
                    if (_options.aws_access_key_id.empty() || _options.aws_secret_access_key.empty()) {
                        throw configuration_error(fmt::format("Could not find credentials for profile {}", _options.aws_profile));
                    }
                    kms_log.debug("Read credentials from {} ({}:{}{})", credentials, _options.aws_access_key_id                    
                        , _options.aws_secret_access_key.substr(0, 2)
                        , std::string(_options.aws_secret_access_key.size()-2, '-')
                    );
                } catch (...) {
                    kms_log.debug("Could not read credentials: {}", std::current_exception());
                }
            }
        }
    }

    auto aws_access_key_id = _options.aws_access_key_id;
    auto aws_secret_access_key = _options.aws_secret_access_key;
    auto session = ""s;

    if (_options.aws_use_ec2_credentials) {
        auto [res, token] = co_await query_ec2_meta("/latest/meta-data/iam/security-credentials/", gtoken);
        auto role = res.body();

        std::tie(res, std::ignore) = co_await query_ec2_meta("/latest/meta-data/iam/security-credentials/" + role, token);
        auto body = rjson::parse(std::string_view(res.body().data(), res.body().size()));

        try {
            aws_access_key_id = rjson::get<std::string>(body, "AccessKeyId");
            aws_secret_access_key = rjson::get<std::string>(body, "SecretAccessKey");
            session = rjson::get<std::string>(body, "Token");
        } catch (rjson::malformed_value&) {
            std::throw_with_nested(kms_error("AccessDenied", fmt::format("Code={}, Message={}"
                , rjson::get_opt<std::string>(body, "Code")
                , rjson::get_opt<std::string>(body, "Message")                
                )));
        }
    }

    // Note: allowing user code to potentially reset aws_assume_role_arn='' -> no assumerole.
    // Not 100% sure this is needed.

    if (!aws_assume_role_arn.empty()) {
        auto sts_host = make_aws_host(_options.aws_region, "sts");
        auto now = db_clock::now();
        auto rs_id = utils::UUID_gen::get_time_UUID(std::chrono::system_clock::time_point(now.time_since_epoch()));
        auto role_session = fmt::format("ScyllaDB-{}", rs_id);

        kms_log.debug("Assume role: {} (RoleSessionID={})", aws_assume_role_arn, role_session);

        auto res = co_await post(aws_query{
            .host = sts_host,
            .service = "sts",
            .content_type = "application/x-www-form-urlencoded; charset=utf-8",
            .content = "Action=AssumeRole&Version=2011-06-15&RoleArn=" 
                + shttp::internal::url_encode(aws_assume_role_arn)
                + "&RoleSessionName=" + role_session,
            .aws_access_key_id = aws_access_key_id,
            .aws_secret_access_key = aws_secret_access_key,
            .security_token = session,
            .port = _options.port,
        });

        if (res.result() != bhttp::status::ok) {
            throw kms_error(get_response_error(res), "AssumeRole");
        }

        rapidxml::xml_document<> doc;
        try {
            doc.parse<0>(res.body().data());

            using node_type = rapidxml::xml_node<char>;
            static auto get_xml_node = [](node_type* node, const char* what) {
                auto res = node->first_node(what);
                if (!res) {
                    throw kms_error("XML parse error", what);
                }
                return res;
            };

            auto arrsp = get_xml_node(&doc, "AssumeRoleResponse");
            auto arres = get_xml_node(arrsp, "AssumeRoleResult");
            auto creds = get_xml_node(arres, "Credentials");
            auto keyid = get_xml_node(creds, "AccessKeyId");
            auto key = get_xml_node(creds, "SecretAccessKey");
            auto token = get_xml_node(creds, "SessionToken");

            aws_access_key_id = keyid->value();
            aws_secret_access_key = key->value();
            session = token->value();

        } catch (const rapidxml::parse_error& e) {
            std::throw_with_nested(kms_error("XML parse error", "AssumeRole"));
        }
    }

    auto res = co_await post(aws_query{
        .host = _options.host,
        .service = "kms",
        .target = target,
        .content_type = "application/x-amz-json-1.1",
        .content = rjson::print(query),
        .aws_access_key_id = aws_access_key_id,
        .aws_secret_access_key = aws_secret_access_key,
        .security_token = session,
        .port = _options.port,
    });

    auto body = rjson::empty_object();

    if (!res.body().empty()) {
        try {
            body = rjson::parse(std::string_view(res.body().data(), res.body().size()));
        } catch (...) {
            if (res.result() == bhttp::status::ok) {
                throw;
            }
            // assume non-json formatted error. fall back to parsing below
        }
    } 

    if (res.result() != bhttp::status::ok) {
        // try to format as good an error as we can.
        static const char* message_lc_header = "message";
        static const char* message_cc_header = "Message";
        static const char* error_type_header = "x-amzn-ErrorType";
        static const char* type_header = "__type";

        auto o = rjson::get_opt<std::string>(body, message_lc_header);
        if (!o) {
            o = rjson::get_opt<std::string>(body, message_cc_header);
        }
        auto msg = o.value_or("Unknown error");

        o = rjson::get_opt<std::string>(body, error_type_header);
        if (!o) {
            o = rjson::get_opt<std::string>(body, type_header);
        }
        // this should never happen with aws, but...
        auto type = o ? *o : get_response_error(res);

        throw kms_error(type, msg);
    }

    co_return body;    
}

// helper to build AWS request and parse result.
future<encryption::kms_host::impl::result_type> encryption::kms_host::impl::post(aws_query query) {
    auto creds = _creds;
    // if we are https, we need at least a credentials object that says "use system trust"
    if (!creds && _options.https) {
        creds = ::make_shared<seastar::tls::certificate_credentials>();

        if (!_options.priority_string.empty()) {
            creds->set_priority_string(_options.priority_string);
        } else {
            creds->set_priority_string(db::config::default_tls_priority);
        }

        if (!_options.certfile.empty()) {
            co_await creds->set_x509_key_file(_options.certfile, _options.keyfile, seastar::tls::x509_crt_format::PEM);
        }
        if (!_options.truststore.empty()) {
            co_await creds->set_x509_trust_file(_options.truststore, seastar::tls::x509_crt_format::PEM);
        } else {
            co_await creds->set_system_trust();
        }
        _creds = creds;
    }

    // some of this could be shared with alternator
    static constexpr const char* CONTENT_TYPE_HEADER = "content-type";
    static constexpr const char* HOST_HEADER = "host";
    static constexpr const char* X_AWS_DATE_HEADER = "X-Amz-Date";
    static constexpr const char* AWS_AUTHORIZATION_HEADER = "authorization";
    static constexpr const char* AMZ_SDK_INVOCATION_ID = "amz-sdk-invocation-id";
    static constexpr const char* X_AMZ_SECURITY_TOKEN = "X-Amz-Security-Token";

    static constexpr const char* AMZ_TARGET_HEADER = "x-amz-target";
    static constexpr const char* AWS_HMAC_SHA256 = "AWS4-HMAC-SHA256";
    static constexpr const char* AWS4_REQUEST = "aws4_request";
    static constexpr const char* SIGNING_KEY = "AWS4";
    static constexpr const char* CREDENTIAL = "Credential";
    static constexpr const char* SIGNATURE = "Signature";
    static constexpr const char* SIGNED_HEADERS = "SignedHeaders";
    [[maybe_unused]] static constexpr const char* ACTION_HEADER = "Action";

    static constexpr const char* ISO_8601_BASIC = "{:%Y%m%dT%H%M%SZ}";
    static constexpr const char* SIMPLE_DATE_FORMAT_STR = "{:%Y%m%d}";
    static constexpr auto NEWLINE = '\n';

    auto now = db_clock::now();
    auto req_id = utils::UUID_gen::get_time_UUID(std::chrono::system_clock::time_point(now.time_since_epoch()));

    kms_log.trace("Building request: {} ({}:{}) {}", query.target, query.host, query.port, req_id);

    httpclient client(std::string(query.host), query.port, std::move(creds));

    auto t_now = fmt::gmtime(db_clock::to_time_t(now));
    auto timestamp = fmt::format(ISO_8601_BASIC, t_now);

    // see https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
    // see AWS SDK.
    // see https://docs.aws.amazon.com/general/latest/gr/sigv4-signed-request-examples.html
    std::stringstream signedHeadersStream;
    std::stringstream canonicalRequestStream;

    canonicalRequestStream 
        << "POST" << NEWLINE 
        << "/" << NEWLINE << NEWLINE
        ;

    auto add_signed_header = [&](std::string_view name, std::string_view value) {
        client.add_header(name, value);
        auto lname = to_lower(name);
        canonicalRequestStream << lname << ":" << value << NEWLINE;
        if (signedHeadersStream.tellp() != 0) {
            signedHeadersStream << ';';
        }
        signedHeadersStream << lname;
    };

    // headers must be sorted!

    add_signed_header(CONTENT_TYPE_HEADER, query.content_type);
    add_signed_header(HOST_HEADER, query.host);
    add_signed_header(X_AWS_DATE_HEADER, timestamp);
    if (!query.target.empty()) {
        add_signed_header(AMZ_TARGET_HEADER, "TrentService."s + std::string(query.target));
    }

    if (!query.security_token.empty()) {
        //add_signed_header(X_AMZ_SECURITY_TOKEN, query.security_token);
        client.add_header(X_AMZ_SECURITY_TOKEN, query.security_token);
    }

    client.add_header(AMZ_SDK_INVOCATION_ID, fmt::format("{}", req_id));
    client.add_header("Accept-Encoding", "identity");
    client.add_header("Accept", "*/*");

    auto make_hash = [&](std::string_view s) {
        auto sha256 = calculate_sha256(bytes_view(reinterpret_cast<const int8_t*>(s.data()), s.size()));
        auto hash = to_hex(sha256);
        return hash;
    };

    auto hash = make_hash(query.content);

    auto signedHeadersValue = signedHeadersStream.str();
    canonicalRequestStream << NEWLINE << signedHeadersValue << NEWLINE << hash;
    auto canonicalRequestString = canonicalRequestStream.str();
    auto canonicalRequestHash = make_hash(canonicalRequestString);

    kms_log.trace("Canonical request: {}", canonicalRequestString);

    auto simpleDate = fmt::format(SIMPLE_DATE_FORMAT_STR, t_now);

    std::stringstream stringToSignStream;
    stringToSignStream << AWS_HMAC_SHA256 << NEWLINE 
        << timestamp << NEWLINE 
        << simpleDate << "/" << _options.aws_region << "/"
        << query.service << "/" << AWS4_REQUEST << NEWLINE 
        << canonicalRequestHash
        ;
    auto stringToSign = stringToSignStream.str();

    // these log messages intentionally made to mimic aws sdk/boto3
    kms_log.trace("StringToSign: {}", stringToSign);

    std::string finalSignature;

    {
        auto tobv = [](std::string_view s) {
            return bytes_view(reinterpret_cast<const int8_t*>(s.data()), s.size());
        };

        auto signingKey = SIGNING_KEY + std::string(query.aws_secret_access_key);
        auto kDate = hmac_sha256(tobv(simpleDate), tobv(signingKey));
        auto kRegion = hmac_sha256(tobv(_options.aws_region), kDate);
        auto kService = hmac_sha256(tobv(query.service), kRegion);
        auto hashResult = hmac_sha256(tobv(AWS4_REQUEST), kService);
        auto finalHash = hmac_sha256(tobv(stringToSign), hashResult);
        finalSignature = to_hex(finalHash);
    }

    std::stringstream authStream;
    authStream << AWS_HMAC_SHA256 << " " 
        << CREDENTIAL << "=" << query.aws_access_key_id << "/" << simpleDate << "/" << _options.aws_region 
        << "/" << query.service << "/" << AWS4_REQUEST << ", " << SIGNED_HEADERS 
        << "=" << signedHeadersValue << ", " << SIGNATURE << "=" << finalSignature
        ;

    auto awsAuthString = authStream.str();

    client.add_header(AWS_AUTHORIZATION_HEADER, awsAuthString);
    client.target("/");
    client.content(query.content);
    client.method(httpclient::method_type::post);

    kms_log.trace("Request: {}", client.request());

    auto res = co_await client.send();

    kms_log.trace("Result: status={}, response={}", res.result_int(), res);

    co_return res;
}

static std::optional<std::string> make_opt(const std::string& s) {
    if (s.empty()) {
        return std::nullopt;
    }
    return s;
}

future<> encryption::kms_host::impl::init() {
    if (_initialized) {
        co_return;
    }

    if (!_options.master_key.empty()) {
        kms_log.debug("Looking up master key");
        auto query = rjson::empty_object();
        rjson::add(query, "KeyId", _options.master_key);
        auto response = co_await post("DescribeKey", _options.aws_assume_role_arn, query);
        kms_log.debug("Master key exists");
    } else {
        kms_log.info("No default master key configured. Not verifying.");
    }
    _initialized = true;
}

future<encryption::kms_host::impl::key_and_id_type> encryption::kms_host::impl::create_key(const attr_cache_key& k) {
    auto& master_key = k.master_key;
    auto& aws_assume_role_arn = k.aws_assume_role_arn;
    auto& info = k.info;

    /**
     * AWS KMS does _not_ allow us to actually have "named keys" that can be used externally,
     * i.e. exported to us, here, for bulk encryption.
     * All named keys are 100% internal, the only options we have is using the
     * "GenerateDataKey" API. This creates a new (epiphermal) key, encrypts it 
     * using a named (internal) key, and gives us both raw and encrypted blobs
     * for usage as a local key.
     * To be able to actually re-use this key again, on decryption of data,
     * we employ the strategy recommended (https://docs.aws.amazon.com/kms/latest/APIReference/API_GenerateDataKey.html)
     * namely actually embedding the encrypted key in the key ID associated with 
     * the locally encrypted data. So ID:s become pretty big.
     * 
     * For ID -> key, we simply split the ID into the encrypted key part, and
     * the master key name part, decrypt the first using the second (AWS KMS Decrypt),
     * and create a local key using the result.
     * 
     * Data recovery:
     * Assuming you have data encrypted using a KMS generated key, you will have
     * metadata detailing algorithm, key length etc (see sstable metadata, and key info).
     * Metadata will also include a byte blob representing the ID of the encryption key.
     * For KMS, the ID will actually be a text string:
     *  <AWS key id>:<base64 encoded blob>
     *
     * I.e. something like:
     *   761f258a-e2e9-40b3-8891-602b1b8b947e:e56sadfafa3324ff=/wfsdfwssdf
     * or 
     *   arn:aws:kms:us-east-1:797456418907:key/761f258a-e2e9-40b3-8891-602b1b8b947e:e56sadfafa3324ff=/wfsdfwssdf
     *
     * (last colon is separator) 
     *
     * The actual data key can be retreived by doing a KMS "Decrypt" of the data blob part
     * using the KMS key referenced by the key ID. This gives back actual key data that can
     * be used to create a symmetric_key with algo, length etc as specified by metadata.
     *
     */

    // avoid creating too many keys and too many calls. If we are not shard 0, delegate there.
    if (this_shard_id() != 0) {
        auto [data, id] = co_await smp::submit_to(0, [this, info, master_key, aws_assume_role_arn]() -> future<std::tuple<bytes, id_type>> {
            auto host = _ctxt.get_kms_host(_name);
            option_override oov {
                .master_key = make_opt(master_key),
                .aws_assume_role_arn = make_opt(aws_assume_role_arn),
            };
            auto [k, id] = co_await host->_impl->get_or_create_key(info, &oov);
            co_return std::make_tuple(k != nullptr ? k->key() : bytes{}, id);
        });
        co_return key_and_id_type{ 
            data.empty() ? nullptr : make_shared<symmetric_key>(info, data), 
            id 
        };
    }

    // note: since external keys are _not_ stored,
    // there is nothing we can "look up" or anything. Always 
    // new key here.

    kms_log.debug("Creating new key: {}", info);

    auto query = rjson::empty_object();

    rjson::add(query, "KeyId", std::string(master_key.begin(), master_key.end()));
    rjson::add(query, "NumberOfBytes", info.len/8);

    auto response = co_await post("GenerateDataKey", aws_assume_role_arn, query);
    auto data = base64_decode(rjson::get<std::string>(response, "Plaintext"));
    auto enc = rjson::get<std::string>(response, "CiphertextBlob");
    auto kid = rjson::get<std::string>(response, "KeyId");

    try {
        auto key = make_shared<symmetric_key>(info, data);
        bytes id(kid.size() + 1 + enc.size(), 0);
        auto i = std::copy(kid.begin(), kid.end(), id.begin());
        *i++ = ':';
        std::copy(enc.begin(), enc.end(), i);

        co_return key_and_id_type{ key, id };
    } catch (std::invalid_argument& e) {
        std::throw_with_nested(configuration_error(e.what()));
    }
}

future<bytes> encryption::kms_host::impl::find_key(const id_cache_key& k) {
    // avoid creating too many keys and too many calls. If we are not shard 0, delegate there.
    if (this_shard_id() != 0) {
        co_return co_await smp::submit_to(0, [this, k]() -> future<bytes> {
            auto host = _ctxt.get_kms_host(_name);
            auto bytes = co_await host->_impl->_id_cache.get(k);
            co_return bytes;
        });
    }

    // See create_key. ID consists of <master id>:<encrypted key blob>.
    // master id can (and will) contain ':', but blob will not.
    // (we are being wasteful, and keeping the base64 encoding - easier to read)
    auto& id = k.id;
    auto pos = id.find_last_of(':');
    if (pos == id_type::npos) {
        throw std::invalid_argument(format("Not a valid key id: {}", id));
    }

    kms_log.debug("Finding key: {}", id);

    std::string kid(id.begin(), id.begin() + pos);
    std::string enc(id.begin() + pos + 1, id.end());

    auto query = rjson::empty_object();
    rjson::add(query, "CiphertextBlob", enc);
    rjson::add(query, "KeyId", kid);

    auto response = co_await post("Decrypt", k.aws_assume_role_arn, query);
    auto data = base64_decode(rjson::get<std::string>(response, "Plaintext"));

    // we know nothing about key type etc, so just return data.
    co_return data;
}

encryption::kms_host::kms_host(encryption_context& ctxt, const std::string& name, const host_options& options)
    : _impl(std::make_unique<impl>(ctxt, name, options))
{}

encryption::kms_host::kms_host(encryption_context& ctxt, const std::string& name, const std::unordered_map<sstring, sstring>& map)
    : kms_host(ctxt, name, [&map] {
        host_options opts;
        map_wrapper<std::unordered_map<sstring, sstring>> m(map);

        opts.aws_access_key_id = m("aws_access_key_id").value_or("");
        opts.aws_secret_access_key = m("aws_secret_access_key").value_or("");
        opts.aws_region = m("aws_region").value_or("");
        opts.aws_profile = m("aws_profile").value_or("");
        opts.aws_assume_role_arn = m("aws_assume_role_arn").value_or("");
        opts.aws_use_ec2_credentials = is_true(m("aws_use_ec2_credentials").value_or("false"));
        opts.aws_use_ec2_region = is_true(m("aws_use_ec2_region").value_or("false"));

        // use "endpoint" semantics to match AWS configs.
        opts.endpoint = m("endpoint").value_or("");
        opts.host = m("host").value_or("");
        opts.port = std::stoi(m("port").value_or("0"));

        opts.master_key = m("master_key").value_or("");
        opts.certfile = m("certfile").value_or("");
        opts.keyfile = m("keyfile").value_or("");
        opts.truststore = m("truststore").value_or("");
        opts.priority_string = m("priority_string").value_or("");

        opts.key_cache_expiry = parse_expiry(m("key_cache_expiry"));
        opts.key_cache_refresh = parse_expiry(m("key_cache_refresh"));
        return opts;
    }())
{}

encryption::kms_host::~kms_host() = default;

future<> encryption::kms_host::init() {
    return _impl->init();
}

const encryption::kms_host::host_options& encryption::kms_host::options() const {
    return _impl->options();
}

future<std::tuple<shared_ptr<encryption::symmetric_key>, encryption::kms_host::id_type>> encryption::kms_host::get_or_create_key(const key_info& info, const option_override* oov) {
    return _impl->get_or_create_key(info, oov);
}

future<shared_ptr<encryption::symmetric_key>> encryption::kms_host::get_key_by_id(const id_type& id, const key_info& info, const option_override* oov) {
    return _impl->get_key_by_id(id, info, oov);
}

