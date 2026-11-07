/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

/*
 * Copyright (C) 2025-present ScyllaDB
 */

#include "http.hh"
#include "rest/client.hh"

#include <array>

#include <boost/regex.hpp>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/coroutine/all.hh>

static logging::logger trust_log("http_trust");

// The bundle is a few hundred KB. Reading it whole would be an oversized
// allocation on the shard (seastar warns above 128 KiB, and SCT turns that
// warning into a test error), so it goes to gnutls one certificate at a time.
static future<> load_ca_bundle(tls::certificate_credentials& cred, const char* path) {
    auto f = co_await open_file_dma(path, open_flags::ro);
    auto in = make_file_input_stream(std::move(f), file_input_stream_options{.buffer_size = 32 * 1024});
    std::exception_ptr ex;
    try {
        static constexpr std::string_view end_marker = "-----END CERTIFICATE-----";
        std::string pending;
        for (auto buf = co_await in.read(); !buf.empty(); buf = co_await in.read()) {
            pending.append(buf.get(), buf.size());
            for (auto pos = pending.find(end_marker); pos != std::string::npos; pos = pending.find(end_marker)) {
                auto end = pos + end_marker.size();
                cred.set_x509_trust(tls::blob(pending.data(), end), tls::x509_crt_format::PEM);
                pending.erase(0, end);
            }
        }
    } catch (...) {
        ex = std::current_exception();
    }
    co_await in.close();
    if (ex) {
        std::rethrow_exception(std::move(ex));
    }
}

// The CA bundles the distributions generate from their trust store. Loading one
// puts the trust list in memory. set_system_trust() instead hands gnutls its
// default trust store, which in the build we ship is the p11-kit trust module
// (the package bundles pkcs11/p11-kit-trust.so and points it at the host's
// trust through SCYLLA_P11_TRUST_PATHS), and every certificate verification
// then goes through that module. Measured against s3.us-east-1.amazonaws.com
// with gnutls 3.8.13: a verification costs ~9.5 ms of CPU through p11-kit and
// ~0.6-2 ms from an in-memory list, on top of ~2.3 ms for the handshake
// itself. Every new object storage connection pays it, on the shard, without
// preemption. Both bundles are generated from the trusted set only, by
// update-ca-trust and update-ca-certificates, so for server authentication
// they trust what p11-kit trusts.
static constexpr std::array ca_bundle_paths = {
    "/etc/pki/tls/certs/ca-bundle.crt",                  // RHEL, Fedora, Amazon Linux
    "/etc/ssl/certs/ca-certificates.crt",                // Debian, Ubuntu
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem", // what the RHEL link points at
};

future<shared_ptr<tls::certificate_credentials>> utils::http::system_trust_credentials() {
    static thread_local shared_ptr<tls::certificate_credentials> system_trust_credentials;
    if (!system_trust_credentials) {
        // can race, and overwrite the object. that is fine.
        auto cred = make_shared<tls::certificate_credentials>();
        bool loaded = false;
        for (auto path : ca_bundle_paths) {
            if (!co_await file_exists(path)) {
                continue;
            }
            try {
                co_await load_ca_bundle(*cred, path);
                trust_log.info("Loaded the CA trust list from {}", path);
                loaded = true;
                break;
            } catch (...) {
                trust_log.warn("Could not load the CA trust list from {}: {}", path, std::current_exception());
            }
        }
        if (!loaded) {
            trust_log.info("No CA bundle file found, using the system trust store");
            co_await cred->set_system_trust();
        }
        system_trust_credentials = std::move(cred);
    }
    co_return system_trust_credentials;
}

future<> utils::http::dns_connection_factory::init_credentials() {
    if (_use_https && !_creds) {
        _creds = co_await system_trust_credentials();
    }
    if (!_use_https) {
        _creds = {};
    }
    _logger.debug("Initialized credentials, tls={}", _creds == nullptr ? "no" : "yes");
}

future<net::inet_address> utils::http::dns_connection_factory::get_address() {
    auto get_addr = [this] -> net::inet_address {
        const auto& addresses = _addr_list.value();
        return addresses[_addr_pos++ % addresses.size()];
    };

    if (_addr_list) {
        co_return get_addr();
    }
    auto units = co_await get_units(_init_semaphore, 1);
    if (!_addr_list) {
        auto hent = co_await net::dns::get_host_by_name(_host, net::inet_address::family::INET);
        _address_ttl = std::ranges::min_element(hent.addr_entries, [](const net::hostent::address_entry& lhs, const net::hostent::address_entry& rhs) {
                           return lhs.ttl < rhs.ttl;
                       })->ttl;
        if (_address_ttl.count() == 0) {
            co_return hent.addr_entries[_addr_pos++ % hent.addr_entries.size()].addr;
        }
        _addr_list = hent.addr_entries | std::views::transform(&net::hostent::address_entry::addr) | std::ranges::to<std::vector>();
        _addr_update_timer.rearm(lowres_clock::now() + _address_ttl);
    }

    co_return get_addr();
}

future<shared_ptr<tls::certificate_credentials>> utils::http::dns_connection_factory::get_creds() {
    if (!_creds_init) [[unlikely]] {
        auto units = co_await get_units(_init_semaphore, 1);
        if (!_creds_init) {
            co_await init_credentials();
            _creds_init = true;
        }
    }
    co_return _creds;
}

future<connected_socket> utils::http::dns_connection_factory::connect(net::inet_address address) {
    auto socket_addr = socket_address(address, _port);
    if (auto creds = co_await get_creds()) {
        _logger.debug("Making new HTTPS connection addr={} host={}", socket_addr, _host);
        co_return co_await tls::connect(creds, socket_addr, tls::tls_options{.server_name = _host});
    }
    _logger.debug("Making new HTTP connection addr={} host={}", socket_addr, _host);
    co_return co_await seastar::connect(socket_addr, {}, transport::TCP);
}

utils::http::dns_connection_factory::dns_connection_factory(std::string host, int port, bool use_https, logging::logger& logger, shared_ptr<tls::certificate_credentials> certs)
    : _host(std::move(host))
    , _port(port)
    , _logger(logger)
    ,_creds(std::move(certs))
    , _use_https(use_https)
    , _addr_update_timer([this] {
        if (auto units = try_get_units(_init_semaphore, 1)) {
            _addr_list.reset();
        }
    }) {
    _addr_update_timer.arm(lowres_clock::now());
}

utils::http::dns_connection_factory::dns_connection_factory(std::string uri, logging::logger& logger, shared_ptr<tls::certificate_credentials> certs) 
    : dns_connection_factory([&] {
        auto url = parse_simple_url(uri);
        if (!url.path.empty()) {
            throw std::invalid_argument("Cannot handle path in URI: " + uri);
        }
        return dns_connection_factory(url.host, url.port, url.is_https(), logger, std::move(certs));
    }())
{}

future<connected_socket> utils::http::dns_connection_factory::make(abort_source*) {
    try {
        auto address = co_await get_address();
        co_return co_await connect(address);
    } catch (...) {
        // On failure, forcefully renew address resolution and try again
        _logger.debug("Connection failed, resetting address provider and retrying: {:t}", std::current_exception());
    }
    _addr_list.reset();
    auto address = co_await get_address();
    co_return co_await connect(address);
}

future<> utils::http::dns_connection_factory::close() {
    _addr_update_timer.cancel();
    co_await get_units(_init_semaphore, 1);
}

static const char HTTPS[] = "https";

utils::http::url_info utils::http::parse_simple_url(std::string_view uri) {
    /**
     * https://en.wikipedia.org/wiki/IPv6#Addressing
     * In case a port is included with a numerical ipv6 address, 
     * the address part is encases in a "[]" wrapper, like
     * http://[2001:db8:4006:812::200e]:8080
     */
    static boost::regex simple_url(R"foo(([a-zA-Z]+):\/\/((?:\[[^\]]+\])|[^\/:]+)(:\d+)?(\/.*)?)foo");

    boost::smatch m;
    std::string tmp(uri);

    if (!boost::regex_match(tmp, m, simple_url)) {
        throw std::invalid_argument(fmt::format("Could not parse URI {}", uri));
    }

    auto scheme = m[1].str();
    auto host = m[2].str();
    auto port = m[3].str();
    auto path = m[4].str();

    bool https = (strcasecmp(scheme.c_str(), HTTPS) == 0);

    // check for numeric ipv6 address + port case
    if (host.size() > 2 && host.front() == '[' && host.back() == ']') {
        host = host.substr(1, host.size() - 2);
    }
    return url_info {
        .scheme = std::move(scheme),
        .host = std::move(host),
        .path = std::move(path),
        .port = uint16_t(port.empty() ? (https ? 443 : 80) : std::stoi(port.substr(1)))
    };
}

bool utils::http::url_info::is_https() const {
    return strcasecmp(scheme.c_str(), HTTPS) == 0;
}

std::optional<utils::http::content_range> utils::http::parse_content_range(std::string_view value) {
    // 19 digits always fit in uint64_t, so stoull cannot throw.
    static const boost::regex pattern(R"(bytes (\d{1,19})-(\d{1,19})/(\d{1,19}))");
    boost::cmatch m;
    if (!boost::regex_match(value.begin(), value.end(), m, pattern)) {
        return std::nullopt;
    }
    const auto first = std::stoull(m[1].str());
    const auto last = std::stoull(m[2].str());
    const auto total = std::stoull(m[3].str());
    // RFC 9110 calls a range invalid when it names no bytes or runs past the end.
    if (first > last || last >= total) {
        return std::nullopt;
    }
    return content_range{first, last, total};
}
