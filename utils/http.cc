/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

/*
 * Copyright (C) 2025-present ScyllaDB
 */

#include "http.hh"
#include "rest/client.hh"

#include <boost/regex.hpp>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/coroutine/all.hh>

static constexpr std::string_view pem_begin_marker = "-----BEGIN CERTIFICATE-----";
static constexpr std::string_view pem_end_marker = "-----END CERTIFICATE-----";
// A certificate in a CA bundle is a few KB. Anything longer without an end
// marker is not a certificate, and must not grow a buffer to the file's size.
static constexpr size_t max_pem_certificate_size = 64 * 1024;

// Text outside the certificates is skipped, as gnutls and p11-kit skip it:
// comments, or what openssl x509 -text prints before a certificate. But a
// PEM marker there, whole or not, is a certificate cut short.
static bool has_pem_marker(std::string_view text) {
    return text.find("-----") != std::string_view::npos;
}

// A truncated, empty or corrupt bundle is never taken for the host's trust.
// The bundle is a few hundred KB. Reading it whole would be an oversized
// allocation on the shard (seastar warns above 128 KiB, and SCT turns that
// warning into a test error), so it is read and checked one certificate at a
// time.
future<std::vector<sstring>> utils::http::internal::read_ca_bundle(const char* path) {
    auto f = co_await open_file_dma(path, open_flags::ro);
    auto in = make_file_input_stream(std::move(f), file_input_stream_options{.buffer_size = 32 * 1024});
    std::vector<sstring> certificates;
    std::exception_ptr ex;
    try {
        // Only to have gnutls reject a malformed certificate here, before the
        // caller has taken any of the bundle.
        tls::certificate_credentials check;
        std::string pending;
        for (auto buf = co_await in.read(); !buf.empty(); buf = co_await in.read()) {
            // Checked before the append, so that not even the append allocates
            // more than the bound.
            if (pending.size() + buf.size() > max_pem_certificate_size) {
                throw std::runtime_error(fmt::format("no certificate end within {} bytes", max_pem_certificate_size));
            }
            pending.append(buf.get(), buf.size());
            for (auto pos = pending.find(pem_end_marker); pos != std::string::npos; pos = pending.find(pem_end_marker)) {
                auto begin = pending.find(pem_begin_marker);
                if (begin > pos) {
                    throw std::runtime_error("certificate end without a beginning");
                }
                // gnutls would skip anything before the BEGIN line, a
                // certificate cut short among them.
                if (has_pem_marker(std::string_view(pending).substr(0, begin))) {
                    throw std::runtime_error("a broken PEM marker before a certificate");
                }
                auto end = pos + pem_end_marker.size();
                check.set_x509_trust(tls::blob(pending.data(), end), tls::x509_crt_format::PEM);
                certificates.emplace_back(pending.data(), end);
                pending.erase(0, end);
            }
        }
        // A marker after the last certificate is a certificate cut short, even
        // inside its BEGIN line, and so is a last line of fewer dashes than a
        // marker has.
        auto tail = std::string_view(pending);
        tail = tail.substr(0, tail.find_last_not_of(" \t\r\n") + 1);
        auto last_line = tail.substr(tail.find_last_of('\n') + 1);
        if (has_pem_marker(tail) || last_line.starts_with('-')) {
            throw std::runtime_error("the file ends inside a certificate");
        }
        if (certificates.empty()) {
            throw std::runtime_error("no certificates");
        }
    } catch (...) {
        ex = std::current_exception();
    }
    co_await in.close();
    if (ex) {
        std::rethrow_exception(std::move(ex));
    }
    co_return certificates;
}

future<> utils::http::add_system_trust(tls::credentials_builder& builder) {
    co_await builder.set_system_trust();
}

future<shared_ptr<tls::certificate_credentials>> utils::http::system_trust_credentials() {
    static thread_local shared_ptr<tls::certificate_credentials> system_trust_credentials;
    if (!system_trust_credentials) {
        // can race, and overwrite the object. that is fine.
        tls::credentials_builder builder;
        co_await add_system_trust(builder);
        system_trust_credentials = builder.build_certificate_credentials();
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
