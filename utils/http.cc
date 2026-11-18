/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

/*
 * Copyright (C) 2025-present ScyllaDB
 */

#include "http.hh"
#include "rest/client.hh"

#include <algorithm>
#include <array>
#include <ranges>
#include <span>

#include <boost/regex.hpp>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/coroutine/all.hh>

static logging::logger trust_log("http_trust");

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

static bool is_not_found(const std::exception_ptr& ex) {
    try {
        std::rethrow_exception(ex);
    } catch (const std::system_error& e) {
        return e.code() == std::errc::no_such_file_or_directory;
    } catch (...) {
        return false;
    }
}

// What add_host_trust() put into the builder, and what went wrong on the
// way.
struct host_trust {
    sstring source;
    std::vector<sstring> problems;
};

// p11-kit can distrust a certificate, an intermediate for instance, which a
// PEM bundle cannot express: a bundle only lists the trusted anchors. On the
// RHEL family an administrator does it by putting the certificate into one of
// these directories (the older releases call them blacklist). When there is
// anything in them, or they cannot be listed, the system trust store stays in
// use.
static constexpr std::array p11_kit_blocklist_dirs = {
    "/etc/pki/ca-trust/source/blocklist",
    "/usr/share/pki/ca-trust-source/blocklist",
    "/etc/pki/ca-trust/source/blacklist",
    "/usr/share/pki/ca-trust-source/blacklist",
};

// The names in a directory, sorted. Listed with plain seastar calls: the
// storage layer's lister treats an error opening a directory as a disk error,
// which isolates the node.
static future<std::vector<sstring>> list_directory_names(const char* dir) {
    auto f = co_await open_directory(dir);
    std::vector<sstring> names;
    std::exception_ptr ex;
    try {
        auto entries = f.experimental_list_directory();
        while (auto entry = co_await entries()) {
            if (entry->name != "." && entry->name != "..") {
                names.push_back(entry->name);
            }
        }
    } catch (...) {
        ex = std::current_exception();
    }
    co_await f.close();
    if (ex) {
        std::rethrow_exception(std::move(ex));
    }
    std::ranges::sort(names);
    co_return names;
}

static future<bool> host_distrusts_certificates(host_trust& result) {
    for (auto dir : p11_kit_blocklist_dirs) {
        try {
            if (!(co_await list_directory_names(dir)).empty()) {
                co_return true;
            }
        } catch (...) {
            if (auto ex = std::current_exception(); !is_not_found(ex)) {
                result.problems.push_back(fmt::format("Could not list {}: {}", dir, ex));
                co_return true;
            }
        }
    }
    co_return false;
}

// Besides the blocklists, p11-kit takes trust from every file directly in
// these directories and in their anchors directories, and such a file can
// distrust a certificate too: a .p11-kit file can mark one x-distrusted, and
// an OpenSSL TRUSTED CERTIFICATE can reject its uses. The bundle does not
// carry that either. Other subdirectories are not read by p11-kit.
static constexpr std::array p11_kit_source_dirs = {
    "/etc/pki/ca-trust/source",
    "/usr/share/pki/ca-trust-source",
    "/etc/pki/ca-trust/source/anchors",
    "/usr/share/pki/ca-trust-source/anchors",
};

// What the distributions put there themselves. ca-bundle.trust.p11-kit is the
// distribution's trust store, which the bundles are generated from, and
// distrusts no certificate in RHEL 8 to 10, Amazon Linux 2023 or Fedora 44;
// ca-bundle.legacy.crt links to a list of plain certificates, empty in all of
// them.
static bool is_distribution_trust_entry(std::string_view name) {
    return name == "README" || name == "ca-bundle.trust.p11-kit" || name == "ca-bundle.legacy.crt"
            || name == "anchors" || name == "blocklist" || name == "blacklist";
}

future<bool> utils::http::internal::may_record_distrust(const char* path) {
    static constexpr std::array markers = {
        std::string_view("[p11-kit-object"),
        std::string_view("-----BEGIN TRUSTED CERTIFICATE-----"),
    };
    // Kept from one read to the next, so that a marker split between two
    // reads is found too.
    static constexpr size_t overlap = markers[1].size() - 1;
    auto f = co_await open_file_dma(path, open_flags::ro);
    auto in = make_file_input_stream(std::move(f), file_input_stream_options{.buffer_size = 32 * 1024});
    bool found = false;
    std::exception_ptr ex;
    try {
        std::string window;
        for (auto buf = co_await in.read(); !buf.empty(); buf = co_await in.read()) {
            window.append(buf.get(), buf.size());
            if (std::ranges::any_of(markers, [&] (std::string_view marker) { return window.find(marker) != std::string::npos; })) {
                found = true;
                break;
            }
            window.erase(0, window.size() - std::min(window.size(), overlap));
        }
    } catch (...) {
        ex = std::current_exception();
    }
    co_await in.close();
    if (ex) {
        std::rethrow_exception(std::move(ex));
    }
    co_return found;
}

// Whether a file the administrator put into the trust source directories may
// distrust a certificate. Plain PEM or DER certificates cannot, so a private
// CA added as an anchor keeps the bundle in use.
static future<bool> host_trust_sources_may_distrust(host_trust& result) {
    for (auto dir : p11_kit_source_dirs) {
        std::vector<sstring> names;
        try {
            names = co_await list_directory_names(dir);
        } catch (...) {
            if (auto ex = std::current_exception(); !is_not_found(ex)) {
                result.problems.push_back(fmt::format("Could not list {}: {}", dir, ex));
                co_return true;
            }
            continue;
        }
        for (const auto& name : names) {
            if (is_distribution_trust_entry(name)) {
                continue;
            }
            auto path = fmt::format("{}/{}", dir, name);
            try {
                if ((co_await file_stat(path)).type == directory_entry_type::directory) {
                    continue;
                }
                if (co_await utils::http::internal::may_record_distrust(path.c_str())) {
                    co_return true;
                }
            } catch (...) {
                // Removed since the listing, or a dangling link p11-kit cannot
                // read either.
                if (auto ex = std::current_exception(); !is_not_found(ex)) {
                    result.problems.push_back(fmt::format("Could not read {}: {}", path, ex));
                    co_return true;
                }
            }
        }
    }
    co_return false;
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
// preemption. The bundles are generated from the trusted set only, by
// update-ca-trust and update-ca-certificates.
static constexpr std::array rhel_bundles = {
    "/etc/pki/tls/certs/ca-bundle.crt",                  // RHEL, Amazon Linux, older Fedora
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem", // what that links to; all Fedora has
};
static constexpr std::array debian_bundles = {
    "/etc/ssl/certs/ca-certificates.crt",                // Debian, Ubuntu
};
static constexpr std::array all_bundles = {rhel_bundles[0], debian_bundles[0], rhel_bundles[1]};

// The p11-kit trust paths the packages configure through SCYLLA_P11_TRUST_PATHS
// (install.sh, dist/redhat/scylla.spec, dist/debian/debian/rules).
static constexpr std::string_view rhel_p11_trust_paths = "/etc/pki/ca-trust/source:/usr/share/pki/ca-trust-source";
static constexpr std::string_view debian_p11_trust_paths = "/etc/ssl/certs/ca-certificates.crt";

// The bundles that hold what p11-kit reads, and whether p11-kit also reads
// the RHEL family's trust source directories, which can distrust what a
// bundle cannot express.
struct trust_bundles {
    std::span<const char* const> bundles;
    bool rhel_sources;
};

// Without SCYLLA_P11_TRUST_PATHS gnutls uses p11-kit's own configuration,
// whichever distribution it is, so every bundle is a candidate. A value other
// than the packaged ones is the site's own trust configuration, which only
// the system trust store reads.
static std::optional<trust_bundles> bundles_for_p11_trust_paths() {
    auto paths = std::getenv("SCYLLA_P11_TRUST_PATHS");
    if (!paths) {
        return trust_bundles{all_bundles, true};
    }
    if (paths == rhel_p11_trust_paths) {
        return trust_bundles{rhel_bundles, true};
    }
    if (paths == debian_p11_trust_paths) {
        return trust_bundles{debian_bundles, false};
    }
    return std::nullopt;
}

static future<host_trust> add_host_trust(tls::credentials_builder& builder) {
    host_trust result;
    auto choice = bundles_for_p11_trust_paths();
    if (!choice) {
        result.source = "SCYLLA_P11_TRUST_PATHS names the site's own trust, using the system trust store";
        co_await builder.set_system_trust();
        co_return result;
    }
    if (choice->rhel_sources && (co_await host_distrusts_certificates(result) || co_await host_trust_sources_may_distrust(result))) {
        result.source = "The host may distrust certificates a CA bundle cannot express, using the system trust store";
        co_await builder.set_system_trust();
        co_return result;
    }
    for (auto path : choice->bundles) {
        std::vector<sstring> certificates;
        try {
            certificates = co_await utils::http::internal::read_ca_bundle(path);
        } catch (...) {
            // Any error makes the bundle unusable; only its absence is expected.
            if (auto ex = std::current_exception(); !is_not_found(ex)) {
                result.problems.push_back(fmt::format("Could not load the CA trust list from {}: {}", path, ex));
            }
            continue;
        }
        for (const auto& certificate : certificates) {
            builder.set_x509_trust(certificate, tls::x509_crt_format::PEM);
        }
        result.source = fmt::format("Loaded {} trusted certificates from {}", certificates.size(), path);
        co_return result;
    }
    result.source = "No usable CA bundle file, using the system trust store";
    co_await builder.set_system_trust();
    co_return result;
}

future<> utils::http::add_system_trust(tls::credentials_builder& builder) {
    auto result = co_await add_host_trust(builder);
    for (const auto& problem : result.problems) {
        trust_log.warn("{}", problem);
    }
    trust_log.info("{}", result.source);
}

struct system_trust_cache {
    shared_ptr<tls::certificate_credentials> credentials;
    // Taken by the fiber doing the first load, so that the others wait for
    // its result instead of each loading the bundle again.
    semaphore first_load{1};
};

static thread_local system_trust_cache system_trust;

future<shared_ptr<tls::certificate_credentials>> utils::http::system_trust_credentials() {
    auto& trust = system_trust;
    if (!trust.credentials) {
        auto units = co_await get_units(trust.first_load, 1);
        if (!trust.credentials) {
            tls::credentials_builder builder;
            co_await add_system_trust(builder);
            trust.credentials = builder.build_certificate_credentials();
        }
    }
    co_return trust.credentials;
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
