/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <string>
#include <memory>
#include <regex>

#include <seastar/core/with_timeout.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/net/inet_address.hh>

#include "azure_kms_fixture.hh"
#include "tmpdir.hh"
#include "proc_utils.hh"
#include "test_utils.hh"

namespace fs = std::filesystem;
namespace tp = tests::proc;

using namespace tests;
using namespace std::chrono_literals;
using namespace std::string_view_literals;

static future<std::tuple<tp::process_fixture, int>> start_fake_azure_server(const tmpdir& tmp, const std::string& host) {
    auto pyexec = tests::proc::find_file_in_path("python");
    auto unshare = tests::proc::find_file_in_path("unshare");
    auto sh = tests::proc::find_file_in_path("sh");

    if (unshare.empty()) {
        throw std::runtime_error("Could not find unshare command");
    }
    if (sh.empty()) {
        throw std::runtime_error("Could not find sh command");
    }

    promise<std::pair<std::string, int>> authority_promise;
    auto fut = authority_promise.get_future();

    BOOST_TEST_MESSAGE("Starting dedicated Azure Vault mock server in network namespace");

    // Build command to bring up loopback and then start Python server
    std::string cmd = fmt::format("ip link set lo up && exec {} test/pylib/start_azure_vault_mock.py --log-level INFO --host {} --port 0",
        pyexec.string(), host);

    auto python = co_await tests::proc::process_fixture::create(unshare,
        { // args - wrap with unshare -n and bring up lo
            unshare.string(),
            "-n",       // Create new network namespace
            sh.string(),// Run shell
            "-c",       // Execute command
            cmd         // Command: bring up lo, then run Python server
        },
        // env
        {},
        // stdout handler
        tests::proc::process_fixture::create_copy_handler(std::cout),
        // stderr handler
        [authority_promise = std::move(authority_promise), b = false](std::string_view line) mutable -> future<consumption_result<char>> {
            static std::regex authority_ex(R"foo(Starting Azure Vault mock server on \('([\d\.]+)', (\d+)\))foo");

            std::cerr << line << std::endl;
            std::match_results<typename std::string_view::const_iterator> m;
            if (!b && std::regex_search(line.begin(), line.end(), m, authority_ex)) {
                authority_promise.set_value(std::make_pair(m[1].str(), std::stoi(m[2].str())));
                BOOST_TEST_MESSAGE("Matched Azure Vault host and port: " + m[1].str() + ":" + m[2].str());
                b = true;
            }
            co_return continue_consuming{};
        }
    );

    std::exception_ptr ep;

    try {
        // arbitrary timeout of 20s for the server to make some output. Very generous.
        auto [host, port] = co_await with_timeout(std::chrono::steady_clock::now() + 20s, std::move(fut));

        // Server is running in an isolated namespace, so we can't connect to it from here.
        // We rely on the "Starting Azure Vault mock server" message in the stderr handler above.
        // The actual connectivity test will happen when the test code enters the namespace.
        BOOST_TEST_MESSAGE(fmt::format("Server startup message received for {}:{}, assuming it's ready", host, port));

        co_return std::make_tuple(std::move(python), port);

    } catch (timed_out_error&) {
        ep = std::make_exception_ptr(std::runtime_error("Could not start dedicated Azure Vault mock server"));
    } catch (...) {
        ep = std::current_exception();
    }

    python.terminate();
    co_await python.wait();
    std::rethrow_exception(ep);
}

class azure_kms_fixture::impl 
    : public azure_test_env
{
public:
    azure_mode mode;
    std::optional<tp::process_fixture> local_azure;
    std::optional<int> _netns_fd;  // Network namespace FD for mock server
    tmpdir tmp;

    impl(azure_mode);

    seastar::future<> setup();
    seastar::future<> teardown();
};

azure_kms_fixture::impl::impl(azure_mode m) 
    : mode(m)
{
    key_name = getenv_or_default("AZURE_KEY_NAME", "");
    tenant_id = getenv_or_default("AZURE_TENANT_ID");
    user_1_client_id = getenv_or_default("AZURE_USER_1_CLIENT_ID");
    user_1_client_secret = getenv_or_default("AZURE_USER_1_CLIENT_SECRET");
    user_1_client_certificate = getenv_or_default("AZURE_USER_1_CLIENT_CERTIFICATE");
    user_2_client_id = getenv_or_default("AZURE_USER_2_CLIENT_ID");
    user_2_client_secret = getenv_or_default("AZURE_USER_2_CLIENT_SECRET");
    user_2_client_certificate = getenv_or_default("AZURE_USER_2_CLIENT_CERTIFICATE", "");
    authority_host = "''";
    imds_endpoint = "''";
}

seastar::future<> azure_kms_fixture::impl::setup() {
    if ((key_name.empty() || mode == azure_mode::local) && mode != azure_mode::real) {
        auto host = getenv_or_default("MOCK_AZURE_VAULT_SERVER_HOST", "127.0.0.1");
        auto [proc, port] = co_await start_fake_azure_server(tmp, host);

        // Get the network namespace FD from the spawned process
        auto netns_path = fmt::format("/proc/{}/ns/net", proc.pid());
        BOOST_TEST_MESSAGE(fmt::format("Opening server process' network namespace file {}", netns_path));
        _netns_fd = ::open(netns_path.c_str(), O_RDONLY);
        if (*_netns_fd < 0) {
            BOOST_FAIL(fmt::format("Failed to open server process' network namespace file {}: {}", netns_path, std::strerror(errno)));
        }

        local_azure.emplace(std::move(proc));
        key_name = fmt::format("http://{}:{}/mock-key", host, port);
        authority_host = imds_endpoint = fmt::format("http://{}:{}", host, port);

        tenant_id = "00000000-1111-2222-3333-444444444444";
        user_1_client_id = "mock-client-id";
        user_1_client_secret = "mock-client-secret";
        user_1_client_certificate = "test/resource/certs/scylla.pem";
        user_2_client_id = "mock-client-id-invalid";
        user_2_client_secret = "mock-client-secret-invalid"; 
        user_2_client_certificate = "/dev/null"; // a cert file with invalid format
    }

    if (!local_azure) {
        if (key_name.empty()) {
            BOOST_ERROR("No 'AZURE_KEY_NAME' provided");
        }
        if (tenant_id.empty()) {
            BOOST_ERROR("No 'AZURE_TENANT_ID' provided");
        }
        if (user_1_client_id.empty() || user_1_client_secret.empty() || user_1_client_certificate.empty()) {
            BOOST_ERROR("Missing or incompete credentials for user 1: All three of 'AZURE_USER_1_CLIENT_ID', 'AZURE_USER_1_CLIENT_SECRET' and 'AZURE_USER_1_CLIENT_CERTIFICATE' must be provided");
        }
        if (user_2_client_id.empty() || user_2_client_secret.empty() || user_2_client_certificate.empty()) {
            BOOST_ERROR("Missing or incompete credentials for user 2: All three of 'AZURE_USER_2_CLIENT_ID', 'AZURE_USER_2_CLIENT_SECRET' and 'AZURE_USER_2_CLIENT_CERTIFICATE' must be provided");
        }
    }
}

seastar::future<> azure_kms_fixture::impl::teardown() {
    if (_netns_fd) {
        ::close(*_netns_fd);
        _netns_fd.reset();
    }
    if (local_azure) {
        local_azure->terminate();
        co_await local_azure->wait();
    }
}

static thread_local azure_kms_fixture* active_azure_kms_fixture = nullptr;

azure_kms_fixture::azure_kms_fixture(azure_mode mode) 
    : _impl(std::make_unique<impl>(mode))
{}

azure_kms_fixture::~azure_kms_fixture() = default;

const azure_test_env& azure_kms_fixture::test_env() const {
    return *_impl;
}

int azure_kms_fixture::netns_fd() const {
    return _impl->_netns_fd.value_or(-1);
}

seastar::future<> azure_kms_fixture::setup() {
    co_await _impl->setup();
    _prev = active_azure_kms_fixture;
    active_azure_kms_fixture = this;
}

seastar::future<> azure_kms_fixture::teardown() {
    active_azure_kms_fixture = _prev;
    return _impl->teardown();
}

azure_kms_fixture* azure_kms_fixture::active() {
    return active_azure_kms_fixture;
}

local_azure_kms_wrapper::local_azure_kms_wrapper(azure_mode mode)
    : _mode(mode)
{}

local_azure_kms_wrapper::~local_azure_kms_wrapper() = default;

seastar::future<> local_azure_kms_wrapper::setup() {
    auto f = azure_kms_fixture::active();
    if (!f || _mode != azure_mode::any) {
        _local = std::make_unique<azure_kms_fixture>(_mode);
        co_await _local->setup();
        f = azure_kms_fixture::active();
    }

    azure_test_env& tmp = *this;
    tmp = f->test_env();

    // Enter network namespace if using mock server
    auto ns_fd = f->netns_fd();
    if (ns_fd >= 0) {
        // Save current namespace
        _orig_netns_fd = ::open("/proc/self/ns/net", O_RDONLY);
        if (_orig_netns_fd < 0) {
            throw std::runtime_error("Failed to save current network namespace");
        }

        // Enter mock server's namespace
        if (::setns(ns_fd, CLONE_NEWNET) < 0) {
            ::close(_orig_netns_fd);
            _orig_netns_fd = -1;
            throw std::runtime_error("Failed to enter network namespace");
        }

        BOOST_TEST_MESSAGE(fmt::format("Entered network namespace (FD {})", ns_fd));
    }
}

seastar::future<> local_azure_kms_wrapper::teardown() {
    // Restore original namespace if we changed it
    if (_orig_netns_fd >= 0) {
        ::setns(_orig_netns_fd, CLONE_NEWNET);
        ::close(_orig_netns_fd);
        _orig_netns_fd = -1;
        BOOST_TEST_MESSAGE("Restored original network namespace");
    }

    if (_local) {
        co_await _local->teardown();
        _local = {};
    }
}
