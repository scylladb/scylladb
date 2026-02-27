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

    promise<std::pair<std::string, int>> authority_promise;
    auto fut = authority_promise.get_future();

    BOOST_TEST_MESSAGE("Starting dedicated Azure Vault mock server");

    auto python = co_await tests::proc::process_fixture::create(pyexec,
        { // args
            pyexec.string(),
            "test/pylib/start_azure_vault_mock.py",
            "--log-level", "INFO",
            "--host", host,
            "--port", "0", // random port
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

        // wait for port.
        auto sleep_interval = 100ms;
        auto timeout = 5s;
        auto end_time = seastar::lowres_clock::now() + timeout;
        bool connected = false;
        while (seastar::lowres_clock::now() < end_time) {
            BOOST_TEST_MESSAGE(fmt::format("Connecting to {}:{}", host, port));
            try {
                // TODO: seastar does not have a connect with timeout. That would be helpful here. But alas...
                co_await seastar::connect(socket_address(net::inet_address(host), uint16_t(port)));
                BOOST_TEST_MESSAGE("Dedicated Azure Vault mock server up and available");
                connected = true;
                break;
            } catch (...) {
            }
            co_await seastar::sleep(sleep_interval);
        }

        if (!connected) {
            throw std::runtime_error(fmt::format("Timed out connecting to Azure Vault mock server at {}:{}", host, port));
        }

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
}

seastar::future<> local_azure_kms_wrapper::teardown() {
    if (_local) {
        co_await _local->teardown();
        _local = {};
    }
}
