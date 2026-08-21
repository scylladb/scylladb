/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <string>
#include <memory>
#include <regex>

#include <seastar/core/with_timeout.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/net/inet_address.hh>

#include "gcp_kms_fixture.hh"
#include "tmpdir.hh"
#include "proc_utils.hh"
#include "test_utils.hh"
#include "ent/encryption/encryption.hh"

namespace fs = std::filesystem;
namespace tp = tests::proc;

using namespace tests;
using namespace std::chrono_literals;
using namespace std::string_view_literals;

static future<std::tuple<tp::process_fixture, int>> start_fake_kms_server(const tmpdir& tmp, const fs::path& seed) {
    auto pyexec = tests::proc::find_file_in_path("python");

    promise<int> port_promise;
    auto port_future = port_promise.get_future();

    auto python = co_await tests::proc::process_fixture::create(pyexec, 
        { // args
            pyexec.string(),
            "test/pylib/gcp_kms_server_mock.py",
            "--seed", seed.string(),
            "--log-level", "DEBUG",
        }
        ,{} // env
        // stdout handler
        , tests::proc::process_fixture::create_copy_handler(std::cout)
        // stderr handler
        , [port_promise = std::move(port_promise), b = false](std::string_view line) mutable -> future<consumption_result<char>> {
            static std::regex port_ex(R"foo(Starting GCP KMS mock server on \('[^']+', (\d+)\))foo");

            std::match_results<typename std::string_view::const_iterator> m;
            if (!b && std::regex_search(line.begin(), line.end(), m, port_ex)) {
                port_promise.set_value(std::stoi(m[1].str()));
                BOOST_TEST_MESSAGE("Matched Mock GCP KMS port: " + m[1].str());
                b = true;
            }
            co_return continue_consuming{};
        }
    );

    // arbitrary timeout of 20s for the server to make some output. Very generous.
    auto port = co_await with_timeout(std::chrono::steady_clock::now() + 20s, std::move(port_future));

    if (port <= 0) {
        throw std::runtime_error("Invalid port");
    }

    // wait for port.
    for (size_t retry = 0; retry < 5; ++retry) {
        try {
            BOOST_TEST_MESSAGE(fmt::format("Attempting to connect to GCP KMS server at {}", port));
            // TODO: seastar does not have a connect with timeout. That would be helpful here. But alas...
            auto c = co_await with_timeout(std::chrono::steady_clock::now() + 20s, seastar::connect(socket_address(net::inet_address("127.0.0.1"), port)));
            BOOST_TEST_MESSAGE("Mock GCP KMS server up and available"); // debug print. Why not.
            c.shutdown_output();
            break;
        } catch (...) {
        }
        co_await sleep(100ms);
    }

    co_return std::make_tuple(std::move(python), port);
}

class gcp_kms_fixture::impl {
public:
    std::optional<tp::process_fixture> local_kms;

    std::string endpoint;
    std::string gcp_key_name;
    std::string gcp_location;
    std::string gcp_project_id;
    std::string gcp_user_1_credentials;
    std::string gcp_user_2_credentials;
    std::string gcp_iam_endpoint_override;

    tmpdir tmp;

    bool created_master_key = false;

    impl();

    seastar::future<> setup();
    seastar::future<> teardown();
};

gcp_kms_fixture::impl::impl()  
    : gcp_key_name(getenv_or_default("GCP_KEY_NAME", "test_ring/test_key"))
    , gcp_location(getenv_or_default("GCP_LOCATION", "global"))
    , gcp_project_id(getenv_or_default("GCP_PROJECT_ID", "scylla-kms-test"))
    , gcp_user_1_credentials(getenv_or_default("GCP_USER_1_CREDENTIALS"))
    , gcp_user_2_credentials(getenv_or_default("GCP_USER_2_CREDENTIALS"))
{}

seastar::future<> gcp_kms_fixture::impl::setup() {
    if (gcp_key_name.empty() || gcp_user_1_credentials.empty() || gcp_user_2_credentials.empty()) {
        auto seed = tmp.path() / "seed.yaml";
        auto cred1 = tmp.path() / "cred1.json";
        auto cred2 = tmp.path() / "cred2.json";
        auto user1 = "user1@apa.org";
        auto user2 = "user2@apa.org";

        co_await encryption::write_text_file_fully(seed.string(), fmt::format(R"foo(
projects:
  {}:
    locations:
      {}:
        keyRings:
          test-ring:
            cryptoKeys:
              test-key:
                purpose: ENCRYPT_DECRYPT
                labels:
                  env: dev
                versions:
                  - {{}}   # creates version 1
                users:
                  - {}
impersonators:
  {}:
    - {}
)foo", gcp_project_id, gcp_location, user1, user2, user1
        ));

        auto [proc, port] = co_await start_fake_kms_server(tmp, seed);
        local_kms.emplace(std::move(proc));

        for (auto [user, cred] : { std::make_pair(user1, cred1), std::make_pair(user2, cred2) }) {
            // create a fake service account file. the private key, id:s etc are ignored
            // by the fake token endpoint. Only the actual email + token endpoint here matters.
            // the mock server will give us a "Bearer" token that works as auth for the mock.
            // nothing even close to actual gcp, but...
            co_await encryption::write_text_file_fully(cred.string(), fmt::format(R"foo(
{{
        "type": "service_account",
        "project_id": "{}",
        "private_key_id": "c68de079f19d430a742bf18fed675a00caac13a2",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDCZ+yap8UNnxeR\nM1Ho10jx6JnpXdktDrBhF/vK/ENYmjiIjV1RsULtqJDHDrVJhK+k7chiA64M70XT\nOP/ABlvoNqPmX90WWroQd4HbEPnXMEMJYBiKg1vUtsUImB8soxKxCp0uY3sn+6cx\n8LNjvmI4waCclTTPIoLeh9s8USu5gr/ZrBQUjUlKLqSdoQfw4Tt1XRY0z4NDT3iX\naIrFeuwrXvnHKLFJ9j+jkdZm5qtjtBDR5C0tQbzUeD0xX0XTrPmgF8yt0JgmqUxs\nfp4/yGAOpuFpj5w11mRo6e4Kq2xvlxRJaBqoyd/70EgccwwBQ4gAN187CYp7PSuG\nFdD4MvAdAgMBAAECggEACVewNbB5VlG+crJqLcvmzAVXHDFv5evuSwQ5jAQ6glAL\nBnjwsqPXqQ8wQfixeqJ/RGhO+HLf0uxOyTtUgxhrI0o47zHNMK1UgsUTfwEeWJqP\npiwxkbqFV8Ae0O5qlR0TIWH2ssuCGCZOXyaHoHP+SWb4vn2nJ4srieEyhoAKH2SV\nOLlhB80QUd1xqVB9D6N6Ee13JQyWbDTqZPd8rSHJ3EmR9qxZtpxdtTKobpGNoFlH\nOXL26LAUMJ+N4A0Z6/RX5i/HJax5k2lrguawRzWibj8JtoH3V7iqpDmtSShpZdY4\n888EMePBo5AN0v01UYpOUQwUuMrPMp0EVUTjnDvnoQKBgQDon5tPo+axMvBImlA6\nZNoI5dnws3x4wZKHbhgGryU4kW9iGHKNU8sTVkvja40bJMNaPT+7wtuB+nOuLNjX\nZNOM4wRcqBgXPQ6elgrBwY5Pf2TVmiqdjvNLNWaI+3lRNiDg6Gf6TRQJ/0wa9IIt\nwlAgenpS1lI4I67oehjEwTYw0QKBgQDV8SWOFoAjNkWsN8/mCxxQZi1YwLS+xJsz\nlDYBlAAxVuyGJXCnJ7Q7AkKbIy365ZPh4sfFnLbfT5LNt//UUP87rpfcVeVuwUnP\nGM2+1Umo2j5ur9edlca97fMywj7c+3lOe/LBTkMP9KgnOAhAqLrsrOzSH77ChLtB\nmePcIER9jQKBgAMbFmzCyHK3NmQRw150OEEEKJvBGblXBEjQnHuCXSHbNzx9DRJ7\n+usgLNU1e2XQYNdUmAQ+vsWGfYLm0GJX00c/RLCkAeZVh1twr2YU2nyPO95qN4Vx\nAiiP5vWPPfhqm5fFIpZB7zGO+gomF5La1E0KtZVjjSd4un4aGziNR9bxAoGAFaB/\n/GIX5/dXibZGpOmgnhwGH3+zhclYKxmjb/tnHZW86T6lqbAgzwpGc2pV/pPwpBgJ\nu9dAwUhI/dTI3sylUIIwxcxFGjId5PqL6euju5b8UrIh6MM4SQDh4dKzCiG9vIpZ\nGuNvchB4YyaN5wNnif9dHUyqOv2x9Eq7NwhoBA0CgYEA1TeeQvYZgRK42jT5yC/R\nuPS+Eb8IhpeWHU52T+1SgSUFYmgNAbE7n0nHVzYE64IvsPmVZLrLhhXADjRHICvK\nhTtML1lBustG7Z9Tu66EZdQkvnJDwmVSZqVr2FmoOlXVS/qj4Tcc5kWvVp5ogI3u\npF0cRpeqEwY4dSWhiCznRkA=\n-----END PRIVATE KEY-----\n",
        "client_email": "{}",
        "client_id": "100849414604266807639",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "http://127.0.0.1:{}/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test1-262%40scylla-kms-test.iam.gserviceaccount.com",
        "universe_domain": "googleapis.com"
}})foo", gcp_project_id, user, port));
        }

        gcp_key_name = "test-ring/test-key";
        gcp_user_1_credentials = cred1.string();
        gcp_user_2_credentials = cred2.string();
        endpoint = "http://127.0.0.1:" + std::to_string(port);
        gcp_iam_endpoint_override = fmt::format("http://127.0.0.1:{}/v1/projects/-/serviceAccounts/{}:generateAccessToken", port, user1);
        BOOST_TEST_MESSAGE(fmt::format("Test server endpoint {}, alias {}", endpoint, gcp_key_name));
    }
}

seastar::future<> gcp_kms_fixture::impl::teardown() {
    if (local_kms) {
        local_kms->terminate();
        co_await local_kms->wait();
    }
}

static thread_local gcp_kms_fixture* active_gcp_kms_fixture = nullptr;

gcp_kms_fixture::gcp_kms_fixture() 
    : _impl(std::make_unique<impl>())
{}

gcp_kms_fixture::~gcp_kms_fixture() = default;

const std::string& gcp_kms_fixture::endpoint() const {
    return _impl->endpoint;
}
const std::string& gcp_kms_fixture::gcp_key_name() const {
    return _impl->gcp_key_name;
}
const std::string& gcp_kms_fixture::gcp_location() const {
    return _impl->gcp_location;
}
const std::string& gcp_kms_fixture::gcp_project_id() const {
    return _impl->gcp_project_id;
}
const std::string& gcp_kms_fixture::gcp_user_1_credentials() const {
    return _impl->gcp_user_1_credentials;
}
const std::string& gcp_kms_fixture::gcp_user_2_credentials() const {
    return _impl->gcp_user_2_credentials;
}
const std::string& gcp_kms_fixture::gcp_iam_endpoint_override() const {
    return _impl->gcp_iam_endpoint_override;
}

seastar::future<> gcp_kms_fixture::setup() {
    co_await _impl->setup();
    active_gcp_kms_fixture = this;
}

seastar::future<> gcp_kms_fixture::teardown() {
    active_gcp_kms_fixture = nullptr;
    return _impl->teardown();
}

gcp_kms_fixture* gcp_kms_fixture::active() {
    return active_gcp_kms_fixture;
}

local_gcp_kms_wrapper::local_gcp_kms_wrapper() = default;
local_gcp_kms_wrapper::~local_gcp_kms_wrapper() = default;

seastar::future<> local_gcp_kms_wrapper::setup() {
    auto f = gcp_kms_fixture::active();
    if (!f) {
        _local = std::make_unique<gcp_kms_fixture>();
        co_await _local->setup();
        f = gcp_kms_fixture::active();;
    }

    endpoint = f->endpoint();
    gcp_key_name = f->gcp_key_name();
    gcp_location = f->gcp_location();
    gcp_project_id = f->gcp_project_id();
    gcp_user_1_credentials = f->gcp_user_1_credentials();
    gcp_user_2_credentials = f->gcp_user_2_credentials();
    gcp_iam_endpoint_override = f->gcp_iam_endpoint_override();
}

seastar::future<> local_gcp_kms_wrapper::teardown() {
    if (_local) {
        co_await _local->teardown();
        _local = {};
    }
}
