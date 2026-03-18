/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <string>
#include <memory>

#include <seastar/core/with_timeout.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>

#include "gcs_fixture.hh"
#include "tmpdir.hh"
#include "proc_utils.hh"
#include "test_utils.hh"

#include "utils/gcp/gcp_credentials.hh"
#include "utils/UUID_gen.hh"

namespace fs = std::filesystem;
namespace tp = tests::proc;

using namespace utils::gcp;
using namespace tests;
using namespace std::chrono_literals;
using namespace std::string_view_literals;

static future<std::optional<google_credentials>> credentials(const std::string& source) {
    try {
        if (!source.empty()) {
            BOOST_TEST_MESSAGE(fmt::format("Loading credentials from {}", source));
            co_return co_await google_credentials::from_file(source);
        }
        BOOST_TEST_MESSAGE("Loading default credentials");
        co_return co_await google_credentials::get_default_credentials();
    } catch (...) {    
        BOOST_TEST_MESSAGE(fmt::format("Warning: could not load {} credentials: {}", source, std::current_exception()));
    }
    co_return std::nullopt; // empty, useless. will work on fake server 
}

static future<std::tuple<tp::process_fixture, int>> start_fake_gcs_server(const tmpdir& tmp) {
    return tp::start_docker_service("local-kms"
        , "docker.io/fsouza/fake-gcs-server:1.52.3"
        , {}
        , [](std::string_view line) {
            if (line.find("server started at") != std::string::npos) {
                return tp::service_parse_state::success;
            }
            if (line.find("address already in use") != std::string::npos) {
                return tp::service_parse_state::failed;
            }
            return tp::service_parse_state::cont;
        }
        , {} 
        , { "-scheme", "http", "-log-level", "debug", "--port", "4443", "-public-host", "127.0.0.1" } // image args
        , 4443
    );
}

class gcs_fixture::impl {
public:
    std::optional<tp::process_fixture> fake_gcs_server;
    std::optional<google_credentials> creds;

    std::vector<std::string> objects_to_delete;
    std::string endpoint;
    std::string project;
    std::string bucket; 
    std::string user_1_creds;
    std::string user_2_creds;

    tmpdir tmp;

    bool created_bucket = false;
    std::unique_ptr<storage::client> client;

    std::vector<tmp_set_env> variables;

    impl();

    seastar::future<> setup();
    seastar::future<> teardown();
};

gcs_fixture::impl::impl()  
    : endpoint(getenv_or_default({"GCP_STORAGE_ENDPOINT"sv, "GS_SERVER_ADDRESS_FOR_TEST"sv}))
    , project(getenv_or_default("GCP_STORAGE_PROJECT"))
    , bucket(getenv_or_default({"GCP_STORAGE_BUCKET"sv, "GS_BUCKET_FOR_TEST"sv}))
    , user_1_creds(getenv_or_default({"GCP_STORAGE_USER_1_CREDENTIALS"sv, "GS_CREDENTIALS_FILE"sv}))
    , user_2_creds(getenv_or_default("GCP_STORAGE_USER_2_CREDENTIALS"))
{}

seastar::future<> gcs_fixture::impl::setup() {
    if (!bucket.empty() && endpoint.empty()) {
        endpoint = storage::client::DEFAULT_ENDPOINT;
    }
    if (endpoint.empty()) {
        auto [proc, port] = co_await start_fake_gcs_server(tmp);
        fake_gcs_server.emplace(std::move(proc));
        endpoint = "http://127.0.0.1:" + std::to_string(port);
        BOOST_TEST_MESSAGE(fmt::format("Test server endpoint {}", endpoint));
        user_1_creds = "none";
    } else {
        creds = co_await credentials(user_1_creds);
    }

    client = std::make_unique<storage::client>(endpoint, std::move(creds));
    std::exception_ptr p;

    try {
        if (bucket.empty()) {
            bucket = "test-" + fmt::format("{}", utils::UUID_gen::get_time_UUID());
            co_await client->create_bucket(project, bucket);
            created_bucket = true;
            BOOST_TEST_MESSAGE(fmt::format("Created test bucket {}", bucket));
        }
    } catch (...) {
        p = std::current_exception();
    }

    if (p) {
        try {
            co_await teardown();
        } catch (...) {
        }
        std::rethrow_exception(p);
    }

    variables.emplace_back("GS_SERVER_ADDRESS_FOR_TEST", endpoint);
    variables.emplace_back("GS_BUCKET_FOR_TEST", bucket);
    variables.emplace_back("GS_CREDENTIALS_FILE", user_1_creds);
}

seastar::future<> gcs_fixture::impl::teardown() {
    variables.clear();

    if (client) {
        for (auto& name : objects_to_delete) {
            try {
                co_await client->delete_object(bucket, name);
            } catch (...) {
                BOOST_TEST_MESSAGE(fmt::format("Warning: could not delete object: {}", name, std::current_exception()));
            }
        }

        if (created_bucket) {
            try {
                auto objects = co_await client->list_objects(bucket);
                for (auto& o : objects) {
                    co_await client->delete_object(bucket, o.name);
                }
                co_await client->delete_bucket(bucket);
            } catch (...) {
                BOOST_TEST_MESSAGE(fmt::format("Warning: could not delete bucket: {}", bucket, std::current_exception()));
            }
        }
    }

    if (fake_gcs_server) {
        fake_gcs_server->terminate();
        co_await fake_gcs_server->wait();
    }

    if (client) {
        co_await client->close();
        client = {};
    }
}

static thread_local gcs_fixture* active_gcs_fixture = nullptr;

gcs_fixture::gcs_fixture() 
    : _impl(std::make_unique<impl>())
{}

gcs_fixture::~gcs_fixture() = default;

utils::gcp::storage::client& gcs_fixture::client() const {
    return *_impl->client;
}

const std::string& gcs_fixture::endpoint() const {
    return _impl->endpoint;
}
const std::string& gcs_fixture::project() const {
    return _impl->project;
}
const std::string& gcs_fixture::bucket() const {
    return _impl->bucket;
}

void gcs_fixture::add_object_to_delete(const std::string& name) {
    _impl->objects_to_delete.emplace_back(name);
}

seastar::future<> gcs_fixture::setup() {
    co_await _impl->setup();
    active_gcs_fixture = this;
}

seastar::future<> gcs_fixture::teardown() {
    active_gcs_fixture = nullptr;
    return _impl->teardown();
}

gcs_fixture* gcs_fixture::active() {
    return active_gcs_fixture;
}

local_gcs_wrapper::local_gcs_wrapper() = default;
local_gcs_wrapper::~local_gcs_wrapper() = default;

utils::gcp::storage::client& local_gcs_wrapper::client() const {
    return gcs_fixture::active()->client();
}

seastar::future<> local_gcs_wrapper::setup() {
    auto f = gcs_fixture::active();
    if (!f) {
        _local = std::make_unique<gcs_fixture>();
        co_await _local->setup();
        f = gcs_fixture::active();;
    }

    endpoint = f->endpoint();
    project = f->project();
    bucket = f->bucket();
}

seastar::future<> local_gcs_wrapper::teardown() {
    auto f = gcs_fixture::active();
    assert(f);

    auto& c = client();
    for (auto& name : objects_to_delete) {
        try {
            co_await c.delete_object(bucket, name);
        } catch (...) {
            BOOST_TEST_MESSAGE(fmt::format("Warning: could not delete object: {}", name, std::current_exception()));
        }
    }

    if (_local) {
        co_await _local->teardown();
        _local = {};
    }
}
