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
    std::vector<std::string> params({
        "", "-filesystem-root", (tmp.path() / "objects").string()
    });

    size_t port_index = 0;

    auto exec = tp::find_file_in_path("fake-gcs-server");
    if (exec.empty()) {
        exec = tp::find_file_in_path("docker");
        if (exec.empty()) {
            exec = tp::find_file_in_path("podman");
        }
        if (exec.empty()) {
            throw std::runtime_error("Could not find fake-gcs-server, docker or podman.");
        }

        // publish port ephemeral, allows parallel instances
        params = {
            "",
            "run", "--rm", "-p", "--", // set below
            "docker.io/fsouza/fake-gcs-server:1.52.3"
        };
        port_index = 4;
    }

    params[0] = exec.string();
    params.emplace_back("-scheme");
    params.emplace_back("http");
    params.emplace_back("-log-level");
    params.emplace_back("debug");

    auto len = params.size();

    struct in_use{};

    constexpr auto max_retries = 8;

    for (int retries = 0;; ++retries) {
        // podman in podman is hell. we run our "dbuild" with host network mode
        // so _cannot_ rely on ephermal ports or anything nice, atomic to 
        // pick a port. And fake-gcs-server does not support port=0 to pick one.
        // *sigh*. Fall back to create a socket, get the port, close it and hope
        // noone manages to steal the port again before we start our test server.
        // This is not reliable. At all. But works most of the time (right...)
        in_port_t port;
        {
            auto tmp_socket = seastar::listen({});
            port = tmp_socket.local_address().port();
        }

        auto port_string = std::to_string(port);

        params.resize(len);
        if (port_index != 0) {
            params[port_index] = port_string + ":" + port_string;
        }
        params.emplace_back("--port");
        params.emplace_back(port_string);

        BOOST_TEST_MESSAGE(fmt::format("Will run {}", params));

        promise<> ready_promise;
        auto ready_fut = ready_promise.get_future();

        auto ps = co_await tp::process_fixture::create(exec
            , params
            , {}
            , tp::process_fixture::create_copy_handler(std::cout)
            // Must look at stderr log for state, because if we are using podman (and docker?)
            // the server port might in fact be connectible even before the actual 
            // fake-gcs-server is up (in container), due to the port publisher.
            // Could do actual HTTP connection etc, but seems tricky.
            , [done = false, ready_promise = std::move(ready_promise), h = tp::process_fixture::create_copy_handler(std::cerr)](std::string_view line) mutable -> future<consumption_result<char>> {
                if (!done && line.find("server started at") != std::string::npos) {
                    BOOST_TEST_MESSAGE(fmt::format("Got start message: {}", line));
                    done = true;
                    ready_promise.set_value();
                }
                if (!done && line.find("address already in use") != std::string::npos) {
                    ready_promise.set_exception(in_use{});
                }
                // podman
                if (!done && line.find("Address already in use") != std::string::npos) {
                    ready_promise.set_exception(in_use{});
                }
                // docker
                if (!done && line.find("port is already allocated") != std::string::npos) {
                    ready_promise.set_exception(in_use{});
                }
                return h(line);
            }
        );

        std::exception_ptr p;
        bool retry = false;

        try {
            BOOST_TEST_MESSAGE("Waiting for fake-gcs-server process to laúnch...");
            // arbitrary timeout of 120s for the server to make some output. Very generous.
            // but since we (maybe) run docker, and might need to pull image, this can take
            // some time if we're unlucky.
            co_await with_timeout(std::chrono::steady_clock::now() + 120s, std::move(ready_fut));
        } catch (in_use&) {
            retry = true;
            p = std::current_exception();
        } catch (...) {
            p = std::current_exception();
        }

        auto backoff = 0ms;

        while (!p) {
            if (backoff > 0ms) {
                co_await seastar::sleep(backoff);
            }
            try {
                BOOST_TEST_MESSAGE("Attempting to connect to fake-gcs-fixture");
                // TODO: seastar does not have a connect with timeout. That would be helpful here. But alas...
                co_await with_timeout(std::chrono::steady_clock::now() + 20s, seastar::connect(socket_address(net::inet_address("127.0.0.1"), port)));
                BOOST_TEST_MESSAGE("fake-gcs-server up and available"); // debug print. Why not.
            } catch (std::system_error&) {
                if (retries < max_retries) {
                    backoff = 100ms;
                    continue;
                }
                p = std::current_exception();
            } catch (...) {
                p = std::current_exception();
            }
            break;
        }

        if (p != nullptr) {
            BOOST_TEST_MESSAGE(fmt::format("Got exception starting fake-gcs-server: {}", p));
            ps.terminate();
            co_await ps.wait();
            if (!retry || retries >= max_retries) {
                std::rethrow_exception(p);
            }
            continue;
        }

        co_return std::make_tuple(std::move(ps), port);
    }
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
