/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <string>
#include <memory>
#include <regex>
#include <iostream>

#include <seastar/core/shared_ptr.hh>
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
    return tp::start_docker_service("fake-gcs-server"
        , "docker.io/fsouza/fake-gcs-server:1.54.0"
        , {}
        , [](std::string_view line) {
            if (line.find("server started at") != std::string::npos) {
                return tp::service_parse_state::success;
            }
            if (line.find("address already in use") != std::string::npos || line.find("Address already in use") != std::string::npos ||
                line.find("port is already allocated") != std::string::npos) {
                return tp::service_parse_state::failed;
            }
            return tp::service_parse_state::cont;
        }
        , {} 
        , { "-scheme", "http", "-log-level", "debug", "--port", "4443", "-public-host", "127.0.0.1" } // image args
        , 4443
    );
}

// fake-gcs-server accepts any Content-Range at all -- it stores the body and
// derives the object size from it -- so protocol level upload bugs are invisible
// when testing against it (SCYLLADB-3889). Put a validator in front of it that
// tracks each upload session and rejects ranges real GCS would reject.
static future<std::tuple<tp::process_fixture, int>> start_upload_validator(int upstream_port) {
    auto pyexec = tp::find_file_in_path("python3");
    if (pyexec.empty()) {
        pyexec = tp::find_file_in_path("python");
    }
    if (pyexec.empty()) {
        throw std::runtime_error("Could not find python3 or python.");
    }

    // Resolved against the working directory, as the tests are run from the
    // source root. Check it here so a bad cwd says so, rather than surfacing
    // as the validator dying without ever printing a port.
    static const std::string script = "test/pylib/gcs_upload_validator.py";
    if (!fs::exists(script)) {
        throw std::runtime_error(fmt::format("Could not find {} (cwd={}). Tests are expected to run from the source root."
            , script, fs::current_path().string()));
    }

    // Held by pointer rather than moved into the handler below: create() takes
    // the handler by value into its coroutine frame, so a spawn failure would
    // destroy the promise and leave port_future failed with broken_promise.
    // Nothing consumes it on that path, and the seastar test runner turns an
    // abandoned failed future into exit code 3 on top of the real error.
    auto port_promise = make_lw_shared<promise<int>>();
    auto port_future = port_promise->get_future();

    auto python = co_await tp::process_fixture::create(pyexec
        , { // args
            pyexec.string(),
            script,
            "--upstream-port", std::to_string(upstream_port),
        }
        , {} // env
        , tp::process_fixture::create_copy_handler(std::cout) // stdout
        , [port_promise, matched = false](std::string_view line) mutable -> future<consumption_result<char>> {
            static std::regex port_ex(R"foo(Starting GCS upload validator on \('[^']+', (\d+)\))foo");

            std::match_results<typename std::string_view::const_iterator> m;
            if (!matched && std::regex_search(line.begin(), line.end(), m, port_ex)) {
                port_promise->set_value(std::stoi(m[1].str()));
                matched = true;
            } else {
                // surface rejections in the test log, they explain the failure
                BOOST_TEST_MESSAGE(std::string(line));
            }
            co_return continue_consuming{};
        }
    );

    // From here on the fixture must not go out of scope with the process still
    // running: the stdout/stderr consumers hold its gate until stream EOF, and
    // ~gate() asserts when it is still held, which would abort the test binary
    // instead of reporting the failure. Reap it first, as start_docker_service
    // does on its own error path.
    std::exception_ptr failure;
    int port = 0;

    try {
        port = co_await with_timeout(std::chrono::steady_clock::now() + 20s, std::move(port_future));
        if (port <= 0) {
            throw std::runtime_error("Invalid upload validator port");
        }

        std::exception_ptr last;
        for (size_t retry = 0;; ++retry) {
            try {
                auto c = co_await with_timeout(std::chrono::steady_clock::now() + 20s
                    , seastar::connect(socket_address(net::inet_address("127.0.0.1"), port)));
                c.shutdown_output();
                last = {};
                break;
            } catch (...) {
                last = std::current_exception();
            }
            if (retry == 4) {
                break;
            }
            co_await sleep(100ms);
        }
        if (last) {
            // Publishing an endpoint nothing serves would turn every later request
            // into an unrelated connection error, so give up here instead.
            throw std::runtime_error(fmt::format("GCS upload validator on port {} never accepted a connection: {}"
                , port, last));
        }
    } catch (...) {
        failure = std::current_exception();
    }

    if (failure) {
        python.terminate();
        co_await python.wait();
        std::rethrow_exception(failure);
    }

    co_return std::make_tuple(std::move(python), port);
}

class gcs_fixture::impl {
public:
    std::optional<tp::process_fixture> fake_gcs_server;
    std::optional<tp::process_fixture> upload_validator;
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

        // Unless explicitly disabled, talk to the mock through a validator that
        // enforces the resumable upload rules the mock itself ignores.
        if (getenv_or_default("GCP_STORAGE_SKIP_UPLOAD_VALIDATOR").empty()) {
            std::exception_ptr p;
            try {
                auto [vproc, vport] = co_await start_upload_validator(port);
                upload_validator.emplace(std::move(vproc));
                endpoint = "http://127.0.0.1:" + std::to_string(vport);
                BOOST_TEST_MESSAGE(fmt::format("Validating uploads to fake gcs server on port {}", port));
            } catch (...) {
                p = std::current_exception();
            }
            if (p) {
                // The mock is up by now and nothing else would stop it, the
                // same reason create_bucket below tears down on failure.
                try {
                    co_await teardown();
                } catch (...) {
                }
                std::rethrow_exception(p);
            }
        }

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

    // Reset both, so that a teardown from a failed setup does not leave a
    // second one re-terminating an already reaped process.
    if (upload_validator) {
        upload_validator->terminate();
        co_await upload_validator->wait();
        upload_validator.reset();
    }

    if (fake_gcs_server) {
        fake_gcs_server->terminate();
        co_await fake_gcs_server->wait();
        fake_gcs_server.reset();
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
        f = gcs_fixture::active();
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
