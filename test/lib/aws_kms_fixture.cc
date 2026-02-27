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
#include <seastar/net/inet_address.hh>

#include "aws_kms_fixture.hh"
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
    std::vector<std::string> params(1);

    size_t port_index = 0;

    auto exec = tp::find_file_in_path("local-kms");
    if (exec.empty()) {
        exec = tp::find_file_in_path("docker");
        if (exec.empty()) {
            exec = tp::find_file_in_path("podman");
        }
        if (exec.empty()) {
            throw std::runtime_error("Could not find local-kms, docker or podman.");
        }

        // publish port ephemeral, allows parallel instances
        params = {
            "",
            "run", "--rm", 
            "-v", seed.string() + ":/init/seed.yaml",
            "-p", "--", // set below
            "-e", "--", // set below
            "docker.io/nsmithuk/local-kms:3"
        };
        port_index = 6;
    }

    params[0] = exec.string();

    struct in_use{};
    constexpr auto max_retries = 8;

    for (int retries = 0;; ++retries) {
        // podman in podman is hell. we run our "dbuild" with host network mode
        // so _cannot_ rely on ephermal ports or anything nice, atomic to 
        // pick a port. And local-kms does not support port=0 to pick one.
        // *sigh*. Fall back to create a socket, get the port, close it and hope
        // noone manages to steal the port again before we start our test server.
        // This is not reliable. At all. But works most of the time (right...)
        in_port_t port;
        {
            auto tmp_socket = seastar::listen({});
            port = tmp_socket.local_address().port();
            tmp_socket.abort_accept();
        }

        auto port_string = std::to_string(port);

        if (port_index != 0) {
            params[port_index] = port_string + ":" + port_string;
            params[port_index + 2] = "PORT=" + port_string;
        }

        BOOST_TEST_MESSAGE(fmt::format("Will run {}", params));

        std::vector<std::string> env;

        if (port_index == 0) { // running actual exec
            env.emplace_back("PORT=" + port_string);
            env.emplace_back("KMS_SEED_PATH=" + seed.string());
        }

        promise<> ready_promise;
        auto ready_fut = ready_promise.get_future();

        auto ps = co_await tp::process_fixture::create(exec
            , params
            , env
            , tp::process_fixture::create_copy_handler(std::cout)
            // Must look at stderr log for state, because if we are using podman (and docker?)
            // the port might in fact be connectible even before the actual 
            // mock server is up (in container), due to the port publisher.
            // Could do actual HTTP connection etc, but seems tricky.
            , [done = false, ready_promise = std::move(ready_promise), h = tp::process_fixture::create_copy_handler(std::cerr)](std::string_view line) mutable -> future<consumption_result<char>> {
                if (!done && line.find("Local KMS started on") != std::string::npos) {
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
            BOOST_TEST_MESSAGE("Waiting for process to laúnch...");
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
                BOOST_TEST_MESSAGE("Attempting to connect to local-kms");
                // TODO: seastar does not have a connect with timeout. That would be helpful here. But alas...
                co_await with_timeout(std::chrono::steady_clock::now() + 20s, seastar::connect(socket_address(net::inet_address("127.0.0.1"), port)));
                BOOST_TEST_MESSAGE("local-kms up and available"); // debug print. Why not.
            } catch (std::system_error&) {
                retry = true;
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
            BOOST_TEST_MESSAGE(fmt::format("Got exception starting local-kms server: {}", p));
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

class aws_kms_fixture::impl {
public:
    std::optional<tp::process_fixture> local_kms;

    std::string endpoint;
    std::string kms_key_alias;
    std::string kms_aws_region; 
    std::string kms_aws_profile;

    tmpdir tmp;

    bool created_master_key = false;

    impl();

    seastar::future<> setup();
    seastar::future<> teardown();
};

aws_kms_fixture::impl::impl()  
    : kms_key_alias(getenv_or_default("KMS_KEY_ALIAS"))
    , kms_aws_region(getenv_or_default("KMS_AWS_REGION", "us-east-1"))
    , kms_aws_profile(getenv_or_default("KMS_AWS_PROFILE", "default"))
{}

seastar::future<> aws_kms_fixture::impl::setup() {
    if (kms_key_alias.empty()) {
        auto seed = tmp.path() / "seed.yaml";
        // TODO: maybe generate unique ID / key material each run
        co_await encryption::write_text_file_fully(seed.string(), R"foo(
Keys:
  Symmetric:
    Aes:
      - Metadata:
          KeyId: bc436485-5092-42b8-92a3-0aa8b93536dc
        BackingKeys:
          - 5cdaead27fe7da2de47945d73cd6d79e36494e73802f3cd3869f1d2cb0b5d7a9

Aliases:
  - AliasName: alias/testing
    TargetKeyId: bc436485-5092-42b8-92a3-0aa8b93536dc
)foo"
        );

        auto [proc, port] = co_await start_fake_kms_server(tmp, seed);
        local_kms.emplace(std::move(proc));
        kms_key_alias = "alias/testing";
        endpoint = "http://127.0.0.1:" + std::to_string(port);
        BOOST_TEST_MESSAGE(fmt::format("Test server endpoint {}, alias {}", endpoint, kms_key_alias));
    }
}

seastar::future<> aws_kms_fixture::impl::teardown() {
    if (local_kms) {
        local_kms->terminate();
        co_await local_kms->wait();
    }
}

static thread_local aws_kms_fixture* active_aws_kms_fixture = nullptr;

aws_kms_fixture::aws_kms_fixture() 
    : _impl(std::make_unique<impl>())
{}

aws_kms_fixture::~aws_kms_fixture() = default;

const std::string& aws_kms_fixture::endpoint() const {
    return _impl->endpoint;
}
const std::string& aws_kms_fixture::kms_key_alias() const {
    return _impl->kms_key_alias;
}
const std::string& aws_kms_fixture::kms_aws_region() const {
    return _impl->kms_aws_region;
}
const std::string& aws_kms_fixture::kms_aws_profile() const {
    return _impl->kms_aws_profile;
}

seastar::future<> aws_kms_fixture::setup() {
    co_await _impl->setup();
    active_aws_kms_fixture = this;
}

seastar::future<> aws_kms_fixture::teardown() {
    active_aws_kms_fixture = nullptr;
    return _impl->teardown();
}

aws_kms_fixture* aws_kms_fixture::active() {
    return active_aws_kms_fixture;
}

local_aws_kms_wrapper::local_aws_kms_wrapper() = default;
local_aws_kms_wrapper::~local_aws_kms_wrapper() = default;

seastar::future<> local_aws_kms_wrapper::setup() {
    auto f = aws_kms_fixture::active();
    if (!f) {
        _local = std::make_unique<aws_kms_fixture>();
        co_await _local->setup();
        f = aws_kms_fixture::active();;
    }

    endpoint = f->endpoint();
    kms_key_alias = f->kms_key_alias();
    kms_aws_region = f->kms_aws_region();
    kms_aws_profile = f->kms_aws_profile();
}

seastar::future<> local_aws_kms_wrapper::teardown() {
    if (_local) {
        co_await _local->teardown();
        _local = {};
    }
}
