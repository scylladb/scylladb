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
    fs::path exec;
    bool use_container = false;
    fs::path container_exec;

    auto local_kms = tp::find_file_in_path("local-kms");
    if (local_kms.empty()) {
        container_exec = tp::find_file_in_path("docker");
        if (container_exec.empty()) {
            container_exec = tp::find_file_in_path("podman");
        }
        if (container_exec.empty()) {
            throw std::runtime_error("Could not find local-kms, docker or podman.");
        }
        use_container = true;
    }

    auto unshare = tp::find_file_in_path("unshare");
    if (unshare.empty()) {
        throw std::runtime_error("Could not find unshare command");
    }

    auto sh = tp::find_file_in_path("sh");
    if (sh.empty()) {
        throw std::runtime_error("Could not find sh command");
    }

    exec = unshare;

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

        // Build the command to run inside the namespace
        std::string cmd;
        std::vector<std::string> env;

        if (use_container) {
            cmd = fmt::format("ip link set lo up && exec {} run --rm -v {}:/init/seed.yaml -p {}:{} -e PORT={} docker.io/nsmithuk/local-kms:3",
                container_exec.string(), seed.string(), port_string, port_string, port_string);
        } else {
            cmd = fmt::format("ip link set lo up && exec {}", local_kms.string());
            env.emplace_back("PORT=" + port_string);
            env.emplace_back("KMS_SEED_PATH=" + seed.string());
        }

        std::vector<std::string> params = {
            unshare.string(),  // argv[0]
            "-n",              // Create new network namespace
            sh.string(),       // Run shell
            "-c",              // Execute command
            cmd                // Command: bring up lo, then run server
        };

        BOOST_TEST_MESSAGE(fmt::format("Will run: unshare -n sh -c '{}'", cmd));

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

        // Server is running in an isolated namespace, so we can't connect to it from here.
        // We rely on the "Local KMS started on" message in the stderr handler above.
        // The actual connectivity test will happen when the test code enters the namespace.
        BOOST_TEST_MESSAGE("Server startup message received, assuming it's ready");

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
    std::optional<int> _netns_fd;  // Network namespace FD for mock server

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

        // Get the network namespace FD from the spawned process
        auto netns_path = fmt::format("/proc/{}/ns/net", proc.pid());
        BOOST_TEST_MESSAGE(fmt::format("Opening server process' network namespace file {}", netns_path));
        _netns_fd = ::open(netns_path.c_str(), O_RDONLY);
        if (*_netns_fd < 0) {
            BOOST_FAIL(fmt::format("Failed to open server process' network namespace file {}: {}", netns_path, std::strerror(errno)));
        }

        local_kms.emplace(std::move(proc));
        kms_key_alias = "alias/testing";
        endpoint = "http://127.0.0.1:" + std::to_string(port);
        BOOST_TEST_MESSAGE(fmt::format("Test server endpoint {}, alias {}", endpoint, kms_key_alias));
    }
}

seastar::future<> aws_kms_fixture::impl::teardown() {
    if (_netns_fd) {
        ::close(*_netns_fd);
        _netns_fd.reset();
    }
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

int aws_kms_fixture::netns_fd() const {
    return _impl->_netns_fd.value_or(-1);
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

seastar::future<> local_aws_kms_wrapper::teardown() {
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
