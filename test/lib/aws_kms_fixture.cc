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
    return tp::start_docker_service("local-kms"
        , "docker.io/nsmithuk/local-kms:3"
        , {}
        , [](std::string_view line) {
            if (line.find("Local KMS started on") != std::string::npos) {
                return tp::service_parse_state::success;
            }
            if (line.find("address already in use") != std::string::npos) {
                return tp::service_parse_state::failed;
            }
            return tp::service_parse_state::cont;
        }
        , { "-v", seed.string() + ":/init/seed.yaml", } // docker args. need to set seed
        , {}
        , 8080
    );
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
