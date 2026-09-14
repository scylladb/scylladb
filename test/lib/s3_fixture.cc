/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <cstdlib>
#include <optional>
#include <regex>
#include <string>

#include <boost/test/unit_test.hpp>
#include <boost/test/unit_test_log.hpp>

#include <seastar/core/shared_ptr.hh>
#include <seastar/util/log.hh>

#include "test/lib/s3_fixture.hh"
#include "test/lib/test_utils.hh"

#include "utils/s3/client.hh"

using namespace seastar;

// S3 bucket names are 3-63 characters of lowercase alphanumerics and hyphens,
// with no leading, trailing or consecutive hyphen. The pid keeps the name
// unique across the test processes running in parallel, the test name keeps it
// unique within one.
static sstring make_bucket_name() {
    static const std::regex non_alnum("[^a-z0-9]+");

    std::string name = boost::unit_test::framework::current_test_unit().p_name.get();
    for (auto& c : name) {
        c = std::tolower(static_cast<unsigned char>(c));
    }
    name = std::regex_replace(name, non_alnum, "-");

    const std::string suffix = fmt::format("-{}", ::getpid());
    // "s3-" keeps a name that starts with a digit valid; the truncation keeps
    // the whole thing within the 63-character limit.
    static constexpr std::string_view prefix = "s3-";
    name = name.substr(0, 63 - prefix.size() - suffix.size());
    while (name.ends_with('-')) {
        name.pop_back();
    }
    return fmt::format("{}{}{}", prefix, name, suffix);
}

class s3_fixture::impl {
public:
    sstring address;
    s3::endpoint_config cfg{};
    sstring bucket;
    std::optional<tests::tmp_set_env> bucket_var;

    // The client lives only for as long as the bucket call it is made for.
    // Keeping one open across the test body would put two clients on the same
    // endpoint and shard, and the second one's register_client_metrics() throws
    // seastar::metrics::double_registration - the per-scheduling-group metrics
    // tolerate such a collision, the per-client ones do not.
    template <typename Func>
    future<> with_client(Func&& func);

    future<> setup();
    future<> teardown();
};

template <typename Func>
future<> s3_fixture::impl::with_client(Func&& func) {
    auto client = s3::client::make(address, make_lw_shared<s3::endpoint_config>(cfg));
    std::exception_ptr ex;
    try {
        co_await func(client);
    } catch (...) {
        ex = std::current_exception();
    }
    co_await client->close();
    if (ex) {
        std::rethrow_exception(ex);
    }
}

future<> s3_fixture::impl::setup() {
    const char* addr = ::getenv("S3_SERVER_ADDRESS_FOR_TEST");
    const char* port = ::getenv("S3_SERVER_PORT_FOR_TEST");
    if (!addr || !*addr || !port || !*port) {
        // No S3 server in this run; the test's own precondition skips it.
        co_return;
    }

    address = addr;
    cfg = {
        .port = static_cast<uint16_t>(std::stoul(port)),
        .use_https = ::getenv("AWS_DEFAULT_REGION") != nullptr,
        .region = ::getenv("AWS_DEFAULT_REGION") ? : "local",
    };

    auto name = make_bucket_name();
    co_await with_client([&name] (shared_ptr<s3::client>& client) -> future<> {
        co_await client->create_bucket(name);
    });
    bucket = std::move(name);
    bucket_var.emplace("S3_BUCKET_FOR_TEST", bucket);
    BOOST_TEST_MESSAGE(fmt::format("Created test bucket {}", bucket));
}

future<> s3_fixture::impl::teardown() {
    if (bucket.empty()) {
        co_return;
    }
    // Dropped before the bucket goes, so that the test's own name stops
    // pointing at a bucket that is on its way out.
    bucket_var.reset();
    try {
        co_await with_client([this] (shared_ptr<s3::client>& client) -> future<> {
            co_await client->delete_bucket_with_objects(bucket);
        });
    } catch (...) {
        BOOST_TEST_MESSAGE(fmt::format("Warning: could not delete bucket {}: {}",
                                       bucket, std::current_exception()));
    }
    bucket = {};
}

s3_fixture::s3_fixture()
    : _impl(std::make_unique<impl>())
{}

s3_fixture::~s3_fixture() = default;

const sstring& s3_fixture::bucket() const {
    return _impl->bucket;
}

future<> s3_fixture::setup() {
    return _impl->setup();
}

future<> s3_fixture::teardown() {
    return _impl->teardown();
}
