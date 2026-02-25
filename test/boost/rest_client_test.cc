/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */


#include "utils/rest/client.hh"
#include "test/lib/scylla_test_case.hh"
#include "test/lib/test_utils.hh"

void simple_rest_client() {
    auto host = tests::getenv_safe("MOCK_S3_SERVER_HOST");
    auto port = std::stoul(tests::getenv_safe("MOCK_S3_SERVER_PORT"));
    rest::httpclient client(host, port);
    for ([[maybe_unused]] auto i : {1, 2}) {
        BOOST_REQUIRE_NO_THROW([&] {
            client.add_header("host", host);
            client.add_header("X-aws-ec2-metadata-token-ttl-seconds", "21600");
            client.method(rest::httpclient::method_type::PUT);
            client.target("/latest/api/token");
            [[maybe_unused]] auto res = client.send().get();
        }());
    }
}

SEASTAR_THREAD_TEST_CASE(test_simple_rest_client) {
    simple_rest_client();
}
