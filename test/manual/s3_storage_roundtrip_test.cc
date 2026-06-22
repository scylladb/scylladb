/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "test/lib/scylla_test_case.hh"

#include <exception>
#include <seastar/core/coroutine.hh>
#include <seastar/util/defer.hh>
#include "alternator/export.hh"
#include "utils/rjson.hh"
#include "utils/s3/client.hh"
#include "utils/s3/creds.hh"
#include "test/lib/test_utils.hh"

// Manual test for s3_storage_sink and s3_storage_source against a real S3 instance.
// To run with MinIO (S3-compatible local storage):
//
// docker run -d --name minio -p 9000:9000 -p 9001:9001 -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data --console-address ":9001"
// docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
// docker exec minio mc mb local/test-bucket
// export S3_SERVER_ADDRESS_FOR_TEST=localhost
// export S3_SERVER_PORT_FOR_TEST=9000
// export S3_BUCKET_FOR_TEST=test-bucket
// export AWS_ACCESS_KEY_ID=minioadmin
// export AWS_SECRET_ACCESS_KEY=minioadmin
// 
// build/debug/test/manual/s3_storage_roundtrip_test
//
// You can run against read AWS S3 by setting appropriate environment variables -
// S3_SERVER_ADDRESS_FOR_TEST, S3_SERVER_PORT_FOR_TEST, S3_BUCKET_FOR_TEST, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and optionally AWS_DEFAULT_REGION.
// You will need to manually create S3 bucket before running the test, and the test will delete the object it created but not the bucket.
SEASTAR_TEST_CASE(test_s3_storage_sink_source_roundtrip) {
    auto port = std::stoul(tests::getenv_safe("S3_SERVER_PORT_FOR_TEST"));
    auto bucket = tests::getenv_safe("S3_BUCKET_FOR_TEST");
    auto server = tests::getenv_safe("S3_SERVER_ADDRESS_FOR_TEST");
    auto region = ::getenv("AWS_DEFAULT_REGION") ? ::getenv("AWS_DEFAULT_REGION") : "local";
    bool use_https = ::getenv("AWS_DEFAULT_REGION") != nullptr;

    s3::endpoint_config cfg = {
        .port = port,
        .use_https = use_https,
        .region = region,
    };

    semaphore mem(16 * 1024 * 1024);
    auto client = s3::client::make(server, make_lw_shared<s3::endpoint_config>(std::move(cfg)), mem);
    const sstring object_name = fmt::format("/{}/alternator-export-test-{}", bucket, ::getpid());

    std::exception_ptr ex;
    std::string large_value;
    for(auto j = 0; j < 1000; ++j) {
        large_value += "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ";
    }
    const auto large_value_size = large_value.size();

    // single item should be ~57kb
    auto make_large_item = [&](unsigned int index) {
        auto ret = rjson::empty_object();
        rjson::add(ret, "id", index);
        large_value.resize(large_value_size);
        large_value += std::to_string(index);
        rjson::add(ret, "value", large_value);
        return ret;
    };
    try {
        std::vector<rjson::value> items;
        // This should produce ~57 mb of data.
        for(auto i = 0; i < 1000; ++i) {
            items.push_back(make_large_item(i));
        }

        // Write via s3_storage_sink.
        {
            auto sink = alternator::create_s3_sink_pipeline(client, object_name);
            for(auto &item : items) {
                co_await sink->process(item);
            }
            co_await sink->flush_and_close();
        }

        // Read back via s3_storage_source and verify data integrity.
        {
            std::vector<rjson::value> received;
            auto source = alternator::create_s3_source_pipeline(client, object_name, [&](rjson::value v) -> seastar::future<> {
                received.push_back(std::move(v));
                co_return;
            });
            co_await source->read();

            BOOST_CHECK_EQUAL(received.size(), items.size());
            for (size_t i = 0; i < items.size(); ++i) {
                BOOST_CHECK_EQUAL(received[i], items[i]);
            }
        }
    } catch (...) {
        ex = std::current_exception();
    }

    try {
        co_await client->delete_object(object_name);
    } catch (const std::exception& e) {
        std::cout << "Failed to delete object " << object_name << ": " << e.what() << std::endl;
    }

    co_await client->close();

    if (ex) {
        std::rethrow_exception(ex);
    }
}
