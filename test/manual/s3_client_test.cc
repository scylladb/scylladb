/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "test/lib/cql_test_env.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/log.hh"
#include "s3/s3.hh"

#include <seastar/testing/test_runner.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/app-template.hh>

struct test_file {
    s3::client_ptr client;
    sstring path;
    file f;
    sstring contents;

    ~test_file() {
        f.close().get();
        client->remove_file(path).get();
    }
};

test_file make_test_file(s3::client_ptr client, size_t size) {
    auto path = fmt::format("/buck/file_{}", tests::random::get_sstring(32));
    auto contents = tests::random::get_sstring(size);

    testlog.debug("file contents: {}", contents);

    {
        data_sink sink = client->upload(path).get0();
        output_stream<char> out(std::move(sink));
        auto close_out = defer([&] { out.close().get(); });
        out.write(contents.begin(), contents.size()).get();
        out.flush().get();
    }

    file f = client->open(path).get0();
    return test_file{
            .client = client,
            .path = std::move(path),
            .f = std::move(f),
            .contents = std::move(contents)
    };
}

static sstring read_to_string(file& f, size_t start, size_t len) {
    file_input_stream_options opt;
    auto in = make_file_input_stream(f, start, len, opt);
    auto buf = in.read_exactly(len).get0();
    return sstring(buf.get(), buf.size());
}

static
void check_equal(sstring expected, sstring actual) {
    if (expected != actual) {
        throw_with_backtrace<std::runtime_error>(fmt::format("Expected {}, but got: {}", expected, actual));
    }
}


// Prepare the S3 server before running the test, e.g. using minio:
// MINIO_HTTP_TRACE=y MINIO_ROOT_USER=admin MINIO_ROOT_PASSWORD=password minio server ./tmp/s3 --console-address ":9001"
// mcli alias set myminio http://192.168.0.115:9000 admin password
// mcli mb myminio/buck
// mcli policy set public myminio/buck
int main(int argc, char** argv) {
    app_template app;
    return app.run(argc, argv, [] {
        return do_with_cql_env_thread([] (cql_test_env& env) {
            try {
                auto client = s3::make_client(s3::make_basic_connection_factory("127.0.0.1", 9000));

                {
                    auto page_size = 512;
                    test_file tf = make_test_file(client, page_size * 3);

                    check_equal(tf.contents.substr(0, 1),
                                read_to_string(tf.f, 0, 1));

                    check_equal(tf.contents.substr(page_size - 1, 10),
                                read_to_string(tf.f, page_size - 1, 10));

                    check_equal(tf.contents.substr(page_size - 1, tf.contents.size() - (page_size - 1)),
                                read_to_string(tf.f, page_size - 1, tf.contents.size() - (page_size - 1)));

                    check_equal(tf.contents.substr(0, tf.contents.size()),
                                read_to_string(tf.f, 0, tf.contents.size()));
                }

                {
                    auto size = 20*1024*1024;
                    test_file tf = make_test_file(client, size);

                    check_equal(tf.contents.substr(0, size),
                                read_to_string(tf.f, 0, size));
                }
            } catch (...) {
                testlog.error("Test failed: {}", std::current_exception());
                return 1;
            }
            return 0;
        });
    });
}
