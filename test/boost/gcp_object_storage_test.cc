/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "utils/gcp/object_storage.hh"
#include "utils/gcp/gcp_credentials.hh"

#include <unordered_set>
#include <filesystem>
#include <boost/test/unit_test.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

#include <seastar/core/thread.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/file.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/with_timeout.hh>

#include "test/lib/scylla_test_case.hh"
#include "test/lib/log.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/test_utils.hh"
#include "test/lib/tmpdir.hh"
#include "test/lib/proc_utils.hh"
#include "utils/assert.hh"
#include "utils/UUID_gen.hh"
#include "utils/io-wrappers.hh"

using namespace std::string_view_literals;
using namespace std::chrono_literals;
using namespace utils::gcp;

/*
    Simple test of GCP object storage provider. Uses either real or local, fake, endpoint.

    Note: the above text blobs are service account credentials, including private keys. 
    _Never_ give any real priviledges to these accounts, as we are obviously exposing them here.

    User1 is assumed to have permissions to read/write the bucket
    User2 is assumed to only have permissions to read the bucket, but permission to 
    impersonate User1.

    Note: fake gcp storage does not have any credentials or permissions, so
    for testing with such, leave them unset to skip those tests.

    This test is parameterized with env vars:
    * ENABLE_GCP_STORAGE_TEST - set to non-zero (1/true) to run
    * GCP_STORAGE_ENDPOINT - set to endpoint host. default is https://storage.googleapis.com
    * GCP_STORAGE_PROJECT - project in which to create bucket (if not specified)
    * GCP_STORAGE_USER_1_CREDENTIALS - set to credentials file for user1
    * GCP_STORAGE_USER_2_CREDENTIALS - set to credentials file for user2
    * GCP_STORAGE_BUCKET - set to test bucket
*/

struct gcp_test_env {
    std::string endpoint;
    std::string project;
    std::string bucket; 
    std::string user_1_creds;
    std::string user_2_creds;
    // for test
    std::vector<std::string> objects_to_delete;
};

static std::string get_var_or_default(const char* var, std::string_view def) {
    const char* val = std::getenv(var);
    if (val == nullptr) {
        return std::string(def);
    }
    return val;
}

using test_func = std::function<future<>(const tmpdir&, storage::client&, gcp_test_env&)>;

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

namespace fs = std::filesystem;
namespace tp = tests::proc;

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
            throw std::runtime_error("Could not find fake-gcs-server or docker.");
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
        params[port_index] = port_string + ":" + port_string;
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
            // the 4443 port will in fact be connectible even before the actual 
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

        if (!p) {
            try {
                BOOST_TEST_MESSAGE("Attempting to connect");
                // TODO: seastar does not have a connect with timeout. That would be helpful here. But alas...
                co_await with_timeout(std::chrono::steady_clock::now() + 20s, seastar::connect(socket_address(net::inet_address("127.0.0.1"), port)));
                BOOST_TEST_MESSAGE("fake-gcs-server up and available"); // debug print. Why not.
            } catch (...) {
                p = std::current_exception();
            }
        }

        if (p != nullptr) {
            BOOST_TEST_MESSAGE(fmt::format("Got exception starting server: {}", p));
            ps.terminate();
            co_await ps.wait();
            if (!retry || ++retries > 8) {
                std::rethrow_exception(p);
            }
        }

        co_return std::make_tuple(std::move(ps), port);
    }
}

static future<> gcp_test_helper(test_func f) {
    gcp_test_env env {
        .endpoint = get_var_or_default("GCP_STORAGE_ENDPOINT", ""),
        .project = get_var_or_default("GCP_STORAGE_PROJECT", ""),
        .bucket = get_var_or_default("GCP_STORAGE_BUCKET", ""),
        .user_1_creds = get_var_or_default("GCP_STORAGE_USER_1_CREDENTIALS", ""),
        .user_2_creds = get_var_or_default("GCP_STORAGE_USER_2_CREDENTIALS", ""),
    };

    tmpdir tmp;

    std::optional<tp::process_fixture> fake_gcs_server;
    std::optional<google_credentials> creds;

    if (!env.bucket.empty() && env.endpoint.empty()) {
        env.endpoint = storage::client::DEFAULT_ENDPOINT;
    }
    if (env.endpoint.empty()) {
        auto [proc, port] = co_await start_fake_gcs_server(tmp);
        fake_gcs_server.emplace(std::move(proc));
        env.endpoint = "http://127.0.0.1:" + std::to_string(port);
        BOOST_TEST_MESSAGE(fmt::format("Test server endpoint {}", env.endpoint));
    } else {
        creds = co_await credentials(env.user_1_creds);
    }

    bool created_bucket = false;
    storage::client c(env.endpoint, std::move(creds));
    std::exception_ptr p;

    try {
        if (env.bucket.empty()) {
            env.bucket = "test-" + fmt::format("{}", utils::UUID_gen::get_time_UUID());
            co_await c.create_bucket(env.project, env.bucket);
            created_bucket = true;
        }
        co_await f(tmp, c, env);
    } catch (...) {
        p = std::current_exception();
    }

    for (auto& name : env.objects_to_delete) {
        try {
            co_await c.delete_object(env.bucket, name);
        } catch (...) {
            BOOST_TEST_MESSAGE(fmt::format("Warning: could not delete object: {}", name, std::current_exception()));
        }
    }

    if (created_bucket) {
        try {
            auto objects = co_await c.list_objects(env.bucket);
            for (auto& o : objects) {
                co_await c.delete_object(env.bucket, o.name);
            }
            co_await c.delete_bucket(env.bucket);
        } catch (...) {
            BOOST_TEST_MESSAGE(fmt::format("Warning: could not delete bucket: {}", env.bucket, std::current_exception()));
        }
    }

    if (fake_gcs_server) {
        fake_gcs_server->terminate();
        co_await fake_gcs_server->wait();
    }

    co_await c.close();

    if (p) {
        std::rethrow_exception(p);
    }
}

static bool check_run_test(const char* var, bool defval = false) {
    auto do_test = get_var_or_default(var, std::to_string(defval));

    if (!strcasecmp(do_test.data(), "0") || !strcasecmp(do_test.data(), "false")) {
        BOOST_TEST_MESSAGE(fmt::format("Skipping test. Set {}=1 to run", var));
        return false;
    }
    return true;
}

static auto check_run_test_decorator(const char* var, bool def = false) {
    return boost::unit_test::precondition(std::bind(&check_run_test, var, def));
}

static auto check_gcp_storage_test_enabled() {
    return check_run_test_decorator("ENABLE_GCP_STORAGE_TEST", true);
}

static future<> create_object_of_size(storage::client& c, std::string_view bucket, std::string_view name, size_t dest_size, std::vector<temporary_buffer<char>>* buffer_store = nullptr) {
    auto sink = c.create_upload_sink(bucket, name);
    seastar::output_stream<char> os(std::move(sink));
    size_t done = 0;
    while (done < dest_size) {
        auto rem = dest_size - done;
        auto len = std::min(rem, tests::random::get_int(size_t(1), size_t(4*1024*1024)));
        auto rnd = tests::random::get_bytes(len);
        auto data = reinterpret_cast<char*>(rnd.data());
        co_await os.write(data, len);
        if (buffer_store) {
            buffer_store->emplace_back(data, len, make_object_deleter(std::move(rnd)));
        }
        done += len;
    }
    co_await os.flush();
    co_await os.close();
}

static future<> test_read_write_helper(size_t dest_size) {
    return gcp_test_helper([dest_size](const tmpdir& tmp, storage::client& c, gcp_test_env& env) -> future<> {
        auto uuid = fmt::format("{}", utils::UUID_gen::get_time_UUID());
        std::vector<temporary_buffer<char>> written;

        // ensure we remove the object
        env.objects_to_delete.emplace_back(uuid);
        co_await create_object_of_size(c, env.bucket, uuid, dest_size, &written);

        auto source = c.create_download_source(env.bucket, uuid);
        auto is1 = seastar::input_stream<char>(std::move(source));
        auto is2 = seastar::input_stream<char>(create_memory_source(std::move(written)));

        uint64_t read = 0;
        while (!is1.eof()) {
            auto buf = co_await is1.read();
            if (buf.empty()) {
                break;
            }
            auto buf2 = co_await is2.read_exactly(buf.size());
            BOOST_REQUIRE_EQUAL(buf, buf2);
            read += buf.size();
        }

        BOOST_REQUIRE_EQUAL(read, dest_size);
    });
}

SEASTAR_TEST_CASE(test_gcp_storage_create_small_object, *check_gcp_storage_test_enabled()) {
    co_await test_read_write_helper(8*4);
}

SEASTAR_TEST_CASE(test_gcp_storage_create_large_object, *check_gcp_storage_test_enabled()) {
    co_await test_read_write_helper(32*1024*1024 + 357 + 1022*67);
}

SEASTAR_TEST_CASE(test_gcp_storage_list_objects, *check_gcp_storage_test_enabled()) {
    return gcp_test_helper([](const tmpdir& tmp, storage::client& c, gcp_test_env& env) -> future<> {
        std::unordered_map<std::string, uint64_t> names;
        for (size_t i = 0; i < 10; ++i) {
            auto name = fmt::format("{}", utils::UUID_gen::get_time_UUID());
            auto size = tests::random::get_int(size_t(1), size_t(2*1024*1024));
            env.objects_to_delete.emplace_back(name);
            co_await create_object_of_size(c, env.bucket, name, size);
            names.emplace(name, size);
        }

        auto infos = co_await c.list_objects(env.bucket);
        size_t n_found = 0;

        for (auto& info : infos) {
            auto i = names.find(info.name);
            if (i != names.end()) {
                BOOST_REQUIRE_EQUAL(info.size, i->second);
                ++n_found;
            }
        }
        BOOST_REQUIRE_EQUAL(n_found, names.size());
    });
}

SEASTAR_TEST_CASE(test_gcp_storage_delete_object, *check_gcp_storage_test_enabled()) {
    return gcp_test_helper([](const tmpdir& tmp, storage::client& c, gcp_test_env& env) -> future<> {
        auto name = fmt::format("{}", utils::UUID_gen::get_time_UUID());
        env.objects_to_delete.emplace_back(name);
        co_await create_object_of_size(c, env.bucket, name, 128);
        {
            // validate object was created.
            auto infos = co_await c.list_objects(env.bucket, name);
            BOOST_REQUIRE(std::find_if(infos.begin(), infos.end(), [&](auto& info) {
                return info.name == name;
            }) != infos.end());
        }

        co_await c.delete_object(env.bucket, name);

        auto infos = co_await c.list_objects(env.bucket, name);
        BOOST_REQUIRE(infos.empty());
    });
}

SEASTAR_TEST_CASE(test_gcp_storage_skip_read, *check_gcp_storage_test_enabled()) {
    return gcp_test_helper([](const tmpdir& tmp, storage::client& c, gcp_test_env& env) -> future<> {
        auto name = fmt::format("{}", utils::UUID_gen::get_time_UUID());
        std::vector<temporary_buffer<char>> bufs;
        constexpr size_t file_size = 12*1024*1024 + 384*7 + 31;

        co_await create_object_of_size(c, env.bucket, name, 12*1024*1024, &bufs);
        for (size_t i = 0; i < 20; ++i) {
            auto source = c.create_download_source(env.bucket, name);
            auto copy = bufs | std::views::transform([](auto& buf) { return buf.share(); });
            auto is1 = seastar::input_stream<char>(std::move(source));
            auto is2 = seastar::input_stream<char>(create_memory_source(std::vector<temporary_buffer<char>>(copy.begin(), copy.end())));

            std::exception_ptr p;
            try {

                size_t pos = 0; 
                while (pos < file_size) {
                    auto rem = file_size - pos;
                    auto skip = tests::random::get_int(std::min(rem, size_t(100)), rem);
                    auto read = std::min(rem - skip, size_t(tests::random::get_int(31, 2048)));
                    co_await is1.skip(skip);
                    co_await is2.skip(skip);

                    auto b1 = co_await is1.read_exactly(read);
                    auto b2 = co_await is2.read_exactly(read);

                    BOOST_REQUIRE_EQUAL(b1.size(), b2.size());
                    if (b1 != b2) {
                        BOOST_TEST_MESSAGE(fmt::format("diff at {} ({} bytes)", pos + skip, read));
                        auto i = std::mismatch(b1.begin(), b1.end(), b2.begin());
                        BOOST_TEST_MESSAGE(fmt::format("offset {}", std::distance(b1.begin(), i.first)));
                    }
                    BOOST_REQUIRE_EQUAL(b1, b2);
                    pos += (skip + read);
                }
            } catch (...) {
                p = std::current_exception();
            }
            co_await is1.close();
            co_await is2.close();
            if (p) {
                std::rethrow_exception(p);
            }
        }
    });
}
