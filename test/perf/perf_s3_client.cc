/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <chrono>
#include <seastar/core/app-template.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/memory.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include "test/lib/test_utils.hh"
#include "test/lib/random_utils.hh"
#include "utils/s3/client.hh"
#include "utils/estimated_histogram.hh"

seastar::logger plog("perf");

class tester {
    std::chrono::seconds _duration;
    unsigned _parallel;
    std::string _object_name;
    size_t _object_size;
    semaphore _mem;
    shared_ptr<s3::client> _client;
    utils::estimated_histogram _reads_hist;
    unsigned _errors = 0;

    static s3::endpoint_config_ptr make_config() {
        s3::endpoint_config cfg;
        cfg.port = 443;
        cfg.use_https = true;
        cfg.aws.emplace();
        cfg.aws->access_key_id = tests::getenv_safe("AWS_ACCESS_KEY_ID");
        cfg.aws->secret_access_key = tests::getenv_safe("AWS_SECRET_ACCESS_KEY");
        if (auto token = ::getenv("AWS_SESSION_TOKEN"); token) {
            cfg.aws->session_token = token;
        }
        cfg.aws->region = tests::getenv_safe("AWS_DEFAULT_REGION");

        return make_lw_shared<s3::endpoint_config>(std::move(cfg));
    }

    static constexpr unsigned chunk_size = 1000;

    std::chrono::steady_clock::time_point now() const { return std::chrono::steady_clock::now(); }

public:
    tester(std::chrono::seconds dur, unsigned prl, size_t obj_size)
            : _duration(dur)
            , _parallel(prl)
            , _object_name(fmt::format("/{}/perfobject-{}-{}", tests::getenv_safe("S3_BUCKET_FOR_TEST"), ::getpid(), this_shard_id()))
            , _object_size(obj_size)
            , _mem(memory::stats().total_memory())
            , _client(s3::client::make(tests::getenv_safe("S3_SERVER_ADDRESS_FOR_TEST"), make_config(), _mem))
    {}

    future<> start() {
        plog.debug("Creating {} of {} bytes", _object_name, _object_size);

        auto out = output_stream<char>(_client->make_upload_sink(_object_name));
        std::exception_ptr ex;
        try {
            auto rnd = tests::random::get_bytes(chunk_size);
            uint64_t written = 0;
            do {
                co_await out.write(reinterpret_cast<char*>(rnd.begin()), std::min(_object_size - written, rnd.size()));
                written += rnd.size();
            } while (written < _object_size);
            co_await out.flush();
        } catch (...) {
            ex = std::current_exception();
            plog.error("Cannot write object: {}", ex);
        }
        co_await out.close();
        if (ex) {
            co_await coroutine::return_exception_ptr(std::move(ex));
        }
    }

private:

    future<> do_run() {
        auto until = now() + _duration;
        uint64_t off = 0;
        do {
            auto start = now();
            try {
                co_await _client->get_object_contiguous(_object_name, s3::range{off, chunk_size});
                off = (off + chunk_size) % (_object_size - chunk_size);
                _reads_hist.add(std::chrono::duration_cast<std::chrono::milliseconds>(now() - start).count());
            } catch (...) {
                _errors++;
            }
        } while (now() < until);
    }

public:
    future<> run() {
        co_await coroutine::parallel_for_each(boost::irange(0u, _parallel), [this] (auto fnr) -> future<> {
            plog.debug("Running {} fiber", fnr);
            co_await seastar::sleep(std::chrono::milliseconds(fnr)); // make some discrepancy
            co_await do_run();
        });
    }

    future<> stop() {
        plog.debug("Removing {}", _object_name);
        co_await _client->delete_object(_object_name);
        co_await _client->close();

        auto print_percentiles = [] (const utils::estimated_histogram& hist) {
            return format("min: {:-6d}, 50%: {:-6d}, 90%: {:-6d}, 99%: {:-6d}, 99.9%: {:-6d}, max: {:-6d} [ms]",
                hist.percentile(0),
                hist.percentile(0.5),
                hist.percentile(0.9),
                hist.percentile(0.99),
                hist.percentile(0.999),
                hist.percentile(1.0)
            );
        };
        plog.info("reads total: {:5}, errors: {:5}; latencies: {}", _reads_hist._count, _errors, print_percentiles(_reads_hist));
    }
};

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("duration", bpo::value<unsigned>()->default_value(10), "seconds to run")
        ("parallel", bpo::value<unsigned>()->default_value(1), "number of parallel fibers")
        ("object_size", bpo::value<size_t>()->default_value(1 << 20), "size of test object")
    ;

    return app.run(argc, argv, [&app] () -> future<> {
        auto dur = std::chrono::seconds(app.configuration()["duration"].as<unsigned>());
        auto prl = app.configuration()["parallel"].as<unsigned>();
        auto osz = app.configuration()["object_size"].as<size_t>();
        sharded<tester> test;
        plog.info("Creating");
        co_await test.start(dur, prl, osz);
        plog.info("Starting");
        co_await test.invoke_on_all(&tester::start);
        try {
            plog.info("Running");
            co_await test.invoke_on_all(&tester::run);
        } catch (...) {
            plog.error("Error running: {}", std::current_exception());
        }
        plog.info("Stopping (and printing results)");
        co_await test.stop();
    });
}
