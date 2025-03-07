/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

 #include <seastar/core/app-template.hh>
 #include <seastar/coroutine/parallel_for_each.hh>
 #include <seastar/core/temporary_buffer.hh>
 #include <seastar/net/socket_defs.hh>
 #include <seastar/core/future.hh>
 #include <seastar/core/shared_ptr.hh>
 #include <seastar/core/thread.hh>

 #include "db/config.hh"
 #include "generic_server.hh"
 #include "test/perf/perf.hh"

 seastar::logger plog("perf");

 struct test_config {
     unsigned duration;
     unsigned concurrency;
     uint64_t requests_per_connection;
     unsigned request_size;
     unsigned response_size;
     std::string server_host;
     uint16_t server_port;
 };

 class test_connection : public generic_server::connection {
     const test_config& conf;

 public:
     test_connection(generic_server::server& server, connected_socket&& fd, named_semaphore& sem, semaphore_units<named_semaphore_exception_factory> initial_sem_units, const test_config& conf)
             : generic_server::connection(server, std::move(fd), sem, std::move(initial_sem_units))
             , conf(conf) {
     }

     virtual void handle_error(future<>&& f) override {
         try {
             f.get();
         } catch (const std::exception& ex) {
             plog.error("Error during processing request: {}", ex);
         }
     }

     virtual future<> process_request() override {
         co_await _read_buf.read_exactly(conf.request_size);
         co_await _write_buf.write(temporary_buffer<char>(conf.response_size));
         co_await _write_buf.flush();
     }
 };

 class test_server : public generic_server::server {
     const test_config& _conf;
 public:
     virtual shared_ptr<generic_server::connection> make_connection(socket_address server_addr, connected_socket&& fd, socket_address addr, named_semaphore& sem, semaphore_units<named_semaphore_exception_factory> initial_sem_units) override {
         return make_shared<test_connection>(*this, std::move(fd), sem, std::move(initial_sem_units), _conf);
     }


     test_server(const test_config& conf)
        : generic_server::server("test_server", plog,
                generic_server::config(std::numeric_limits<uint32_t>::max()))
        , _conf(conf) {}
 };

 struct tester {
     test_config conf;
     test_server server;

     tester(const test_config& cfg) : conf(cfg) , server(conf) {}

     socket_address addr() {
         return socket_address(net::inet_address(conf.server_host), conf.server_port);
     }

      future<> start() {
         return server.listen(addr(), nullptr, false, false, {});
      }

      future<> run() {
         return seastar::async([&] {
             auto results = time_parallel([&] -> future<> {
                 connected_socket sock = co_await seastar::connect(addr());
                 auto out = sock.output();
                 auto in = sock.input();
                 for (uint64_t i = 0; i <= conf.requests_per_connection; i++) {
                     co_await out.write(temporary_buffer<char>(conf.request_size));
                     co_await out.flush();
                     co_await in.read_exactly(conf.response_size);
                 }
                 co_await out.close();
                 co_await in.close();
             }, conf.concurrency, conf.duration);

             for (auto& result : results) {
                 result.throughput *= conf.requests_per_connection;
                 result.mallocs_per_op /= conf.requests_per_connection;
                 result.logallocs_per_op /= conf.requests_per_connection;
                 result.tasks_per_op  /= conf.requests_per_connection;
                 result.instructions_per_op  /= conf.requests_per_connection;
                 result.cpu_cycles_per_op /= conf.requests_per_connection;
             }
             // Technically, we're measuring the client side here,
             // but since it's a single process with the server, both sides are included.
             std::cout << aggregated_perf_results(results) << std::endl;
         }).or_terminate();
     }

      future<> stop() {
         co_await server.stop();
      }
  };


 int main(int argc, char** argv) {
     namespace bpo = boost::program_options;
     app_template app;
     app.add_options()
         ("duration", bpo::value<unsigned>()->default_value(10), "seconds to run")
         ("concurrency", bpo::value<unsigned>()->default_value(200), "clients per shard")
         ("requests-per-connection", bpo::value<uint64_t>()->default_value(1024), "number of requests issued before closing the connection and making new one")
         ("request-size", bpo::value<unsigned>()->default_value(1024), "request size")
         ("response-size", bpo::value<unsigned>()->default_value(1024), "response size")
         ("server-host", bpo::value<std::string>()->default_value("127.0.0.1"), "server address, defaults to localhost")
         ("server-port", bpo::value<uint16_t>()->default_value(1234), "server port")
     ;
     return app.run(argc, argv, [&app] () -> future<> {
         test_config conf;
         conf.duration = app.configuration()["duration"].as<unsigned>();
         conf.concurrency = app.configuration()["concurrency"].as<unsigned>();
         conf.requests_per_connection = app.configuration()["requests-per-connection"].as<uint64_t>();
         conf.request_size = app.configuration()["request-size"].as<unsigned>();
         conf.response_size = app.configuration()["response-size"].as<unsigned>();
         conf.server_host = app.configuration()["server-host"].as<std::string>();
         conf.server_port = app.configuration()["server-port"].as<uint16_t>();

         sharded<tester> test;
         plog.info("Starting");
         co_await test.start(std::cref(conf));
         co_await test.invoke_on_all(&tester::start);
         try {
             plog.info("Running");
             co_await test.invoke_on_all(&tester::run);
         } catch (...) {
             plog.error("Error running: {}", std::current_exception());
         }
         co_await test.stop();
     });
 }
