
/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <chrono>
#include <optional>
#include <seastar/core/reactor.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>
#include <seastar/rpc/rpc_types.hh>
#include <seastar/util/closeable.hh>
#include "message/messaging_service.hh"
#include "gms/gossip_digest_syn.hh"
#include "gms/gossip_digest_ack.hh"
#include "gms/gossip_digest_ack2.hh"
#include "gms/gossip_digest.hh"
#include "api/api.hh"
#include "utils/UUID.hh"
#include "log.hh"
#include "locator/token_metadata.hh"
#include "db/schema_tables.hh"
#include "idl/gossip.dist.hh"

using namespace std::chrono_literals;
using namespace netw;

logging::logger test_logger("message_test");

class tester {
private:
    messaging_service& ms;
    gms::inet_address _server;
    uint32_t _cpuid;
public:
    tester(netw::messaging_service& ms_) : ms(ms_) {}
    using msg_addr = netw::messaging_service::msg_addr;
    using inet_address = gms::inet_address;
    using endpoint_state = gms::endpoint_state;
    msg_addr get_msg_addr() {
        return msg_addr{_server, _cpuid};
    }
    void set_server_ip(sstring ip) {
        _server = inet_address(ip);
    }
    void set_server_cpuid(uint32_t cpu) {
        _cpuid = cpu;
    }
    future<> stop() {
        return make_ready_future<>();
    }
    promise<> digest_test_done;

    uint16_t port() const { return ms.port(); }
public:
    void init_handler() {
        ser::gossip_rpc_verbs::register_gossip_digest_syn(&ms, [this] (const rpc::client_info& cinfo, gms::gossip_digest_syn msg) {
            test_logger.info("Server got syn msg = {}", msg);

            auto from = netw::messaging_service::get_source(cinfo);
            auto ep1 = inet_address("1.1.1.1");
            auto ep2 = inet_address("2.2.2.2");
            gms::generation_type gen(800);
            gms::version_type ver(900);
            utils::chunked_vector<gms::gossip_digest> digests;
            digests.push_back(gms::gossip_digest(ep1, gen++, ver++));
            digests.push_back(gms::gossip_digest(ep2, gen++, ver++));
            std::map<inet_address, endpoint_state> eps{
                {ep1, endpoint_state()},
                {ep2, endpoint_state()},
            };
            gms::gossip_digest_ack ack(std::move(digests), std::move(eps));
            // FIXME: discarded future.
            (void)ser::gossip_rpc_verbs::send_gossip_digest_ack(&ms, from, std::move(ack)).handle_exception([] (auto ep) {
                test_logger.error("Fail to send ack : {}", ep);
            });
            return make_ready_future<rpc::no_wait_type>(netw::messaging_service::no_wait());
        });

        ser::gossip_rpc_verbs::register_gossip_digest_ack(&ms, [this] (const rpc::client_info& cinfo, gms::gossip_digest_ack msg) {
            test_logger.info("Server got ack msg = {}", msg);
            auto from = netw::messaging_service::get_source(cinfo);
            // Prepare gossip_digest_ack2 message
            auto ep1 = inet_address("3.3.3.3");
            std::map<inet_address, endpoint_state> eps{
                {ep1, endpoint_state()},
            };
            gms::gossip_digest_ack2 ack2(std::move(eps));
            // FIXME: discarded future.
            (void)ser::gossip_rpc_verbs::send_gossip_digest_ack2(&ms, from, std::move(ack2)).handle_exception([] (auto ep) {
                test_logger.error("Fail to send ack2 : {}", ep);
            });
            digest_test_done.set_value();
            return make_ready_future<rpc::no_wait_type>(netw::messaging_service::no_wait());
        });

        ser::gossip_rpc_verbs::register_gossip_digest_ack2(&ms, [] (const rpc::client_info& cinfo, gms::gossip_digest_ack2 msg) {
            test_logger.info("Server got ack2 msg = {}", msg);
            return make_ready_future<rpc::no_wait_type>(netw::messaging_service::no_wait());
        });

        ser::gossip_rpc_verbs::register_gossip_shutdown(&ms, [] (inet_address from, rpc::optional<int64_t> generation_number_opt) {
            test_logger.info("Server got shutdown msg = {}", from);
            return make_ready_future<rpc::no_wait_type>(netw::messaging_service::no_wait());
        });

        ser::gossip_rpc_verbs::register_gossip_echo(&ms, [] (const rpc::client_info& cinfo, rpc::opt_time_point, rpc::optional<int64_t> gen_opt, rpc::optional<bool> notify_up) {
            test_logger.info("Server got gossip echo msg");
            throw std::runtime_error("I'm throwing runtime_error exception");
            return make_ready_future<>();
        });
    }

    future<> deinit_handler() {
        co_await ser::gossip_rpc_verbs::unregister(&ms);
        test_logger.info("tester deinit_hadler done");
    }

public:
    future<> test_gossip_digest() {
        test_logger.info("=== {} ===", __func__);
        // Prepare gossip_digest_syn message
        auto id = get_msg_addr();
        auto ep1 = inet_address("1.1.1.1");
        auto ep2 = inet_address("2.2.2.2");
        gms::generation_type gen(100);
        gms::version_type ver(900);
        utils::chunked_vector<gms::gossip_digest> digests;
        digests.push_back(gms::gossip_digest(ep1, gen++, ver++));
        digests.push_back(gms::gossip_digest(ep2, gen++, ver++));
        gms::gossip_digest_syn syn("my_cluster", "my_partition", digests, utils::null_uuid());
        return ser::gossip_rpc_verbs::send_gossip_digest_syn(&ms, id, std::move(syn)).then([this] {
            test_logger.info("Sent gossip sigest syn. Waiting for digest_test_done...");
            return digest_test_done.get_future();
        });
    }

    future<> test_gossip_shutdown() {
        test_logger.info("=== {} ===", __func__);
        auto id = get_msg_addr();
        inet_address from("127.0.0.1");
        int64_t gen = 0x1;
        return ser::gossip_rpc_verbs::send_gossip_shutdown(&ms, id, from, gen).then([] () {
            test_logger.info("Client sent gossip_shutdown got reply = void");
            return make_ready_future<>();
        });
    }

    future<> test_echo() {
        test_logger.info("=== {} ===", __func__);
        auto id = get_msg_addr();
        int64_t gen = 0x1;
        return ser::gossip_rpc_verbs::send_gossip_echo(&ms, id, netw::messaging_service::clock_type::now() + std::chrono::seconds(10), gen, false).then_wrapped([] (auto&& f) {
            try {
                f.get();
                return make_ready_future<>();
            } catch (std::runtime_error& e) {
                test_logger.error("test_echo: {}", e.what());
            }
            return make_ready_future<>();
        });
    }
};

namespace bpo = boost::program_options;

// Usage example: build/dev/test/manual/message --listen 127.0.0.1 --server 127.0.0.1
int main(int ac, char ** av) {
    app_template app;
    app.add_options()
        ("server", bpo::value<std::string>(), "Server ip")
        ("listen-address", bpo::value<std::string>()->default_value("0.0.0.0"), "IP address to listen")
        ("api-port", bpo::value<uint16_t>()->default_value(10000), "Http Rest API port")
        ("stay-alive", bpo::value<bool>()->default_value(false), "Do not kill the test server after the test")
        ("cpuid", bpo::value<uint32_t>()->default_value(0), "Server cpuid");

    distributed<replica::database> db;

    return app.run_deprecated(ac, av, [&app] {
        return seastar::async([&app] {
            auto config = app.configuration();
            bool stay_alive = config["stay-alive"].as<bool>();
            const gms::inet_address listen = gms::inet_address(config["listen-address"].as<std::string>());
            auto my_address = listen != gms::inet_address("0.0.0.0") ? listen : gms::inet_address("localhost");
            locator::token_metadata::config tm_cfg;
            tm_cfg.topo_cfg.this_endpoint = my_address;
            sharded<locator::shared_token_metadata> token_metadata;
            token_metadata.start([] () noexcept { return db::schema_tables::hold_merge_lock(); }, tm_cfg).get();
            auto stop_tm = deferred_stop(token_metadata);
            seastar::sharded<netw::messaging_service> messaging;
            messaging.start(locator::host_id{}, listen, 7000).get();
            auto stop_messaging = deferred_stop(messaging);
            seastar::sharded<tester> testers;
            testers.start(std::ref(messaging)).get();
            auto stop_testers = deferred_stop(testers);
            auto port = testers.local().port();
            test_logger.info("Messaging server listening on {} port {}", listen, port);
            testers.invoke_on_all(&tester::init_handler).get();
            auto deinit_testers = deferred_action([&testers] {
                testers.invoke_on_all(&tester::deinit_handler).get();
            });
            messaging.invoke_on_all(&netw::messaging_service::start_listen, std::ref(token_metadata)).get();
            if (config.contains("server")) {
                auto ip = config["server"].as<std::string>();
                auto cpuid = config["cpuid"].as<uint32_t>();
                auto t = &testers.local();
                t->set_server_ip(ip);
                t->set_server_cpuid(cpuid);
                test_logger.info("=============TEST START===========");
                test_logger.info("Sending to server ....");
                t->test_gossip_digest().get();
                t->test_gossip_shutdown().get();
                t->test_echo().get();
                test_logger.info("=============TEST DONE===========");
            }
            while (stay_alive) {
                seastar::sleep(1s).get();
            }
        }).finally([] {
            exit(0);
        });
    });
}
