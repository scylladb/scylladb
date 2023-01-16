
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
#include <seastar/rpc/rpc_types.hh>
#include "message/messaging_service.hh"
#include "gms/gossip_digest_syn.hh"
#include "gms/gossip_digest_ack.hh"
#include "gms/gossip_digest_ack2.hh"
#include "gms/gossip_digest.hh"
#include "api/api.hh"
#include "utils/UUID.hh"
#include "utils/fb_utilities.hh"

using namespace std::chrono_literals;
using namespace netw;

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
        ms.register_gossip_digest_syn([this] (const rpc::client_info& cinfo, gms::gossip_digest_syn msg) {
            fmt::print("Server got syn msg = {}\n", msg);

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
            (void)ms.send_gossip_digest_ack(from, std::move(ack)).handle_exception([] (auto ep) {
                fmt::print("Fail to send ack : {}", ep);
            });
            return messaging_service::no_wait();
        });

        ms.register_gossip_digest_ack([this] (const rpc::client_info& cinfo, gms::gossip_digest_ack msg) {
            fmt::print("Server got ack msg = {}\n", msg);
            auto from = netw::messaging_service::get_source(cinfo);
            // Prepare gossip_digest_ack2 message
            auto ep1 = inet_address("3.3.3.3");
            std::map<inet_address, endpoint_state> eps{
                {ep1, endpoint_state()},
            };
            gms::gossip_digest_ack2 ack2(std::move(eps));
            // FIXME: discarded future.
            (void)ms.send_gossip_digest_ack2(from, std::move(ack2)).handle_exception([] (auto ep) {
                fmt::print("Fail to send ack2 : {}", ep);
            });
            digest_test_done.set_value();
            return messaging_service::no_wait();
        });

        ms.register_gossip_digest_ack2([] (const rpc::client_info& cinfo, gms::gossip_digest_ack2 msg) {
            fmt::print("Server got ack2 msg = {}\n", msg);
            return messaging_service::no_wait();
        });

        ms.register_gossip_shutdown([] (inet_address from, rpc::optional<int64_t> generation_number_opt) {
            fmt::print("Server got shutdown msg = {}\n", from);
            return messaging_service::no_wait();
        });

        ms.register_gossip_echo([] (const rpc::client_info& cinfo, rpc::optional<int64_t> gen_opt) {
            fmt::print("Server got gossip echo msg\n");
            throw std::runtime_error("I'm throwing runtime_error exception");
            return make_ready_future<>();
        });
    }

public:
    future<> test_gossip_digest() {
        fmt::print("=== {} ===\n", __func__);
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
        return ms.send_gossip_digest_syn(id, std::move(syn)).then([this] {
            return digest_test_done.get_future();
        });
    }

    future<> test_gossip_shutdown() {
        fmt::print("=== {} ===\n", __func__);
        auto id = get_msg_addr();
        inet_address from("127.0.0.1");
        int64_t gen = 0x1;
        return ms.send_gossip_shutdown(id, from, gen).then([] () {
            fmt::print("Client sent gossip_shutdown got reply = void\n");
            return make_ready_future<>();
        });
    }

    future<> test_echo() {
        fmt::print("=== {} ===\n", __func__);
        auto id = get_msg_addr();
        int64_t gen = 0x1;
        return ms.send_gossip_echo(id, gen, std::chrono::seconds(10)).then_wrapped([] (auto&& f) {
            try {
                f.get();
                return make_ready_future<>();
            } catch (std::runtime_error& e) {
                fmt::print("test_echo: {}\n", e.what());
            }
            return make_ready_future<>();
        });
    }
};

namespace bpo = boost::program_options;

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
        auto config = app.configuration();
        bool stay_alive = config["stay-alive"].as<bool>();
        const gms::inet_address listen = gms::inet_address(config["listen-address"].as<std::string>());
        utils::fb_utilities::set_broadcast_address(listen);
        utils::fb_utilities::set_host_id(locator::host_id::create_random_id());
        seastar::sharded<netw::messaging_service> messaging;
        return messaging.start(locator::host_id{}, listen, 7000).then([config, stay_alive, &messaging] () {
            auto testers = new distributed<tester>;
            return testers->start(std::ref(messaging)).then([testers]{
                auto port = testers->local().port();
                std::cout << "Messaging server listening on port " << port << " ...\n";
                return testers->invoke_on_all(&tester::init_handler);
            }).then([testers, config, stay_alive, &messaging] {
                auto t = &testers->local();
                if (!config.contains("server")) {
                    return make_ready_future<>();
                }
                auto ip = config["server"].as<std::string>();
                auto cpuid = config["cpuid"].as<uint32_t>();
                t->set_server_ip(ip);
                t->set_server_cpuid(cpuid);
                fmt::print("=============TEST START===========\n");
                fmt::print("Sending to server ....\n");
                return t->test_gossip_digest().then([t] {
                    return t->test_gossip_shutdown();
                }).then([t] {
                    return t->test_echo();
                }).then([testers, stay_alive, &messaging] {
                    if (stay_alive) {
                        return make_ready_future<>();
                    }
                    fmt::print("=============TEST DONE===========\n");
                    return testers->stop().then([testers, &messaging] {
                        delete testers;
                        return messaging.stop().then([]{
                            engine().exit(0);
                        });
                    });
                });
            });
        });
    });
}
