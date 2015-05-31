#include "core/reactor.hh"
#include "core/app-template.hh"
#include "core/sstring.hh"
#include "message/messaging_service.hh"
#include "gms/gossip_digest_syn.hh"
#include "gms/gossip_digest_ack.hh"
#include "gms/gossip_digest_ack2.hh"
#include "gms/gossip_digest.hh"
#include "core/sleep.hh"

using namespace std::chrono_literals;
using namespace net;

struct empty_msg {
    void serialize(bytes::iterator& out) const {
    }
    static empty_msg deserialize(bytes_view& v) {
        return empty_msg();
    }
    size_t serialized_size() const {
        return 0;
    }
    friend inline std::ostream& operator<<(std::ostream& os, const empty_msg& ack) {
        return os << "empty_msg";
    }
};

class tester {
private:
    messaging_service& ms;
    gms::inet_address _server;
    uint32_t _cpuid;
public:
    tester()
       : ms(get_local_messaging_service()) {
    }
    using shard_id = net::messaging_service::shard_id;
    using inet_address = gms::inet_address;
    using endpoint_state = gms::endpoint_state;
    shard_id get_shard_id() {
        return shard_id{_server, _cpuid};
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
public:
    void init_handler() {
        ms.register_handler(messaging_verb::GOSSIP_DIGEST_SYN, [] (gms::gossip_digest_syn msg) {
            print("Server got syn msg = %s\n", msg);
            auto ep1 = inet_address("1.1.1.1");
            auto ep2 = inet_address("2.2.2.2");
            int32_t gen = 800;
            int32_t ver = 900;
            std::vector<gms::gossip_digest> digests{
                {ep1, gen++, ver++},
                {ep2, gen++, ver++},
            };
            std::map<inet_address, endpoint_state> eps{
                {ep1, endpoint_state()},
                {ep2, endpoint_state()},
            };
            gms::gossip_digest_ack ack(std::move(digests), std::move(eps));
            return make_ready_future<gms::gossip_digest_ack>(ack);
        });
        ms.register_handler(messaging_verb::GOSSIP_DIGEST_ACK2, [] (gms::gossip_digest_ack2 msg) {
            print("Server got ack2 msg = %s\n", msg);
            return messaging_service::no_wait();
        });
        ms.register_handler(messaging_verb::GOSSIP_SHUTDOWN, [] (empty_msg msg) {
            print("Server got shutdown msg = %s\n", msg);
            return messaging_service::no_wait();
        });
        ms.register_handler(messaging_verb::ECHO, [] (int x, int y) {
            print("Server got echo msg = (%d, %ld) \n", x, y);
            return make_ready_future<int, long>(x*x, y*y);
        });
        ms.register_handler(messaging_verb::UNUSED_1, [] (int x, int y) {
            print("Server got echo msg = (%d, %ld) \n", x, y);
            throw std::runtime_error("I'm throwing runtime_error exception");
            long ret = x + y;
            return make_ready_future<decltype(ret)>(ret);
        });
    }

public:
    future<> test_gossip_digest() {
        print("=== %s ===\n", __func__);
        // Prepare gossip_digest_syn message
        auto id = get_shard_id();
        auto ep1 = inet_address("1.1.1.1");
        auto ep2 = inet_address("2.2.2.2");
        int32_t gen = 100;
        int32_t ver = 900;
        std::vector<gms::gossip_digest> digests{
            {ep1, gen++, ver++},
            {ep2, gen++, ver++},
        };
        gms::gossip_digest_syn syn("my_cluster", "my_partition", digests);
        using RetMsg = gms::gossip_digest_ack;
        return ms.send_message<RetMsg>(messaging_verb::GOSSIP_DIGEST_SYN, std::move(id), std::move(syn)).then([this, id] (RetMsg ack) {
            print("Client sent gossip_digest_syn got gossip_digest_ack reply = %s\n", ack);
            // Prepare gossip_digest_ack2 message
            auto ep1 = inet_address("3.3.3.3");
            std::map<inet_address, endpoint_state> eps{
                {ep1, endpoint_state()},
            };
            gms::gossip_digest_ack2 ack2(std::move(eps));
            return ms.send_message_oneway(messaging_verb::GOSSIP_DIGEST_ACK2, std::move(id), std::move(ack2)).then([] () {
                print("Client sent gossip_digest_ack2 got reply = void\n");
                return make_ready_future<>();
            });
        });
    }

    future<> test_gossip_shutdown() {
        print("=== %s ===\n", __func__);
        auto id = get_shard_id();
        empty_msg msg;
        return ms.send_message_oneway(messaging_verb::GOSSIP_SHUTDOWN, std::move(id), std::move(msg)).then([] () {
            print("Client sent gossip_shutdown got reply = void\n");
            return make_ready_future<>();
        });
    }

    future<> test_echo() {
        print("=== %s ===\n", __func__);
        auto id = get_shard_id();
        using RetMsg = future<int, long>;
        return ms.send_message<RetMsg>(messaging_verb::ECHO, id, 30, 60).then_wrapped([] (future<int, long> f) {
            try {
                auto msg = f.get();
                print("Client sent echo got reply = (%d , %ld)\n", std::get<0>(msg), std::get<1>(msg));
                return sleep(100ms).then([]{
                    return make_ready_future<>();
                });
            } catch (std::runtime_error& e) {
                print("test_echo: %s\n", e.what());
            }
            return make_ready_future<>();
        });
    }

    future<> test_exception() {
        print("=== %s ===\n", __func__);
        auto id = get_shard_id();
        return ms.send_message<long>(messaging_verb::UNUSED_1, id, 3, 6).then_wrapped([] (future<long> f) {
            try {
                auto ret = std::get<0>(f.get());
                print("Client sent UNUSED_1 got reply = %ld\n", ret);
                return make_ready_future<>();
            } catch (std::runtime_error& e) {
                print("Client sent UNUSED_1 got exception: %s\n", e.what());
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
        ("cpuid", bpo::value<uint32_t>()->default_value(0), "Server cpuid");
    return app.run(ac, av, [&app] {
        auto config = app.configuration();
        const gms::inet_address listen = gms::inet_address(config["listen-address"].as<std::string>());
        net::get_messaging_service().start(listen).then([config] () {
            auto testers = new distributed<tester>;
            testers->start().then([testers]{
                auto& server = net::get_local_messaging_service();
                auto port = server.port();
                std::cout << "Messaging server listening on port " << port << " ...\n";
                return testers->invoke_on_all(&tester::init_handler);
            }).then([testers, config] {
                auto t = &testers->local();
                if (!config.count("server")) {
                    return;
                }
                auto ip = config["server"].as<std::string>();
                auto cpuid = config["cpuid"].as<uint32_t>();
                t->set_server_ip(ip);
                t->set_server_cpuid(cpuid);
                print("=============TEST START===========\n");
                print("Sending to server ....\n");
                t->test_gossip_digest().then([testers, t] {
                    return t->test_gossip_shutdown();
                }).then([testers, t] {
                    return t->test_echo();
                }).then([testers, t] {
                    return t->test_exception();
                }).then([testers, t] {
                    print("=============TEST DONE===========\n");
                    testers->stop().then([testers] {
                        delete testers;
                        net::get_messaging_service().stop().then([]{
                            engine().exit(0);
                        });
                    });
                });
            });
        });
    });
}
