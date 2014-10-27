/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "core/app-template.hh"
#include "core/future-util.hh"

using namespace net;
using namespace std::chrono_literals;

class server {
private:
    udp_channel _chan;
    timer _stats_timer;
    uint64_t _n_sent {};
public:
    void start() {
        ipv4_addr listen_addr{10000};
        _chan = engine.net().make_udp_channel(listen_addr);

        std::cout << "Listening on " << listen_addr << std::endl;

        _stats_timer.set_callback([this] {
            std::cout << "Out: " << _n_sent << " pps" << std::endl;
            _n_sent = 0;
        });
        _stats_timer.arm_periodic(1s);

        keep_doing([this] {
            return _chan.receive().then([this] (udp_datagram dgram) {
                return _chan.send(dgram.get_src(), std::move(dgram.get_data())).then([this] {
                    _n_sent++;
                });
            });
        });
    }
};

int main(int ac, char ** av) {
    server s;
    app_template app;
    return app.run(ac, av, [&s] { s.start(); });
}
