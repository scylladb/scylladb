/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "net/ip.hh"
#include "net/virtio.hh"

using namespace net;

struct tcp_test {
    ipv4& inet;
    using tcp = net::tcp<ipv4_traits>;
    struct connection {
        tcp::connection tcp_conn;
        explicit connection(tcp::connection tc) : tcp_conn(std::move(tc)) {}
        void run() {
            tcp_conn.receive().then([this] (packet p) {
                tcp_conn.send(std::move(p));
                run();
            });
        }
    };
    tcp_test(ipv4& inet) : inet(inet) {}
    void run() {
        inet.get_tcp().listen(10000).then([this] (tcp::connection conn) {
            (new connection(std::move(conn)))->run();
            run();
        });
    }
};

int main(int ac, char** av) {
    auto vnet = create_virtio_net_device("tap0");
    interface netif(std::move(vnet));
    netif.run();
    ipv4 inet(&netif);
    inet.set_host_address(ipv4_address(0xc0a87a02));
    tcp_test tt(inet);
    the_reactor.start().then([&tt] { tt.run(); });
    the_reactor.run();
}


