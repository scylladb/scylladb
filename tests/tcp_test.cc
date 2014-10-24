/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "net/ip.hh"
#include "net/virtio.hh"
#include "net/tcp.hh"

using namespace net;

struct tcp_test {
    ipv4& inet;
    using tcp = net::tcp<ipv4_traits>;
    tcp::listener _listener;
    struct connection {
        tcp::connection tcp_conn;
        explicit connection(tcp::connection tc) : tcp_conn(std::move(tc)) {}
        void run() {
            tcp_conn.wait_for_data().then([this] {
                auto p = tcp_conn.read();
                if (!p.len()) {
                    tcp_conn.close_write();
                    return;
                }
                print("read %d bytes\n", p.len());
                tcp_conn.send(std::move(p));
                run();
            });
        }
    };
    tcp_test(ipv4& inet) : inet(inet), _listener(inet.get_tcp().listen(10000)) {}
    void run() {
        _listener.accept().then([this] (tcp::connection conn) {
            (new connection(std::move(conn)))->run();
            run();
        });
    }
};

int main(int ac, char** av) {
    auto vnet = create_virtio_net_device("tap0");
    interface netif(std::move(vnet));
    ipv4 inet(&netif);
    inet.set_host_address(ipv4_address("192.168.122.2"));
    tcp_test tt(inet);
    engine.when_started().then([&tt] { tt.run(); });
    engine.run();
}


