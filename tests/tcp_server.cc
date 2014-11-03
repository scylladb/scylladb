/*
 * Copyright 2014 Cloudius Systems
 */

#include "core/reactor.hh"
#include "core/app-template.hh"
#include "core/temporary_buffer.hh"
#include "core/smp.hh"
#include <vector>
#include <iostream>

class http_server {
    std::vector<server_socket> _listeners;
public:
    future<> listen(ipv4_addr addr) {
        listen_options lo;
        lo.reuse_address = true;
        _listeners.push_back(engine.listen(make_ipv4_address(addr), lo));
        do_accepts(_listeners.size() - 1);
        return make_ready_future<>();
    }
    void do_accepts(int which) {
        _listeners[which].accept().then([this, which] (connected_socket fd, socket_address addr) mutable {
            auto conn = new connection(*this, std::move(fd), addr);
            conn->process().rescue([this, conn] (auto get_ex) {
                delete conn;
                try {
                    get_ex();
                } catch (std::exception& ex) {
                    std::cout << "request error " << ex.what() << "\n";
                }
            });
            do_accepts(which);
        }).rescue([] (auto get_ex) {
            try {
                get_ex();
            } catch (std::exception& ex) {
                std::cout << "accept failed: " << ex.what() << "\n";
            }
        });
    }
    class connection {
        connected_socket _fd;
        input_stream<char> _read_buf;
        output_stream<char> _write_buf;
    public:
        connection(http_server& server, connected_socket&& fd, socket_address addr)
            : _fd(std::move(fd))
            , _read_buf(_fd.input())
            , _write_buf(_fd.output()) {}
        future<> process() {
             return read();
        }
        future<> read() {
            if (_read_buf.eof()) {
                return make_ready_future();
            }
            // Expect a ping from client.
            size_t n = 4;
            return _read_buf.read_exactly(n).then([this] (temporary_buffer<char> buf) {
                if (buf.size() == 0) {
                    return make_ready_future();
                }
                return _write_buf.write("pong", 4).then([this] {
                    return _write_buf.flush();
                }).then([this] {
                    return this->read();
                });
            });
        }
    };
};

int main(int ac, char** av) {
    app_template app;
    app.add_options()
        ("port", bpo::value<uint16_t>()->default_value(10000), "TCP server port") ;
    return app.run(ac, av, [&] {
        auto&& config = app.configuration();
        uint16_t port = config["port"].as<uint16_t>();
        auto server = new distributed<http_server>;
        server->start().then([server = std::move(server), port] () mutable {
            server->invoke_on_all(&http_server::listen, ipv4_addr{port});
        }).then([port] {
            std::cout << "Seastar TCP server listening on port " << port << " ...\n";
        });
    });
}
