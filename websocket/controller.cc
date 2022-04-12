#include "websocket/controller.hh"
#include <seastar/core/coroutine.hh>

namespace websocket {

sstring controller::name() const {
    return "websocket";
}

sstring controller::protocol() const {
    return "websocket";
}

sstring controller::protocol_version() const {
    return ::websocket::version;
}

std::vector<socket_address> controller::listen_addresses() const {
    return _listen_addresses;
}

future<> controller::start_server() {
    co_await _server.start();
    co_await _server.invoke_on_all([this] (experimental::websocket::server& ws) {
        ws.register_handler("cql", [this] (input_stream<char>& in, output_stream<char>& out) -> future<> {
            auto unix_server_socket = seastar::listen(socket_address{unix_domain_addr{""}});
            auto unix_addr = unix_server_socket.local_address();
            auto accept_fut = unix_server_socket.accept();
            auto sock = co_await connect(unix_addr);
            auto accept_result = co_await std::move(accept_fut);
            auto connection = _cql_server.local().inject_connection(unix_addr,
                    std::move(accept_result.connection), std::move(accept_result.remote_address));

            input_stream<char> cql_in = sock.input();
            output_stream<char> cql_out = sock.output();

            auto bridge = [] (input_stream<char>& input, output_stream<char>& output) -> future<> {
                while (true) {
                    temporary_buffer<char> buff = co_await input.read();
                    if (buff.empty()) {
                        co_return;
                    }
                    co_await output.write(std::move(buff));
                    co_await output.flush();
                }
            };

            auto read_loop = bridge(in, cql_out);
            auto write_loop = bridge(cql_in, out);

            co_await std::move(write_loop);
        });
        ws.listen(socket_address(ipv4_addr("127.0.0.1", 8222)));
    });
}

future<> controller::stop_server() {
    co_await _server.stop();
    _listen_addresses.clear();
}

future<> controller::request_stop_server() {
    return stop_server();
}

}
