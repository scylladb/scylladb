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
}

future<> controller::stop_server() {
    co_await _server.stop();
    _listen_addresses.clear();
}

future<> controller::request_stop_server() {
    return stop_server();
}

}
