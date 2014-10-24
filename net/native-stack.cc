/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "native-stack.hh"
#include "native-stack-impl.hh"
#include "net.hh"
#include "ip.hh"
#include "tcp.hh"
#include "udp.hh"
#include "virtio.hh"
#include "proxy.hh"
#include <memory>
#include <queue>

namespace net {

// native_network_stack
class native_network_stack : public network_stack {
    static std::unique_ptr<native_network_stack> _s;
    interface _netif;
    ipv4 _inet;
    udp_v4 _udp;
    using tcp4 = tcp<ipv4_traits>;
public:
    explicit native_network_stack(boost::program_options::variables_map opts);
    virtual server_socket listen(socket_address sa, listen_options opt) override;
    virtual udp_channel make_udp_channel(ipv4_addr addr) override;
    static std::unique_ptr<network_stack> create(boost::program_options::variables_map opts) {
        return std::make_unique<native_network_stack>(opts);
    }
    friend class native_server_socket_impl<tcp4>;
};

udp_channel
native_network_stack::make_udp_channel(ipv4_addr addr) {
    return _udp.make_channel(addr);
}

native_network_stack::native_network_stack(boost::program_options::variables_map opts)
    : _netif(smp::main_thread() ? create_virtio_net_device(opts["tap-device"].as<std::string>(), opts) : create_proxy_net_device(opts))
    , _inet(&_netif)
    , _udp(_inet) {
    _inet.set_host_address(ipv4_address(opts["host-ipv4-addr"].as<std::string>()));
    _inet.set_gw_address(ipv4_address(opts["gw-ipv4-addr"].as<std::string>()));
    _inet.set_netmask_address(ipv4_address(opts["netmask-ipv4-addr"].as<std::string>()));
    _udp.set_queue_size(opts["udpv4-queue-size"].as<int>());
}

server_socket
native_network_stack::listen(socket_address sa, listen_options opts) {
    assert(sa.as_posix_sockaddr().sa_family == AF_INET);
    return server_socket(std::make_unique<native_server_socket_impl<tcp4>>(
            _inet.get_tcp(), ntohs(sa.as_posix_sockaddr_in().sin_port), opts));
}

std::unique_ptr<native_network_stack> native_network_stack::_s;

boost::program_options::options_description nns_options() {
    boost::program_options::options_description opts(
            "Native networking stack options");
    opts.add_options()
        ("tap-device",
                boost::program_options::value<std::string>()->default_value("tap0"),
                "tap device to connect to")
        ("host-ipv4-addr",
                boost::program_options::value<std::string>()->default_value("192.168.122.2"),
                "static IPv4 address to use")
        ("gw-ipv4-addr",
                boost::program_options::value<std::string>()->default_value("192.168.122.1"),
                "static IPv4 gateway to use")
        ("netmask-ipv4-addr",
                boost::program_options::value<std::string>()->default_value("255.255.255.0"),
                "static IPv4 netmask to use")
        ("udpv4-queue-size",
                boost::program_options::value<int>()->default_value(udp_v4::default_queue_size),
                "Default size of the UDPv4 per-channel packet queue")
        ;
    opts.add(get_virtio_net_options_description());
    return opts;
}

network_stack_registrator nns_registrator{
    "native", nns_options(), native_network_stack::create
};

}
