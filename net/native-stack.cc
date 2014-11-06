/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "native-stack.hh"
#include "native-stack-impl.hh"
#include "net.hh"
#include "ip.hh"
#include "tcp-stack.hh"
#include "udp.hh"
#include "virtio.hh"
#include "xenfront.hh"
#include "proxy.hh"
#include "dhcp.hh"
#include <memory>
#include <queue>
#ifdef HAVE_OSV
#include <osv/firmware.hh>
#include <gnu/libc-version.h>
#endif
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

namespace net {

// native_network_stack
class native_network_stack : public network_stack {
    static std::unique_ptr<native_network_stack> _s;
    interface _netif;
    ipv4 _inet;
    udp_v4 _udp;
    bool _dhcp = false;
    promise<> _config;
    timer _timer;

    void on_dhcp(bool, const dhcp::lease &);

    using tcp4 = tcp<ipv4_traits>;
public:
    explicit native_network_stack(boost::program_options::variables_map opts);
    virtual server_socket listen(socket_address sa, listen_options opt) override;
    virtual udp_channel make_udp_channel(ipv4_addr addr) override;
    virtual future<> initialize() override;
    static std::unique_ptr<network_stack> create(boost::program_options::variables_map opts) {
        return std::make_unique<native_network_stack>(opts);
    }
    friend class native_server_socket_impl<tcp4>;
};

udp_channel
native_network_stack::make_udp_channel(ipv4_addr addr) {
    return _udp.make_channel(addr);
}

enum class xen_info {
    nonxen = 0,
    userspace = 1,
    osv = 2,
};

#ifdef HAVE_XEN
static xen_info is_xen()
{
    struct stat buf;
    if (!stat("/proc/xen", &buf) || !stat("/dev/xen", &buf)) {
        return xen_info::userspace;
    }

#ifdef HAVE_OSV
    const char *str = gnu_get_libc_release();
    if (std::string("OSv") != str) {
        return xen_info::nonxen;
    }
    auto firmware = osv::firmware_vendor();
    if (firmware == "Xen") {
        return xen_info::osv;
    }
#endif

    return xen_info::nonxen;
}
#endif

std::unique_ptr<net::device> create_native_net_device(boost::program_options::variables_map opts) {

    if (!smp::main_thread()) {
        return create_proxy_net_device(opts);
    }

#ifdef HAVE_XEN
    auto xen = is_xen();
    if (xen != xen_info::nonxen) {
        return create_xenfront_net_device(opts, xen == xen_info::userspace);
    }
#endif
    return create_virtio_net_device(opts["tap-device"].as<std::string>(), opts);
}

void
add_native_net_options_description(boost::program_options::options_description &opts) {

#ifdef HAVE_XEN
    auto xen = is_xen();
    if (xen != xen_info::nonxen) {
        opts.add(get_xenfront_net_options_description());
        return;
    }
#endif
    opts.add(get_virtio_net_options_description());
}

native_network_stack::native_network_stack(boost::program_options::variables_map opts)
    : _netif(create_native_net_device(opts))
    , _inet(&_netif)
    , _udp(_inet) {
    _inet.set_host_address(ipv4_address(opts["host-ipv4-addr"].as<std::string>()));
    _inet.set_gw_address(ipv4_address(opts["gw-ipv4-addr"].as<std::string>()));
    _inet.set_netmask_address(ipv4_address(opts["netmask-ipv4-addr"].as<std::string>()));
    _udp.set_queue_size(opts["udpv4-queue-size"].as<int>());
    _dhcp = opts["host-ipv4-addr"].defaulted()
            && opts["gw-ipv4-addr"].defaulted()
            && opts["netmask-ipv4-addr"].defaulted() && opts["dhcp"].as<bool>();
}

server_socket
native_network_stack::listen(socket_address sa, listen_options opts) {
    assert(sa.as_posix_sockaddr().sa_family == AF_INET);
    return tcpv4_listen(_inet.get_tcp(), ntohs(sa.as_posix_sockaddr_in().sin_port), opts);
}

void native_network_stack::on_dhcp(bool success, const dhcp::lease & res) {
    if (success) {
        _inet.set_host_address(res.ip);
        _inet.set_gw_address(res.gateway);
        _inet.set_netmask_address(res.netmask);
    }

    // Signal waiters.
    _config.set_value();

    if (smp::main_thread()) {
        // And the other cpus, which, in the case of initial discovery,
        // will be waiting for us.
        for (unsigned i = 1; i < smp::count; i++) {
            smp::submit_to(i, [success, res]() {
                auto & ns = static_cast<native_network_stack&>(engine.net());
                ns.on_dhcp(success, res);
            });
        }
        // And set up to renew the lease later on.
        _timer.set_callback([this, res]() {
            shared_ptr<dhcp> d = make_shared<dhcp>(_inet);
            d->renew(res).then([this, d](bool success, const dhcp::lease & res) {
                on_dhcp(success, res);
            });
        });
        _timer.arm(std::chrono::duration_cast<clock_type::duration>(res.lease_time));
    }
}

future<> native_network_stack::initialize() {
    return network_stack::initialize().then([this]() {
        if (!_dhcp) {
            return make_ready_future();
        }
        // Only run actual discover on main cpu.
        // All other cpus must simply for main thread to complete and signal them.
        if (smp::main_thread()) {
            shared_ptr<dhcp> d = make_shared<dhcp>(_inet);
            return d->discover().then([this, d](bool success, const dhcp::lease & res) {
                on_dhcp(success, res);
            });
        }
        return _config.get_future();
    });
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
        ("dhcp",
                boost::program_options::value<bool>()->default_value(true),
                        "Use DHCP discovery")
        ;

    add_native_net_options_description(opts);
    return opts;
}

network_stack_registrator nns_registrator{
    "native", nns_options(), native_network_stack::create
};

}
