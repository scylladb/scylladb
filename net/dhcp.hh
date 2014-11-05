/*
 * Copyright 2014 Cloudius Systems
 */

#ifndef NET_DHCP_HH_
#define NET_DHCP_HH_

#include "ip.hh"
#include "core/reactor.hh"

namespace net {

/*
 * Simplistic DHCP query class.
 * Due to the nature of the native stack,
 * it operates on an "ipv4" object instead of,
 * for example, an interface.
 */
class dhcp {
public:
    dhcp(ipv4 &);
    dhcp(dhcp &&);
    ~dhcp();

    static const clock_type::duration default_timeout;

    struct lease {
        ipv4_address ip;
        ipv4_address netmask;
        ipv4_address broadcast;

        ipv4_address gateway;
        ipv4_address dhcp_server;

        std::vector<ipv4_address> name_servers;

        std::chrono::seconds lease_time;
        std::chrono::seconds renew_time;
        std::chrono::seconds rebind_time;

        uint16_t mtu = 0;
    };

    typedef future<bool, lease> result_type;

    /**
     * Runs a discover/request sequence on the ipv4 "stack".
     * During this execution the ipv4 will be "hijacked"
     * more or less (through packet filter), and while not
     * inoperable, most likely quite less efficient.
     *
     * Please note that this does _not_ modify the ipv4 object bound.
     * It only makes queries and records replys for the related NIC.
     * It is up to caller to use the returned information as he se fit.
     */
    result_type discover(const clock_type::duration & = default_timeout);
    result_type renew(const lease &, const clock_type::duration & = default_timeout);
private:
    class impl;
    std::unique_ptr<impl> _impl;
};

}

#endif /* NET_DHCP_HH_ */
