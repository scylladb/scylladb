#ifndef PROXY_HH_
#define PROXY_HH_

#include <memory>
#include "net.hh"
#include "packet.hh"

namespace net {

std::unique_ptr<slave_device> create_proxy_net_device(unsigned cpu, master_device* dev);

}
#endif
