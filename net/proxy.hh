#ifndef PROXY_HH_
#define PROXY_HH_

#include <memory>
#include "net.hh"
#include "packet.hh"

namespace net {

std::unique_ptr<qp> create_proxy_net_device(unsigned master_cpu, device* dev);

}
#endif
