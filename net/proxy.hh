#ifndef PROXY_HH_
#define PROXY_HH_

#include <memory>
#include "net.hh"
#include "packet.hh"

namespace net {

std::unique_ptr<device> create_proxy_net_device(boost::program_options::variables_map opts = boost::program_options::variables_map());

}
#endif
