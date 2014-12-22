#ifndef DPDK_RTE_HH_
#define DPDK_RTE_HH_

#ifdef HAVE_DPDK

#include <rte_config.h>
#include <rte_ethdev.h>
#include <boost/program_options.hpp>

namespace dpdk {

// DPDK Environment Abstraction Layer
class eal {
public:
    static void init(boost::program_options::variables_map opts);
    static bool initialized;
};

} // namespace dpdk
#endif // HAVE_DPDK
#endif // DPDK_RTE_HH_
