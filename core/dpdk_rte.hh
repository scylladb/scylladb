#ifndef DPDK_RTE_HH_
#define DPDK_RTE_HH_

#ifdef HAVE_DPDK

#include <bitset>
#include <rte_config.h>
#include <rte_ethdev.h>
#include <boost/program_options.hpp>

namespace dpdk {

// DPDK Environment Abstraction Layer
class eal {
public:
    using cpuset = std::bitset<RTE_MAX_LCORE>;

    static void init(cpuset cpus, boost::program_options::variables_map opts);
    static bool initialized;
};

} // namespace dpdk
#endif // HAVE_DPDK
#endif // DPDK_RTE_HH_
