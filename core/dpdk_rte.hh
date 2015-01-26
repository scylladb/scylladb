#ifndef DPDK_RTE_HH_
#define DPDK_RTE_HH_

#ifdef HAVE_DPDK

#include <bitset>
#include <rte_config.h>
#include <rte_ethdev.h>
#include <rte_version.h>
#include <boost/program_options.hpp>

/*********************** Compat section ***************************************/
// We currently support only versions 1.7 and above.
// So, since currently the only above version is 1.8.x we will use it as "else"
// of ver. 1.7.x.
#if (RTE_VERSION >= RTE_VERSION_NUM(1,7,0,0)) && \
    (RTE_VERSION  < RTE_VERSION_NUM(1,8,0,0))
#define RTE_VERSION_1_7
#endif

#ifdef RTE_VERSION_1_7

#define rte_mbuf_vlan_tci(m) ((m)->pkt.vlan_macip.f.vlan_tci)
#define rte_mbuf_rss_hash(m) ((m)->pkt.hash.rss)
#define rte_mbuf_data_len(m) ((m)->pkt.data_len)
#define rte_mbuf_pkt_len(m)  ((m)->pkt.pkt_len)
#define rte_mbuf_next(m)     ((m)->pkt.next)
#define rte_mbuf_nb_segs(m)  ((m)->pkt.nb_segs)
#define rte_mbuf_l2_len(m)   ((m)->pkt.vlan_macip.f.l2_len)
#define rte_mbuf_l3_len(m)   ((m)->pkt.vlan_macip.f.l3_len)

#else

#define rte_mbuf_vlan_tci(m) ((m)->vlan_tci)
#define rte_mbuf_rss_hash(m) ((m)->hash.rss)
#define rte_mbuf_data_len(m) ((m)->data_len)
#define rte_mbuf_pkt_len(m)  ((m)->pkt_len)
#define rte_mbuf_next(m)     ((m)->next)
#define rte_mbuf_nb_segs(m)  ((m)->nb_segs)
#define rte_mbuf_l2_len(m)   ((m)->l2_len)
#define rte_mbuf_l3_len(m)   ((m)->l3_len)

#endif

/******************************************************************************/

namespace dpdk {

// DPDK Environment Abstraction Layer
class eal {
public:
    using cpuset = std::bitset<RTE_MAX_LCORE>;

    static void init(cpuset cpus, boost::program_options::variables_map opts);
    /**
     * Returns the amount of memory needed for DPDK
     * @param num_cpus Number of CPUs the application is going to use
     *
     * @return
     */
    static size_t mem_size(int num_cpus);
    static bool initialized;
};

} // namespace dpdk
#endif // HAVE_DPDK
#endif // DPDK_RTE_HH_
