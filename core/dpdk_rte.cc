#ifdef HAVE_DPDK

#include "core/dpdk_rte.hh"
#include "util/conversions.hh"
#include <experimental/optional>
#include <rte_pci.h>

namespace dpdk {

bool eal::initialized = false;

void eal::init(boost::program_options::variables_map opts)
{
    if (initialized) {
        return;
    }

    /* probe to determine the NIC devices available */
    if (rte_eal_pci_probe() < 0) {
        rte_exit(EXIT_FAILURE, "Cannot probe PCI\n");
    }

    if (rte_eth_dev_count() == 0) {
        rte_exit(EXIT_FAILURE, "No Ethernet ports - bye\n");
    } else {
        printf("ports number: %d\n", rte_eth_dev_count());
    }

    initialized = true;
}

} // namespace dpdk

#endif // HAVE_DPDK
