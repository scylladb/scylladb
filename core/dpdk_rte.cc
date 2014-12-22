#ifdef HAVE_DPDK

#include "core/dpdk_rte.hh"
#include "util/conversions.hh"
#include <experimental/optional>
#include <rte_pci.h>

namespace dpdk {

bool eal::initialized = false;

void eal::init(cpuset cpus, boost::program_options::variables_map opts)
{
    if (initialized) {
        return;
    }

    std::stringstream mask;
    mask << std::hex << cpus.to_ulong();

    // TODO: Inherit these from the app parameters - "opts"
    std::vector<std::vector<char>> args {
        string2vector("dpdk_args"),
        string2vector("-c"), string2vector(mask.str()),
        string2vector("-n"), string2vector("1")
    };

    std::experimental::optional<std::string> hugepages_path;
    if (opts.count("hugepages")) {
        hugepages_path = opts["hugepages"].as<std::string>();
    }

    // If "hugepages" is not provided and DPDK PMD drivers mode is requested -
    // use the default DPDK huge tables configuration.
    if (hugepages_path) {
        args.push_back(string2vector("--huge-dir"));
        args.push_back(string2vector(hugepages_path.value()));
    } else if (!opts.count("dpdk-pmd")) {
        args.push_back(string2vector("--no-huge"));
    }

    std::vector<char*> cargs;

    for (auto&& a: args) {
        cargs.push_back(a.data());
    }
    /* initialise the EAL for all */
    int ret = rte_eal_init(cargs.size(), cargs.data());
    if (ret < 0) {
        rte_exit(EXIT_FAILURE, "Cannot init EAL\n");
    }

    /* probe the PCI devices in case we need to use DPDK drivers */
    if (rte_eal_pci_probe() < 0) {
        rte_exit(EXIT_FAILURE, "Cannot probe PCI\n");
    }

    initialized = true;
}

} // namespace dpdk

#endif // HAVE_DPDK
