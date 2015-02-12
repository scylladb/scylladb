#ifdef HAVE_DPDK

#include "net/dpdk.hh"
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

        //
        // We don't know what is going to be our networking configuration so we
        // assume there is going to be a queue per-CPU. Plus we'll give a DPDK
        // 64MB for "other stuff".
        //
        size_t size_MB = mem_size(cpus.count()) >> 20;
        std::stringstream size_MB_str;
        size_MB_str << size_MB;

        args.push_back(string2vector("-m"));
        args.push_back(string2vector(size_MB_str.str()));
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

    // For 1.8 rte_eal_pci_probe() has been moved into the rte_eal_init().
#ifdef RTE_VERSION_1_7
    /* probe the PCI devices in case we need to use DPDK drivers */
    if (rte_eal_pci_probe() < 0) {
        rte_exit(EXIT_FAILURE, "Cannot probe PCI\n");
    }
#endif

    initialized = true;
}

size_t eal::mem_size(int num_cpus)
{
    size_t memsize = 0;
    //
    // PMD mempool memory:
    //
    // We don't know what is going to be our networking configuration so we
    // assume there is going to be a queue per-CPU.
    //
    memsize += num_cpus * qp_mempool_obj_size();

    // Plus we'll give a DPDK 64MB for "other stuff".
    memsize += (64UL << 20);

    return memsize;
}

} // namespace dpdk

#endif // HAVE_DPDK
