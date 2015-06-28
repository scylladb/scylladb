Seastar and DPDK
================

Seastar uses the Data Plane Development Kit to drive NIC hardware directly.  This
provides an enormous performance boost.

To enable DPDK, specify `--enable-dpdk` to `./configure.py`, and `--dpdk-pmd` as a
run-time parameter.  This will use the DPDK package provided as a git submodule with the
seastar sources.

To use your own self-compiled DPDK package, follow this procedure:

1. Setup host to compile DPDK:
   - Ubuntu 
     `sudo apt-get install -y build-essential linux-image-extra-$(uname -r)` 
2. Prepare a DPDK SDK:
   - Download the latest DPDK release: `wget http://dpdk.org/browse/dpdk/snapshot/dpdk-1.8.0.tar.gz`
   - Untar it.
   - Edit config/common_linuxapp: set CONFIG_RTE_MBUF_REFCNT and CONFIG_RTE_LIBRTE_KNI to 'n'.
   - For DPDK 1.7.x: edit config/common_linuxapp: 
     - Set CONFIG_RTE_LIBRTE_PMD_BOND  to 'n'.
     - Set CONFIG_RTE_MBUF_SCATTER_GATHER to 'n'.
     - Set CONFIG_RTE_LIBRTE_IP_FRAG to 'n'.
   - Start the tools/setup.sh script as root.
   - Compile a linuxapp target (option 9).
   - Install IGB_UIO module (option 11).
   - Bind some physical port to IGB_UIO (option 17).
   - Configure hugepage mappings (option 14/15).
3. Run a configure.py: `./configure.py --dpdk-target <Path to untared dpdk-1.8.0 above>/x86_64-native-linuxapp-gcc`.
