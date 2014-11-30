/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifdef HAVE_DPDK

#include "core/posix.hh"
#include "core/vla.hh"
#include "virtio-interface.hh"
#include "core/reactor.hh"
#include "core/stream.hh"
#include "core/circular_buffer.hh"
#include "core/align.hh"
#include "util/function_input_iterator.hh"
#include "util/transform_iterator.hh"
#include <atomic>
#include <vector>
#include <queue>
#include "ip.hh"
#include "const.hh"
#include "dpdk.hh"

#include <getopt.h>

#include <rte_config.h>
#include <rte_common.h>
#include <rte_eal.h>
#include <rte_pci.h>
#include <rte_ethdev.h>
#include <rte_cycles.h>
#include <rte_memzone.h>

using namespace net;

namespace dpdk {

/******************* Net device related constatns *****************************/

static constexpr uint16_t mbufs_per_queue        = 1536;
static constexpr uint16_t mbuf_cache_size        = 512;
static constexpr uint16_t mbuf_overhead          =
                                 sizeof(struct rte_mbuf) + RTE_PKTMBUF_HEADROOM;
static constexpr size_t   mbuf_data_size         = 2048;

// MBUF_DATA_SIZE(2K) * 32 = 64K = Max TSO/LRO size
static constexpr uint8_t  max_frags              = 32;

static constexpr uint16_t mbuf_size            = mbuf_data_size + mbuf_overhead;

static constexpr uint16_t default_rx_ring_size   = 512;
static constexpr uint16_t default_tx_ring_size   = 512;

/*
 * RX and TX Prefetch, Host, and Write-back threshold values should be
 * carefully set for optimal performance. Consult the network
 * controller's datasheet and supporting DPDK documentation for guidance
 * on how these parameters should be set.
 */
/* Default configuration for rx and tx thresholds etc. */
/*
 * These default values are optimized for use with the Intel(R) 82599 10 GbE
 * Controller and the DPDK ixgbe PMD. Consider using other values for other
 * network controllers and/or network drivers.
 */
static constexpr uint8_t default_pthresh         = 36;
static constexpr uint8_t default_rx_hthresh      = 8;
static constexpr uint8_t default_tx_hthresh      = 0;
static constexpr uint8_t default_wthresh         = 0;

static constexpr const char* pktmbuf_pool_name   = "dpdk_net_pktmbuf_pool";

/*
 * When doing reads from the NIC queues, use this batch size
 */
static constexpr uint8_t packet_read_size        = 32;
/******************************************************************************/

class net_device : public net::device {
public:
    explicit net_device(boost::program_options::variables_map opts,
                             uint8_t num_queues);

    virtual subscription<packet> receive(
        std::function<future<>(packet)> next_packet) override;

    virtual future<> send(packet p) override;
    virtual future<> l2inject(packet p) { assert(0); return make_ready_future(); }
    virtual ethernet_address hw_address() override;
    virtual net::hw_features hw_features() override;

private:

    /**
     * Initialise an individual port:
     * - configure number of rx and tx rings
     * - set up each rx ring, to pull from the main mbuf pool
     * - set up each tx ring
     * - start the port and report its status to stdout
     *
     * @param port_num index of the port to initialize
     *
     * @return 0 in case of success and an appropriate error code in case of an
     *         error.
     */
    int init_port(uint8_t port_num);

    /**
     * Initialise the mbuf pool for packet reception for the NIC, and any other
     * buffer pools needed by the app - currently none.
     */
    bool init_mbuf_pools();

    void usage();

    /**
     * The ports to be used by the application are passed in
     * the form of a bitmask. This function parses the bitmask
     * and places the port numbers to be used into the _ports[]
     * array.
     *
     * @param max_ports Total number of present NIC ports
     * @param portmask
     *
     * @return TRUE in case of success
     */
    bool parse_portmask(uint8_t max_ports, const char *portmask);

    /**
     * The application specific arguments follow the DPDK-specific
     * arguments which are stripped by the DPDK init. This function
     * processes these application arguments, printing usage info
     * on error.
     *
     * Currently only "-p <portmask>" parameter is supported.
     *
     * TODO: Move this to standard seastar arguments.
     *
     * @param max_ports Total number of present NIC ports
     * @param argc
     * @param argv
     *
     * @return TRUE in case of success
     */
    bool parse_app_args(uint8_t max_ports, int argc, const char* *argv);

    /**
     * Check the link status of given ports in up to 9s, and print them
     * finally.
     *
     * @param port_mask Mask of the ports to check (~0x0 will check all
     *                  available ports)
     */
    void check_all_ports_link_status(uint32_t port_mask);

    /**
     * Polls for a burst of incoming packets. This function will not block and
     * will immediately return after processing all available packets.
     *
     * @param port_num Port index
     * @param qid  Queue index
     */
    void poll_rx_once(uint8_t port_num, uint16_t qid);

    /**
     * Translates an rte_mbuf's into net::packet and feeds them to _rx_stream.
     *
     * @param bufs An array of received rte_mbuf's
     * @param count Number of buffers in the bufs[]
     */
    void process_packets(struct rte_mbuf **bufs, uint16_t count);

    /**
     * Copies one net::fragment into the cluster of rte_mbuf's.
     *
     * @param frag Fragment to copy (in)
     * @param head Head of the cluster (out)
     * @param last_seg Last segment of the cluster (out)
     * @param nsegs Number of segments in the cluster (out)
     *
     * We return the "last_seg" to avoid traversing the cluster in order to get
     * it.
     *
     * @return TRUE in case of success
     */
    bool copy_one_frag(fragment& frag, rte_mbuf*& head, rte_mbuf*& last_seg,
                       unsigned& nsegs);

    /**
     * Allocates a single rte_mbuf and copies a given data into it.
     *
     * @param m New allocated rte_mbuf (out)
     * @param data Data to copy from (in)
     * @param l length of the data to copy (in)
     *
     * @return The actual number of bytes that has been copied
     */
    size_t copy_one_data_buf(rte_mbuf*& m, char* data, size_t l);

private:
    struct rte_eth_rxconf _rx_conf_default = {};
    struct rte_eth_txconf _tx_conf_default = {};
    struct port_info {
        uint8_t num_ports;
        uint8_t id[RTE_MAX_ETHPORTS];
    } _ports;

    stream<packet> _rx_stream;
    net::hw_features _hw_features;
    rte_mempool *_pktmbuf_pool;
    uint8_t _num_queues;
};

int net_device::init_port(uint8_t port_num)
{
    /* for port configuration all features are off by default */
    rte_eth_conf port_conf = { 0 };
    port_conf.rxmode.mq_mode = ETH_MQ_RX_RSS;

    const uint16_t rx_ring_size = default_rx_ring_size ;
    const uint16_t tx_ring_size = default_tx_ring_size;

    uint16_t q;
    int retval;

    printf("Port %u init ... ", (unsigned)port_num);
    fflush(stdout);

    /*
     * Standard DPDK port initialisation - config port, then set up
     * rx and tx rings.
      */
    if ((retval = rte_eth_dev_configure(port_num, _num_queues, _num_queues,
                                        &port_conf)) != 0) {
        return retval;
    }

    //
    // TODO: We may want to rework the initialization of the queues by moving
    // the queue setup to the corresponding CPU. However this will require a
    // handshake to ensure that all queues are set up and after that call
    // rte_eth_dev_start().
    //
    for (q = 0; q < _num_queues; q++) {
        retval = rte_eth_rx_queue_setup(port_num, q, rx_ring_size,
                                        rte_eth_dev_socket_id(port_num),
                                        &_rx_conf_default, _pktmbuf_pool);
        if (retval < 0) {
            return retval;
        }
    }

    for (q = 0; q < _num_queues; q++) {
        retval = rte_eth_tx_queue_setup(port_num, q, tx_ring_size,
                                        rte_eth_dev_socket_id(port_num),
                                        &_tx_conf_default);
        if (retval < 0) {
            return retval;
        }
    }

    //rte_eth_promiscuous_enable(port_num);

    retval  = rte_eth_dev_start(port_num);
    if (retval < 0) {
        return retval;
    }

    printf("done: \n");

    return 0;
}

bool net_device::init_mbuf_pools()
{
    // Allocate the same amount of buffers for Rx and Tx.
    const unsigned num_mbufs = 2 * _num_queues * mbufs_per_queue;

    /* don't pass single-producer/single-consumer flags to mbuf create as it
     * seems faster to use a cache instead */
    printf("Creating mbuf pool '%s' [%u mbufs] ...\n",
        pktmbuf_pool_name, num_mbufs);

    _pktmbuf_pool = rte_mempool_create(pktmbuf_pool_name, num_mbufs,
        mbuf_size, mbuf_cache_size,
        sizeof(struct rte_pktmbuf_pool_private), rte_pktmbuf_pool_init,
        NULL, rte_pktmbuf_init, NULL, rte_socket_id(), 0);

    return _pktmbuf_pool != NULL;
}

bool net_device::parse_portmask(uint8_t max_ports, const char *portmask)
{
    char *end = NULL;
    unsigned long pm;
    uint8_t count = 0;

    if (portmask == NULL)
        return false;

    /* convert parameter to a number and verify */
    pm = strtoul(portmask, &end, 16);
    if (end == NULL || *end != '\0' || pm == 0)
        return false;

    /* loop through bits of the mask and mark ports */
    while (pm != 0) {
        if (pm & 0x01) { /* bit is set in mask, use port */
            if (count >= max_ports)
                printf("WARNING: requested port %u not present"
                    " - ignoring\n", (unsigned)count);
            else
                _ports.id[_ports.num_ports++] = count;
        }
        pm = (pm >> 1);
        count++;
    }

    return true;
}

bool net_device::parse_app_args(uint8_t max_ports, int argc,
                                const char* *argv)
{
    // argv should be: {"app name", "-p", "<mask>"}
    if (argc < 3) {
        usage();
        return false;
    }

    argc++;
    argv++;

    if (std::strcmp(argv[0], "-p")) {
        usage();
        return false;
    }

    argc++;
    argv++;

    if (!parse_portmask(max_ports, *argv)) {
        usage();
        return false;
    }

    if (_ports.num_ports != 1) {
        printf("ERROR: We support only a single port configuration at the "
               "moment\n");
        return false;
    }

    return true;
}

/**
 * Prints out usage information to stdout
 */
void net_device::usage()
{
    printf(
        " [EAL options] -- -p PORTMASK\n"
        " -p PORTMASK: hexadecimal bitmask of ports to use\n");
}

void net_device::check_all_ports_link_status(uint32_t port_mask)
{
    using namespace std::literals::chrono_literals;
    constexpr auto check_interval = 100ms;
    const int max_check_time = 90;  /* 9s (90 * 100ms) in total */
    uint8_t portid, count, all_ports_up, print_flag = 0;
    struct rte_eth_link link;

    printf("\nChecking link status");
    fflush(stdout);
    for (count = 0; count <= max_check_time; count++) {
        all_ports_up = 1;
        for (portid = 0; portid < _ports.num_ports; portid++) {
            if ((port_mask & (1 << _ports.id[portid])) == 0) {
                continue;
            }

            memset(&link, 0, sizeof(link));
            rte_eth_link_get_nowait(_ports.id[portid], &link);
            /* print link status if flag set */
            if (print_flag == 1) {
                if (link.link_status) {
                    printf("Port %d Link Up - speed %u "
                        "Mbps - %s\n", _ports.id[portid],
                        (unsigned)link.link_speed,
                        (link.link_duplex == ETH_LINK_FULL_DUPLEX) ?
                        ("full-duplex") : ("half-duplex\n"));
                } else {
                    printf("Port %d Link Down\n",
                        (uint8_t)_ports.id[portid]);
                }
                continue;
            }
            /* clear all_ports_up flag if any link down */
            if (link.link_status == 0) {
                all_ports_up = 0;
                break;
            }
        }
        /* after finally printing all link status, get out */
        if (print_flag == 1) {
            break;
        }

        if (all_ports_up == 0) {
            printf(".");
            fflush(stdout);
            std::this_thread::sleep_for(check_interval);
        }

        /* set the print_flag if all ports up or timeout */
        if (all_ports_up == 1 || count == (max_check_time - 1)) {
            print_flag = 1;
            printf("done\n");
        }
    }
}


net_device::net_device(boost::program_options::variables_map opts,
                                 uint8_t num_queues) :
    _ports({0}), _rx_stream(), _num_queues(num_queues)
{
    _rx_stream.started();

    _rx_conf_default.rx_thresh.pthresh = default_pthresh;
    _rx_conf_default.rx_thresh.hthresh = default_rx_hthresh;
    _rx_conf_default.rx_thresh.wthresh = default_wthresh;


    _tx_conf_default.tx_thresh.pthresh = default_pthresh;
    _tx_conf_default.tx_thresh.hthresh = default_tx_hthresh;
    _tx_conf_default.tx_thresh.wthresh = default_wthresh;

    _tx_conf_default.tx_free_thresh = 0; /* Use PMD default values */
    _tx_conf_default.tx_rs_thresh   = 0; /* Use PMD default values */

    const char *argv[] = {"dpdk_args", "-c", "0x1",  "-n", "1", "--", "-p",
                          "1"};
    int argc = sizeof(argv) / sizeof(char*);

    /* initialise the EAL for all */
    int ret = rte_eal_init(argc, const_cast<char**>(argv));
    if (ret < 0) {
        rte_exit(EXIT_FAILURE, "Cannot init EAL\n");
    }

    int argc_app = argc - ret;
    const char* *argv_app = argv + ret;

    /* probe to determine the NIC devices available */
    if (rte_eal_pci_probe() < 0) {
        rte_exit(EXIT_FAILURE, "Cannot probe PCI\n");
    }

    uint8_t dev_count = rte_eth_dev_count();
    if (dev_count == 0) {
        rte_exit(EXIT_FAILURE, "No Ethernet ports - bye\n");
    } else {
        printf("ports number: %d\n", dev_count);
    }

    /* parse additional, application arguments */
    if (!parse_app_args(dev_count, argc_app, argv_app)) {
        rte_exit(EXIT_FAILURE, "Failed to parse app args\n");
    }

    if (!init_mbuf_pools()) {
        rte_exit(EXIT_FAILURE, "Cannot initialise mbuf pools\n");
    }

    /* now initialise the ports we will use */
    for (uint8_t i = 0; i < _ports.num_ports; i++) {
        ret = init_port(_ports.id[i]);
        if (ret != 0) {
            rte_exit(EXIT_FAILURE, "Cannot initialise port %u\n", i);
        }
    }

    // Print the MAC
    hw_address();

    check_all_ports_link_status(~0x0);

    printf("Created DPDK device\n");

    // Register a DPDK poller.
    engine.register_new_poller([&] { poll_rx_once(0, 0); return false; });
}

/**
 * Start polling for new packets
 *
 * TODO: Rework this naive implementation to utilize the dpdk_bulk_receive()
 * @param next the packet processing callback.
 *
 * @return the subscription crap
 */
subscription<packet>
net_device::receive(std::function<future<>(packet)> next)
{
    return _rx_stream.listen(std::move(next));
}

void net_device::process_packets(struct rte_mbuf **bufs, uint16_t count)
{
    for (uint16_t i = 0; i < count; i++) {
        struct rte_mbuf *m = bufs[i];

        if (!rte_pktmbuf_is_contiguous(m)) {
            rte_exit(EXIT_FAILURE,
                     "DPDK-Rx: Have got a fragmented buffer - not supported\n");
        }

        fragment f{rte_pktmbuf_mtod(m, char*), rte_pktmbuf_data_len(m)};

        packet p(f, make_deleter(deleter(), [m] { rte_pktmbuf_free(m); }));

        _rx_stream.produce(std::move(p));
    }
}

void net_device::poll_rx_once(uint8_t port_num, uint16_t qid)
{
    struct rte_mbuf *buf[packet_read_size];

    /* read a port */
    uint16_t rx_count = rte_eth_rx_burst(_ports.id[port_num], qid,
                                         buf, packet_read_size);

    /* Now process the NIC packets read */
    if (likely(rx_count > 0)) {
        process_packets(buf, rx_count);
    }
}

size_t net_device::copy_one_data_buf(rte_mbuf*& m, char* data, size_t l)
{
    m = rte_pktmbuf_alloc(_pktmbuf_pool);
    if (!m) {
        return 0;
    }

    size_t len = std::min(l, mbuf_data_size);

    // mbuf_put()
    m->pkt.data_len += len;
    m->pkt.pkt_len += len;

    rte_memcpy(rte_pktmbuf_mtod(m, void*), data, len);

    return len;
}


bool net_device::copy_one_frag(fragment& frag, rte_mbuf*& head,
                               rte_mbuf*& last_seg, unsigned& nsegs)
{
    size_t len, left_to_copy = frag.size;
    char* base = frag.base;
    rte_mbuf* m;

    if (!frag.size) {
        rte_exit(EXIT_FAILURE, "DPDK Tx: Zero-size fragment");
    }

    // Create a HEAD of mbufs' cluster and copy the first bytes into it
    len = copy_one_data_buf(head, base, left_to_copy);
    if (!len) {
        return false;
    }

    left_to_copy -= len;
    base += len;
    nsegs = 1;

    // Copy the rest of the data into the new mbufs and chain them to the
    // cluster
    rte_mbuf* prev_seg = head;
    while (left_to_copy) {
        len = copy_one_data_buf(m, base, left_to_copy);
        if (!len) {
            rte_pktmbuf_free(head);
            return false;
        }

        left_to_copy -= len;
        base += len;
        nsegs++;

        prev_seg->pkt.next = m;
        prev_seg = m;
    }

    // Return the last mbuf in the cluster
    last_seg = prev_seg;

    return true;
}

future<> net_device::send(packet p)
{
    // sanity
    if (!p.len()) {
        return make_ready_future<>();
    }

    // Too fragmented - linearize
    if (p.nr_frags() > max_frags) {
        p.linearize();
    }

    /* TODO: configure the offload features here if any */

    //
    // We will copy the data for now and will implement a zero-copy in the
    // future.

    rte_mbuf *head = NULL, *last_seg = NULL;
    unsigned total_nsegs = 0, nsegs = 0;

    // Create a HEAD of the fragmented packet
    if (!copy_one_frag(p.frag(0), head, last_seg, nsegs)) {
        // Drop if we failed to allocate new mbuf
        return make_ready_future<>();
    }

    total_nsegs += nsegs;

    for (unsigned i = 1; i < p.nr_frags(); i++) {

        rte_mbuf *h = NULL, *new_last_seg = NULL;
        if (!copy_one_frag(p.frag(i), h, new_last_seg, nsegs)) {
            rte_pktmbuf_free(head);
            return make_ready_future<>();
        }

        total_nsegs += nsegs;

        // Attach a new buffers' chain to the packet chain
        last_seg->pkt.next = h;
        last_seg = new_last_seg;
    }

    // Update the HEAD buffer with the packet info
    head->pkt.pkt_len = p.len();
    head->pkt.nb_segs = total_nsegs;

    //
    // DEBUG DEBUG: Sending on port 0, queue 0
    // Currently we will spin till completion.
    // TODO: implement a poller + xmit queue
    //
    while(rte_eth_tx_burst(_ports.id[0], 0, &head, 1) < 1);

    return make_ready_future<>();
}

/**
 * @note We currently always use the first configured port
 *
 * @return port's MAC address
 */
ethernet_address net_device::hw_address()
{
    struct ether_addr mac;
    rte_eth_macaddr_get(_ports.id[0], &mac);
    printf("%02x:%02x:%02x:%02x:%02x:%02x\n",
        mac.addr_bytes[0], mac.addr_bytes[1], mac.addr_bytes[2],
        mac.addr_bytes[3], mac.addr_bytes[4], mac.addr_bytes[5]);

    return mac.addr_bytes;
}

net::hw_features net_device::hw_features()
{
    return _hw_features;
}

} // namespace dpdk

/******************************** Interface functions *************************/

std::unique_ptr<net::device> create_dpdk_net_device(
                                    boost::program_options::variables_map opts,
                                    uint8_t num_queues)
{
    net::device *ptr;

    ptr = new dpdk::net_device(opts, num_queues);

    // Vlad: A global variable?! - Demn, this is ugly!
    //
    // This assumes only one device per cpu. Will need to be fixed when this
    // assumption will no longer hold.
    dev = ptr;
    return std::unique_ptr<net::device>(ptr);
}

boost::program_options::options_description
get_dpdk_net_options_description()
{
    boost::program_options::options_description opts(
            "DPDK net options");
#if 0
    opts.add_options()
        ("csum-offload",
                boost::program_options::value<std::string>()->default_value("on"),
                "Enable checksum offload feature (on / off)")
        ("tso",
                boost::program_options::value<std::string>()->default_value("on"),
                "Enable TCP segment offload feature (on / off)")
        ("ufo",
                boost::program_options::value<std::string>()->default_value("on"),
                "Enable UDP fragmentation offload feature (on / off)")
        ;
#endif
    return opts;
}

#endif // HAVE_DPDK
