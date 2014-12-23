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
#include "core/sstring.hh"
#include "util/function_input_iterator.hh"
#include "util/transform_iterator.hh"
#include <atomic>
#include <vector>
#include <queue>
#include "ip.hh"
#include "const.hh"
#include "core/dpdk_rte.hh"
#include "dpdk.hh"
#include "toeplitz.hh"

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

#ifdef RTE_VERSION_1_7
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
#endif

static constexpr const char* pktmbuf_pool_name   = "dpdk_net_pktmbuf_pool";

/*
 * When doing reads from the NIC queues, use this batch size
 */
static constexpr uint8_t packet_read_size        = 32;
/******************************************************************************/


class dpdk_device : public device {
    uint8_t _port_idx;
    uint16_t _num_queues;
    net::hw_features _hw_features;
    uint8_t _queues_ready = 0;
    unsigned _home_cpu;
    std::vector<uint8_t> _redir_table;
#ifdef RTE_VERSION_1_7
    struct rte_eth_rxconf _rx_conf_default = {};
    struct rte_eth_txconf _tx_conf_default = {};
#endif

public:
    rte_eth_dev_info _dev_info = {};

private:
    /**
     * Port initialization consists of 3 main stages:
     * 1) General port initialization which ends with a call to
     *    rte_eth_dev_configure() where we request the needed number of Rx and
     *    Tx queues.
     * 2) Individual queues initialization. This is done in the constructor of
     *    dpdk_qp class. In particular the memory pools for queues are allocated
     *    in this stage.
     * 3) The final stage of the initialization which starts with the call of
     *    rte_eth_dev_start() after which the port becomes fully functional. We
     *    will also wait for a link to get up in this stage.
     */


    /**
     * First stage of the port initialization.
     *
     * @return 0 in case of success and an appropriate error code in case of an
     *         error.
     */
    int init_port_start();

    /**
     * The final stage of a port initialization.
     * @note Must be called *after* all queues from stage (2) have been
     *       initialized.
     */
    void init_port_fini();

    /**
     * Check the link status of out port in up to 9s, and print them finally.
     */
    void check_port_link_status();

public:
    dpdk_device(uint8_t port_idx, uint16_t num_queues)
        : _port_idx(port_idx)
        , _num_queues(num_queues)
        , _home_cpu(engine.cpu_id()) {

        /* now initialise the port we will use */
        int ret = init_port_start();
        if (ret != 0) {
            rte_exit(EXIT_FAILURE, "Cannot initialise port %u\n", _port_idx);
        }
    }
    ethernet_address hw_address() override {
        struct ether_addr mac;
        rte_eth_macaddr_get(_port_idx, &mac);

        return mac.addr_bytes;
    }
    net::hw_features hw_features() override {
        return _hw_features;
    }

    const rte_eth_rxconf* def_rx_conf() const {
#ifdef RTE_VERSION_1_7
        return &_rx_conf_default;
#else
        return &_dev_info.default_rxconf;
#endif
    }

    const rte_eth_txconf* def_tx_conf() const {
#ifdef RTE_VERSION_1_7
        return &_tx_conf_default;
#else
        return &_dev_info.default_txconf;
#endif
    }

    /**
    *  Read the RSS table from the device and store it in the internal vector.
    *  We will need it when we forward the reassembled IP frames
     * (after IP fragmentation) to the correct HW queue.
     */
    void get_rss_table();

    virtual uint16_t hw_queues_count() override { return _num_queues; }
    virtual std::unique_ptr<qp> init_local_queue(boost::program_options::variables_map opts, uint16_t qid) override;
    virtual unsigned hash2qid(uint32_t hash) override {
        return _redir_table[hash & (_redir_table.size() - 1)];
    }
    uint8_t port_idx() { return _port_idx; }
};

class dpdk_qp : public net::qp {
public:
    explicit dpdk_qp(dpdk_device* dev, uint8_t qid);

    virtual future<> send(packet p) override;

private:

    bool init_mbuf_pools();

    /**
     * Polls for a burst of incoming packets. This function will not block and
     * will immediately return after processing all available packets.
     *
     */
    void poll_rx_once();

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
    dpdk_device* _dev;
    uint8_t _qid;
    rte_mempool* _pktmbuf_pool;
    reactor::poller _rx_poller;
};

int dpdk_device::init_port_start()
{
    assert(_port_idx < rte_eth_dev_count());

    rte_eth_dev_info_get(_port_idx, &_dev_info);

#ifdef RTE_VERSION_1_7
    _rx_conf_default.rx_thresh.pthresh = default_pthresh;
    _rx_conf_default.rx_thresh.hthresh = default_rx_hthresh;
    _rx_conf_default.rx_thresh.wthresh = default_wthresh;


    _tx_conf_default.tx_thresh.pthresh = default_pthresh;
    _tx_conf_default.tx_thresh.hthresh = default_tx_hthresh;
    _tx_conf_default.tx_thresh.wthresh = default_wthresh;

    _tx_conf_default.tx_free_thresh = 0; /* Use PMD default values */
    _tx_conf_default.tx_rs_thresh   = 0; /* Use PMD default values */
#else
    // Clear txq_flags - we want to support all available offload features.
    _dev_info.default_txconf.txq_flags = 0;
#endif

    /* for port configuration all features are off by default */
    rte_eth_conf port_conf = { 0 };

    printf("Port %d: max_rx_queues %d max_tx_queues %d\n",
           _port_idx, _dev_info.max_rx_queues, _dev_info.max_tx_queues);

    _num_queues = std::min({_num_queues, _dev_info.max_rx_queues, _dev_info.max_tx_queues});

    printf("Port %d: using %d %s\n", _port_idx, _num_queues,
           (_num_queues > 1) ? "queues" : "queue");

    // Set RSS mode: enable RSS if seastar is configured with more than 1 CPU.
    // Even if port has a single queue we still want the RSS feature to be
    // available in order to make HW calculate RSS hash for us.
    if (smp::count > 1) {
        port_conf.rxmode.mq_mode = ETH_MQ_RX_RSS;
        port_conf.rx_adv_conf.rss_conf.rss_hf = ETH_RSS_IPV4 | ETH_RSS_IPV4_UDP | ETH_RSS_IPV4_TCP;
        port_conf.rx_adv_conf.rss_conf.rss_key = const_cast<uint8_t*>(rsskey.data());
    } else {
        port_conf.rxmode.mq_mode = ETH_MQ_RX_NONE;
    }

    if (_num_queues > 1) {
#ifdef RTE_VERSION_1_7
        _redir_table.resize(ETH_RSS_RETA_NUM_ENTRIES);
        // This comes from the ETH_RSS_RETA_NUM_ENTRIES being 128
        _rss_table_bits = 7;
#else
        // Check that the returned RETA size is sane:
        // greater than 0 and is a power of 2.
        assert(_dev_info.reta_size &&
               (_dev_info.reta_size & (_dev_info.reta_size - 1)) == 0);

        // Set the RSS table to the correct size
        _redir_table.resize(_dev_info.reta_size);
        _rss_table_bits = std::lround(std::log2(_dev_info.reta_size));
        printf("Port %d: RSS table size is %d\n",
               _port_idx, _dev_info.reta_size);

#endif
    }

    // Set Rx VLAN stripping
    if (_dev_info.rx_offload_capa & DEV_RX_OFFLOAD_VLAN_STRIP) {
        port_conf.rxmode.hw_vlan_strip = 1;
    }

    // Set Rx checksum checking
    if (  (_dev_info.rx_offload_capa & DEV_RX_OFFLOAD_IPV4_CKSUM) &&
          (_dev_info.rx_offload_capa & DEV_RX_OFFLOAD_UDP_CKSUM) &&
          (_dev_info.rx_offload_capa & DEV_RX_OFFLOAD_TCP_CKSUM)) {
        printf("RX checksum offload supported\n");
        port_conf.rxmode.hw_ip_checksum = 1;
        _hw_features.rx_csum_offload = 1;
    }

    if ((_dev_info.tx_offload_capa & DEV_TX_OFFLOAD_IPV4_CKSUM)) {
        printf("TX ip checksum offload supported\n");
        _hw_features.tx_csum_ip_offload = 1;
    }
    if (  (_dev_info.tx_offload_capa & DEV_TX_OFFLOAD_UDP_CKSUM) &&
          (_dev_info.tx_offload_capa & DEV_TX_OFFLOAD_TCP_CKSUM)) {
        printf("TX TCP&UDP checksum offload supported\n");
        _hw_features.tx_csum_l4_offload = 1;
    }

    int retval;

    printf("Port %u init ... ", _port_idx);
    fflush(stdout);

    /*
     * Standard DPDK port initialisation - config port, then set up
     * rx and tx rings.
      */
    if ((retval = rte_eth_dev_configure(_port_idx, _num_queues, _num_queues,
                                        &port_conf)) != 0) {
        return retval;
    }

    //rte_eth_promiscuous_enable(port_num);
    printf("done: \n");

    return 0;
}

void dpdk_device::init_port_fini()
{
    if (rte_eth_dev_start(_port_idx) < 0) {
        rte_exit(EXIT_FAILURE, "Cannot start port %d\n", _port_idx);
    }

    if (_num_queues > 1) {
        get_rss_table();
    }

    // Wait for a link
    check_port_link_status();

    printf("Created DPDK device\n");
}

bool dpdk_qp::init_mbuf_pools()
{
    // Allocate the same amount of buffers for Rx and Tx.
    const unsigned num_mbufs = 2 * mbufs_per_queue;
    sstring name = to_sstring(pktmbuf_pool_name) + to_sstring(_qid);
    /* don't pass single-producer/single-consumer flags to mbuf create as it
     * seems faster to use a cache instead */
    printf("Creating mbuf pool '%s' [%u mbufs] ...\n", name.c_str(), num_mbufs);

    //
    // We currently allocate a one big mempool on the current CPU to fit all
    // requested queues.
    // TODO: Allocate a separate pool for each queue on the appropriate CPU.
    //
    _pktmbuf_pool = rte_mempool_create(name.c_str(), num_mbufs,
        mbuf_size, mbuf_cache_size,
        sizeof(struct rte_pktmbuf_pool_private), rte_pktmbuf_pool_init,
        NULL, rte_pktmbuf_init, NULL, rte_socket_id(), 0);

    return _pktmbuf_pool != NULL;
}

void dpdk_device::check_port_link_status()
{
    using namespace std::literals::chrono_literals;
    constexpr auto check_interval = 100ms;
    const int max_check_time = 90;  /* 9s (90 * 100ms) in total */
    int count;
    struct rte_eth_link link;

    printf("\nChecking link status");
    fflush(stdout);
    for (count = 0; count <= max_check_time; count++) {
            memset(&link, 0, sizeof(link));
            rte_eth_link_get_nowait(_port_idx, &link);

            if (link.link_status == 0) {
                printf(".");
                fflush(stdout);
                std::this_thread::sleep_for(check_interval);
            } else {
                break;
            }
    }

    /* print link status */
    if (link.link_status) {
        printf("done\nPort %d Link Up - speed %u "
            "Mbps - %s\n", _port_idx,
            (unsigned)link.link_speed,
            (link.link_duplex == ETH_LINK_FULL_DUPLEX) ?
            ("full-duplex") : ("half-duplex\n"));
    } else {
        printf("done\nPort %d Link Down\n", _port_idx);
    }
}


dpdk_qp::dpdk_qp(dpdk_device* dev, uint8_t qid)
     : _dev(dev), _qid(qid), _rx_poller([&] { poll_rx_once(); return true; })
{
    if (!init_mbuf_pools()) {
        rte_exit(EXIT_FAILURE, "Cannot initialize mbuf pools\n");
    }

    const uint16_t rx_ring_size = default_rx_ring_size;
    const uint16_t tx_ring_size = default_tx_ring_size;

    if (rte_eth_rx_queue_setup(_dev->port_idx(), _qid, rx_ring_size,
            rte_eth_dev_socket_id(_dev->port_idx()),
            _dev->def_rx_conf(), _pktmbuf_pool) < 0) {
        rte_exit(EXIT_FAILURE, "Cannot initialize rx queue\n");
    }

    if (rte_eth_tx_queue_setup(_dev->port_idx(), _qid, tx_ring_size,
            rte_eth_dev_socket_id(_dev->port_idx()), _dev->def_tx_conf()) < 0) {
        rte_exit(EXIT_FAILURE, "Cannot initialize tx queue\n");
    }
}

void dpdk_qp::process_packets(struct rte_mbuf **bufs, uint16_t count)
{
    for (uint16_t i = 0; i < count; i++) {
        struct rte_mbuf *m = bufs[i];
        offload_info oi;

        if (!rte_pktmbuf_is_contiguous(m)) {
            rte_exit(EXIT_FAILURE,
                     "DPDK-Rx: Have got a fragmented buffer - not supported\n");
        }

        fragment f{rte_pktmbuf_mtod(m, char*), rte_pktmbuf_data_len(m)};

        packet p(f, make_deleter(deleter(), [m] { rte_pktmbuf_free(m); }));

        // Set stipped VLAN value if available
        if ((_dev->_dev_info.rx_offload_capa & DEV_RX_OFFLOAD_VLAN_STRIP) &&
            (m->ol_flags & PKT_RX_VLAN_PKT)) {

            oi.vlan_tci = rte_mbuf_vlan_tci(m);
        }

        if (_dev->hw_features().rx_csum_offload) {
            if (m->ol_flags & (PKT_RX_IP_CKSUM_BAD | PKT_RX_L4_CKSUM_BAD)) {
                // Packet with bad checksum, just drop it.
                continue;
            }
            // Note that when _hw_features.rx_csum_offload is on, the receive
            // code for ip, tcp and udp will assume they don't need to check
            // the checksum again, because we did this here.
        }

        p.set_offload_info(oi);
        if (m->ol_flags & PKT_RX_RSS_HASH) {
            p.set_rss_hash(rte_mbuf_rss_hash(m));
        }

        _dev->l2receive(std::move(p));
    }
}

void dpdk_qp::poll_rx_once()
{
    struct rte_mbuf *buf[packet_read_size];

    /* read a port */
    uint16_t rx_count = rte_eth_rx_burst(_dev->port_idx(), _qid,
                                         buf, packet_read_size);

    /* Now process the NIC packets read */
    if (likely(rx_count > 0)) {
        process_packets(buf, rx_count);
    }
}

size_t dpdk_qp::copy_one_data_buf(rte_mbuf*& m, char* data, size_t l)
{
    m = rte_pktmbuf_alloc(_pktmbuf_pool);
    if (!m) {
        return 0;
    }

    size_t len = std::min(l, mbuf_data_size);

    // mbuf_put()
    rte_mbuf_data_len(m) += len;
    rte_mbuf_pkt_len(m) += len;

    rte_memcpy(rte_pktmbuf_mtod(m, void*), data, len);

    return len;
}


bool dpdk_qp::copy_one_frag(fragment& frag, rte_mbuf*& head,
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

        rte_mbuf_next(prev_seg) = m;
        prev_seg = m;
    }

    // Return the last mbuf in the cluster
    last_seg = prev_seg;

    return true;
}

future<> dpdk_qp::send(packet p)
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
        rte_mbuf_next(last_seg) = h;
        last_seg = new_last_seg;
    }

    // Update the HEAD buffer with the packet info
    rte_mbuf_pkt_len(head) = p.len();
    rte_mbuf_nb_segs(head) = total_nsegs;

    // Handle TCP checksum offload
    auto oi = p.offload_info();
    if (oi.needs_ip_csum) {
        head->ol_flags |= PKT_TX_IP_CKSUM;
        rte_mbuf_l2_len(head) = sizeof(struct ether_hdr);
        rte_mbuf_l3_len(head) = oi.ip_hdr_len;
    }
    if (_dev->hw_features().tx_csum_l4_offload) {
        if (oi.protocol == ip_protocol_num::tcp) {
            head->ol_flags |= PKT_TX_TCP_CKSUM;
            rte_mbuf_l2_len(head) = sizeof(struct ether_hdr);
            rte_mbuf_l3_len(head) = oi.ip_hdr_len;
        } else if (oi.protocol == ip_protocol_num::udp) {
            head->ol_flags |= PKT_TX_UDP_CKSUM;
            rte_mbuf_l2_len(head) = sizeof(struct ether_hdr);
            rte_mbuf_l3_len(head) = oi.ip_hdr_len;
        }
    }

    //
    // Currently we will spin till completion.
    // TODO: implement a poller + xmit queue
    //
    while(rte_eth_tx_burst(_dev->port_idx(), _qid, &head, 1) < 1);

    return make_ready_future<>();
}

#ifdef RTE_VERSION_1_7
void dpdk_device::get_rss_table()
{
    rte_eth_rss_reta reta_conf { ~0ULL, ~0ULL };
    if (rte_eth_dev_rss_reta_query(_port_idx, &reta_conf)) {
        rte_exit(EXIT_FAILURE, "Cannot get redirection table for pot %d\n",
                               _port_idx);
    }
    assert(sizeof(reta_conf.reta) == _redir_table.size());
    std::copy(reta_conf.reta,
              reta_conf.reta + _redir_table.size(),
              _redir_table.begin());
}
#else
void dpdk_device::get_rss_table()
{
    assert(_dev_info.reta_size);

    int i, reta_conf_size =
        std::max(1, _dev_info.reta_size / RTE_RETA_GROUP_SIZE);
    rte_eth_rss_reta_entry64 reta_conf[reta_conf_size];

    for (i = 0; i < reta_conf_size; i++) {
        reta_conf[i].mask = ~0ULL;
    }

    if (rte_eth_dev_rss_reta_query(_port_idx, reta_conf,
                                   _dev_info.reta_size)) {
        rte_exit(EXIT_FAILURE, "Cannot get redirection table for "
                               "port %d\n", _port_idx);
    }

    for (int i = 0; i < reta_conf_size; i++) {
        std::copy(reta_conf[i].reta,
                  reta_conf[i].reta + RTE_RETA_GROUP_SIZE,
                  _redir_table.begin() + i * RTE_RETA_GROUP_SIZE);
    }
}
#endif

std::unique_ptr<qp> dpdk_device::init_local_queue(boost::program_options::variables_map opts, uint16_t qid) {
    auto qp = std::make_unique<dpdk_qp>(this, qid);
    smp::submit_to(_home_cpu, [this] () mutable {
        if (++_queues_ready == _num_queues) {
            init_port_fini();
        }
    });
    return std::move(qp);
}
} // namespace dpdk

/******************************** Interface functions *************************/

std::unique_ptr<net::device> create_dpdk_net_device(
                                    uint8_t port_idx,
                                    uint8_t num_queues)
{
    static bool called = false;

    assert(!called);
    assert(dpdk::eal::initialized);

    called = true;

    // Check that we have at least one DPDK-able port
    if (rte_eth_dev_count() == 0) {
        rte_exit(EXIT_FAILURE, "No Ethernet ports - bye\n");
    } else {
        printf("ports number: %d\n", rte_eth_dev_count());
    }

    return std::make_unique<dpdk::dpdk_device>(port_idx, num_queues);
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
