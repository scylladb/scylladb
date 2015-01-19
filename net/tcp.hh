/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef TCP_HH_
#define TCP_HH_

#include "core/shared_ptr.hh"
#include "core/queue.hh"
#include "core/semaphore.hh"
#include "net.hh"
#include "ip_checksum.hh"
#include "ip.hh"
#include "const.hh"
#include "packet-util.hh"
#include <unordered_map>
#include <map>
#include <functional>
#include <deque>
#include <chrono>
#include <experimental/optional>
#include <random>
#include <stdexcept>

#define CRYPTOPP_ENABLE_NAMESPACE_WEAK 1
#include <cryptopp/md5.h>

using namespace std::chrono_literals;

namespace net {

class tcp_hdr;

class tcp_error : public std::runtime_error {
public:
    tcp_error(const std::string& msg) : std::runtime_error(msg) {}
};

class tcp_reset_error : public tcp_error {
public:
    tcp_reset_error() : tcp_error("connection is reset") {}
};

struct tcp_option {
    // The kind and len field are fixed and defined in TCP protocol
    enum class option_kind: uint8_t { mss = 2, win_scale = 3, sack = 4, timestamps = 8,  nop = 1, eol = 0 };
    enum class option_len:  uint8_t { mss = 4, win_scale = 3, sack = 2, timestamps = 10, nop = 1, eol = 1 };
    struct mss {
        option_kind kind = option_kind::mss;
        option_len len = option_len::mss;
        packed<uint16_t> mss;
        template <typename Adjuster>
        void adjust_endianness(Adjuster a) { a(mss); }
    } __attribute__((packed));
    struct win_scale {
        option_kind kind = option_kind::win_scale;
        option_len len = option_len::win_scale;
        uint8_t shift;
    } __attribute__((packed));
    struct sack {
        option_kind kind = option_kind::sack;
        option_len len = option_len::sack;
    } __attribute__((packed));
    struct timestamps {
        option_kind kind = option_kind::timestamps;
        option_len len = option_len::timestamps;
        packed<uint32_t> t1;
        packed<uint32_t> t2;
        template <typename Adjuster>
        void adjust_endianness(Adjuster a) { a(t1, t2); }
    } __attribute__((packed));
    struct nop {
        option_kind kind = option_kind::nop;
    } __attribute__((packed));
    struct eol {
        option_kind kind = option_kind::eol;
    } __attribute__((packed));
    static const uint8_t align = 4;

    void parse(uint8_t* beg, uint8_t* end);
    uint8_t fill(tcp_hdr* th, uint8_t option_size);
    uint8_t get_size(bool syn_on, bool ack_on);

    // For option negotiattion
    bool _mss_received = false;
    bool _win_scale_received = false;
    bool _timestamps_received = false;
    bool _sack_received = false;

    // Option data
    uint16_t _remote_mss = 536;
    uint16_t _local_mss;
    uint8_t _remote_win_scale = 0;
    uint8_t _local_win_scale = 0;
};
inline uint8_t*& operator+=(uint8_t*& x, tcp_option::option_len len) { x += uint8_t(len); return x; }
inline uint8_t& operator+=(uint8_t& x, tcp_option::option_len len) { x += uint8_t(len); return x; }

struct tcp_seq {
    uint32_t raw;
};

inline tcp_seq ntoh(tcp_seq s) {
    return tcp_seq { ntoh(s.raw) };
}

inline tcp_seq hton(tcp_seq s) {
    return tcp_seq { hton(s.raw) };
}

inline
std::ostream& operator<<(std::ostream& os, tcp_seq s) {
    return os << s.raw;
}

inline tcp_seq make_seq(uint32_t raw) { return tcp_seq{raw}; }
inline tcp_seq& operator+=(tcp_seq& s, int32_t n) { s.raw += n; return s; }
inline tcp_seq& operator-=(tcp_seq& s, int32_t n) { s.raw -= n; return s; }
inline tcp_seq operator+(tcp_seq s, int32_t n) { return s += n; }
inline tcp_seq operator-(tcp_seq s, int32_t n) { return s -= n; }
inline int32_t operator-(tcp_seq s, tcp_seq q) { return s.raw - q.raw; }
inline bool operator==(tcp_seq s, tcp_seq q)  { return s.raw == q.raw; }
inline bool operator!=(tcp_seq s, tcp_seq q) { return !(s == q); }
inline bool operator<(tcp_seq s, tcp_seq q) { return s - q < 0; }
inline bool operator>(tcp_seq s, tcp_seq q) { return q < s; }
inline bool operator<=(tcp_seq s, tcp_seq q) { return !(s > q); }
inline bool operator>=(tcp_seq s, tcp_seq q) { return !(s < q); }

struct tcp_hdr {
    packed<uint16_t> src_port;
    packed<uint16_t> dst_port;
    packed<tcp_seq> seq;
    packed<tcp_seq> ack;
    uint8_t rsvd1 : 4;
    uint8_t data_offset : 4;
    uint8_t f_fin : 1;
    uint8_t f_syn : 1;
    uint8_t f_rst : 1;
    uint8_t f_psh : 1;
    uint8_t f_ack : 1;
    uint8_t f_urg : 1;
    uint8_t rsvd2 : 2;
    packed<uint16_t> window;
    packed<uint16_t> checksum;
    packed<uint16_t> urgent;

    template <typename Adjuster>
    void adjust_endianness(Adjuster a) { a(src_port, dst_port, seq, ack, window, checksum, urgent); }
} __attribute__((packed));

template <typename InetTraits>
class tcp {
public:
    using ipaddr = typename InetTraits::address_type;
    using inet_type = typename InetTraits::inet_type;
    using connid = l4connid<InetTraits>;
    using connid_hash = typename connid::connid_hash;
    class connection;
    class listener;
private:
    class tcb;

    class tcb : public enable_lw_shared_from_this<tcb> {
        using clock_type = lowres_clock;
        // Instead of tracking state through an enum, track individual
        // bits of the state.  This reduces duplication in state handling.
        bool _local_syn_sent = false;
        bool _local_syn_acked = false;
        bool _foreign_syn_received = false;
        bool _local_fin_sent = false;
        bool _local_fin_acked = false;
        bool _foreign_fin_received = false;
        // Connection was reset by peer
        bool _nuked = false;
        tcp& _tcp;
        connection* _conn = nullptr;
        ipaddr _local_ip;
        ipaddr _foreign_ip;
        uint16_t _local_port;
        uint16_t _foreign_port;
        struct unacked_segment {
            packet p;
            uint16_t data_len;
            uint16_t data_remaining;
            unsigned nr_transmits;
            clock_type::time_point tx_time;
        };
        struct send {
            tcp_seq unacknowledged;
            tcp_seq next;
            uint32_t window;
            uint8_t window_scale;
            uint16_t mss;
            tcp_seq urgent;
            tcp_seq wl1;
            tcp_seq wl2;
            tcp_seq initial;
            std::deque<unacked_segment> data;
            std::deque<packet> unsent;
            uint32_t unsent_len = 0;
            bool closed = false;
            promise<> _window_opened;
            // Wait for all data are acked
            std::experimental::optional<promise<>> _all_data_acked_promise;
            // Limit number of data queued into send queue
            semaphore user_queue_space = {212992};
            // Round-trip time variation
            std::chrono::milliseconds rttvar;
            // Smoothed round-trip time
            std::chrono::milliseconds srtt;
            bool first_rto_sample = true;
            clock_type::time_point syn_tx_time;
            // Congestion window
            uint32_t cwnd;
            // Slow start threshold
            uint32_t ssthresh;
            // Duplicated ACKs
            uint16_t dupacks = 0;
        } _snd;
        struct receive {
            tcp_seq next;
            uint32_t window;
            uint8_t window_scale;
            uint16_t mss;
            tcp_seq urgent;
            tcp_seq initial;
            std::deque<packet> data;
            packet_merger<tcp_seq> out_of_order;
            std::experimental::optional<promise<>> _data_received_promise;
        } _rcv;
        tcp_option _option;
        timer<lowres_clock> _delayed_ack;
        // Retransmission timeout
        std::chrono::milliseconds _rto{1000};
        static constexpr std::chrono::milliseconds _rto_min{1000};
        static constexpr std::chrono::milliseconds _rto_max{60000};
        // Clock granularity
        static constexpr std::chrono::milliseconds _rto_clk_granularity{1};
        static constexpr uint16_t _max_nr_retransmit{5};
        timer<lowres_clock> _retransmit;
        uint16_t _nr_full_seg_received = 0;
        struct isn_secret {
            // 512 bits secretkey for ISN generating
            uint32_t key[16];
            isn_secret () {
                std::random_device rd;
                std::default_random_engine e(rd());
                std::uniform_int_distribution<uint32_t> dist{};
                for (auto& k : key) {
                    k = dist(e);
                }
            }
        };
        static isn_secret _isn_secret;
        tcp_seq get_isn();
        circular_buffer<typename InetTraits::l4packet> _packetq;
        bool _poll_active = false;
    public:
        tcb(tcp& t, connid id);
        void input(tcp_hdr* th, packet p);
        void output_one();
        future<> wait_for_data();
        future<> wait_for_all_data_acked();
        future<> send(packet p);
        packet read();
        void close();
        void remove_from_tcbs() {
            auto id = connid{_local_ip, _foreign_ip, _local_port, _foreign_port};
            _tcp._tcbs.erase(id);
        }
        bool both_closed() { return _foreign_fin_received && _local_fin_acked; }
        std::experimental::optional<typename InetTraits::l4packet> get_packet();
        void output() {
            if (!_poll_active) {
                _poll_active = true;
                _tcp.poll_tcb(_foreign_ip, this->shared_from_this());
            }
        }
    private:
        void respond_with_reset(tcp_hdr* th);
        bool merge_out_of_order();
        void insert_out_of_order(tcp_seq seq, packet p);
        void trim_receive_data_after_window();
        bool should_send_ack(uint16_t seg_len);
        void clear_delayed_ack();
        packet get_transmit_packet();
        void start_retransmit_timer() {
            auto now = clock_type::now();
            start_retransmit_timer(now);
        };
        void start_retransmit_timer(clock_type::time_point now) {
            auto tp = now + _rto;
            _retransmit.rearm(tp);
        };
        void stop_retransmit_timer() { _retransmit.cancel(); };
        void retransmit();
        void fast_retransmit();
        void update_rto(clock_type::time_point tx_time);
        void update_cwnd(uint32_t acked_bytes);
        void cleanup();
        uint32_t can_send() {
            // Can not send more than advertised window allows
            auto x = std::min(uint32_t(_snd.unacknowledged + _snd.window - _snd.next), _snd.unsent_len);
            // Can not send more than congestion window allows
            x = std::min(_snd.cwnd, x);
            if (_snd.dupacks == 1 || _snd.dupacks == 2) {
                // TODO: Send cwnd + 2*smss per RFC3042
            } else if (_snd.dupacks >= 3) {
                // Sent 1 full-sized segment at most
                x = std::min(uint32_t(_snd.mss), x);
            }
            return x;
        }
        uint32_t flight_size() {
            uint32_t size = 0;
            std::for_each(_snd.data.begin(), _snd.data.end(), [&] (unacked_segment& seg) { size += seg.data_remaining; });
            return size;
        }
        void queue_packet(packet p) {
            _packetq.emplace_back(typename InetTraits::l4packet{_foreign_ip, std::move(p)});
        }
        friend class connection;
    };
    inet_type& _inet;
    std::unordered_map<connid, lw_shared_ptr<tcb>, connid_hash> _tcbs;
    std::unordered_map<uint16_t, listener*> _listening;
    std::random_device _rd;
    std::default_random_engine _e;
    std::uniform_int_distribution<uint16_t> _port_dist{41952, 65535};
    circular_buffer<std::pair<lw_shared_ptr<tcb>, ethernet_address>> _poll_tcbs;
    // queue for packets that do not belong to any tcb
    circular_buffer<ipv4_traits::l4packet> _packetq;
    semaphore _queue_space = {212992};
public:
    class connection {
        lw_shared_ptr<tcb> _tcb;
    public:
        explicit connection(lw_shared_ptr<tcb> tcbp) : _tcb(std::move(tcbp)) { _tcb->_conn = this; }
        connection(const connection&) = delete;
        connection(connection&& x) noexcept : _tcb(std::move(x._tcb)) {
            _tcb->_conn = this;
        }
        ~connection();
        void operator=(const connection&) = delete;
        connection& operator=(connection&& x) {
            if (this != &x) {
                this->~connection();
                new (this) connection(std::move(x));
            }
            return *this;
        }
        future<> send(packet p) {
            return _tcb->send(std::move(p));
        }
        future<> wait_for_data() {
            return _tcb->wait_for_data();
        }
        packet read() {
            return _tcb->read();
        }
        void close_read();
        void close_write();
    };
    class listener {
        tcp& _tcp;
        uint16_t _port;
        queue<connection> _q;
    private:
        listener(tcp& t, uint16_t port, size_t queue_length)
            : _tcp(t), _port(port), _q(queue_length) {
            _tcp._listening.emplace(_port, this);
        }
    public:
        listener(listener&& x)
            : _tcp(x._tcp), _port(x._port), _q(std::move(x._q)) {
            _tcp._listening[_port] = this;
            _port = 0;
        }
        ~listener() {
            if (_port) {
                _tcp._listening.erase(_port);
            }
        }
        future<connection> accept() {
            return _q.not_empty().then([this] {
                return make_ready_future<connection>(_q.pop());
            });
        }
        friend class tcp;
    };
public:
    explicit tcp(inet_type& inet);
    void received(packet p, ipaddr from, ipaddr to);
    bool forward(forward_hash& out_hash_data, packet& p, size_t off);
    listener listen(uint16_t port, size_t queue_length = 100);
    connection connect(socket_address sa);
    net::hw_features hw_features() { return _inet._inet.hw_features(); }
    void poll_tcb(ipaddr to, lw_shared_ptr<tcb> tcb);
private:
    void send(ipaddr from, ipaddr to, packet p);
    void respond_with_reset(tcp_hdr* rth, ipaddr local_ip, ipaddr foreign_ip);
    friend class listener;
};

template <typename InetTraits>
tcp<InetTraits>::tcp(inet_type& inet) : _inet(inet), _e(_rd()) {
    _inet.register_packet_provider([this, tcb_polled = 0u] () mutable {
        std::experimental::optional<typename InetTraits::l4packet> l4p;
        auto c = _poll_tcbs.size();
        if (!_packetq.empty() && (!(tcb_polled % 128) || c == 0)) {
            l4p = std::move(_packetq.front());
            _packetq.pop_front();
            _queue_space.signal(l4p.value().p.len());
        } else {
            while (c--) {
                tcb_polled++;
                lw_shared_ptr<tcb> tcb;
                ethernet_address dst;
                std::tie(tcb, dst) = std::move(_poll_tcbs.front());
                _poll_tcbs.pop_front();
                l4p = tcb->get_packet();
                if (l4p) {
                    l4p.value().e_dst = dst;
                    break;
                }
            }
        }
        return l4p;
    });
}

template <typename InetTraits>
void tcp<InetTraits>::poll_tcb(ipaddr to, lw_shared_ptr<tcb> tcb) {
    _inet.get_l2_dst_address(to).then([this, tcb = std::move(tcb)] (ethernet_address dst) {
        _poll_tcbs.emplace_back(std::move(tcb), dst);
    });
}

template <typename InetTraits>
auto tcp<InetTraits>::listen(uint16_t port, size_t queue_length) -> listener {
    return listener(*this, port, queue_length);
}

template <typename InetTraits>
auto tcp<InetTraits>::connect(socket_address sa) -> connection {
    uint16_t src_port;
    connid id;
    auto src_ip = _inet._inet.host_address();
    auto dst_ip = ipv4_address(sa);
    auto dst_port = net::ntoh(sa.u.in.sin_port);

    do {
        src_port = _port_dist(_e);
        id = connid{src_ip, dst_ip, src_port, dst_port};
    } while (_inet._inet.netif()->hash2cpu(id.hash()) != engine.cpu_id()
            || _tcbs.find(id) != _tcbs.end());

    auto tcbp = make_lw_shared<tcb>(*this, id);
    _tcbs.insert({id, tcbp});
    tcbp->output();

    return connection(tcbp);
}

template <typename InetTraits>
bool tcp<InetTraits>::forward(forward_hash& out_hash_data, packet& p, size_t off) {
    auto th = p.get_header<tcp_hdr>(off);
    if (th) {
        out_hash_data.push_back(th->src_port);
        out_hash_data.push_back(th->dst_port);
    }
    return true;
}

template <typename InetTraits>
void tcp<InetTraits>::received(packet p, ipaddr from, ipaddr to) {
    auto th = p.get_header<tcp_hdr>(0);
    if (!th) {
        return;
    }
    // th->data_offset is correct even before ntoh()
    if (unsigned(th->data_offset * 4) < sizeof(*th)) {
        return;
    }

    if (!hw_features().rx_csum_offload) {
        checksummer csum;
        InetTraits::tcp_pseudo_header_checksum(csum, from, to, p.len());
        csum.sum(p);
        if (csum.get() != 0) {
            return;
        }
    }
    auto h = ntoh(*th);
    auto id = connid{to, from, h.dst_port, h.src_port};
    auto tcbi = _tcbs.find(id);
    lw_shared_ptr<tcb> tcbp;
    if (tcbi == _tcbs.end()) {
        if (h.f_syn && !h.f_ack) {
            auto listener = _listening.find(id.local_port);
            if (listener == _listening.end() || listener->second->_q.full()) {
                return respond_with_reset(&h, id.local_ip, id.foreign_ip);
            }
            tcbp = make_lw_shared<tcb>(*this, id);
            listener->second->_q.push(connection(tcbp));
            _tcbs.insert({id, tcbp});
        }
    } else {
        tcbp = tcbi->second;
    }
    if (tcbp) {
        tcbp->input(&h, std::move(p));
    } else {
        respond_with_reset(&h, id.local_ip, id.foreign_ip);
    }
}

template <typename InetTraits>
void tcp<InetTraits>::send(ipaddr from, ipaddr to, packet p) {
    if (_queue_space.try_wait(p.len())) { // drop packets that do not fit the queue
        _inet.get_l2_dst_address(to).then([this, to, p = std::move(p)] (ethernet_address e_dst) mutable {
                _packetq.emplace_back(ipv4_traits::l4packet{to, std::move(p), e_dst, ip_protocol_num::tcp});
        });
    }
}

template <typename InetTraits>
tcp<InetTraits>::connection::~connection() {
    if (_tcb) {
        _tcb->_conn = nullptr;
        close_read();
        close_write();
    }
}

template <typename InetTraits>
tcp<InetTraits>::tcb::tcb(tcp& t, connid id)
    : _tcp(t)
    , _local_ip(id.local_ip)
    , _foreign_ip(id.foreign_ip)
    , _local_port(id.local_port)
    , _foreign_port(id.foreign_port) {
        _delayed_ack.set_callback([this] { _nr_full_seg_received = 0; output(); });
        _retransmit.set_callback([this] { retransmit(); });
        _snd.next = _snd.initial = get_isn();
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::respond_with_reset(tcp_hdr* rth) {
    _tcp.respond_with_reset(rth, _local_ip, _foreign_ip);
}

template <typename InetTraits>
void tcp<InetTraits>::respond_with_reset(tcp_hdr* rth, ipaddr local_ip, ipaddr foreign_ip) {
    if (rth->f_rst) {
        return;
    }
    packet p;
    auto th = p.prepend_header<tcp_hdr>();
    th->src_port = rth->dst_port;
    th->dst_port = rth->src_port;
    if (rth->f_ack) {
        th->seq = rth->ack;
    }
    // If this RST packet is in response to a SYN packet. We ACK the ISN.
    if (rth->f_syn) {
        th->ack = rth->seq + 1;
        th->f_ack = true;
    }
    th->f_rst = true;
    th->data_offset = sizeof(*th) / 4;
    th->checksum = 0;
    *th = hton(*th);

    checksummer csum;
    InetTraits::tcp_pseudo_header_checksum(csum, local_ip, foreign_ip, sizeof(*th));
    if (hw_features().tx_csum_l4_offload) {
        th->checksum = ~csum.get();
    } else {
        csum.sum(p);
        th->checksum = csum.get();
    }

    offload_info oi;
    oi.protocol = ip_protocol_num::tcp;
    oi.tcp_hdr_len = sizeof(tcp_hdr);
    p.set_offload_info(oi);

    send(local_ip, foreign_ip, std::move(p));
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::input(tcp_hdr* th, packet p) {
    auto opt_start = p.get_header<uint8_t>(sizeof(tcp_hdr));
    auto opt_end = opt_start + th->data_offset * 4;

    p.trim_front(th->data_offset * 4);

    bool do_output = false;
    bool do_output_data = false;
    tcp_seq seg_seq = th->seq;
    auto seg_len = p.len();

    if (th->f_rst) {
        // Fake end-of-connection so reads return immediately
        _nuked = true;
        // Free packets to be sent which are waiting for _snd.user_queue_space
        _snd.user_queue_space.broken(tcp_reset_error());
        cleanup();
        if (_rcv._data_received_promise) {
            // FIXME: set_exception() instead?
            _rcv._data_received_promise->set_value();
            _rcv._data_received_promise = {};
        }
        if (_snd._all_data_acked_promise) {
            // FIXME: set_exception() instead?
            _snd._all_data_acked_promise->set_value();
            _snd._all_data_acked_promise = {};
        }
        return;
    }
    if (th->f_syn) {
        if (!_foreign_syn_received) {
            _foreign_syn_received = true;
            _rcv.initial = seg_seq;
            _rcv.next = _rcv.initial + 1;
            _rcv.urgent = _rcv.next;
            _snd.wl1 = th->seq;
            _option.parse(opt_start, opt_end);
            // Remote receive window scale factor
            _snd.window_scale = _option._remote_win_scale;
            // Local receive window scale factor
            _rcv.window_scale = _option._local_win_scale;
            // Maximum segment size remote can receive
            _snd.mss = _option._remote_mss;
            // Maximum segment size local can receive
            _rcv.mss = _option._local_mss =
                _tcp.hw_features().mtu - net::tcp_hdr_len_min - InetTraits::ip_hdr_len_min;
            // Linux's default window size
            _rcv.window = 29200 << _rcv.window_scale;

            // Setup initial congestion window
            if (2190 < _snd.mss) {
                _snd.cwnd = 2 * _snd.mss;
            } else if (1095 < _snd.mss && _snd.mss <= 2190) {
                _snd.cwnd = 3 * _snd.mss;
            } else {
                _snd.cwnd = 4 * _snd.mss;
            }
            // Setup initial slow start threshold
            _snd.ssthresh = th->window << _snd.window_scale;

            // Send <SYN,ACK> back
            do_output = true;
            _snd.syn_tx_time = clock_type::now();
        } else {
            if (seg_seq != _rcv.initial) {
                return respond_with_reset(th);
            }
        }
    } else {
        // data segment
        if (seg_len
                && (seg_seq >= _rcv.next || seg_seq + seg_len <= _rcv.next + _rcv.window)) {
            // FIXME: handle urgent data (not urgent)
            if (seg_seq < _rcv.next) {
                // ignore already acknowledged data
                auto dup = std::min(uint32_t(_rcv.next - seg_seq), seg_len);
                p.trim_front(dup);
                seg_len -= dup;
                seg_seq += dup;
            }
            if (seg_len) {
                if (seg_seq == _rcv.next) {
                    _rcv.data.push_back(std::move(p));
                    _rcv.next += seg_len;
                    auto merged = merge_out_of_order();
                    if (_rcv._data_received_promise) {
                        _rcv._data_received_promise->set_value();
                        _rcv._data_received_promise = {};
                    }
                    if (merged) {
                        // TCP receiver SHOULD send an immediate ACK when the
                        // incoming segment fills in all or part of a gap in the
                        // sequence space.
                        do_output = true;
                    } else {
                        do_output = should_send_ack(seg_len);
                    }
                } else {
                    insert_out_of_order(seg_seq, std::move(p));
                    // A TCP receiver SHOULD send an immediate duplicate ACK
                    // when an out-of-order segment arrives.
                    do_output = true;
                }
            }
        }
    }
    if (th->f_fin) {
        if (!_local_syn_acked) {
            return respond_with_reset(th);
        }
        auto fin_seq = seg_seq + seg_len;
        if (!_foreign_fin_received) {
            if (fin_seq < _rcv.next || fin_seq > _rcv.next + _rcv.window) {
                // FIXME: we did queue the data.  Do we care?
                return respond_with_reset(th);
            } else if (fin_seq == _rcv.next) {
                // FIXME: we might queue an out-of-order FIN.  Is it worthwhile?
                _foreign_fin_received = true;
                _rcv.next = fin_seq + 1;
                if (_rcv._data_received_promise) {
                    _rcv._data_received_promise->set_value();
                    _rcv._data_received_promise = {};
                }
                // If this <FIN> packet contains data as well, we can ACK both data
                // and <FIN> in a single packet, so canncel the previous ACK.
                clear_delayed_ack();
                do_output = false;
                output();

                // FIXME: Implement TIME-WAIT state
                if (both_closed()) {
                    cleanup();
                    return;
                }
            }
        } else {
            if (fin_seq + 1 != _rcv.next) {
                return;
            }
        }
    }
    if (th->f_ack) {
        if (!_local_syn_sent) {
            return; // FIXME: reset too?
        }
        if (!_local_syn_acked) {
            if (th->ack == _snd.initial + 1) {
                _snd.unacknowledged = _snd.next = _snd.initial + 1;
                _local_syn_acked = true;
                _snd.wl2 = th->ack;
                update_rto(_snd.syn_tx_time);
            } else {
                return respond_with_reset(th);
            }
        }

        // Is this a duplicated ACK
        if (!_snd.data.empty() && seg_len == 0 &&
            th->f_fin == 0 && th->f_syn == 0 &&
            th->ack == _snd.unacknowledged &&
            uint32_t(th->window << _snd.window_scale) == _snd.window) {
            _snd.dupacks++;
            uint32_t smss = _snd.mss;
            // 3 duplicated ACKs trigger a fast retransmit
            if (_snd.dupacks == 3) {
                _snd.ssthresh = std::max(flight_size() / 2, 2 * smss);
                _snd.cwnd = _snd.ssthresh + 3 * smss;
                fast_retransmit();
            } if (_snd.dupacks > 3) {
                _snd.cwnd += smss;
            }
        }

        // If we've sent out FIN, remote can ack both data and FIN, skip FIN
        // when acking data
        auto data_ack = th->ack;
        if (_local_fin_sent && th->ack == _snd.next + 1) {
            data_ack = th->ack - 1;
        }

        if (data_ack > _snd.unacknowledged && data_ack <= _snd.next) {
            // Full ACK of segment
            while (!_snd.data.empty()
                    && (_snd.unacknowledged + _snd.data.front().data_remaining <= data_ack)) {
                auto acked_bytes = _snd.data.front().data_remaining;
                _snd.unacknowledged += acked_bytes;
                // Ignore retransmitted segments when setting the RTO
                if (_snd.data.front().nr_transmits == 0) {
                    update_rto(_snd.data.front().tx_time);
                }
                update_cwnd(acked_bytes);
                _snd.user_queue_space.signal(_snd.data.front().data_len);
                _snd.data.pop_front();
            }

            // Partial ACK of segment
            if (_snd.unacknowledged < data_ack) {
                // For simplicity sake, do not trim the partially acked data
                // off the unacked segment. So we do not need to recalculate
                // the tcp header when retransmit the segment, but the downside
                // is that we will retransmit the whole segment although part
                // of them are already acked.
                auto acked_bytes = data_ack - _snd.unacknowledged;
                _snd.data.front().data_remaining -= acked_bytes;
                _snd.unacknowledged = data_ack;
                update_cwnd(acked_bytes);
            }

            // some data is acked, try send more data
            do_output_data = true;

            if (_snd.data.empty()) {
                // All outstanding segments are acked, turn off the timer.
                stop_retransmit_timer();
                // Signal the waiter of this event
                if (_snd._all_data_acked_promise) {
                    _snd._all_data_acked_promise->set_value();
                    _snd._all_data_acked_promise = {};
                }
            } else {
                // Restart the timer becasue new data is acked.
                start_retransmit_timer();
            }

            // New data is acked, exit fast recovery
            if (_snd.dupacks >= 3) {
                _snd.cwnd = _snd.ssthresh;
            }
            _snd.dupacks = 0;
        }
        if (_local_fin_sent && th->ack == _snd.next + 1) {
            _local_fin_acked = true;
            _snd.unacknowledged += 1;
            _snd.next += 1;
            if (both_closed()) {
                cleanup();
                return;
            }
        }
    }

    if (th->seq >= _snd.wl1 && th->ack >= _snd.wl2) {
        if (!_snd.window && th->window && _snd.unsent_len) {
            do_output_data = true;
        }
        _snd.window = th->window << _snd.window_scale;
        _snd.wl1 = th->seq;
        _snd.wl2 = th->ack;
    }
    // send some stuff
    if (do_output || (do_output_data && can_send())) {
        // Since we will do output, we can canncel scheduled delayed ACK.
        clear_delayed_ack();
        output();
    }
}

template <typename InetTraits>
packet tcp<InetTraits>::tcb::get_transmit_packet() {
    // easy case: empty queue
    if (_snd.unsent.empty()) {
        return packet();
    }
    auto can_send = this->can_send();
    // Max number of TCP payloads we can pass to NIC
    uint32_t len;
    if (_tcp.hw_features().tx_tso) {
        // FIXME: Info tap device the size of the splitted packet
        len = _tcp.hw_features().max_packet_len - net::tcp_hdr_len_min - InetTraits::ip_hdr_len_min;
    } else {
        len = std::min(uint16_t(_tcp.hw_features().mtu - net::tcp_hdr_len_min - InetTraits::ip_hdr_len_min), _snd.mss);
    }
    can_send = std::min(can_send, len);
    // easy case: one small packet
    if (_snd.unsent.size() == 1 && _snd.unsent.front().len() <= can_send) {
        auto p = std::move(_snd.unsent.front());
        _snd.unsent.pop_front();
        _snd.unsent_len -= p.len();
        return p;
    }
    // moderate case: need to split one packet
    if (_snd.unsent.front().len() > can_send) {
        auto p = _snd.unsent.front().share(0, can_send);
        _snd.unsent.front().trim_front(can_send);
        _snd.unsent_len -= p.len();
        return p;
    }
    // hard case: merge some packets, possibly split last
    auto p = std::move(_snd.unsent.front());
    _snd.unsent.pop_front();
    can_send -= p.len();
    while (!_snd.unsent.empty()
            && _snd.unsent.front().len() <= can_send) {
        can_send -= _snd.unsent.front().len();
        p.append(std::move(_snd.unsent.front()));
        _snd.unsent.pop_front();
    }
    if (!_snd.unsent.empty() && can_send) {
        auto& q = _snd.unsent.front();
        p.append(q.share(0, can_send));
        q.trim_front(can_send);
    }
    _snd.unsent_len -= p.len();
    return p;
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::output_one() {
    if (_nuked) {
        return;
    }

    packet p = get_transmit_packet();
    uint16_t len = p.len();
    bool syn_on = !_local_syn_acked;
    bool ack_on = _foreign_syn_received;

    auto options_size = _option.get_size(syn_on, ack_on);
    auto th = p.prepend_header<tcp_hdr>(options_size);

    th->src_port = _local_port;
    th->dst_port = _foreign_port;

    th->f_syn = syn_on;
    _local_syn_sent |= syn_on;
    th->f_ack = ack_on;
    if (ack_on) {
        clear_delayed_ack();
    }
    th->f_urg = false;
    th->f_psh = false;

    th->seq = _snd.next;
    th->ack = _rcv.next;
    th->data_offset = (sizeof(*th) + options_size) / 4;
    th->window = _rcv.window >> _rcv.window_scale;
    th->checksum = 0;

    _snd.next += len;

    // FIXME: does the FIN have to fit in the window?
    th->f_fin = _snd.closed && _snd.unsent_len == 0 && !_local_fin_acked;
    _local_fin_sent |= th->f_fin;

    // Add tcp options
    _option.fill(th, options_size);
    *th = hton(*th);

    offload_info oi;
    checksummer csum;
    InetTraits::tcp_pseudo_header_checksum(csum, _local_ip, _foreign_ip, sizeof(*th) + options_size + len);
    if (_tcp.hw_features().tx_csum_l4_offload) {
        // tx checksum offloading - both virtio-net's VIRTIO_NET_F_CSUM
        // dpdk's PKT_TX_TCP_CKSUM - requires th->checksum to be
        // initialized to ones' complement sum of the pseudo header.
        th->checksum = ~csum.get();
        oi.needs_csum = true;
    } else {
        csum.sum(p);
        th->checksum = csum.get();
        oi.needs_csum = false;
    }
    oi.protocol = ip_protocol_num::tcp;
    oi.tcp_hdr_len = sizeof(tcp_hdr) + options_size;
    p.set_offload_info(oi);

    if (len) {
        unsigned nr_transmits = 0;
        auto now = clock_type::now();
        _snd.data.emplace_back(unacked_segment{p.share(), len, len, nr_transmits, now});
        if (!_retransmit.armed()) {
            start_retransmit_timer(now);
        }
    }

    queue_packet(std::move(p));
}

template <typename InetTraits>
future<> tcp<InetTraits>::tcb::wait_for_data() {
    if (!_rcv.data.empty() || _foreign_fin_received || _nuked) {
        return make_ready_future<>();
    }
    _rcv._data_received_promise = promise<>();
    return _rcv._data_received_promise->get_future();
}

template <typename InetTraits>
future<> tcp<InetTraits>::tcb::wait_for_all_data_acked() {
    if (_snd.data.empty()) {
        return make_ready_future<>();
    }
    _snd._all_data_acked_promise = promise<>();
    return _snd._all_data_acked_promise->get_future();
}

template <typename InetTraits>
packet tcp<InetTraits>::tcb::read() {
    packet p;
    for (auto&& q : _rcv.data) {
        p.append(std::move(q));
    }
    _rcv.data.clear();
    return p;
}

template <typename InetTraits>
future<> tcp<InetTraits>::tcb::send(packet p) {
    // We can not send after the connection is closed
    assert(!_snd.closed);

    if (_nuked) {
        return make_exception_future<>(tcp_reset_error());
    }

    // TODO: Handle p.len() > max user_queue_space case
    auto len = p.len();
    return _snd.user_queue_space.wait(len).then([this, zis = this->shared_from_this(), p = std::move(p)] () mutable {
        _snd.unsent_len += p.len();
        _snd.unsent.push_back(std::move(p));
        if (can_send() > 0) {
            output();
        }
        return make_ready_future<>();
    });
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::close() {
    if (_snd.closed) {
        return;
    }
    // TODO: We should return a future to upper layer
    wait_for_all_data_acked().then([this, zis = this->shared_from_this()] () mutable {
        _snd.closed = true;
        if (_snd.unsent_len == 0) {
            output();
        }
    });
}

template <typename InetTraits>
bool tcp<InetTraits>::tcb::should_send_ack(uint16_t seg_len) {
    // We've received a TSO packet, do ack immediately
    if (seg_len > _rcv.mss) {
        _nr_full_seg_received = 0;
        _delayed_ack.cancel();
        return true;
    }

    // We've received a full sized segment, ack for every second full sized segment
    if (seg_len == _rcv.mss) {
        if (_nr_full_seg_received++ >= 1) {
            _nr_full_seg_received = 0;
            _delayed_ack.cancel();
            return true;
        }
    }

    // If the timer is armed and its callback hasn't been run.
    if (_delayed_ack.armed()) {
        return false;
    }

    // If the timer is not armed, schedule a delayed ACK.
    // The maximum delayed ack timer allowed by RFC1122 is 500ms, most
    // implementations use 200ms.
    _delayed_ack.arm(200ms);
    return false;
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::clear_delayed_ack() {
    _delayed_ack.cancel();
}

template <typename InetTraits>
bool tcp<InetTraits>::tcb::merge_out_of_order() {
    bool merged = false;
    if (_rcv.out_of_order.map.empty()) {
        return merged;
    }
    for (auto it = _rcv.out_of_order.map.begin(); it != _rcv.out_of_order.map.end();) {
        auto& p = it->second;
        auto seg_beg = it->first;
        auto seg_len = p.len();
        auto seg_end = seg_beg + seg_len;
        if (seg_beg <= _rcv.next && _rcv.next < seg_end) {
            // This segment has been received out of order and its previous
            // segment has been received now
            auto trim = _rcv.next - seg_beg;
            if (trim) {
                p.trim_front(trim);
                seg_len -= trim;
            }
            _rcv.next += seg_len;
            _rcv.data.push_back(std::move(p));
            // Since c++11, erase() always returns the value of the following element
            it = _rcv.out_of_order.map.erase(it);
            merged = true;
        } else if (_rcv.next >= seg_end) {
            // This segment has been receive already, drop it
            it = _rcv.out_of_order.map.erase(it);
        } else {
            // seg_beg > _rcv.need, can not merge. Note, seg_beg can grow only,
            // so we can stop looking here.
            it++;
            break;
        }
    }
    return merged;
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::insert_out_of_order(tcp_seq seg, packet p) {
    _rcv.out_of_order.merge(seg, std::move(p));
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::trim_receive_data_after_window() {
    abort();
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::retransmit() {
    if (_snd.data.empty()) {
        return;
    }

    // If there are unacked data, retransmit the earliest segment
    auto& unacked_seg = _snd.data.front();

    // According to RFC5681
    // Update ssthresh only for the first retransmit
    uint32_t smss = _snd.mss;
    if (unacked_seg.nr_transmits == 0) {
        _snd.ssthresh = std::max(flight_size() / 2, 2 * smss);
    }
    // Start the slow start process
    _snd.cwnd = smss;
    _snd.dupacks = 0;

    if (unacked_seg.nr_transmits < _max_nr_retransmit) {
        unacked_seg.nr_transmits++;
    } else {
        // Delete connection when max num of retransmission is reached
        cleanup();
        return;
    }
    // TODO: If the Path MTU changes, we need to split the segment if it is larger than current MSS
    queue_packet(unacked_seg.p.share());
    output();

    // According to RFC6298, Update RTO <- RTO * 2 to perform binary exponential back-off
    _rto = std::min(_rto * 2, _rto_max);
    start_retransmit_timer();
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::fast_retransmit() {
    if (!_snd.data.empty()) {
        auto& unacked_seg = _snd.data.front();
        unacked_seg.nr_transmits++;
        queue_packet(unacked_seg.p.share());
        output();
    }
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::update_rto(clock_type::time_point tx_time) {
    // Update RTO according to RFC6298
    auto R = std::chrono::duration_cast<std::chrono::milliseconds>(clock_type::now() - tx_time);
    if (_snd.first_rto_sample) {
        _snd.first_rto_sample = false;
        // RTTVAR <- R/2
        // SRTT <- R
        _snd.rttvar = R / 2;
        _snd.srtt = R;
    } else {
        // RTTVAR <- (1 - beta) * RTTVAR + beta * |SRTT - R'|
        // SRTT <- (1 - alpha) * SRTT + alpha * R'
        // where alpha = 1/8 and beta = 1/4
        auto delta = _snd.srtt > R ? (_snd.srtt - R) : (R - _snd.srtt);
        _snd.rttvar = _snd.rttvar * 3 / 4 + delta / 4;
        _snd.srtt = _snd.srtt * 7 / 8 +  R / 8;
    }
    // RTO <- SRTT + max(G, K * RTTVAR)
    _rto =  _snd.srtt + std::max(_rto_clk_granularity, 4 * _snd.rttvar);

    // Make sure 1 sec << _rto << 60 sec
    _rto = std::max(_rto, _rto_min);
    _rto = std::min(_rto, _rto_max);
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::update_cwnd(uint32_t acked_bytes) {
    uint32_t smss = _snd.mss;
    if (_snd.cwnd < _snd.ssthresh) {
        // In slow start phase
        _snd.cwnd += std::min(acked_bytes, smss);
    } else {
        // In congestion avoidance phase
        uint32_t round_up = 1;
        _snd.cwnd += std::max(round_up, smss * smss / _snd.cwnd);
    }
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::cleanup() {
    _snd.unsent.clear();
    _snd.data.clear();
    _rcv.out_of_order.map.clear();
    _rcv.data.clear();
    stop_retransmit_timer();
    clear_delayed_ack();
    remove_from_tcbs();
}

template <typename InetTraits>
tcp_seq tcp<InetTraits>::tcb::get_isn() {
    // Per RFC6528, TCP SHOULD generate its Initial Sequence Numbers
    // with the expression:
    //   ISN = M + F(localip, localport, remoteip, remoteport, secretkey)
    //   M is the 4 microsecond timer
    using namespace std::chrono;
    uint32_t hash[4];
    hash[0] = _local_ip.ip;
    hash[1] = _foreign_ip.ip;
    hash[2] = (_local_port << 16) + _foreign_port;
    hash[3] = _isn_secret.key[15];
    CryptoPP::Weak::MD5::Transform(hash, _isn_secret.key);
    auto seq = hash[0];
    auto m = duration_cast<microseconds>(clock_type::now().time_since_epoch());
    seq += m.count() / 4;
    return make_seq(seq);
}

template <typename InetTraits>
std::experimental::optional<typename InetTraits::l4packet> tcp<InetTraits>::tcb::get_packet() {
    _poll_active = false;
    if (_packetq.empty()) {
        output_one();
    }

    if (_nuked) {
        return std::experimental::optional<typename InetTraits::l4packet>();
    }

    assert(!_packetq.empty());

    auto p = std::move(_packetq.front());
    _packetq.pop_front();
    if (!_packetq.empty() || (_snd.dupacks < 3 && can_send() > 0)) {
        // If there are packets to send in the queue or tcb is allowed to send
        // more add tcp back to polling set to keep sending. In addition, dupacks >= 3
        // is an indication that an segment is lost, stop sending more in this case.
        output();
    }
    return std::move(p);
}

template <typename InetTraits>
void tcp<InetTraits>::connection::close_read() {
}

template <typename InetTraits>
void tcp<InetTraits>::connection::close_write() {
    _tcb->close();
}

template <typename InetTraits>
constexpr uint16_t tcp<InetTraits>::tcb::_max_nr_retransmit;

template <typename InetTraits>
constexpr std::chrono::milliseconds tcp<InetTraits>::tcb::_rto_min;

template <typename InetTraits>
constexpr std::chrono::milliseconds tcp<InetTraits>::tcb::_rto_max;

template <typename InetTraits>
constexpr std::chrono::milliseconds tcp<InetTraits>::tcb::_rto_clk_granularity;

template <typename InetTraits>
typename tcp<InetTraits>::tcb::isn_secret tcp<InetTraits>::tcb::_isn_secret;

}



#endif /* TCP_HH_ */
