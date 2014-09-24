/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef TCP_HH_
#define TCP_HH_

#include "core/shared_ptr.hh"
#include "core/queue.hh"
#include "net.hh"
#include "ip_checksum.hh"
#include <unordered_map>
#include <map>
#include <functional>
#include <deque>
#include <chrono>

using namespace std::chrono_literals;

namespace net {

struct tcp_seq {
    uint32_t raw;
};

inline void ntoh(tcp_seq& s) {
    ntoh(s.raw);
}

inline void hton(tcp_seq& s) {
    hton(s.raw);
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
    class connection;
    class listener;
private:
    struct connid;
    class tcb;

    struct connid {
        ipaddr local_ip;
        ipaddr foreign_ip;
        uint16_t local_port;
        uint16_t foreign_port;

        bool operator==(const connid& x) const {
            return local_ip == x.local_ip
                    && foreign_ip == x.foreign_ip
                    && local_port == x.local_port
                    && foreign_port == x.foreign_port;
        }
    };
    struct connid_hash;
    class tcb {
        // Instead of tracking state through an enum, track individual
        // bits of the state.  This reduces duplication in state handling.
        bool _local_syn_sent = false;
        bool _local_syn_acked = false;
        bool _foreign_syn_received = false;
        bool _local_fin_sent = false;
        bool _local_fin_acked = false;
        bool _foreign_fin_received = false;
        tcp& _tcp;
        connection* _conn = nullptr;
        ipaddr _local_ip;
        ipaddr _foreign_ip;
        uint16_t _local_port;
        uint16_t _foreign_port;
        struct send {
            tcp_seq unacknowledged;
            tcp_seq next;
            uint32_t window;
            tcp_seq urgent;
            tcp_seq wl1;
            tcp_seq wl2;
            tcp_seq initial;
            std::deque<packet> data;
            std::deque<packet> unsent;
            uint32_t unsent_len = 0;
            bool closed = false;
            promise<> _window_opened;
        } _snd;
        struct receive {
            tcp_seq next;
            uint32_t window = 20000;
            tcp_seq urgent;
            tcp_seq initial;
            std::deque<packet> data;
            std::map<tcp_seq, packet> out_of_order;
            bool _user_waiting = false;
            promise<> _data_received;
        } _rcv;
        timer _delayed_ack;
    public:
        tcb(tcp& t, connid id);
        void input(tcp_hdr* th, packet p);
        void output();
        future<> wait_for_data();
        future<> send(packet p);
        packet read();
        void close();
    private:
        void respond_with_reset(tcp_hdr* th);
        void merge_out_of_order();
        void insert_out_of_order(tcp_seq seq, packet p);
        void trim_receive_data_after_window();
        bool should_send_ack();
        void clear_delayed_ack();
        void process_delayed_ack();
        packet get_transmit_packet();
        friend class connection;
    };
    inet_type& _inet;
    std::unordered_map<connid, shared_ptr<tcb>, connid_hash> _tcbs;
    std::unordered_map<uint16_t, listener*> _listening;
public:
    class connection {
        shared_ptr<tcb> _tcb;
    public:
        explicit connection(shared_ptr<tcb> tcbp) : _tcb(std::move(tcbp)) { _tcb->_conn = this; }
        connection(const connection&) = delete;
        connection(connection&& x) : _tcb(std::move(x._tcb)) {
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
    explicit tcp(inet_type& inet) : _inet(inet) {}
    void received(packet p, ipaddr from, ipaddr to);
    listener listen(uint16_t port, size_t queue_length = 100);
private:
    void send(ipaddr from, ipaddr to, packet p);
    void respond_with_reset(tcp_hdr* rth, ipaddr local_ip, ipaddr foreign_ip);
    void connection_refused();
    friend class listener;
};

template <typename InetTraits>
struct tcp<InetTraits>::connid_hash : private std::hash<ipaddr>, private std::hash<uint16_t> {
    size_t operator()(const tcp<InetTraits>::connid& id) const noexcept {
        using h1 = std::hash<ipaddr>;
        using h2 = std::hash<uint16_t>;
        return h1::operator()(id.local_ip)
            ^ h1::operator()(id.foreign_ip)
            ^ h2::operator()(id.local_port)
            ^ h2::operator()(id.foreign_port);
    }
};

template <typename InetTraits>
auto tcp<InetTraits>::listen(uint16_t port, size_t queue_length) -> listener {
    return listener(*this, port, queue_length);
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
    checksummer csum;
    InetTraits::pseudo_header_checksum(csum, from, to, p.len());
    csum.sum(p);
    if (csum.get() != 0) {
        return;
    }
    // FIXME: process options
    p.trim_front(th->data_offset * 4);
    ntoh(*th);
    auto id = connid{to, from, th->dst_port, th->src_port};
    auto tcbi = _tcbs.find(id);
    shared_ptr<tcb> tcbp;
    if (tcbi == _tcbs.end()) {
        if (th->f_syn && !th->f_ack) {
            auto listener = _listening.find(id.local_port);
            if (listener == _listening.end() || listener->second->_q.full()) {
                return connection_refused();
            }
            tcbp = make_shared<tcb>(*this, id);
            listener->second->_q.push(connection(tcbp));
            _tcbs.insert({id, tcbp});
        }
    } else {
        tcbp = tcbi->second;
    }
    if (tcbp) {
        tcbp->input(th, std::move(p));
    } else {
        respond_with_reset(th, id.local_ip, id.foreign_ip);
    }
}

template <typename InetTraits>
void tcp<InetTraits>::send(ipaddr from, ipaddr to, packet p) {
    _inet.send(from, to, std::move(p));
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
    process_delayed_ack();
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
    th->f_rst = true;
    th->data_offset = sizeof(*th) / 4;
    hton(*th);

    checksummer csum;
    InetTraits::pseudo_header_checksum(csum, local_ip, foreign_ip, sizeof(*th));
    csum.sum(p);
    th->checksum = csum.get();

    send(local_ip, foreign_ip, std::move(p));
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::input(tcp_hdr* th, packet p) {
    bool do_output = false;

    tcp_seq seg_seq = th->seq;
    auto seg_len = p.len();
    if (th->f_syn) {
        do_output = true;
        if (!_foreign_syn_received) {
            _foreign_syn_received = true;
            _rcv.initial = seg_seq;
            _rcv.next = _rcv.initial + 1;
            _rcv.window = 4500; // FIXME: what?
            _rcv.urgent = _rcv.next;
            _snd.wl1 = th->seq;
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
                    merge_out_of_order();
                    if (_rcv._user_waiting) {
                        _rcv._user_waiting = false;
                        _rcv._data_received.set_value();
                    }
                } else {
                    insert_out_of_order(seg_seq, std::move(p));
                }
            }
        }
        do_output = should_send_ack();
    }
    if (th->f_fin) {
        do_output = true;
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
                _rcv.window = 0;
                if (_rcv._user_waiting) {
                    _rcv._user_waiting = false;
                    _rcv._data_received.set_value();
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
            } else {
                return respond_with_reset(th);
            }
        }
        if (th->ack > _snd.unacknowledged && th->ack <= _snd.next) {
            while (!_snd.data.empty()
                    && (_snd.unacknowledged + _snd.data.front().len() <= th->ack)) {
                _snd.unacknowledged += _snd.data.front().len();
                _snd.data.pop_front();
            }
            if (_snd.unacknowledged < th->ack) {
                _snd.data.front().trim_front(th->ack - _snd.unacknowledged);
                _snd.unacknowledged = th->ack;
            }
        }
        if (_local_fin_sent && th->ack == _snd.next + 1) {
            _local_fin_acked = true;
        }
    }

    if (th->seq >= _snd.wl1 && th->ack >= _snd.wl2) {
        if (!_snd.window && th->window && _snd.unsent_len) {
            do_output = true;
        }
        _snd.window = th->window;
        _snd.wl1 = th->seq;
        _snd.wl2 = th->ack;
    }
    // send some stuff
    if (do_output) {
        output();
    }
}

template <typename InetTraits>
packet tcp<InetTraits>::tcb::get_transmit_packet() {
    // easy case: empty queue
    if (_snd.unsent.empty()) {
        return packet();
    }
    auto can_send = std::min(
            uint32_t(_snd.unacknowledged + _snd.window - _snd.next),
            _snd.unsent_len);
    auto mss = 1400U;
    can_send = std::min(can_send, mss);
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
void tcp<InetTraits>::tcb::output() {
    packet p = get_transmit_packet();
    auto len = p.len();
    if (len) {
        _snd.data.push_back(p.share());
    }

    auto th = p.prepend_header<tcp_hdr>();
    th->src_port = _local_port;
    th->dst_port = _foreign_port;

    th->f_syn = !_local_syn_acked;
    _local_syn_sent |= th->f_syn;
    th->f_ack = _foreign_syn_received;
    if (th->f_ack) {
        clear_delayed_ack();
    }
    th->f_urg = false;
    th->f_psh = false;

    th->seq = _snd.next;
    th->ack = _rcv.next;
    th->data_offset = sizeof(*th) / 4; // FIXME: options
    th->window = _rcv.window;
    th->checksum = 0;

    _snd.next += len;

    // FIXME: does the FIN have to fit in the window?
    th->f_fin = _snd.closed && _snd.unsent_len == 0;
    _local_fin_sent |= th->f_fin;

    hton(*th);

    checksummer csum;
    InetTraits::pseudo_header_checksum(csum, _local_ip, _foreign_ip, sizeof(*th) + len);
    csum.sum(p);
    th->checksum = csum.get();

    _tcp.send(_local_ip, _foreign_ip, std::move(p));
}

template <typename InetTraits>
future<> tcp<InetTraits>::tcb::wait_for_data() {
    if (!_rcv.data.empty() || _foreign_fin_received) {
        return make_ready_future<>();
    }
    _rcv._user_waiting = true;
    _rcv._data_received = promise<>();
    return _rcv._data_received.get_future();
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
    _snd.unsent_len += p.len();
    _snd.unsent.push_back(std::move(p));
    output();
    return make_ready_future<>();
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::close() {
    _snd.closed = true;
    if (_snd.unsent_len == 0) {
        output();
    }
}

template <typename InetTraits>
bool tcp<InetTraits>::tcb::should_send_ack() {
    if (_delayed_ack.armed()) {
        _delayed_ack.suspend();
        return true;
    } else {
        _delayed_ack.arm(400ms);
        return false;
    }
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::clear_delayed_ack() {
    if (_delayed_ack.armed()) {
        _delayed_ack.suspend();
    }
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::process_delayed_ack() {
    _delayed_ack.expired().then([this] {
        output();
        process_delayed_ack();
    });
}

template <typename InetTraits>
void tcp<InetTraits>::connection_refused() {
    // FIXME: send ICMP packet
    print("tcp: refusing connection\n");
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::merge_out_of_order() {
    if (_rcv.out_of_order.empty()) {
        return;
    }
    abort();
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::insert_out_of_order(tcp_seq seq, packet p) {
    abort();
}

template <typename InetTraits>
void tcp<InetTraits>::tcb::trim_receive_data_after_window() {
    abort();
}

template <typename InetTraits>
void tcp<InetTraits>::connection::close_read() {
}

template <typename InetTraits>
void tcp<InetTraits>::connection::close_write() {
    _tcb->close();
}

}



#endif /* TCP_HH_ */
