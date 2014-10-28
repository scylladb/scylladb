/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 *
 * This work is open source software, licensed under the terms of the
 * BSD license as described in the LICENSE file in the top-level directory.
 */

#include <functional>
#include <unordered_map>
#include <forward_list>
#include <utility>
#include <string>
#include <map>
#include <iostream>
#include <unordered_map>

#include "scollectd.hh"
#include "core/shared_ptr.hh"
#include "core/app-template.hh"
#include "core/future-util.hh"
#include "core/shared_ptr.hh"

using namespace std::chrono_literals;

class scollectd::impl {
    net::udp_channel _chan;
    timer _timer;

    std::string _host = "localhost";
    ipv4_addr _addr = default_addr;
    clock_type::duration _period = default_period;
    uint64_t _num_packets = 0;
    uint64_t _millis = 0;
    uint64_t _bytes = 0;
    double _avg = 0;

public:
    // Note: we use std::shared_ptr, not the C* one. This is because currently
    // seastar sp does not handle polymorphism. And we use it.
    typedef std::map<type_instance_id, std::shared_ptr<value_list> > value_list_map;
    typedef value_list_map::value_type value_list_pair;

    void add_polled(const type_instance_id & id,
            const std::shared_ptr<value_list> & values) {
        _values.insert(std::make_pair(id, values));
    }
    void remove_polled(const type_instance_id & id) {
        _values.insert(std::make_pair(id, std::shared_ptr<value_list>()));
    }
    // explicitly send a type_instance value list (outside polling)
    future<> send_metric(const type_instance_id & id,
            const value_list & values) {
        if (values.empty()) {
            return make_ready_future();
        }
        cpwriter out;
        out.put(_host, clock_type::duration(), id, values);
        return _chan.send(_addr, net::packet(out.data(), out.size()));
    }
    future<> send_notification(const type_instance_id & id,
            const std::string & msg) {
        cpwriter out;
        out.put(_host, id);
        out.put(part_type::Message, msg);
        return _chan.send(_addr, net::packet(out.data(), out.size()));
    }
    // initiates actual value polling -> send to target "loop"
    void start(const std::string & host, const ipv4_addr & addr,
            const clock_type::duration period) {
        _period = period;
        _addr = addr;
        _host = host;
        _chan = engine.net().make_udp_channel();
        _timer.set_callback(std::bind(&impl::run, this));

        // dogfood ourselves
        _regs = {
            // total_bytes      value:DERIVE:0:U
            add_polled_metric(
                    type_instance_id("scollectd", per_cpu_plugin_instance,
                            "total_bytes", "sent"),
                    make_typed(data_type::DERIVE, _bytes)),
            // total_requests      value:DERIVE:0:U
            add_polled_metric(
                    type_instance_id("scollectd", per_cpu_plugin_instance,
                            "total_requests"),
                    make_typed(data_type::DERIVE, _num_packets)
            ),
            // latency          value:GAUGE:0:U
            add_polled_metric(
                    type_instance_id("scollectd", per_cpu_plugin_instance,
                            "latency"), _avg),
            // total_time_in_ms    value:DERIVE:0:U
            add_polled_metric(
                    type_instance_id("scollectd", per_cpu_plugin_instance,
                            "total_time_in_ms"),
                    make_typed(data_type::DERIVE, _millis)
            ),
            // total_values     value:DERIVE:0:U
            add_polled_metric(
                    type_instance_id("scollectd", per_cpu_plugin_instance,
                            "total_values"),
                    make_typed(data_type::DERIVE, std::bind(&value_list_map::size, &_values))
            )
        };

        send_notification(
                type_instance_id("scollectd", per_cpu_plugin_instance,
                        "network"), "daemon started");
        arm();
    }
    void stop() {
        _timer.cancel();
        _regs.clear();
    }

private:
    static const size_t payload_size = 1024;

    void arm() {
        if (_period != clock_type::duration()) {
            _timer.arm(_period);
        }
    }

    enum class part_type
        : uint16_t {
            Host = 0x0000, // The name of the host to associate with subsequent data values
        Time = 0x0001, // Time 	Numeric The timestamp to associate with subsequent data values, unix time format (seconds since epoch)
        TimeHr = 0x0008, // Time (high resolution) 	Numeric The timestamp to associate with subsequent data values. Time is defined in 2–30 seconds since epoch. New in Version 5.0.
        Plugin = 0x0002, // Plugin String The plugin name to associate with subsequent data values, e.g. "cpu"
        PluginInst = 0x0003, // Plugin instance String 	The plugin instance name to associate with subsequent data values, e.g. "1"
        Type = 0x0004, // Type String The type name to associate with subsequent data values, e.g. "cpu"
        TypeInst = 0x0005, // Type instance 	String 	The type instance name to associate with subsequent data values, e.g. "idle"
        Values = 0x0006, //	Values 	other 	Data values, see above
        Interval = 0x0007, // Interval Numeric Interval used to set the "step" when creating new RRDs unless rrdtool plugin forces StepSize. Also used to detect values that have timed out.
        IntervalHr = 0x0009, //	Interval (high resolution) 	Numeric 	The interval in which subsequent data values are collected. The interval is given in 2–30 seconds. New in Version 5.0.
        Message = 0x0100, // Message (notifications) String
        Severity = 0x0101, // Severity 	Numeric
        Signature = 0x0200, // Signature (HMAC-SHA-256) 	other (todo)
        Encryption = 0x0210, //	Encryption (AES-256/OFB
    };

    // "Time is defined in 2^–30 seconds since epoch. New in Version 5.0."
    typedef std::chrono::duration<uint64_t, std::ratio<1, 0x40000000>> collectd_hres_duration;

    // yet another writer type, this one to construct collectd network
    // protocol data.
    struct cpwriter {
        typedef std::array<char, payload_size> buffer_type;
        typedef buffer_type::iterator mark_type;
        typedef buffer_type::const_iterator const_mark_type;

        buffer_type _buf;
        mark_type _pos;

        std::unordered_map<uint16_t, std::string> _cache;

        cpwriter()
                : _pos(_buf.begin()) {
        }
        mark_type mark() const {
            return _pos;
        }
        void reset(mark_type m) {
            _pos = m;
        }
        size_t size() const {
            return std::distance(_buf.begin(), const_mark_type(_pos));
        }
        bool empty() const {
            return _pos == _buf.begin();
        }
        void clear() {
            reset(_buf.begin());
            _cache.clear();
        }
        const char * data() const {
            return &_buf.at(0);
        }
        char * data() {
            return &_buf.at(0);
        }
        void check(size_t sz) const {
            size_t av = std::distance(const_mark_type(_pos), _buf.end());
            if (av < sz) {
                throw std::length_error("buffer overflow");
            }
        }

        template<typename _Iter>
        cpwriter & write(_Iter s, _Iter e) {
            check(std::distance(s, e));
            _pos = std::copy(s, e, _pos);
            return *this;
        }
        template<typename T>
        typename std::enable_if<std::is_integral<T>::value, cpwriter &>::type write(
                const T & t) {
            T tmp = t;
            net::hton(tmp);
            auto * p = reinterpret_cast<const uint8_t *>(&tmp);
            auto * e = p + sizeof(T);
            write(p, e);
            return *this;
        }
        cpwriter & write(const std::string & s) {
            write(s.begin(), s.end() + 1); // include \0
            return *this;
        }
        cpwriter & put(part_type type, const std::string & s) {
            write(uint16_t(type));
            write(uint16_t(4 + s.size() + 1)); // include \0
            write(s); // include \0
            return *this;
        }
        cpwriter & put_cached(part_type type, const std::string & s) {
            auto & cached = _cache[uint16_t(type)];
            if (cached != s) {
                put(type, s);
                cached = s;
            }
            return *this;
        }
        template<typename T>
        typename std::enable_if<std::is_integral<T>::value, cpwriter &>::type put(
                part_type type, T & t) {
            write(uint16_t(type));
            write(uint16_t(4 + sizeof(t)));
            write(t);
            return *this;
        }
        cpwriter & put(part_type type, const value_list & v) {
            auto s = v.size();
            auto sz = 6 + s + s * sizeof(uint64_t);
            check(sz);
            write(uint16_t(type));
            write(uint16_t(sz));
            write(uint16_t(s));
            v.types(reinterpret_cast<data_type *>(&(*_pos)));
            _pos += s;
            v.values(reinterpret_cast<net::packed<uint64_t> *>(&(*_pos)));
            _pos += s * sizeof(uint64_t);
            return *this;
        }
        cpwriter & put(const std::string & host, const type_instance_id & id) {
            const auto ts =
                    std::chrono::duration_cast<collectd_hres_duration>(
                            std::chrono::system_clock::now().time_since_epoch()).count();

            put_cached(part_type::Host, host);
            // TODO: we're only sending hi-res time stamps.
            // Is this a problem?
            put(part_type::TimeHr, ts);
            put_cached(part_type::Plugin, id.plugin());
            // Optional
            put_cached(part_type::PluginInst,
                    id.plugin_instance() == per_cpu_plugin_instance ?
                            std::to_string(engine._id) : id.plugin_instance());
            put_cached(part_type::Type, id.type());
            // Optional
            put_cached(part_type::TypeInst, id.type_instance());
            return *this;
        }
        cpwriter & put(const std::string & host,
                const clock_type::duration & period,
                const type_instance_id & id, const value_list & v) {
            const auto ps = std::chrono::duration_cast<collectd_hres_duration>(
                    period).count();
            put(host, id);
            put(part_type::Values, v);
            if (ps != 0) {
                put(part_type::IntervalHr, ps);
            }
            return *this;
        }
    };

    void run() {
        typedef value_list_map::iterator iterator;
        typedef std::tuple<iterator, cpwriter> context;

        auto ctxt = make_shared<context>();

        // note we're doing this unsynced since we assume
        // all registrations to this instance will be done on the
        // same cpu, and without interuptions (no wait-states)
        std::get<iterator>(*ctxt) = _values.begin();

        auto stop_when = [this, ctxt]() {
            auto done = std::get<iterator>(*ctxt) == _values.end();
            return done;
        };
        // append as many values as we can fit into a packet (1024 bytes)
        auto send_packet = [this, ctxt]() {
            auto start = std::chrono::high_resolution_clock::now();
            auto & i = std::get<iterator>(*ctxt);
            auto & out = std::get<cpwriter>(*ctxt);

            out.clear();

            while (i != _values.end()) {
                // nullptr value list means removed value. so remove.
                if (!i->second) {
                    i = _values.erase(i);
                    continue;
                }
                auto m = out.mark();
                try {
                    out.put(_host, _period, i->first, *i->second);
                } catch (std::length_error &) {
                    out.reset(m);
                    break;
                }
                ++i;
            }
            if (out.empty()) {
                return make_ready_future();
            }
            return _chan.send(_addr, net::packet(out.data(), out.size())).then([start, ctxt, this]() {
                        auto & out = std::get<cpwriter>(*ctxt);
                        auto now = std::chrono::high_resolution_clock::now();
                        // dogfood stats
                        ++_num_packets;
                        _millis += std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count();
                        _bytes += out.size();
                        _avg = double(_millis) / _num_packets;
                    }).rescue([] (auto get_ex) {
                        try {
                            get_ex();
                        } catch (std::exception & ex) {
                            std::cout << "send failed: " << ex.what() << std::endl;
                        } catch (...) {
                            std::cout << "send failed: - unknown exception" << std::endl;
                        }
                    });
        };
        do_until(stop_when, send_packet).then([this]() {
            arm();
        });
    }
private:
    value_list_map _values;
    std::vector<registration> _regs;
};

const ipv4_addr scollectd::default_addr("239.192.74.66:25826");
const clock_type::duration scollectd::default_period(1s);
const scollectd::plugin_instance_id scollectd::per_cpu_plugin_instance("#cpu");

scollectd::impl & scollectd::get_impl() {
    static thread_local impl per_cpu_instance;
    return per_cpu_instance;
}

void scollectd::add_polled(const type_instance_id & id,
        const std::shared_ptr<value_list> & values) {
    get_impl().add_polled(id, values);
}

void scollectd::remove_polled_metric(const type_instance_id & id) {
    get_impl().remove_polled(id);
}

future<> scollectd::send_notification(const type_instance_id & id,
        const std::string & msg) {
    return get_impl().send_notification(id, msg);
}

future<> scollectd::send_metric(const type_instance_id & id,
        const value_list & values) {
    return get_impl().send_metric(id, values);
}

void scollectd::configure(const boost::program_options::variables_map & opts) {
    bool enable = opts["collectd"].as<bool>();
    if (!enable) {
        return;
    }
    auto addr = ipv4_addr(opts["collectd-address"].as<std::string>());
    auto period = std::chrono::duration_cast<clock_type::duration>(
            std::chrono::milliseconds(
                    opts["collectd-poll-period"].as<unsigned>()));

    auto host = opts["collectd-hostname"].as<std::string>();

    // Now create send loops on each cpu
    for (unsigned c = 0; c < smp::count; c++) {
        smp::submit_to(c, [=] () {
            get_impl().start(host, addr, period);
        });
    }
}

boost::program_options::options_description scollectd::get_options_description() {
    namespace bpo = boost::program_options;
    bpo::options_description opts("COLLECTD options");
    opts.add_options()("collectd", bpo::value<bool>()->default_value(true),
            "enable collectd daemon")("collectd-address",
            bpo::value<std::string>()->default_value("239.192.74.66:25826"),
            "address to send/broadcast metrics to")("collectd-poll-period",
            bpo::value<unsigned>()->default_value(1000),
            "poll period - frequency of sending counter metrics (default: 1000ms, 0 disables)")(
            "collectd-hostname",
            bpo::value<std::string>()->default_value("localhost"),
            "collectd host name");
    return opts;
}
