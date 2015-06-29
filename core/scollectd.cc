/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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
#include "core/future-util.hh"
#include "core/reactor.hh"
#include "net/api.hh"
#include "scollectd_api.hh"

bool scollectd::type_instance_id::operator<(
        const scollectd::type_instance_id& id2) const {
    auto& id1 = *this;
    return std::tie(id1.plugin(), id1.plugin_instance(), id1.type(),
            id1.type_instance())
            < std::tie(id2.plugin(), id2.plugin_instance(), id2.type(),
                    id2.type_instance());
}
bool scollectd::type_instance_id::operator==(
        const scollectd::type_instance_id & id2) const {
    auto& id1 = *this;
    return std::tie(id1.plugin(), id1.plugin_instance(), id1.type(),
            id1.type_instance())
            == std::tie(id2.plugin(), id2.plugin_instance(), id2.type(),
                    id2.type_instance());
}

namespace scollectd {

using namespace std::chrono_literals;
using clock_type = std::chrono::high_resolution_clock;


static const ipv4_addr default_addr("239.192.74.66:25826");
static const clock_type::duration default_period(1s);
const plugin_instance_id per_cpu_plugin_instance("#cpu");

future<> send_metric(const type_instance_id &, const value_list &);

class impl {
    net::udp_channel _chan;
    timer<> _timer;

    sstring _host = "localhost";
    ipv4_addr _addr = default_addr;
    clock_type::duration _period = default_period;
    uint64_t _num_packets = 0;
    uint64_t _millis = 0;
    uint64_t _bytes = 0;
    double _avg = 0;

public:
    typedef std::map<type_instance_id, shared_ptr<value_list> > value_list_map;
    typedef value_list_map::value_type value_list_pair;

    void add_polled(const type_instance_id & id,
            const shared_ptr<value_list> & values) {
        _values[id] = values;
    }
    void remove_polled(const type_instance_id & id) {
        auto i = _values.find(id);
        if (i != _values.end()) {
            i->second = nullptr;
        }
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
            const sstring & msg) {
        cpwriter out;
        out.put(_host, id);
        out.put(part_type::Message, msg);
        return _chan.send(_addr, net::packet(out.data(), out.size()));
    }
    // initiates actual value polling -> send to target "loop"
    void start(const sstring & host, const ipv4_addr & addr,
            const clock_type::duration period) {
        _period = period;
        _addr = addr;
        _host = host;
        _chan = engine().net().make_udp_channel();
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
            ),
            // records          value:GAUGE:0:U
            add_polled_metric(
                    type_instance_id("scollectd", per_cpu_plugin_instance,
                            "records"),
                    make_typed(data_type::GAUGE, std::bind(&value_list_map::size, &_values))
            ),
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
        bool _overflow = false;

        std::unordered_map<uint16_t, sstring> _cache;

        cpwriter()
                : _pos(_buf.begin()) {
        }
        mark_type mark() const {
            return _pos;
        }
        bool overflow() const {
            return _overflow;
        }
        void reset(mark_type m) {
            _pos = m;
            _overflow = false;
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
            _overflow = false;
        }
        const char * data() const {
            return &_buf.at(0);
        }
        char * data() {
            return &_buf.at(0);
        }
        cpwriter& check(size_t sz) {
            size_t av = std::distance(_pos, _buf.end());
            _overflow |= av < sz;
            return *this;
        }
        explicit operator bool() const {
            return !_overflow;
        }
        bool operator!() const {
            return !operator bool();
        }
        template<typename _Iter>
        cpwriter & write(_Iter s, _Iter e) {
            if (check(std::distance(s, e))) {
                _pos = std::copy(s, e, _pos);
            }
            return *this;
        }
        template<typename T>
        typename std::enable_if<std::is_integral<T>::value, cpwriter &>::type write(
                const T & t) {
            T tmp = net::hton(t);
            auto * p = reinterpret_cast<const uint8_t *>(&tmp);
            auto * e = p + sizeof(T);
            write(p, e);
            return *this;
        }
        cpwriter & write(const sstring & s) {
            write(s.begin(), s.end() + 1); // include \0
            return *this;
        }
        cpwriter & put(part_type type, const sstring & s) {
            write(uint16_t(type));
            write(uint16_t(4 + s.size() + 1)); // include \0
            write(s); // include \0
            return *this;
        }
        cpwriter & put_cached(part_type type, const sstring & s) {
            auto & cached = _cache[uint16_t(type)];
            if (cached != s) {
                put(type, s);
                cached = s;
            }
            return *this;
        }
        template<typename T>
        typename std::enable_if<std::is_integral<T>::value, cpwriter &>::type put(
                part_type type, T t) {
            write(uint16_t(type));
            write(uint16_t(4 + sizeof(t)));
            write(t);
            return *this;
        }
        cpwriter & put(part_type type, const value_list & v) {
            auto s = v.size();
            auto sz = 6 + s + s * sizeof(uint64_t);
            if (check(sz)) {
                write(uint16_t(type));
                write(uint16_t(sz));
                write(uint16_t(s));
                v.types(reinterpret_cast<data_type *>(&(*_pos)));
                _pos += s;
                v.values(reinterpret_cast<net::packed<uint64_t> *>(&(*_pos)));
                _pos += s * sizeof(uint64_t);
            }
            return *this;
        }
        cpwriter & put(const sstring & host, const type_instance_id & id) {
            const auto ts = std::chrono::system_clock::now().time_since_epoch();
            const auto lrts =
                    std::chrono::duration_cast<std::chrono::seconds>(ts).count();

            put_cached(part_type::Host, host);
            put(part_type::Time, uint64_t(lrts));
            // Seems hi-res timestamp does not work very well with
            // at the very least my default collectd in fedora (or I did it wrong?)
            // Use lo-res ts for now, it is probably quite sufficient.
            put_cached(part_type::Plugin, id.plugin());
            // Optional
            put_cached(part_type::PluginInst,
                    id.plugin_instance() == per_cpu_plugin_instance ?
                            to_sstring(engine().cpu_id()) : id.plugin_instance());
            put_cached(part_type::Type, id.type());
            // Optional
            put_cached(part_type::TypeInst, id.type_instance());
            return *this;
        }
        cpwriter & put(const sstring & host,
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

        auto ctxt = make_lw_shared<context>();

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
                out.put(_host, _period, i->first, *i->second);
                if (!out) {
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
                    }).then_wrapped([] (auto&& f) {
                        try {
                            f.get();
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
public:
    shared_ptr<value_list> get_values(const type_instance_id & id) {
        return _values[id];
    }

    std::vector<type_instance_id> get_instance_ids() {
        std::vector<type_instance_id> res;
        for (auto i: _values) {
            // Need to check for empty value_list, since unreg is two-stage.
            // Not an issue for most uses, but unit testing etc that would like
            // fully deterministic operation here would like us to only return
            // actually active ids
            if (i.second) {
                res.emplace_back(i.first);
            }
        }
        return res;
    }

private:
    value_list_map _values;
    registrations _regs;
};

impl & get_impl() {
    static thread_local impl per_cpu_instance;
    return per_cpu_instance;
}

void add_polled(const type_instance_id & id,
        const shared_ptr<value_list> & values) {
    get_impl().add_polled(id, values);
}

void remove_polled_metric(const type_instance_id & id) {
    get_impl().remove_polled(id);
}

future<> send_notification(const type_instance_id & id,
        const sstring & msg) {
    return get_impl().send_notification(id, msg);
}

future<> send_metric(const type_instance_id & id,
        const value_list & values) {
    return get_impl().send_metric(id, values);
}

void configure(const boost::program_options::variables_map & opts) {
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

boost::program_options::options_description get_options_description() {
    namespace bpo = boost::program_options;
    bpo::options_description opts("COLLECTD options");
    char hostname[PATH_MAX];
    gethostname(hostname, sizeof(hostname));
    hostname[PATH_MAX-1] = '\0';
    opts.add_options()("collectd", bpo::value<bool>()->default_value(true),
            "enable collectd daemon")("collectd-address",
            bpo::value<std::string>()->default_value("239.192.74.66:25826"),
            "address to send/broadcast metrics to")("collectd-poll-period",
            bpo::value<unsigned>()->default_value(1000),
            "poll period - frequency of sending counter metrics (default: 1000ms, 0 disables)")(
            "collectd-hostname",
            bpo::value<std::string>()->default_value(hostname),
            "collectd host name");
    return opts;
}

std::vector<collectd_value> get_collectd_value(
        const scollectd::type_instance_id& id) {
    std::vector<collectd_value> res_values;
    auto raw_types = get_impl().get_values(id);
    if (raw_types == nullptr) {
        return res_values;
    }
    std::vector<data_type> types(raw_types->size());
    raw_types->types(&(*types.begin()));
    std::vector<net::packed<uint64_t>> value(raw_types->size());
    raw_types->values(&(*value.begin()));

    auto i = value.begin();
    for (auto t : types) {
        collectd_value c(t, be64toh(*i));
        res_values.push_back(c);
    }
    return res_values;
}

std::vector<data_type> get_collectd_types(
        const scollectd::type_instance_id& id) {
    auto res = get_impl().get_values(id);
    if (res == nullptr) {
        return std::vector<data_type>();
    }
    std::vector<data_type> vals(res->size());
    res->types(&(*vals.begin()));
    return vals;
}

std::vector<scollectd::type_instance_id> get_collectd_ids() {
    return get_impl().get_instance_ids();
}
}
