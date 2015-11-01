#include "locator/ec2_snitch.hh"

namespace locator {

ec2_snitch::ec2_snitch(const sstring& fname, unsigned io_cpuid) : production_snitch_base(fname) {
    if (engine().cpu_id() == io_cpuid) {
        io_cpu_id() = io_cpuid;
    }
}

/**
 * Read AWS and property file configuration and distribute it among other shards
 *
 * @return
 */
future<> ec2_snitch::load_config() {
    using namespace boost::algorithm;

    if (engine().cpu_id() == io_cpu_id()) {
        return aws_api_call(AWS_QUERY_SERVER_ADDR, ZONE_NAME_QUERY_REQ).then([this](sstring az){
            assert(az.size());

            std::vector<std::string> splits;

            // Split "us-east-1a" or "asia-1a" into "us-east"/"1a" and "asia"/"1a".
            split(splits, az, is_any_of("-"));
            assert(splits.size() > 1);

            _my_rack = splits[splits.size() - 1];

            // hack for CASSANDRA-4026
            _my_dc = az.substr(0, az.size() - 1);
            if (_my_dc[_my_dc.size() - 1] == '1') {
                _my_dc = az.substr(0, az.size() - 3);
            }

            return read_property_file().then([this] (sstring datacenter_suffix) {
                _my_dc += datacenter_suffix;
                logger().info("Ec2Snitch using region: {}, zone: {}.", _my_dc, _my_rack);

                return _my_distributed->invoke_on_all(
                    [this] (snitch_ptr& local_s) {

                    // Distribute the new values on all CPUs but the current one
                    if (engine().cpu_id() != io_cpu_id()) {
                        local_s->set_my_dc(_my_dc);
                        local_s->set_my_rack(_my_rack);
                    }
                });
            });
        });
    }

    return make_ready_future<>();
}

future<> ec2_snitch::start() {
    _state = snitch_state::initializing;

    return load_config().then([this] {
        set_snitch_ready();
    });
}

future<sstring> ec2_snitch::aws_api_call(sstring addr, sstring cmd) {
    return engine().net().connect(make_ipv4_address(ipv4_addr{addr}))
    .then([this, addr, cmd] (connected_socket fd) {
        _sd = std::move(fd);
        _in = std::move(_sd.input());
        _out = std::move(_sd.output());
        _zone_req = sstring("GET ") + cmd +
                    sstring(" HTTP/1.1\r\nHost: ") +addr +
                    sstring("\r\n\r\n");

        return _out.write(_zone_req.c_str()).then([this] {
            return _out.flush();
        });
    }).then([this] {
        _parser.init();
        return _in.consume(_parser).then([this] {
            if (_parser.eof()) {
                return make_exception_future<sstring>("Bad HTTP response");
            }

            // Read HTTP response header first
            auto _rsp = _parser.get_parsed_response();
            auto it = _rsp->_headers.find("Content-Length");
            if (it == _rsp->_headers.end()) {
                return make_exception_future<sstring>("Error: HTTP response does not contain: Content-Length\n");
            }

            auto content_len = std::stoi(it->second);

            // Read HTTP response body
            return _in.read_exactly(content_len).then([this] (temporary_buffer<char> buf) {
                sstring res(buf.get(), buf.size());

                return make_ready_future<sstring>(std::move(res));
            });
        });
    });
}

future<sstring> ec2_snitch::read_property_file() {
    return load_property_file().then([this] {
        sstring dc_suffix;

        if (_prop_values.count(dc_suffix_property_key)) {
            dc_suffix = _prop_values[dc_suffix_property_key];
        }

        return dc_suffix;
    });
}

using registry_2_params = class_registrator<i_endpoint_snitch, ec2_snitch, const sstring&, unsigned>;
static registry_2_params registrator2("org.apache.cassandra.locator.Ec2Snitch");
static registry_2_params registrator2_short_name("Ec2Snitch");


using registry_1_param = class_registrator<i_endpoint_snitch, ec2_snitch, const sstring&>;
static registry_1_param registrator1("org.apache.cassandra.locator.Ec2Snitch");
static registry_1_param registrator1_short_name("Ec2Snitch");

using registry_default = class_registrator<i_endpoint_snitch, ec2_snitch>;
static registry_default registrator_default("org.apache.cassandra.locator.Ec2Snitch");
static registry_default registrator_default_short_name("Ec2Snitch");
} // namespace locator
