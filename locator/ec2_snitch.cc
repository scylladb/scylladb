#include "locator/ec2_snitch.hh"
#include <seastar/core/seastar.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/do_with.hh>
#include <seastar/http/reply.hh>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

#include "utils/assert.hh"
#include "utils/class_registrator.hh"

namespace locator {

ec2_snitch::ec2_snitch(const snitch_config& cfg) : production_snitch_base(cfg) {
    if (this_shard_id() == cfg.io_cpu_id) {
        io_cpu_id() = cfg.io_cpu_id;
    }
}

/**
 * Read AWS and property file configuration and distribute it among other shards
 *
 * @return
 */
future<> ec2_snitch::load_config(bool prefer_local) {
    using namespace boost::algorithm;

    if (this_shard_id() == io_cpu_id()) {
        auto token = co_await aws_api_call(AWS_QUERY_SERVER_ADDR, AWS_QUERY_SERVER_PORT, TOKEN_REQ_ENDPOINT, std::nullopt);
        auto az = co_await aws_api_call(AWS_QUERY_SERVER_ADDR, AWS_QUERY_SERVER_PORT, ZONE_NAME_QUERY_REQ, token);
        SCYLLA_ASSERT(az.size());

        std::vector<std::string> splits;

        // Split "us-east-1a" or "asia-1a" into "us-east"/"1a" and "asia"/"1a".
        split(splits, az, is_any_of("-"));
        SCYLLA_ASSERT(splits.size() > 1);

        sstring my_rack = splits[splits.size() - 1];

        // hack for CASSANDRA-4026
        sstring my_dc = az.substr(0, az.size() - 1);
        if (my_dc[my_dc.size() - 1] == '1') {
            my_dc = az.substr(0, az.size() - 3);
        }

        auto datacenter_suffix = co_await read_property_file();
        my_dc += datacenter_suffix;
        logger().info("Ec2Snitch using region: {}, zone: {}.", my_dc, my_rack);
        co_await container().invoke_on_all([prefer_local, my_dc, my_rack] (snitch_ptr& local_s) {
            local_s->set_my_dc_and_rack(my_dc, my_rack);
            local_s->set_prefer_local(prefer_local);
        });
    }
}

future<> ec2_snitch::start() {
    _state = snitch_state::initializing;

    return load_config(false).then([this] {
        set_snitch_ready();
    });
}

future<sstring> ec2_snitch::aws_api_call(sstring addr, uint16_t port, sstring cmd, std::optional<sstring> token) {
    return do_with(int(0), [this, addr, port, cmd, token] (int& i) {
        return repeat_until_value([this, addr, port, cmd, token, &i]() -> future<std::optional<sstring>> {
            ++i;
            return aws_api_call_once(addr, port, cmd, token).then([] (auto res) {
                return make_ready_future<std::optional<sstring>>(std::move(res));
            }).handle_exception([this, &i] (auto ep) {
                try {
                    std::rethrow_exception(ep);
                } catch (const std::system_error &e) {
                    if (i >= AWS_API_CALL_RETRIES - 1) {
                        logger().error("EC2 API call failed: {}. Maximum number of retries exceeded", e.what());
                        throw e;
                    } else {
                        logger().error("EC2 API call failed: {}. Will retry in {} seconds", e.what(), std::chrono::duration_cast<std::chrono::seconds>(_ec2_api_retry.sleep_time()).count());
                    }
                }
                return _ec2_api_retry.retry().then([] {
                    return make_ready_future<std::optional<sstring>>(std::nullopt);
                });
            });
        });
    });
}

future<sstring> ec2_snitch::aws_api_call_once(sstring addr, uint16_t port, sstring cmd, std::optional<sstring> token) {
    return connect(socket_address(inet_address{addr}, port))
    .then([this, addr, cmd, token] (connected_socket fd) {
        _sd = std::move(fd);
        _in = _sd.input();
        _out = _sd.output();

        if (token) {
            _req = sstring("GET ") + cmd +
                   sstring(" HTTP/1.1\r\nHost: ") +addr +
                   sstring("\r\nX-aws-ec2-metadata-token: ") + *token +
                   sstring("\r\n\r\n");
        } else {
            _req = sstring("PUT ") + cmd +
                   sstring(" HTTP/1.1\r\nHost: ") + addr +
                   sstring("\r\nX-aws-ec2-metadata-token-ttl-seconds: 60") +
                   sstring("\r\n\r\n");
        }

        return _out.write(_req.c_str()).then([this] {
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
            auto rc = _rsp->_status;
            // Verify EC2 instance metadata access
            if (rc == http::reply::status_type(403)) {
                return make_exception_future<sstring>(std::runtime_error("Error: Unauthorized response received when trying to communicate with instance metadata service."));
            }
            if (_rsp->_status != http::reply::status_type::ok) {
                return make_exception_future<sstring>(std::runtime_error(format("Error: HTTP response status {}", _rsp->_status)));
            }

            auto it = _rsp->_headers.find("Content-Length");
            if (it == _rsp->_headers.end()) {
                return make_exception_future<sstring>("Error: HTTP response does not contain: Content-Length\n");
            }

            auto content_len = std::stoi(it->second);

            // Read HTTP response body
            return _in.read_exactly(content_len).then([] (temporary_buffer<char> buf) {
                sstring res(buf.get(), buf.size());

                return make_ready_future<sstring>(std::move(res));
            });
        });
    });
}

future<sstring> ec2_snitch::read_property_file() {
    return load_property_file().then([this] {
        sstring dc_suffix;

        if (_prop_values.contains(dc_suffix_property_key)) {
            dc_suffix = _prop_values[dc_suffix_property_key];
        }

        return dc_suffix;
    });
}

using registry_default = class_registrator<i_endpoint_snitch, ec2_snitch, const snitch_config&>;
static registry_default registrator_default("org.apache.cassandra.locator.Ec2Snitch");
static registry_default registrator_default_short_name("Ec2Snitch");
} // namespace locator
