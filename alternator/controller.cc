/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/net/dns.hh>
#include "controller.hh"
#include "server.hh"
#include "executor.hh"
#include "rmw_operation.hh"
#include "db/config.hh"
#include "cdc/generation_service.hh"
#include "service/memory_limiter.hh"
#include "auth/service.hh"
#include "service/qos/service_level_controller.hh"

using namespace seastar;

namespace alternator {

static logging::logger logger("alternator_controller");

controller::controller(
        sharded<gms::gossiper>& gossiper,
        sharded<service::storage_proxy>& proxy,
        sharded<service::migration_manager>& mm,
        sharded<db::system_distributed_keyspace>& sys_dist_ks,
        sharded<cdc::generation_service>& cdc_gen_svc,
        sharded<service::memory_limiter>& memory_limiter,
        sharded<auth::service>& auth_service,
        sharded<qos::service_level_controller>& sl_controller,
        const db::config& config,
        seastar::scheduling_group sg)
    : protocol_server(sg)
    , _gossiper(gossiper)
    , _proxy(proxy)
    , _mm(mm)
    , _sys_dist_ks(sys_dist_ks)
    , _cdc_gen_svc(cdc_gen_svc)
    , _memory_limiter(memory_limiter)
    , _auth_service(auth_service)
    , _sl_controller(sl_controller)
    , _config(config)
{
}

sstring controller::name() const {
    return "alternator";
}

sstring controller::protocol() const {
    return "dynamodb";
}

sstring controller::protocol_version() const {
    return version;
}

std::vector<socket_address> controller::listen_addresses() const {
    return _listen_addresses;
}

future<> controller::start_server() {
    seastar::thread_attributes attr;
    attr.sched_group = _sched_group;
    return seastar::async(std::move(attr), [this] {
        _listen_addresses.clear();

        auto preferred = _config.listen_interface_prefer_ipv6() ? std::make_optional(net::inet_address::family::INET6) : std::nullopt;
        auto family = _config.enable_ipv6_dns_lookup() || preferred ? std::nullopt : std::make_optional(net::inet_address::family::INET);

        // Create an smp_service_group to be used for limiting the
        // concurrency when forwarding Alternator request between
        // shards - if necessary for LWT.
        smp_service_group_config c;
        c.max_nonlocal_requests = 5000;
        _ssg = create_smp_service_group(c).get();

        rmw_operation::set_default_write_isolation(_config.alternator_write_isolation());

        net::inet_address addr = utils::resolve(_config.alternator_address, family).get();

        auto get_cdc_metadata = [] (cdc::generation_service& svc) { return std::ref(svc.get_cdc_metadata()); };
        auto get_timeout_in_ms = [] (const db::config& cfg) -> utils::updateable_value<uint32_t> {
            return cfg.alternator_timeout_in_ms;
        };
        _executor.start(std::ref(_gossiper), std::ref(_proxy), std::ref(_mm), std::ref(_sys_dist_ks),
                        sharded_parameter(get_cdc_metadata, std::ref(_cdc_gen_svc)), _ssg.value(),
                        sharded_parameter(get_timeout_in_ms, std::ref(_config))).get();
        _server.start(std::ref(_executor), std::ref(_proxy), std::ref(_gossiper), std::ref(_auth_service), std::ref(_sl_controller)).get();
        // Note: from this point on, if start_server() throws for any reason,
        // it must first call stop_server() to stop the executor and server
        // services we just started - or Scylla will cause an assertion
        // failure when the controller object is destroyed in the exception
        // unwinding.
        std::optional<uint16_t> alternator_port;
        if (_config.alternator_port()) {
            alternator_port = _config.alternator_port();
            _listen_addresses.push_back({addr, *alternator_port});
        }
        std::optional<uint16_t> alternator_https_port;
        std::optional<tls::credentials_builder> creds;
        if (_config.alternator_https_port()) {
            alternator_https_port = _config.alternator_https_port();
            _listen_addresses.push_back({addr, *alternator_https_port});
            creds.emplace();
            auto opts = _config.alternator_encryption_options();
            if (opts.empty()) {
                // Earlier versions mistakenly configured Alternator's
                // HTTPS parameters via the "server_encryption_option"
                // configuration parameter. We *temporarily* continue
                // to allow this, for backward compatibility.
                opts = _config.server_encryption_options();
                if (!opts.empty()) {
                logger.warn("Setting server_encryption_options to configure "
                        "Alternator's HTTPS encryption is deprecated. Please "
                        "switch to setting alternator_encryption_options instead.");
                }
            }
            opts.erase("require_client_auth");
            opts.erase("truststore");
            try {
                utils::configure_tls_creds_builder(creds.value(), std::move(opts)).get();
            } catch(...) {
                logger.error("Failed to set up Alternator TLS credentials: {}", std::current_exception());
                stop_server().get();
                std::throw_with_nested(std::runtime_error("Failed to set up Alternator TLS credentials"));
            }
        }
        bool alternator_enforce_authorization = _config.alternator_enforce_authorization();
        _server.invoke_on_all(
                [this, addr, alternator_port, alternator_https_port, creds = std::move(creds), alternator_enforce_authorization] (server& server) mutable {
            return server.init(addr, alternator_port, alternator_https_port, creds, alternator_enforce_authorization,
                    &_memory_limiter.local().get_semaphore(),
                    _config.max_concurrent_requests_per_shard);
        }).handle_exception([this, addr, alternator_port, alternator_https_port] (std::exception_ptr ep) {
            logger.error("Failed to set up Alternator HTTP server on {} port {}, TLS port {}: {}",
                    addr, alternator_port ? std::to_string(*alternator_port) : "OFF", alternator_https_port ? std::to_string(*alternator_https_port) : "OFF", ep);
            return stop_server().then([ep = std::move(ep)] { return make_exception_future<>(ep); });
        }).then([addr, alternator_port, alternator_https_port] {
            logger.info("Alternator server listening on {}, HTTP port {}, HTTPS port {}",
                    addr, alternator_port ? std::to_string(*alternator_port) : "OFF", alternator_https_port ? std::to_string(*alternator_https_port) : "OFF");
        }).get();
    });
}

future<> controller::stop_server() {
    return seastar::async([this] {
        if (!_ssg) {
            return;
        }
        _server.stop().get();
        _executor.stop().get();
        _listen_addresses.clear();
        destroy_smp_service_group(_ssg.value()).get();
    });
}

future<> controller::request_stop_server() {
    return with_scheduling_group(_sched_group, [this] {
        return stop_server();
    });
}

}
