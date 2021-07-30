/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <seastar/net/dns.hh>
#include "controller.hh"
#include "server.hh"
#include "executor.hh"
#include "rmw_operation.hh"
#include "db/config.hh"
#include "cdc/generation_service.hh"
#include "service/memory_limiter.hh"

using namespace seastar;

namespace alternator {

static logging::logger logger("alternator_controller");

controller::controller(sharded<service::storage_proxy>& proxy,
        sharded<service::migration_manager>& mm,
        sharded<db::system_distributed_keyspace>& sys_dist_ks,
        sharded<cdc::generation_service>& cdc_gen_svc,
        sharded<cql3::query_processor>& qp,
        sharded<service::memory_limiter>& memory_limiter,
        const db::config& config)
    : _proxy(proxy)
    , _mm(mm)
    , _sys_dist_ks(sys_dist_ks)
    , _cdc_gen_svc(cdc_gen_svc)
    , _qp(qp)
    , _memory_limiter(memory_limiter)
    , _config(config)
{
}

future<> controller::start() {
    return seastar::async([this] {
        auto preferred = _config.listen_interface_prefer_ipv6() ? std::make_optional(net::inet_address::family::INET6) : std::nullopt;
        auto family = _config.enable_ipv6_dns_lookup() || preferred ? std::nullopt : std::make_optional(net::inet_address::family::INET);

        // Create an smp_service_group to be used for limiting the
        // concurrency when forwarding Alternator request between
        // shards - if necessary for LWT.
        smp_service_group_config c;
        c.max_nonlocal_requests = 5000;
        _ssg = create_smp_service_group(c).get0();

        rmw_operation::set_default_write_isolation(_config.alternator_write_isolation());
        executor::set_default_timeout(std::chrono::milliseconds(_config.alternator_timeout_in_ms()));

        net::inet_address addr;
        try {
            addr = net::dns::get_host_by_name(_config.alternator_address(), family).get0().addr_list.front();
        } catch (...) {
            std::throw_with_nested(std::runtime_error(fmt::format("Unable to resolve alternator_address {}", _config.alternator_address())));
        }

        auto get_cdc_metadata = [] (cdc::generation_service& svc) { return std::ref(svc.get_cdc_metadata()); };

        _executor.start(std::ref(_proxy), std::ref(_mm), std::ref(_sys_dist_ks), sharded_parameter(get_cdc_metadata, std::ref(_cdc_gen_svc)), _ssg.value()).get();
        _server.start(std::ref(_executor), std::ref(_qp), std::ref(_proxy)).get();
        std::optional<uint16_t> alternator_port;
        if (_config.alternator_port()) {
            alternator_port = _config.alternator_port();
        }
        std::optional<uint16_t> alternator_https_port;
        std::optional<tls::credentials_builder> creds;
        if (_config.alternator_https_port()) {
            alternator_https_port = _config.alternator_https_port();
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
            creds->set_dh_level(tls::dh_params::level::MEDIUM);
            auto cert = utils::get_or_default(opts, "certificate", db::config::get_conf_sub("scylla.crt").string());
            auto key = utils::get_or_default(opts, "keyfile", db::config::get_conf_sub("scylla.key").string());
            creds->set_x509_key_file(cert, key, tls::x509_crt_format::PEM).get();
            auto prio = utils::get_or_default(opts, "priority_string", sstring());
            creds->set_priority_string(db::config::default_tls_priority);
            if (!prio.empty()) {
                creds->set_priority_string(prio);
            }
        }
        bool alternator_enforce_authorization = _config.alternator_enforce_authorization();
        _server.invoke_on_all(
                [this, addr, alternator_port, alternator_https_port, creds = std::move(creds), alternator_enforce_authorization] (server& server) mutable {
            return server.init(addr, alternator_port, alternator_https_port, creds, alternator_enforce_authorization,
                    &_memory_limiter.local().get_semaphore(),
                    _config.max_concurrent_requests_per_shard);
        }).then([addr, alternator_port, alternator_https_port] {
            logger.info("Alternator server listening on {}, HTTP port {}, HTTPS port {}",
                    addr, alternator_port ? std::to_string(*alternator_port) : "OFF", alternator_https_port ? std::to_string(*alternator_https_port) : "OFF");
        }).get();
    });
}

future<> controller::stop() {
    return seastar::async([this] {
        _server.stop().get();
        _executor.stop().get();
        destroy_smp_service_group(_ssg.value()).get();
    });
}

}
