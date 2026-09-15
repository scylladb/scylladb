/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <ranges>
#include <fmt/ranges.h>
#include <seastar/core/with_scheduling_group.hh>
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
#include "vector_search/vector_store_client.hh"

using namespace seastar;

namespace alternator {

static logging::logger logger("alternator_controller");

controller::controller(
        sharded<gms::gossiper>& gossiper,
        sharded<service::storage_proxy>& proxy,
        sharded<service::storage_service>& ss,
        sharded<service::migration_manager>& mm,
        sharded<db::system_distributed_keyspace>& sys_dist_ks,
        sharded<db::system_keyspace>& sys_ks,
        sharded<cdc::generation_service>& cdc_gen_svc,
        sharded<service::memory_limiter>& memory_limiter,
        sharded<auth::service>& auth_service,
        sharded<qos::service_level_controller>& sl_controller,
        sharded<vector_search::vector_store_client>& vsc,
        sharded<updateable_timeout_config>& timeout_config,
        const db::config& config,
        db::listener_configs listeners,
        seastar::scheduling_group sg)
    : protocol_server(sg)
    , _gossiper(gossiper)
    , _proxy(proxy)
    , _ss(ss)
    , _mm(mm)
    , _sys_dist_ks(sys_dist_ks)
    , _sys_ks(sys_ks)
    , _cdc_gen_svc(cdc_gen_svc)
    , _memory_limiter(memory_limiter)
    , _auth_service(auth_service)
    , _sl_controller(sl_controller)
    , _vsc(vsc)
    , _timeout_config(timeout_config)
    , _config(config)
    , _listeners(std::move(listeners))
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

// The TLS options of a listener, with the empty set meaning the
// Alternator-wide defaults.
std::unordered_map<sstring, sstring> controller::listener_tls_options(const db::listener_config& listener) const {
    if (!listener.tls_options.empty()) {
        return listener.tls_options;
    }
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
            // server_encryption_options is for internode encryption.
            // Its require_client_auth and truststore settings must not
            // bleed into Alternator's client-facing HTTPS endpoint.
            opts.erase("require_client_auth");
            opts.erase("truststore");
        }
    }
    // "enabled" is the legacy on/off switch of alternator_encryption_options
    // (and of server_encryption_options), which a listener replaces with its
    // own "tls" key. It is meaningless once TLS was selected per listener, and
    // configure_tls_creds_builder() doesn't know it, so drop it.
    opts.erase("enabled");
    return opts;
}


// Work out what it takes to serve a set of listeners: the address of each resolved, and
// one set of credentials per distinct set of TLS options. Everything that can fail
// happens here, so a set that cannot be served is rejected before any socket is touched.
controller::resolved_listeners controller::resolve_listeners(const db::listener_configs& listeners) const {
    auto preferred = _config.listen_interface_prefer_ipv6() ? std::make_optional(net::inet_address::family::INET6) : std::nullopt;
    auto family = _config.enable_ipv6_dns_lookup() || preferred ? std::nullopt : std::make_optional(net::inet_address::family::INET);

    resolved_listeners resolved;
    // Listeners sharing the same address, or the same set of TLS options, share the
    // resolved address and the credentials built out of those options.
    std::unordered_map<sstring, net::inet_address> resolved_addresses;

    for (auto& [name, l] : listeners) {
        auto addr_name = l.address.empty() ? _config.alternator_address() : l.address;
        auto [addr_it, inserted] = resolved_addresses.try_emplace(addr_name);
        if (inserted) {
            addr_it->second = utils::resolve_listen_address(addr_name, "listener address", family, preferred).get();
        }

        std::optional<size_t> credentials_idx;
        if (l.tls) {
            auto opts = listener_tls_options(l);
            auto it = std::ranges::find(resolved.tls_options, opts);
            credentials_idx = it - resolved.tls_options.begin();
            if (it == resolved.tls_options.end()) {
                resolved.tls_options.push_back(opts);
                utils::configure_tls_creds_builder(resolved.credentials.emplace_back(), std::move(opts)).get();
            }
        }

        resolved.listeners.push_back(server::listener{
            .addr = socket_address{addr_it->second, l.port},
            .proxy_protocol = l.proxy_protocol,
            .credentials_idx = credentials_idx,
        });
    }
    return resolved;
}

future<> controller::apply_listeners(db::listener_configs listeners) {
    auto units = co_await get_units(_ops_sem, 1);

    if (!_serving) {
        // Nothing is being served, so there is nothing to reconcile: remember the set. If
        // the server is meant to run, it was only waiting for something to listen on, so
        // start it now; otherwise start_server() opens the set when it runs.
        auto previous = std::exchange(_listeners, std::move(listeners));
        if (_wanted) {
            try {
                co_await do_start_server(true);
            } catch (...) {
                _listeners = std::move(previous);
                throw;
            }
        }
        co_return;
    }

    if (listeners.empty()) {
        // Don't keep an Alternator server around that nobody can reach. It is still
        // wanted, and starts again with the next set that has a listener in it.
        logger.info("No Alternator listeners are left, stopping the Alternator server");
        _listeners = std::move(listeners);
        co_await do_stop_server();
        co_return;
    }

    // resolve_listeners() blocks on futures, so it needs a thread of its own.
    auto resolved = co_await seastar::async([this, &listeners] { return resolve_listeners(listeners); });

    auto failed = co_await reconcile(resolved);
    _listeners = std::move(listeners);
    if (_listen_addresses.empty()) {
        logger.error("None of the Alternator listeners could be opened, stopping the Alternator server");
        co_await do_stop_server();
        co_return;
    }
    if (!failed.empty()) {
        logger.error("Could not open the Alternator listeners on {}; serving the others", failed);
    }
    logger.info("Alternator server listening on {}", _listen_addresses);
}

// Have every shard serve `resolved`, as far as it can be: a listener that cannot be opened
// is reported and left out, while the others are opened regardless. Undoing the change
// instead would not help - the listeners closed on the way have already dropped their
// connections. Returns the addresses that could not be listened on.
future<std::vector<socket_address>> controller::reconcile(const resolved_listeners& resolved) {
    auto per_shard = co_await _server.map([&resolved] (server& s) {
        return s.apply_listeners(resolved.listeners, resolved.credentials, resolved.tls_options);
    });

    std::vector<socket_address> failed;
    for (const auto& failures : per_shard) {
        for (const auto& f : failures) {
            if (std::ranges::find(failed, f.addr) == failed.end()) {
                logger.error("Failed to listen for Alternator clients on {}: {}", f.addr, f.error);
                failed.push_back(f.addr);
            }
        }
    }
    if (!failed.empty()) {
        // A listener is served on every shard or on none: close it on the shards that
        // did manage to open it.
        co_await _server.invoke_on_all([&failed] (server& s) {
            return s.stop_listening(failed);
        });
    }

    _listen_addresses.clear();
    for (const auto& l : resolved.listeners) {
        if (std::ranges::find(failed, l.addr) == failed.end()) {
            _listen_addresses.push_back(l.addr);
        }
    }
    co_return failed;
}

future<> controller::start_server() {
    auto units = co_await get_units(_ops_sem, 1);
    _wanted = true;
    co_await do_start_server(_started_before);
}

future<> controller::do_start_server(bool best_effort) {
    if (_serving) {
        return make_ready_future<>();
    }
    if (_listeners.empty()) {
        logger.info("No Alternator listeners are configured, the Alternator server will start when one is");
        return make_ready_future<>();
    }

    seastar::thread_attributes attr;
    attr.sched_group = _sched_group;
    return seastar::async(std::move(attr), [this, best_effort] {
        _listen_addresses.clear();

        // Create an smp_service_group to be used for limiting the
        // concurrency when forwarding Alternator request between
        // shards - if necessary for LWT.
        smp_service_group_config c;
        c.max_nonlocal_requests = 5000;
        _ssg = create_smp_service_group(c).get();

        rmw_operation::set_default_write_isolation(_config.alternator_write_isolation());

        auto get_cdc_metadata = [] (cdc::generation_service& svc) { return std::ref(svc.get_cdc_metadata()); };
        auto get_timeout_in_ms = [] (const db::config& cfg) -> utils::updateable_value<uint32_t> {
            return cfg.alternator_timeout_in_ms;
        };
        _executor.start(std::ref(_gossiper), std::ref(_proxy), std::ref(_ss), std::ref(_mm), std::ref(_sys_dist_ks), std::ref(_sys_ks),
                        sharded_parameter(get_cdc_metadata, std::ref(_cdc_gen_svc)), std::ref(_vsc), _ssg.value(),
                        sharded_parameter(get_timeout_in_ms, std::ref(_config))).get();
        _server.start(std::ref(_executor), std::ref(_proxy), std::ref(_gossiper), std::ref(_auth_service), std::ref(_sl_controller), std::ref(_timeout_config)).get();
        // Note: from this point on, if start_server() throws for any reason,
        // it must first call do_stop_server() to stop the executor and server
        // services we just started - or Scylla will cause an assertion
        // failure when the controller object is destroyed in the exception
        // unwinding.

        resolved_listeners resolved;
        try {
            resolved = resolve_listeners(_listeners);
        } catch (...) {
            logger.error("Failed to set up Alternator listeners: {:t}", std::current_exception());
            do_stop_server().get();
            std::throw_with_nested(std::runtime_error("Failed to set up Alternator listeners"));
        }
        _server.invoke_on_all([this] (server& server) mutable {
            return server.init(_config.alternator_enforce_authorization,
                    _config.alternator_warn_authorization,
                    _config.alternator_max_users_query_size_in_trace_output,
                    &_memory_limiter.local().get_semaphore(),
                    _config.max_concurrent_requests_per_shard);
        }).then([this, &resolved] {
            return reconcile(resolved);
        }).then([this, best_effort] (std::vector<socket_address> failed) {
            // Starting for the first time, with the node, any listener that cannot be
            // opened fails the start, so that the operator finds out right away. Starting
            // again, or because a change gave the server its first listeners, it is enough
            // that some could be opened.
            if (_listen_addresses.empty() || (!best_effort && !failed.empty())) {
                throw std::runtime_error(fmt::format("Could not listen for Alternator clients on {}", failed));
            }
            if (!failed.empty()) {
                logger.error("Could not open the Alternator listeners on {}; serving the others", failed);
            }
        }).handle_exception([this] (std::exception_ptr ep) {
            logger.error("Failed to set up Alternator HTTP server on {}: {}", _listeners, ep);
            return do_stop_server().then([ep = std::move(ep)] { return make_exception_future<>(ep); });
        }).then([this] {
            _serving = true;
            _started_before = true;
            logger.info("Alternator server listening on {}", _listen_addresses);
        }).get();
    });
}

future<> controller::stop_server() {
    auto units = co_await get_units(_ops_sem, 1);
    _wanted = false;
    co_await do_stop_server();
}

// Safe to call when nothing is running, and leaves the controller able to start the
// server again: the listeners can take it down and bring it back.
future<> controller::do_stop_server() {
    return seastar::async([this] {
        if (!_ssg) {
            return;
        }
        _serving = false;
        _server.stop().get();
        _executor.stop().get();
        _listen_addresses.clear();
        destroy_smp_service_group(std::exchange(_ssg, std::nullopt).value()).get();
    });
}

future<> controller::request_stop_server() {
    return with_scheduling_group(_sched_group, [this] {
        return stop_server();
    });
}

future<utils::chunked_vector<foreign_ptr<std::unique_ptr<client_data>>>> controller::get_client_data() {
    // The server is not started while there is nothing for it to listen on.
    return _serving ? _server.local().get_client_data() : protocol_server::get_client_data();
}

}
