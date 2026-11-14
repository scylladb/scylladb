/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <seastar/testing/perf_tests.hh>
#include <seastar/http/reply.hh>

#include "alternator/executor_util.hh"
#include "locator/abstract_replication_strategy.hh"
#include "locator/token_metadata.hh"
#include "locator/tablets.hh"
#include "service/strong_consistency/serving_set.hh"

struct routing_header {
    alternator::tablet_routing_response response;

    explicit routing_header(size_t rf) {
        response.info.token_range = {dht::token{-1000}, dht::token{2000}};
        response.info.hash = locator::tablet_version{0x123456789abcdef0};
        for (size_t i = 0; i < rf; ++i) {
            response.info.tablet_replicas.push_back({locator::host_id(utils::UUID(0, i + 1)), unsigned(i)});
        }
    }

    void format() const {
        perf_tests::do_not_optimize(alternator::format_tablet_routing_info(response));
    }

    void attach() const {
        seastar::http::reply reply;
        alternator::add_tablet_routing_header(reply, response);
        perf_tests::do_not_optimize(reply);
    }
};

struct routing_header_rf1 : routing_header { routing_header_rf1() : routing_header(1) {} };
struct routing_header_rf3 : routing_header { routing_header_rf3() : routing_header(3) {} };
struct routing_header_rf5 : routing_header { routing_header_rf5() : routing_header(5) {} };

PERF_TEST_F(routing_header_rf1, format) { format(); }
PERF_TEST_F(routing_header_rf3, format) { format(); }
PERF_TEST_F(routing_header_rf5, format) { format(); }
PERF_TEST_F(routing_header_rf1, attach) { attach(); }
PERF_TEST_F(routing_header_rf3, attach) { attach(); }
PERF_TEST_F(routing_header_rf5, attach) { attach(); }

struct regular_routing {
    semaphore sem{1};
    std::optional<locator::shared_token_metadata> stm;
    locator::replication_strategy_ptr strategy;
    locator::effective_replication_map_ptr erm;
    std::vector<std::pair<dht::token, locator::tablet_version_block>> requests;
    size_t next = 0;
    locator::host_id leader{utils::UUID(0, 2)};
    const locator::tablet_map* tablet_map = nullptr;

    ~regular_routing() {
        // The performance runner destroys fixtures in a Seastar thread.
        // Drain snapshot disposal before destroying its shared owner.
        erm = nullptr;
        stm->stop().get();
    }

    regular_routing(size_t rf, size_t working_set) {
        locator::token_metadata::config cfg;
        cfg.topo_cfg.this_host_id = locator::host_id(utils::UUID(0, 1));
        cfg.topo_cfg.this_endpoint = gms::inet_address("127.0.0.1");
        cfg.topo_cfg.this_cql_address = cfg.topo_cfg.this_endpoint;
        cfg.topo_cfg.local_dc_rack = {"dc1", "rack1"};
        stm.emplace([this] () noexcept { return get_units(sem, 1); }, cfg);
        const auto table = table_id(utils::UUID(0, 100));
        auto tm = stm->make_token_metadata_ptr();
        locator::tablet_replica_set replicas;
        for (size_t i = rf; i > 0; --i) {
            const auto host = locator::host_id(utils::UUID(0, i));
            tm->update_topology(host, locator::endpoint_dc_rack{"dc1", "rack1"}, locator::node::state::normal);
            replicas.push_back({host, unsigned(i)});
        }
        locator::tablet_map tablets(dht::get_uniform_tokens(512));
        for (auto tid : tablets.tablet_ids()) {
            tablets.set_tablet(tid, locator::tablet_info{replicas});
        }
        tm->tablets().set_tablet_map(table, std::move(tablets));
        strategy = locator::abstract_replication_strategy::create_replication_strategy("NetworkTopologyStrategy",
                locator::replication_strategy_params({{"dc1", sstring(std::to_string(rf))}}, 0, std::nullopt),
                tm->get_topology());
        erm = strategy->maybe_as_per_table()->make_replication_map(table, std::move(tm));
        const auto& map = erm->get_token_metadata().tablets().get_tablet_map(table);
        tablet_map = &map;
        auto info = erm->check_tablet_version(map.get_last_token(map.first_tablet()), std::nullopt);
        const auto block = locator::tablet_version_block{info->hash.value() & 0xf};
        for (size_t i = 0; i < working_set; ++i) {
            requests.emplace_back(map.get_last_token(locator::tablet_id(i)), block);
        }
        for (const auto& [token, block] : requests) {
            erm->check_tablet_version(token, block);
        }
    }

    void check() {
        const auto& [token, block] = requests[next];
        if (++next == requests.size()) {
            next = 0;
        }
        perf_tests::do_not_optimize(erm->check_tablet_version(token, block));
    }

    // Reference cost of recomputing each version while reusing replica storage.
    void check_uncached() {
        const auto& [token, block] = requests[next];
        if (++next == requests.size()) {
            next = 0;
        }
        static thread_local locator::tablet_replica_set replicas;
        replicas = tablet_map->get_tablet_info(tablet_map->get_tablet_id(token)).replicas;
        std::ranges::sort(replicas);
        perf_tests::do_not_optimize(locator::compare_tablet_version_block(locator::internal::hash_replica_list(replicas), block));
    }

    void check_sc() {
        const auto& [token, block] = requests[next];
        if (++next == requests.size()) {
            next = 0;
        }
        const auto tid = tablet_map->get_tablet_id(token);
        const auto serving = service::strong_consistency::replica_set_view(tablet_map->get_tablet_info(tid).replicas);
        perf_tests::do_not_optimize(service::strong_consistency::make_tablet_routing_info_v2(
                *tablet_map, tid, serving, leader, sc_block));
    }

    locator::tablet_version_block sc_block{0};
    void warm_sc() {
        const auto tid = tablet_map->first_tablet();
        const auto info = service::strong_consistency::make_tablet_routing_info_v2(*tablet_map, tid,
                service::strong_consistency::replica_set_view(tablet_map->get_tablet_info(tid).replicas), leader, std::nullopt);
        sc_block = locator::tablet_version_block{info->hash.value() & 0xf};
        for (size_t i = 0; i < requests.size(); ++i) {
            check_sc();
        }
    }
};

struct regular_routing_rf3_hot : regular_routing { regular_routing_rf3_hot() : regular_routing(3, 64) {} };
struct regular_routing_rf3_128 : regular_routing { regular_routing_rf3_128() : regular_routing(3, 128) {} };
struct regular_routing_rf3_129 : regular_routing { regular_routing_rf3_129() : regular_routing(3, 129) {} };
struct regular_routing_rf3_wide : regular_routing { regular_routing_rf3_wide() : regular_routing(3, 512) {} };
struct regular_routing_rf5_hot : regular_routing { regular_routing_rf5_hot() : regular_routing(5, 64) {} };
struct regular_routing_rf5_128 : regular_routing { regular_routing_rf5_128() : regular_routing(5, 128) {} };
struct regular_routing_rf5_129 : regular_routing { regular_routing_rf5_129() : regular_routing(5, 129) {} };
struct regular_routing_rf5_wide : regular_routing { regular_routing_rf5_wide() : regular_routing(5, 512) {} };

PERF_TEST_F(regular_routing_rf3_hot, matching) { check(); }
PERF_TEST_F(regular_routing_rf3_128, matching) { check(); }
PERF_TEST_F(regular_routing_rf3_129, matching) { check(); }
PERF_TEST_F(regular_routing_rf3_wide, matching) { check(); }
PERF_TEST_F(regular_routing_rf5_hot, matching) { check(); }
PERF_TEST_F(regular_routing_rf5_128, matching) { check(); }
PERF_TEST_F(regular_routing_rf5_129, matching) { check(); }
PERF_TEST_F(regular_routing_rf5_wide, matching) { check(); }
PERF_TEST_F(regular_routing_rf3_hot, uncached) { check_uncached(); }
PERF_TEST_F(regular_routing_rf3_wide, uncached) { check_uncached(); }
PERF_TEST_F(regular_routing_rf5_hot, uncached) { check_uncached(); }
PERF_TEST_F(regular_routing_rf5_wide, uncached) { check_uncached(); }

struct sc_routing_rf3_hot : regular_routing { sc_routing_rf3_hot() : regular_routing(3, 64) { warm_sc(); } };
struct sc_routing_rf3_129 : regular_routing { sc_routing_rf3_129() : regular_routing(3, 129) { warm_sc(); } };
struct sc_routing_rf5_hot : regular_routing { sc_routing_rf5_hot() : regular_routing(5, 64) { warm_sc(); } };
struct sc_routing_rf5_129 : regular_routing { sc_routing_rf5_129() : regular_routing(5, 129) { warm_sc(); } };

PERF_TEST_F(sc_routing_rf3_hot, matching) { check_sc(); }
PERF_TEST_F(sc_routing_rf3_129, matching) { check_sc(); }
PERF_TEST_F(sc_routing_rf5_hot, matching) { check_sc(); }
PERF_TEST_F(sc_routing_rf5_129, matching) { check_sc(); }
