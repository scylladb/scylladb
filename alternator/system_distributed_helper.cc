/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "alternator/system_distributed_helper.hh"

#include <stdexcept>

#include <seastar/core/coroutine.hh>

#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "db/consistency_level_type.hh"
#include "db/system_distributed_keyspace.hh"
#include "locator/token_metadata.hh"
#include "service/client_state.hh"
#include "service/query_state.hh"
#include "service/storage_proxy.hh"
#include "service_permit.hh"
#include "timeout_config.hh"

namespace alternator {

// system_distributed uses SimpleStrategy with RF=3, but in a cluster with a
// single token owner there is only one replica, so QUORUM (computed from RF)
// can never be reached. Use ONE in that case, like the rest of
// system_distributed_keyspace does (see quorum_if_many()).
static db::consistency_level get_consistency_level(cql3::query_processor& qp) {
    return qp.proxy().get_token_metadata_ptr()->count_normal_token_owners() > 1
            ? db::consistency_level::QUORUM : db::consistency_level::ONE;
}

// The 10 s timeout system_distributed_keyspace reads its tables with, rather than the hour an
// internal query gets by default.
static service::query_state& internal_distributed_query_state() {
    using namespace std::chrono_literals;
    const auto t = 10s;
    static timeout_config tc{ t, t, t, t, t, t, t };
    static thread_local service::client_state cs(service::client_state::internal_tag{}, tc);
    static thread_local service::query_state qs(cs, empty_service_permit());
    return qs;
}

future<std::optional<export_row>> get_export(cql3::query_processor& qp, sstring export_arn) {
    using sdks = db::system_distributed_keyspace;
    auto rs = co_await qp.execute_internal(
            format("SELECT * FROM {}.{} WHERE export_arn = ?", sdks::NAME, sdks::ALTERNATOR_EXPORT_TO_S3_EXPORTS),
            get_consistency_level(qp),
            internal_distributed_query_state(),
            { export_arn },
            cql3::query_processor::cache_internal::yes);

    if (rs->empty()) {
        co_return std::nullopt;
    }
    const auto& row = rs->one();
    // The statement creating the export row must fill all of these in, so a row missing any of them
    // is corrupt.
    for (auto column : {"client_token", "request", "export_status", "table_id", "export_time", "accepted_at"}) {
        if (!row.has(column)) {
            throw std::runtime_error(format("Export '{}' has no {} in {}.{}", export_arn, column, sdks::NAME, sdks::ALTERNATOR_EXPORT_TO_S3_EXPORTS));
        }
    }
    co_return export_row {
        .client_token = row.get_as<sstring>("client_token"),
        .request = row.get_as<sstring>("request"),
        .export_status = row.get_as<sstring>("export_status"),
        .table_id = ::table_id(row.get_as<utils::UUID>("table_id")),
        .export_time = row.get_as<db_clock::time_point>("export_time"),
        .accepted_at = row.get_as<db_clock::time_point>("accepted_at"),
        .export_manifest = row.get_opt<sstring>("export_manifest"),
        .failure_code = row.get_opt<sstring>("failure_code"),
        .failure_message = row.get_opt<sstring>("failure_message"),
        .item_count = row.get_opt<int64_t>("item_count"),
        .billed_size_bytes = row.get_opt<int64_t>("billed_size_bytes"),
        .completed_at = row.get_opt<db_clock::time_point>("completed_at"),
    };
}

}
