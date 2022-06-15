/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <seastar/core/coroutine.hh>

#include "log.hh"
#include "utils/UUID_gen.hh"

#include "db/system_events.hh"
#include "db/system_keyspace.hh"
#include "db/query_context.hh"

static logging::logger selogger("system_events");

namespace std {

std::ostream& operator<<(std::ostream& out, const db::system_event& event) {
    out << "system_event("
        << "category=" << event.category
        << ", type=" << event.type
        << ", params=" << event.params;
    if (event.completed_at) {
        out << ", completion_status=" << event.completion_status;
    }
    return out << ')';
}

}

namespace db {

future<> record_system_event(sstring category, sstring event_type, std::unordered_map<sstring, sstring> params, const sstring& completion_status, gc_clock::duration ttl) {
    if (!db::qctx) {
        co_return;
    }

    auto using_ttl = ttl != NO_TTL ? format(" USING TTL {}", gc_clock::as_int32(ttl)) : "";
    const sstring req = format("INSERT INTO {}.{} (category, started_at, type, params, completed_at, completion_status) VALUES (?, ?, ?, ?, ?, ?){}",
            system_keyspace::NAME, system_keyspace::EVENTS, using_ttl);

    // FIXME: for now, format all params into a string.
    // Column should be turned into a map
    std::stringstream params_str;
    sstring delim = "";
    for (const auto& [k, v] : params) {
        params_str << delim << k << '=' << v;
        delim = ",";
    }

    auto now = utils::UUID_gen::get_time_UUID();
    auto event = system_event{
        .id = now,
        .category = std::move(category),
        .type = std::move(event_type),
        .params = std::move(params),
        .completed_at = now,
        .completion_status = completion_status,
    };

    try {
        co_await db::qctx->execute_cql(req, event.category, event.id, event.type, params_str.view(), event.completed_at, event.completion_status).discard_result();
    } catch (...) {
        selogger.warn("Failed to insert to {}.{}: {}: {}", system_keyspace::NAME, system_keyspace::EVENTS, event, std::current_exception());
    }
}

future<system_event> start_system_event(sstring category, sstring event_type, event_params params, gc_clock::duration ttl) {
    if (!db::qctx) {
        co_return system_event{};
    }

    auto using_ttl = ttl != NO_TTL ? format(" USING TTL {}", gc_clock::as_int32(ttl)) : "";
    const sstring req = format("INSERT INTO {}.{} (category, started_at, type, params) VALUES (?, ?, ?, ?){}",
            system_keyspace::NAME, system_keyspace::EVENTS, using_ttl);

    // FIXME: for now, format all params into a string.
    // Column should be turned into a map
    std::stringstream params_str;
    sstring delim = "";
    for (const auto& [k, v] : params) {
        params_str << delim << k << '=' << v;
        delim = ",";
    }

    auto now = utils::UUID_gen::get_time_UUID();
    auto event = system_event{
        .id = now,
        .category = std::move(category),
        .type = std::move(event_type),
        .params = std::move(params),
    };

    try {
        co_await db::qctx->execute_cql(req, event.category, event.id, event.type, params_str.view()).discard_result();
    } catch (...) {
        auto ex = std::current_exception();
        event.completed_at = event.id;
        event.completion_status = format("{}", ex);
        event.id = utils::null_uuid();
        selogger.warn("Failed to insert to {}.{}: {}", system_keyspace::NAME, system_keyspace::EVENTS, event);
    }

    co_return event;
}

static future<> _end_system_event(system_event& event, const sstring& completion_status) {
    event.completed_at = utils::UUID_gen::get_time_UUID();
    event.completion_status = completion_status;
    const sstring req = format("UPDATE {}.{} SET completed_at = ?, completion_status = ? WHERE category = ? AND started_at = ?",
            system_keyspace::NAME, system_keyspace::EVENTS);

    try {
        co_await db::qctx->execute_cql(req, event.completed_at, event.completion_status, event.category, event.id).discard_result();
    } catch (...) {
        selogger.warn("Failed to update {}.{}: {}: {}", system_keyspace::NAME, system_keyspace::EVENTS, event, std::current_exception());
    }
}

future<> end_system_event(system_event& event, const sstring& completion_status) {
    if (!db::qctx || !event.id) {
        return make_ready_future<>();
    }

    return _end_system_event(event, completion_status);
}

future<> end_system_event(system_event& event, std::exception_ptr ex) {
    if (!db::qctx || !event.id) {
        co_return;
    }

    sstring completion_status = ex ? format("FAILED: {}", ex) : "OK";
    co_await _end_system_event(event, completion_status);
}

} // namespace db
