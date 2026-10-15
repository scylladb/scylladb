/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/logstor.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/util/log.hh>
#include <seastar/core/future.hh>
#include <seastar/core/metrics.hh>
#include "keys/keys.hh"
#include "mutation/frozen_mutation.hh"
#include "replica/logstor/key_utils.hh"
#include "replica/logstor/record_value.hh"
#include "replica/logstor/segment_manager.hh"
#include "replica/logstor/types.hh"
#include "utils/managed_bytes.hh"
#include <random>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
#include <xxhash.h>
#pragma GCC diagnostic pop

namespace replica::logstor {

seastar::logger logstor_logger("logstor");

const uint64_t key_hash_seed = [] {
    std::random_device rd;
    return (uint64_t(rd()) << 32) | uint64_t(rd());
}();

key_hash compute_key_hash(managed_bytes_view key) {
    XXH128_hash_t digest;
    if (!key.is_linearized()) [[unlikely]] {
        XXH3_state_t state;
        XXH3_128bits_reset_withSeed(&state, key_hash_seed);
        for (bytes_view frag : fragment_range(key)) {
            XXH3_128bits_update(&state, frag.data(), frag.size());
        }
        digest = XXH3_128bits_digest(&state);
    } else {
        auto frag = key.current_fragment();
        digest = XXH3_128bits_withSeed(frag.data(), frag.size(), key_hash_seed);
    }
    return key_hash{.low64 = digest.low64, .high64 = digest.high64};
}

primary_index_key::primary_index_key(const dht::decorated_key& dk)
    : primary_index_key(dk.token(), compute_key_hash(dk.key())) {
}

logstor::logstor(logstor_config config, ::cache_tracker& shared_cache_tracker)
    : _segment_manager(config.segment_manager_cfg)
    , _write_buffer(buffered_writer_config{
            .buffer_size = _segment_manager.get_segment_size(),
            .ring_size = config.write_buffer_ring_size,
            .flush_sg = config.flush_sg,
            .max_queued_write_bytes = config.max_queued_write_bytes,
        }, [&sm = _segment_manager] (write_buffer& buf) { return sm.write(buf); })
    , _cache_tracker(shared_cache_tracker) {

    namespace sm = seastar::metrics;

    _metrics.add_group("logstor", {
        sm::make_gauge("queued_write_count", [this] { return _write_buffer.queued_write_count(); },
                       sm::description("Number of writes currently queued in the write buffer.")),
        sm::make_counter("write_failures", [this] { return _stats.write_failures; },
                       sm::description("Number of writes that failed to be persisted.")),
    });
}

future<recovery_summary> logstor::do_recovery(recovery_catalog& catalog) {
    return _segment_manager.do_recovery(catalog);
}

future<> logstor::start() {
    logstor_logger.info("Starting logstor");

    co_await _segment_manager.start();
    co_await _write_buffer.start();

    logstor_logger.info("logstor started");
}

future<> logstor::stop() {
    if (_async_gate.is_closed()) {
        co_return;
    }
    logstor_logger.info("Stopping logstor");

    co_await _async_gate.close();
    co_await _write_buffer.stop();
    co_await _segment_manager.stop();

    logstor_logger.info("logstor stopped");
}

size_t logstor::get_memory_usage() const {
    return _segment_manager.get_usage().memory_usage + _write_buffer.memory_usage();
}

segment_manager& logstor::get_segment_manager() noexcept {
    return _segment_manager;
}

const segment_manager& logstor::get_segment_manager() const noexcept {
    return _segment_manager;
}

compaction_manager& logstor::get_compaction_manager() noexcept {
    return _segment_manager.get_compaction_manager();
}

const compaction_manager& logstor::get_compaction_manager() const noexcept {
    return _segment_manager.get_compaction_manager();
}

std::unique_ptr<primary_index> logstor::make_primary_index(bool cache_enabled) {
    return std::make_unique<primary_index>(_segment_manager, cache_enabled ? &_cache_tracker : nullptr);
}

future<> logstor::flush_to_separator() {
    auto gate_holder = _async_gate.hold();
    co_await _write_buffer.flush();
    co_await _segment_manager.await_pending_writes();
}

future<> logstor::write(const mutation& m, write_target target, db::timeout_clock::time_point timeout) noexcept {
    try {
        return do_write(*m.schema(), m.decorated_key(), record_timestamp(m), encode_record_value(m),
                std::move(target), timeout);
    } catch (...) {
        return current_exception_as_future();
    }
}

future<> logstor::write(const frozen_mutation& m, const schema& s, write_target target, db::timeout_clock::time_point timeout) noexcept {
    try {
        check_schema_version(m.schema_version(), s);
        return do_write(s, m.decorated_key(s), record_timestamp(m), encode_record_value(m, s),
                std::move(target), timeout);
    } catch (...) {
        return current_exception_as_future();
    }
}

future<> logstor::do_write(const schema& s, dht::decorated_key dk, api::timestamp_type ts, record_value value,
        write_target target, db::timeout_clock::time_point timeout) {
    auto gate_holder = _async_gate.hold();

    auto& cg = *target.cg;
    primary_index_key key(dk);
    auto& index = cg.logstor_index();

    log_record record {
        .header = {
            .key = std::move(dk),
            .timestamp = ts,
            .table = s.id(),
        },
        .value = std::move(value),
    };

    auto writer = log_record_writer(std::move(record));

    // Two waits, not one: first for the record to be taken into a buffer, then for that buffer to
    // reach a segment. They are awaited here rather than behind a call of their own, which would
    // cost a coroutine frame per write to do nothing else.
    auto accepted_f = co_await coroutine::as_future(_write_buffer.write_to_buffer(std::move(writer), timeout, std::move(target)));
    if (accepted_f.failed()) {
        _stats.write_failures++;
        co_await coroutine::return_exception_ptr(accepted_f.get_exception());
    }
    auto persisted_f = co_await coroutine::as_future(std::move(accepted_f.get().persisted));
    if (persisted_f.failed()) {
        _stats.write_failures++;
        co_await coroutine::return_exception_ptr(persisted_f.get_exception());
    }
    auto [location, op] = persisted_f.get();
    index_entry new_entry {
        .location = location,
        .timestamp = ts,
    };
    index.insert(key, std::move(new_entry));
}

}
