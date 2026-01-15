/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#include "replica/logstor/logstor.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/util/log.hh>
#include <seastar/core/future.hh>
#include "readers/from_mutations.hh"

namespace replica {
namespace logstor {

seastar::logger logstor_logger("logstor");

logstor::logstor(logstor_config config)
    : _segment_manager(config.segment_manager_cfg, _index)
    , _write_buffer(_segment_manager, config.flush_sg) {
}

future<> logstor::start() {
    logstor_logger.info("Starting logstor");

    co_await _segment_manager.start();
    co_await _write_buffer.start();

    logstor_logger.info("logstor started");
}

future<> logstor::stop() {
    logstor_logger.info("Stopping logstor");

    co_await _write_buffer.stop();
    co_await _segment_manager.stop();

    logstor_logger.info("logstor stopped");
}

void logstor::enable_auto_compaction() {
    _segment_manager.enable_auto_compaction();
}

future<> logstor::disable_auto_compaction() {
    return _segment_manager.disable_auto_compaction();
}

future<> logstor::trigger_compaction(bool major) {
    return _segment_manager.trigger_compaction(major);
}

future<> logstor::write(const mutation& m) {
    auto key = calculate_key(*m.schema(), m.decorated_key());

    log_record record {
        .key = key,
        .mut = canonical_mutation(m)
    };

    return _write_buffer.write(std::move(record)).then([this, key = std::move(key)] (log_location location) {
        index_entry new_entry {
            .location = location,
        };

        auto old_entry = _index.exchange(key, std::move(new_entry));

        // If overwriting, free old record
        if (old_entry) {
            _segment_manager.free_record(old_entry->location);
        }
    }).handle_exception([] (std::exception_ptr ep) {
        logstor_logger.error("Error writing mutation: {}", ep);
        return make_exception_future<>(ep);
    });
}

future<std::optional<log_record>> logstor::read(index_key key) {
    auto entry_opt = _index.get(key);
    if (!entry_opt.has_value()) {
        return make_ready_future<std::optional<log_record>>(std::nullopt);
    }

    const auto& entry = *entry_opt;

    return _segment_manager.read(entry.location).then([key = std::move(key)] (log_record record) {
        if (record.key != key) [[unlikely]] {
            throw std::runtime_error(fmt::format(
                "Key mismatch reading log entry: expected {}, got {}",
                key, record.key
            ));
        }
        return std::optional<log_record>(std::move(record));
    }).handle_exception([] (std::exception_ptr ep) {
        logstor_logger.error("Error reading record: {}", ep);
        return make_exception_future<std::optional<log_record>>(ep);
    });
}

future<std::optional<canonical_mutation>> logstor::read(const schema& s, const dht::decorated_key& key) {
    return read(calculate_key(s, key)).then([&key] (std::optional<log_record> record_opt) -> std::optional<canonical_mutation> {
        if (!record_opt.has_value()) {
            return std::nullopt;
        }

        auto& record = *record_opt;

        if (record.mut.key() != key.key()) [[unlikely]] {
            throw std::runtime_error(fmt::format(
                "Key mismatch reading log entry: expected {}, got {}",
                key.key(), record.mut.key()
            ));
        }

        return std::optional<canonical_mutation>(std::move(record.mut));
    });
}

index_key logstor::calculate_key(const schema& s, const dht::decorated_key& key) {
    // hash of (ks name, table name, partition key)
    return index_key {
        utils::hash_combine(
            dht::token::to_int64(key.token()),
            utils::hash_combine(
                std::hash<sstring>()(s.ks_name()),
                std::hash<sstring>()(s.cf_name())
            )
        )
    };
}

}
}
