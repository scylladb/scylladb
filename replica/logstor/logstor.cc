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
#include "readers/from_mutations.hh"
#include "keys/keys.hh"
#include "replica/logstor/segment_manager.hh"
#include "replica/logstor/types.hh"
#include "utils/managed_bytes.hh"
#include <openssl/ripemd.h>
#include <openssl/evp.h>

namespace replica::logstor {

seastar::logger logstor_logger("logstor");

logstor::logstor(logstor_config config)
    : _segment_manager(config.segment_manager_cfg)
    , _write_buffer(_segment_manager, config.flush_sg) {
}

future<> logstor::do_recovery(replica::database& db) {
    co_await _segment_manager.do_recovery(db);
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

size_t logstor::get_memory_usage() const {
    return _segment_manager.get_memory_usage();
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

future<> logstor::write(const mutation& m, compaction_group& cg, seastar::gate::holder cg_holder) {
    primary_index_key key(m.decorated_key());
    table_id table = m.schema()->id();
    auto& index = cg.get_logstor_index();

    // TODO ?
    record_generation gen = index.get(key)
        .transform([](const index_entry& entry) {
            return entry.generation + 1;
         }).value_or(record_generation(1));

    log_record record {
        .header = {
            .key = key,
            .generation = gen,
            .table = table,
        },
        .mut = canonical_mutation(m)
    };

    return _write_buffer.write(std::move(record), &cg, std::move(cg_holder)).then_unpack([this, &index, gen, key = std::move(key)]
            (log_location location, seastar::gate::holder op) {
        index_entry new_entry {
            .location = location,
            .generation = gen,
        };

        auto old_entry = index.exchange(key, std::move(new_entry));

        // If overwriting, free old record
        if (old_entry) {
            _segment_manager.free_record(old_entry->location);
        }
    }).handle_exception([] (std::exception_ptr ep) {
        logstor_logger.error("Error writing mutation: {}", ep);
        return make_exception_future<>(ep);
    });
}

future<std::optional<log_record>> logstor::read(const primary_index& index, primary_index_key key) {
    auto op = index.start_read();

    auto entry_opt = index.get(key);
    if (!entry_opt.has_value()) {
        return make_ready_future<std::optional<log_record>>(std::nullopt);
    }

    const auto& entry = *entry_opt;

    return _segment_manager.read(entry.location).then([key = std::move(key), op = std::move(op)] (log_record record) {
        return std::optional<log_record>(std::move(record));
    }).handle_exception([] (std::exception_ptr ep) {
        logstor_logger.error("Error reading record: {}", ep);
        return make_exception_future<std::optional<log_record>>(ep);
    });
}

future<std::optional<canonical_mutation>> logstor::read(const schema& s, const primary_index& index, const dht::decorated_key& dk) {
    primary_index_key key(dk);
    return read(index, key).then([&dk] (std::optional<log_record> record_opt) -> std::optional<canonical_mutation> {
        if (!record_opt.has_value()) {
            return std::nullopt;
        }

        auto& record = *record_opt;

        if (record.mut.key() != dk.key()) [[unlikely]] {
            throw std::runtime_error(fmt::format(
                "Key mismatch reading log entry: expected {}, got {}",
                dk.key(), record.mut.key()
            ));
        }

        return std::optional<canonical_mutation>(std::move(record.mut));
    });
}

mutation_reader logstor::make_reader(schema_ptr schema,
                                            const primary_index& index,
                                            reader_permit permit,
                                            const dht::partition_range& pr,
                                            const query::partition_slice& slice,
                                            tracing::trace_state_ptr trace_state) {

    class logstor_range_reader : public mutation_reader::impl {
        logstor* _logstor;
        const primary_index& _index;
        dht::partition_range _pr;
        query::partition_slice _slice;
        tracing::trace_state_ptr _trace_state;
        std::optional<dht::decorated_key> _last_key; // owns the key, safe across yields
        mutation_reader_opt _current_partition_reader;
        dht::ring_position_comparator _cmp;

        // Finds the next iterator to process, safe to call after any co_await
        primary_index::partitions_type::const_iterator find_next() const {
            auto it = _last_key
                ? _index.upper_bound(*_last_key)                        // strictly after last key
                : position_at_range_start();                            // initial positioning
            // If start was exclusive and we haven't yet seen a key
            return it;
        }

        primary_index::partitions_type::const_iterator position_at_range_start() const {
            if (!_pr.start()) {
                return _index.begin();
            }
            auto it = _index.lower_bound(_pr.start()->value());
            if (!_pr.start()->is_inclusive() && it != _index.end()) {
                if (_cmp(it->key(), _pr.start()->value()) == 0) {
                    ++it;
                }
            }
            return it;
        }

        bool exceeds_range_end(const primary_index_entry& e) const {
            if (!_pr.end()) return false;
            auto c = _cmp(e.key(), _pr.end()->value());
            return _pr.end()->is_inclusive() ? c > 0 : c >= 0;
        }

    public:
        logstor_range_reader(schema_ptr s, const primary_index& idx, reader_permit p,
                    logstor* ls, dht::partition_range pr,
                    query::partition_slice slice, tracing::trace_state_ptr ts)
            : impl(std::move(s), std::move(p))
            , _logstor(ls), _index(idx), _pr(std::move(pr))
            , _slice(std::move(slice)), _trace_state(std::move(ts))
            , _cmp(*_schema)
        {}

        virtual future<> fill_buffer() override {
            while (!is_buffer_full() && !_end_of_stream) {
                // Drain current partition's reader first
                if (_current_partition_reader) {
                    co_await _current_partition_reader->fill_buffer();
                    _current_partition_reader->move_buffer_content_to(*this);
                    if (!_current_partition_reader->is_end_of_stream()) {
                        continue;
                    }
                    co_await _current_partition_reader->close();
                    _current_partition_reader = std::nullopt;
                    // _last_key was already set when we opened the reader
                }

                // Find next key in range (safe after co_await since we use _last_key)
                auto it = find_next();
                if (it == _index.end() || exceeds_range_end(*it)) {
                    _end_of_stream = true;
                    break;
                }

                // Snapshot the key before yielding
                auto current_key = it->key();

                auto guard = reader_permit::awaits_guard(_permit);
                auto cmut = co_await _logstor->read(*_schema, _index, current_key);

                _last_key = current_key; // mark as visited even if not found (tombstoned)

                if (!cmut) {
                    continue; // key was removed between index lookup and read
                }

                tracing::trace(_trace_state, "logstor_range_reader: fetched key {}", current_key);

                _current_partition_reader = make_mutation_reader_from_mutations(
                    _schema, _permit, cmut->to_mutation(_schema),
                    _slice, streamed_mutation::forwarding::no
                );
            }
        }

        virtual future<> next_partition() override {
            clear_buffer_to_next_partition();
            if (!is_buffer_empty()) return make_ready_future<>();
            _end_of_stream = false;
            if (_current_partition_reader) {
                auto fut = _current_partition_reader->close();
                _current_partition_reader = std::nullopt;
                return fut;
            }
            return make_ready_future<>();
        }

        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            clear_buffer();
            _end_of_stream = false;
            _pr = pr;
            _last_key = std::nullopt;      // re-position from new range start
            if (_current_partition_reader) {
                auto fut = _current_partition_reader->close();
                _current_partition_reader = std::nullopt;
                return fut;
            }
            return make_ready_future<>();
        }

        virtual future<> fast_forward_to(position_range pr) override {
            if (_current_partition_reader) {
                clear_buffer();
                return _current_partition_reader->fast_forward_to(std::move(pr));
            }
            return make_ready_future<>();
        }

        virtual future<> close() noexcept override {
            if (_current_partition_reader) {
                return _current_partition_reader->close();
            }
            return make_ready_future<>();
        }
    };

    return make_mutation_reader<logstor_range_reader>(
        std::move(schema), index, std::move(permit), this, pr, slice, std::move(trace_state)
    );
}

void logstor::set_trigger_compaction_hook(std::function<void()> fn) {
    _segment_manager.set_trigger_compaction_hook(std::move(fn));
}

void logstor::set_trigger_separator_flush_hook(std::function<void(size_t)> fn) {
    _segment_manager.set_trigger_separator_flush_hook(std::move(fn));
}

}
