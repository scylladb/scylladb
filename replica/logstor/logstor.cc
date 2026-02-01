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
#include "keys/keys.hh"
#include "replica/logstor/types.hh"
#include "utils/managed_bytes.hh"
#include <openssl/ripemd.h>
#include <openssl/evp.h>

namespace replica {
namespace logstor {

seastar::logger logstor_logger("logstor");

logstor::logstor(logstor_config config)
    : _segment_manager(config.segment_manager_cfg, _index)
    , _write_buffer(_segment_manager, config.flush_sg) {
}

future<> logstor::start() {
    logstor_logger.info("Starting logstor");

    init_crypto();

    co_await _segment_manager.start();
    co_await _write_buffer.start();

    logstor_logger.info("logstor started");
}

future<> logstor::stop() {
    logstor_logger.info("Stopping logstor");

    co_await _write_buffer.stop();
    co_await _segment_manager.stop();

    free_crypto();

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

    // TODO ?
    record_generation gen = _index.get(key)
        .transform([](const index_entry& entry) {
            return entry.generation + 1;
         }).value_or(record_generation(1));

    log_record record {
        .key = key,
        .generation = gen,
        .mut = canonical_mutation(m)
    };

    return _write_buffer.write(std::move(record)).then([this, gen, key = std::move(key)] (log_location location) {
        index_entry new_entry {
            .location = location,
            .generation = gen,
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

mutation_reader logstor::make_reader_for_key(schema_ptr schema,
                                            reader_permit permit,
                                            const dht::decorated_key& key,
                                            const query::partition_slice& slice,
                                            tracing::trace_state_ptr trace_state) {

    // Create a simple reader that reads the mutation and delegates to the existing infrastructure
    class single_key_reader : public mutation_reader::impl {
    private:
        logstor* _logstor;
        dht::decorated_key _key;
        query::partition_slice _slice;
        tracing::trace_state_ptr _trace_state;
        mutation_reader_opt _delegate_reader;

    public:
        single_key_reader(schema_ptr schema,
                         reader_permit permit,
                         logstor* logstor,
                         dht::decorated_key key,
                         query::partition_slice slice,
                         tracing::trace_state_ptr trace_state)
            : impl(std::move(schema), std::move(permit))
            , _logstor(logstor)
            , _key(std::move(key))
            , _slice(std::move(slice))
            , _trace_state(std::move(trace_state)) {
        }

        virtual future<> fill_buffer() override {
            if (_end_of_stream) {
                co_return;
            }

            // Create delegate reader if not already created
            if (!_delegate_reader) {
                // Mark as awaiting I/O during disk read
                auto await_guard = reader_permit::awaits_guard(_permit);

                auto cmut = co_await _logstor->read(*_schema, _key);

                if (!cmut.has_value()) {
                    // Key not found - end of stream
                    _end_of_stream = true;
                    co_return;
                }

                tracing::trace(_trace_state, "Retrieved mutation for key {} from key-value storage", _key);

                _delegate_reader = make_mutation_reader_from_mutations(
                    _schema,
                    _permit,
                    cmut->to_mutation(_schema),
                    _slice,
                    streamed_mutation::forwarding::no
                );
            }

            // Delegate to the mutation reader
            co_await _delegate_reader->fill_buffer();
            _delegate_reader->move_buffer_content_to(*this);
            _end_of_stream = _delegate_reader->is_end_of_stream();
        }

        virtual future<> next_partition() override {
            clear_buffer_to_next_partition();
            _end_of_stream = true;
            return make_ready_future<>();
        }

        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            // Single key reader doesn't support range forwarding beyond the current key
            if (!pr.contains(_key, dht::ring_position_comparator(*_schema))) {
                _end_of_stream = true;
            }
            return make_ready_future<>();
        }

        virtual future<> fast_forward_to(position_range pr) override {
            // Delegate position forwarding to the underlying reader if it exists
            if (_delegate_reader) {
                clear_buffer();
                return _delegate_reader->fast_forward_to(std::move(pr));
            }
            return make_ready_future<>();
        }

        virtual future<> close() noexcept override {
            if (_delegate_reader) {
                return _delegate_reader->close();
            }
            return make_ready_future<>();
        }
    };

    return make_mutation_reader<single_key_reader>(
        schema,
        std::move(permit),
        this,
        key,
        slice,
        std::move(trace_state)
    );
}

// Cache RIPEMD-160 algorithm descriptor and context
namespace {
thread_local EVP_MD* cached_ripemd160_md = nullptr;
thread_local EVP_MD_CTX* cached_ripemd160_ctx = nullptr;
}

void logstor::init_crypto() {
    cached_ripemd160_md = EVP_MD_fetch(nullptr, "RIPEMD160", nullptr);
    if (!cached_ripemd160_md) {
        throw std::runtime_error("Failed to fetch RIPEMD160 algorithm");
    }

    cached_ripemd160_ctx = EVP_MD_CTX_new();
    if (!cached_ripemd160_ctx) {
        throw std::runtime_error("Failed to create RIPEMD160 context");
    }
}

void logstor::free_crypto() {
    if (cached_ripemd160_ctx) {
        EVP_MD_CTX_free(cached_ripemd160_ctx);
        cached_ripemd160_ctx = nullptr;
    }
    if (cached_ripemd160_md) {
        EVP_MD_free(cached_ripemd160_md);
        cached_ripemd160_md = nullptr;
    }
}

index_key logstor::calculate_key(const schema& s, const dht::decorated_key& key) {
    // hash of (ks name, table name, partition key)

    constexpr char separator = ':';

    EVP_MD_CTX* ctx = cached_ripemd160_ctx;
    EVP_DigestInit_ex(ctx, cached_ripemd160_md, nullptr);

    auto ks_bytes = to_bytes_view(s.ks_name());
    EVP_DigestUpdate(ctx, ks_bytes.data(), ks_bytes.size());
    EVP_DigestUpdate(ctx, &separator, 1);

    auto cf_bytes = to_bytes_view(s.cf_name());
    EVP_DigestUpdate(ctx, cf_bytes.data(), cf_bytes.size());
    EVP_DigestUpdate(ctx, &separator, 1);

    managed_bytes_view key_view(key.key());
    for (bytes_view frag : fragment_range(key_view)) {
        EVP_DigestUpdate(ctx, frag.data(), frag.size());
    }

    index_key result;
    EVP_DigestFinal_ex(ctx, result.digest.data(), nullptr);

    return result;
}

}
}
